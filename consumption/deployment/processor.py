import logging
from datetime import datetime, timedelta
from typing import Iterable, List, Optional

import numpy as np
import pandas as pd
from elasticsearch import Elasticsearch

from .monitoring_stats_connector import (ClusterStats, ClusterStatsV7,
                                         IndexStats, IndexStatsV7,
                                         NodeStats, NodeStatsV7,
                                         ShardStats, ShardStatsV7,
                                         elasticsearch_id_filter,
                                         range_filter)

logger = logging.getLogger(__name__)


# Helper function to compute the percentage of the total for a given column
# compared to the entire group, per the "group" list.
def _compute_pct(datastreams: pd.DataFrame, column: str, group: List[str]) -> pd.Series:
    """
    Compute the percentage of the total for a given column.
    """
    # Catch division by zero
    return datastreams[column].astype(float) / (
        datastreams.groupby(group)[column]
        .transform("sum")
        .astype(float)
        .replace(0, np.nan)
    )


class DeploymentDataProcessor:
    # TODO: link this to the Stats class
    chunk_size = "10min"
    chunk_size_seconds = pd.to_timedelta(chunk_size).total_seconds()
    hour_ratio = chunk_size_seconds / 3600

    def __init__(
        self,
        es: Elasticsearch,
        elasticsearch_id: str,
        from_ts: datetime,
        price_df: pd.DataFrame,
        monitoring_index_pattern: Optional[str] = None,
        parsing_regex_str: Optional[str] = None,
        monitoring_version: str = "8",
    ):
        self.es = es
        self.elasticsearch_id = elasticsearch_id
        self.from_ts = from_ts
        self.to_ts = from_ts + timedelta(hours=1)
        self.monitoring_version = monitoring_version

        # Select the appropriate Stats classes for V7 or V8
        # "7@" means V7 indices but using @timestamp field
        is_v7 = monitoring_version.startswith("7")
        if is_v7:
            index_stats_cls = IndexStatsV7
            cluster_stats_cls = ClusterStatsV7
            node_stats_cls = NodeStatsV7
            shard_stats_cls = ShardStatsV7
        else:
            index_stats_cls = IndexStats
            cluster_stats_cls = ClusterStats
            node_stats_cls = NodeStats
            shard_stats_cls = ShardStats

        id_filter = elasticsearch_id_filter(self.elasticsearch_id, "7" if is_v7 else "8")
        ts_field = "@timestamp" if monitoring_version == "7@" else ("timestamp" if is_v7 else "@timestamp")

        self.cost_data = price_df
        self.skip_prices = self.cost_data.empty
        if self.skip_prices:
            logger.warning(
                f"No cost data found for {self.elasticsearch_id} "
                f"at {self.from_ts.strftime('%Y-%m-%d %H:%M:%S')}, "
                f"cost computation will be skipped"
            )

        # V7 Stats must use .monitoring-es-7-* to avoid mapping conflicts
        # with V8 indices when a custom pattern like .monitoring* is used
        from .monitoring_stats_connector import DEFAULT_MONITORING_INDEX_PATTERN_V7
        stats_index_pattern = (
            DEFAULT_MONITORING_INDEX_PATTERN_V7 if is_v7 else monitoring_index_pattern
        )

        # Helper: create Stats object with correct timestamp_field
        def _make_stats(cls, *args):
            obj = cls(*args)
            obj.timestamp_field = ts_field
            return obj

        range_flt = range_filter(
            self.from_ts - timedelta(seconds=self.chunk_size_seconds),
            self.to_ts,
            ts_field,
        )

        # Fetch cluster data first — needed for node tier mapping
        self.cluster_data = _make_stats(
            cluster_stats_cls, es, stats_index_pattern
        ).search_as_dataframe([range_flt, id_filter])

        if self.cluster_data.empty:
            logger.warning(
                f"No cluster data found for {self.elasticsearch_id} "
                f"at {self.from_ts.strftime('%Y-%m-%d %H:%M:%S')}, "
                f"skipping further processing"
            )
            self.index_data = pd.DataFrame()
            return

        # Fetch node data and merge with cluster data for tier info
        node_df = _make_stats(node_stats_cls, es, stats_index_pattern).search_as_dataframe(
            [range_flt, id_filter]
        )

        if node_df.empty:
            logger.warning(
                f"No node data found for {self.elasticsearch_id} "
                f"at {self.from_ts.strftime('%Y-%m-%d %H:%M:%S')}, "
                f"skipping further processing"
            )
            self.index_data = pd.DataFrame()
            return

        self.node_data = (
            node_df
            .merge(
                self.cluster_data,
                left_on=["@timestamp", "id"],
                right_on=["@timestamp", "id"],
                how="left",
                suffixes=(None, ""),
            )
            .dropna(subset=["tier"])  # No tier information => not a data node (we drop)
        )

        # Cap memory_limit_bytes: when cgroup has no limit, ES reports max int64
        # which makes any per-GB calculation produce absurd numbers
        MAX_MEMORY_BYTES = 2 * 1024 * 1024 * 1024 * 1024  # 2 TB cap
        self.node_data.loc[
            self.node_data["memory_limit_bytes"] > MAX_MEMORY_BYTES,
            "memory_limit_bytes",
        ] = None

        # If cloud metadata is missing (V7 path), fetch it from V8 metricbeat data
        has_cloud = (
            "instance_type" in self.node_data.columns
            and self.node_data["instance_type"].notna().any()
        )
        if not has_cloud:
            self._enrich_cloud_metadata(es, monitoring_index_pattern)
            has_cloud = (
                "instance_type" in self.node_data.columns
                and self.node_data["instance_type"].notna().any()
            )

        if not self.skip_prices:

            if has_cloud:
                from ..utils.aws_pricing import get_ec2_hourly_price

                # Get account ID from first node with cloud data
                account_id = (
                    self.node_data.loc[
                        self.node_data["instance_type"].notna(), "cloud_account_id"
                    ].iloc[0]
                    if "cloud_account_id" in self.node_data.columns
                    else None
                )
                logger.info(
                    f"AWS cloud metadata found (account={account_id}), "
                    f"fetching real EC2 pricing"
                )
                # EC2 price is per-instance, NOT per-GB — already includes full instance cost
                self.node_data["cost"] = self.node_data.apply(
                    lambda row: (
                        get_ec2_hourly_price(
                            row.get("instance_type", ""),
                            row.get("cloud_region", "us-east-1"),
                            account_id=account_id,
                        )
                        or 0.0
                    )
                    * self.hour_ratio,
                    axis=1,
                )
            else:
                self.node_data = self.node_data.join(self.cost_data, on="tier")

                # For nodes without memory info, estimate from cost_data
                # Avoid multiplying by None or absurd values
                mem_gb = self.node_data["memory_limit_bytes"].fillna(0) / 1024 / 1024 / 1024
                self.node_data["cost"] = (
                    self.node_data["price_per_hour_per_gb"] * mem_gb * self.hour_ratio
                )

            # Compute the cost of each tier
            self.node_data["tier_cost"] = self.node_data.groupby(
                ["tier", "@timestamp"]
            )["cost"].transform("sum")
        else:
            self.node_data["tier_cost"] = None

        # Fetch index data (may be empty if index metricset not collected)
        self.index_data = _make_stats(
            index_stats_cls, es, stats_index_pattern, parsing_regex_str
        ).search_as_dataframe([range_flt, id_filter])

        if self.index_data.empty:
            logger.warning(
                f"No index data found for {self.elasticsearch_id} "
                f"at {self.from_ts.strftime('%Y-%m-%d %H:%M:%S')}, "
                f"node data will still be produced"
            )
            return

        # Enrich index data with tier info from shard data
        shard_range_flt = range_filter(
            self.from_ts - timedelta(seconds=5 * self.chunk_size_seconds),
            self.to_ts,
            ts_field,
        )
        shard_data = (
            _make_stats(shard_stats_cls, es, stats_index_pattern)
            .search_as_dataframe([shard_range_flt, id_filter])
        )

        if shard_data.empty:
            logger.warning(
                f"No shard data found for {self.elasticsearch_id}, "
                f"skipping index-tier enrichment"
            )
            return

        shard_data = shard_data.join(
            self.node_data[
                [
                    "id",
                    "tier",
                    "deployment_name",
                    "elasticsearch_id",
                    "tier_cost",
                ]
            ]
            .drop_duplicates()
            .set_index("id"),
            on="node_uuid",
        )

        logger.debug(
            f"Enriching index data with tier information for {self.elasticsearch_id} "
            f"at {self.from_ts.strftime('%Y-%m-%d %H:%M:%S')}"
        )

        self.index_data = self.index_data.merge(
            shard_data[
                [
                    "@timestamp",
                    "index_name",
                    "tier",
                    "tier_cost",
                    "deployment_name",
                    "elasticsearch_id",
                ]
            ],
            left_on=["@timestamp", "name"],
            right_on=["@timestamp", "index_name"],
            how="left",
            suffixes=(None, ""),
        )

    def _enrich_cloud_metadata(self, es, monitoring_index_pattern):
        """
        Fetch cloud metadata (instance_type, region, account_id) from V8 metricbeat
        data and apply it to node_data. V7 internal monitoring doesn't include cloud
        fields, but V8 metricbeat monitoring the same nodes does.
        """
        node_ids = self.node_data["id"].unique().tolist()
        if not node_ids:
            return

        try:
            # Query V8 metricbeat data for cloud info per node
            response = es.search(
                index=monitoring_index_pattern or ".monitoring-es-8-*,.monitoring-es-9-*",
                size=0,
                allow_no_indices=True,
                expand_wildcards=["open", "hidden"],
                query={
                    "bool": {
                        "filter": [
                            {"exists": {"field": "cloud.machine.type"}},
                            {"terms": {"elasticsearch.node.id": node_ids}},
                        ]
                    }
                },
                aggs={
                    "per_node": {
                        "terms": {
                            "field": "elasticsearch.node.id",
                            "size": len(node_ids),
                        },
                        "aggs": {
                            "cloud_info": {
                                "top_metrics": {
                                    "metrics": [
                                        {"field": "cloud.machine.type"},
                                        {"field": "cloud.region"},
                                        {"field": "cloud.account.id"},
                                    ],
                                    "sort": {"@timestamp": "desc"},
                                }
                            }
                        },
                    }
                },
            )

            buckets = (
                response.get("aggregations", {})
                .get("per_node", {})
                .get("buckets", [])
            )

            if not buckets:
                logger.debug("No cloud metadata found in V8 monitoring data")
                return

            # Build node_id -> cloud info mapping
            cloud_map = {}
            for bucket in buckets:
                node_id = bucket["key"]
                metrics = bucket["cloud_info"]["top"][0]["metrics"]
                cloud_map[node_id] = {
                    "instance_type": metrics.get("cloud.machine.type"),
                    "cloud_region": metrics.get("cloud.region"),
                    "cloud_account_id": metrics.get("cloud.account.id"),
                }

            # Apply to node_data
            for col in ["instance_type", "cloud_region", "cloud_account_id"]:
                if col not in self.node_data.columns:
                    self.node_data[col] = None
                self.node_data[col] = self.node_data["id"].map(
                    lambda nid: cloud_map.get(nid, {}).get(col)
                ).fillna(self.node_data[col])

            logger.info(
                f"Enriched {len(cloud_map)} nodes with AWS cloud metadata "
                f"from V8 monitoring data"
            )

        except Exception as e:
            logger.warning(f"Failed to fetch cloud metadata from V8 data: {e}")

    def _get_datastreams_usages(self) -> Iterable:
        if self.index_data.empty:
            logger.warning("Empty dataset, skipping datastreams usages")
            return ()

        logger.debug(
            f"Grouping {len(self.index_data)} indices "
            f"in datastream usages for {self.elasticsearch_id} "
            f"at {self.from_ts.strftime('%Y-%m-%d %H:%M:%S')}"
        )

        datastream_usages = self.index_data.groupby(
            ["datastream", "tier", "age_days", "@timestamp"]
        ).agg(
            {
                "deployment_name": "first",
                "elasticsearch_id": "first",
                "tier_cost": "first",
                "primary_docs_count": "sum",
                "primary_store_size_in_bytes": "sum",
                "total_docs_count": "sum",
                "total_store_size_in_bytes": "sum",
                "search_query_total_delta": "sum",
                "search_query_time_in_seconds_delta": "sum",
            }
        )

        logger.debug(
            f"Computing datastream usages percentages for {self.elasticsearch_id} "
            f"at {self.from_ts.strftime('%Y-%m-%d %H:%M:%S')}"
        )

        group = ["tier", "@timestamp"]

        # Compute the percentage of the total for each column
        datastream_usages["search_query_time_in_seconds_pct"] = _compute_pct(
            datastream_usages, "search_query_time_in_seconds_delta", group
        )
        datastream_usages["total_store_size_in_bytes_pct"] = _compute_pct(
            datastream_usages, "total_store_size_in_bytes", group
        )

        if not self.skip_prices:
            logger.debug(
                f"Computing datastream usages costs for {self.elasticsearch_id} "
                f"at {self.from_ts.strftime('%Y-%m-%d %H:%M:%S')}"
            )

            # Compute the cost for each datastream/tier for the chunk
            datastream_usages["search_cost"] = (
                datastream_usages["search_query_time_in_seconds_pct"]
                * datastream_usages["tier_cost"]
            )
            datastream_usages["storage_cost"] = (
                datastream_usages["total_store_size_in_bytes_pct"]
                * datastream_usages["tier_cost"]
            )

        datastream_usages = datastream_usages.drop(
            columns=[
                "tier_cost",
            ],
        )

        datastream_usages = datastream_usages.reset_index().rename(
            columns={"@timestamp": "timestamp"}
        )

        # Put None instead of NaN
        datastream_usages = datastream_usages.apply(np.nan_to_num).fillna(0)

        logger.debug(
            f"{len(datastream_usages)} datastream usages found for "
            f"{self.elasticsearch_id} at {self.from_ts.strftime('%Y-%m-%d %H:%M:%S')}"
        )

        datastream_usages["dataset"] = "datastream_usage"

        return list(datastream_usages.reset_index().itertuples(index=False))

    def _get_datastreams(self) -> Iterable:
        if self.index_data.empty:
            logger.warning("Empty dataset, skipping datastreams")
            return ()

        logger.debug(
            f"Grouping {len(self.index_data)} indices "
            f"in datastreams for {self.elasticsearch_id} "
            f"at {self.from_ts.strftime('%Y-%m-%d %H:%M:%S')}"
        )

        # Compute stats per datastream
        datastreams = self.index_data.groupby(["datastream", "tier", "@timestamp"]).agg(
            {
                "deployment_name": "first",
                "elasticsearch_id": "first",
                "tier_cost": "first",
                "primary_docs_count": "sum",
                "primary_store_size_in_bytes": "sum",
                "total_docs_count": "sum",
                "total_store_size_in_bytes": "sum",
                "total_store_size_in_bytes_delta": "sum",
                "search_query_total_delta": "sum",
                "search_query_time_in_seconds_delta": "sum",
                "index_total_delta": "sum",
                "index_time_in_seconds_delta": "sum",
                "primary_docs_count_delta": "sum",
                "primary_store_size_in_bytes_delta": "sum",
            }
        )

        logger.debug(
            f"Computing datastreams percentages for {self.elasticsearch_id} "
            f"at {self.from_ts.strftime('%Y-%m-%d %H:%M:%S')}"
        )

        group = ["tier", "@timestamp"]

        # Compute the percentage of the total for each column
        datastreams["search_query_time_in_seconds_pct"] = _compute_pct(
            datastreams, "search_query_time_in_seconds_delta", group
        )
        datastreams["index_time_in_seconds_pct"] = _compute_pct(
            datastreams, "index_time_in_seconds_delta", group
        )
        datastreams["total_store_size_in_bytes_pct"] = _compute_pct(
            datastreams, "total_store_size_in_bytes", group
        )

        if not self.skip_prices:
            logger.debug(
                f"Computing datastreams costs for {self.elasticsearch_id} "
                f"at {self.from_ts.strftime('%Y-%m-%d %H:%M:%S')}"
            )

            # Compute the cost for each datastream/tier for the chunk
            datastreams["search_cost"] = (
                datastreams["search_query_time_in_seconds_pct"]
                * datastreams["tier_cost"]
            )
            datastreams["indexing_cost"] = (
                datastreams["index_time_in_seconds_pct"] * datastreams["tier_cost"]
            )
            datastreams["storage_cost"] = (
                datastreams["total_store_size_in_bytes_pct"] * datastreams["tier_cost"]
            )

        datastreams = datastreams.drop(
            columns=[
                "tier_cost",
            ],
        )

        datastreams = datastreams.reset_index().rename(
            columns={"@timestamp": "timestamp"}
        )

        # Put None instead of NaN
        datastreams = datastreams.apply(np.nan_to_num).fillna(0)

        logger.debug(
            f"{len(datastreams)} datastreams found for "
            f"{self.elasticsearch_id} at {self.from_ts.strftime('%Y-%m-%d %H:%M:%S')}"
        )

        datastreams["dataset"] = "datastream"

        return list(datastreams.reset_index().itertuples(index=False))

    def _get_nodes(self):
        logger.debug(
            f"Computing nodes for {self.elasticsearch_id} "
            f"at {self.from_ts.strftime('%Y-%m-%d %H:%M:%S')}"
        )

        if not hasattr(self, "node_data") or self.node_data.empty:
            logger.warning("No node data, skipping nodes")
            return ()

        nodes = self.node_data.reset_index().rename(columns={"@timestamp": "timestamp"})
        nodes = nodes.apply(np.nan_to_num).fillna(0)

        logger.debug(
            f"{len(nodes)} nodes found for {self.elasticsearch_id} "
            f"at {self.from_ts.strftime('%Y-%m-%d %H:%M:%S')}"
        )

        nodes["dataset"] = "node"

        return list(nodes.reset_index().itertuples(index=False))

    def process(self, compute_usages: bool = False):
        res = self._get_datastreams() + self._get_nodes()
        if compute_usages:
            res += self._get_datastreams_usages()

        return res
