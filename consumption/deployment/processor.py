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
        daily_cost_usd: Optional[float] = None,
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

        # For multi-tier nodes (e.g., cold+warm on same node), split cost evenly
        # across the tiers the node serves
        tiers_per_node = self.node_data.groupby(["@timestamp", "id"])["tier"].transform("count")
        self.node_data["_tier_split"] = tiers_per_node

        if daily_cost_usd:
            num_nodes = self.node_data["id"].nunique()
            hourly_per_node = daily_cost_usd / 24.0 / max(num_nodes, 1)
            self.node_data["cost"] = (hourly_per_node * self.hour_ratio) / self.node_data["_tier_split"]
            logger.info(
                f"Using AWS daily cost ${daily_cost_usd:.2f} / {num_nodes} nodes "
                f"= ${hourly_per_node:.4f}/hr/node"
            )
            self.node_data["tier_cost"] = self.node_data.groupby(
                ["tier", "@timestamp"]
            )["cost"].transform("sum")
        elif not self.skip_prices:
            self.node_data = self.node_data.join(self.cost_data, on="tier")

            # Compute the actual price of the node for the corresponding time chunk
            # Split evenly across tiers for multi-tier nodes
            self.node_data["cost"] = (
                self.node_data["price_per_hour_per_gb"]
                * (self.node_data["memory_limit_bytes"] / 1024 / 1024 / 1024)
                * self.hour_ratio
                / self.node_data["_tier_split"]
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

        # Look up 'app' field from actual data indices
        self._enrich_app_field(es)

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

    def _enrich_app_field(self, es):
        """
        Extract app name from index name pattern: foo-appname-*
        Takes the second segment (between first and second dash).
        E.g., "foo-myapp-2024.01.01" → datastream "foo-myapp" → app "myapp"
        """
        if "datastream" not in self.index_data.columns:
            return

        def _extract_app(ds_name):
            parts = ds_name.split("-", 2)
            if len(parts) >= 2:
                return parts[1]
            return ds_name

        self.index_data["app"] = self.index_data["datastream"].apply(_extract_app)
        logger.info(
            f"Extracted app names from {self.index_data['app'].nunique()} "
            f"unique apps: {self.index_data['app'].unique()[:10].tolist()}"
        )

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
        # app field from index_data lookup
        if "app" in self.index_data.columns:
            app_map = (
                self.index_data[["datastream", "app"]]
                .dropna(subset=["app"])
                .drop_duplicates(subset=["datastream"])
                .set_index("datastream")["app"]
            )
            ds_names = datastream_usages.index.get_level_values("datastream") if "datastream" in datastream_usages.index.names else datastream_usages.get("datastream", pd.Series())
            datastream_usages["app"] = ds_names.map(app_map).fillna(ds_names).values
        else:
            ds_names = datastream_usages.index.get_level_values("datastream") if "datastream" in datastream_usages.index.names else datastream_usages.get("datastream", "")
            datastream_usages["app"] = ds_names.values if hasattr(ds_names, 'values') else ds_names

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

        # app field comes from index_data (looked up from actual data indices)
        if "app" in self.index_data.columns:
            app_map = (
                self.index_data[["datastream", "app"]]
                .dropna(subset=["app"])
                .drop_duplicates(subset=["datastream"])
                .set_index("datastream")["app"]
            )
            datastreams["app"] = datastreams["datastream"].map(app_map).fillna(datastreams["datastream"])
        else:
            datastreams["app"] = datastreams["datastream"]

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
