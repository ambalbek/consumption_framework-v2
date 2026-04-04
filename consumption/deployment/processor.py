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
                                         range_filter,
                                         _roles_to_tiers)

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

        # Fetch cluster data — needed for node tier mapping
        self.cluster_data = _make_stats(
            cluster_stats_cls, es, stats_index_pattern
        ).search_as_dataframe([range_flt, id_filter])

        # If V7 cluster_stats didn't have cluster_state.nodes, try V8 metricbeat
        if self.cluster_data.empty and is_v7:
            logger.warning(
                f"V7 cluster_state.nodes not found, "
                f"falling back to V8 metricbeat for node tier info"
            )
            self.cluster_data = self._get_tier_from_v8_metricbeat(
                es, monitoring_index_pattern, range_flt
            )

        logger.info(
            f"[{self.elasticsearch_id}] cluster_data: "
            f"{len(self.cluster_data)} rows, "
            f"tiers={self.cluster_data['tier'].unique().tolist() if not self.cluster_data.empty and 'tier' in self.cluster_data.columns else 'N/A'}"
        )

        if self.cluster_data.empty:
            logger.error(
                f"No cluster/tier data found for {self.elasticsearch_id} "
                f"at {self.from_ts.strftime('%Y-%m-%d %H:%M:%S')}, "
                f"skipping further processing"
            )
            self.index_data = pd.DataFrame()
            return

        # Fetch node data and merge with cluster data for tier info
        node_df = _make_stats(node_stats_cls, es, stats_index_pattern).search_as_dataframe(
            [range_flt, id_filter]
        )

        logger.info(
            f"[{self.elasticsearch_id}] node_df: {len(node_df)} rows"
        )

        if node_df.empty:
            logger.error(
                f"No node data found for {self.elasticsearch_id} "
                f"at {self.from_ts.strftime('%Y-%m-%d %H:%M:%S')}, "
                f"skipping further processing"
            )
            self.index_data = pd.DataFrame()
            return

        merged = node_df.merge(
            self.cluster_data,
            left_on=["@timestamp", "id"],
            right_on=["@timestamp", "id"],
            how="left",
            suffixes=(None, ""),
        )

        logger.info(
            f"[{self.elasticsearch_id}] after merge: {len(merged)} rows, "
            f"tier null count: {merged['tier'].isna().sum()}, "
            f"tier values: {merged['tier'].dropna().unique().tolist()[:5]}"
        )

        self.node_data = merged.dropna(subset=["tier"])

        logger.info(
            f"[{self.elasticsearch_id}] after dropna(tier): {len(self.node_data)} rows"
        )

        if self.node_data.empty:
            logger.error(
                f"All nodes dropped after tier merge for {self.elasticsearch_id}. "
                f"Possible @timestamp mismatch between cluster_data and node_data. "
                f"cluster_data timestamps: {self.cluster_data['@timestamp'].unique()[:3].tolist()}, "
                f"node_df timestamps: {node_df['@timestamp'].unique()[:3].tolist()}"
            )
            self.index_data = pd.DataFrame()
            return

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

    def _get_tier_from_v8_metricbeat(self, es, monitoring_index_pattern, range_flt):
        """
        Fallback: get node tier info from V8 metricbeat node.stats documents.
        These have elasticsearch.node.roles which we can map to tiers.
        Returns a DataFrame matching ClusterStats output format.
        """
        try:
            v8_pattern = monitoring_index_pattern or ".monitoring-es-8-*,.monitoring-es-9-*"
            response = es.search(
                index=v8_pattern,
                size=0,
                allow_no_indices=True,
                ignore_unavailable=True,
                expand_wildcards=["open", "hidden"],
                query={
                    "bool": {
                        "filter": [
                            {"exists": {"field": "elasticsearch.node.roles"}},
                            range_flt,
                        ]
                    }
                },
                aggs={
                    "per_node": {
                        "terms": {"field": "elasticsearch.node.id", "size": 1000},
                        "aggs": {
                            "info": {
                                "top_metrics": {
                                    "metrics": [
                                        {"field": "elasticsearch.node.name"},
                                        {"field": "elasticsearch.cluster.name"},
                                    ],
                                    "sort": {"@timestamp": "desc"},
                                }
                            },
                            "roles": {
                                "top_hits": {
                                    "size": 1,
                                    "_source": ["elasticsearch.node.roles"],
                                    "sort": [{"@timestamp": "desc"}],
                                }
                            },
                            "per_10m": {
                                "date_histogram": {
                                    "field": "@timestamp",
                                    "fixed_interval": "10m",
                                },
                            },
                        },
                    }
                },
            )

            records = []
            for node_bucket in response.get("aggregations", {}).get("per_node", {}).get("buckets", []):
                node_id = node_bucket["key"]
                metrics = node_bucket["info"]["top"][0]["metrics"]
                node_name = metrics.get("elasticsearch.node.name", "unknown")
                cluster_name = metrics.get("elasticsearch.cluster.name", self.elasticsearch_id)

                # Get roles from top_hits
                roles_hits = node_bucket["roles"]["hits"]["hits"]
                if not roles_hits:
                    continue
                roles = (
                    roles_hits[0]["_source"]
                    .get("elasticsearch", {})
                    .get("node", {})
                    .get("roles", [])
                )

                tiers = _roles_to_tiers(roles)
                if not tiers:
                    continue

                # Create one record per tier per 10-min bucket
                for time_bucket in node_bucket["per_10m"]["buckets"]:
                    ts = datetime.fromisoformat(time_bucket["key_as_string"])
                    for tier in tiers:
                        records.append({
                            "@timestamp": ts,
                            "id": node_id,
                            "deployment_name": cluster_name,
                            "elasticsearch_id": self.elasticsearch_id,
                            "version": "unknown",
                            "tier": tier,
                        })

            if records:
                logger.info(f"V8 metricbeat fallback: found {len(records)} tier records for {len(set(r['id'] for r in records))} nodes")
                return pd.DataFrame(records)
            else:
                logger.warning("V8 metricbeat fallback: no node roles found")
                return pd.DataFrame()

        except Exception as e:
            logger.error(f"V8 metricbeat tier fallback failed: {e}")
            return pd.DataFrame()

    def _enrich_app_field(self, es):
        """
        Extract app name from datastream name pattern: foo*-appname-*
        Splits by dash, takes everything after the first segment (the prefix).
        E.g., "foobar-payments" → "payments"
              "foo1-auth-service" → "auth-service"
              "foo-logging" → "logging"
        """
        if "datastream" not in self.index_data.columns:
            return

        def _extract_app(name):
            parts = name.split("-", 1)  # split into max 2 parts at first dash
            return parts[1] if len(parts) >= 2 else name

        self.index_data["app"] = self.index_data["datastream"].apply(_extract_app)
        logger.info(
            f"Extracted app names: "
            f"{self.index_data['app'].unique()[:10].tolist()}"
        )

    def _get_datastreams_usages(self) -> Iterable:
        if self.index_data.empty:
            logger.warning("Empty dataset, skipping datastreams usages")
            return []

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

        has_costs = not self.skip_prices or "tier_cost" in datastream_usages.columns and datastream_usages["tier_cost"].notna().any()
        if has_costs:
            logger.debug(
                f"Computing datastream usages costs for {self.elasticsearch_id} "
                f"at {self.from_ts.strftime('%Y-%m-%d %H:%M:%S')}"
            )

            # Compute the cost for each datastream/tier for the chunk
            # Divide by 2 (only search + storage here) so they sum to tier_cost
            cost_per_dimension = datastream_usages["tier_cost"] / 2.0
            datastream_usages["search_cost"] = (
                datastream_usages["search_query_time_in_seconds_pct"]
                * cost_per_dimension
            )
            datastream_usages["storage_cost"] = (
                datastream_usages["total_store_size_in_bytes_pct"]
                * cost_per_dimension
            )

        datastream_usages = datastream_usages.drop(
            columns=[
                "tier_cost",
            ],
        )

        datastream_usages = datastream_usages.reset_index().rename(
            columns={"@timestamp": "timestamp"}
        )

        # Put None instead of NaN (numeric columns only)
        num_cols = datastream_usages.select_dtypes(include="number").columns
        datastream_usages[num_cols] = datastream_usages[num_cols].apply(np.nan_to_num).fillna(0)

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
            datastream_usages["app"] = datastream_usages["datastream"].map(app_map).fillna(datastream_usages["datastream"])
        else:
            datastream_usages["app"] = datastream_usages["datastream"]

        return list(datastream_usages.itertuples(index=False))

    def _get_datastreams(self) -> Iterable:
        if self.index_data.empty:
            logger.warning("Empty dataset, skipping datastreams")
            return []

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

        has_costs = not self.skip_prices or "tier_cost" in datastreams.columns and datastreams["tier_cost"].notna().any()
        if has_costs:
            logger.debug(
                f"Computing datastreams costs for {self.elasticsearch_id} "
                f"at {self.from_ts.strftime('%Y-%m-%d %H:%M:%S')}"
            )

            # Compute the cost for each datastream/tier for the chunk
            # Divide tier_cost by 3 so sum(search + indexing + storage) = tier_cost
            cost_per_dimension = datastreams["tier_cost"] / 3.0
            datastreams["search_cost"] = (
                datastreams["search_query_time_in_seconds_pct"]
                * cost_per_dimension
            )
            datastreams["indexing_cost"] = (
                datastreams["index_time_in_seconds_pct"] * cost_per_dimension
            )
            datastreams["storage_cost"] = (
                datastreams["total_store_size_in_bytes_pct"] * cost_per_dimension
            )

        datastreams = datastreams.drop(
            columns=[
                "tier_cost",
            ],
        )

        datastreams = datastreams.reset_index().rename(
            columns={"@timestamp": "timestamp"}
        )

        # Put None instead of NaN (numeric columns only)
        num_cols = datastreams.select_dtypes(include="number").columns
        datastreams[num_cols] = datastreams[num_cols].apply(np.nan_to_num).fillna(0)

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

        return list(datastreams.itertuples(index=False))

    def _get_nodes(self):
        logger.debug(
            f"Computing nodes for {self.elasticsearch_id} "
            f"at {self.from_ts.strftime('%Y-%m-%d %H:%M:%S')}"
        )

        if not hasattr(self, "node_data") or self.node_data.empty:
            logger.warning("No node data, skipping nodes")
            return []

        nodes = self.node_data.reset_index(drop=True).rename(columns={"@timestamp": "timestamp"})
        num_cols = nodes.select_dtypes(include="number").columns
        nodes[num_cols] = nodes[num_cols].apply(np.nan_to_num).fillna(0)

        logger.debug(
            f"{len(nodes)} nodes found for {self.elasticsearch_id} "
            f"at {self.from_ts.strftime('%Y-%m-%d %H:%M:%S')}"
        )

        nodes["dataset"] = "node"

        return list(nodes.itertuples(index=False))

    def process(self, compute_usages: bool = False):
        res = self._get_datastreams() + self._get_nodes()
        if compute_usages:
            res += self._get_datastreams_usages()

        return res
