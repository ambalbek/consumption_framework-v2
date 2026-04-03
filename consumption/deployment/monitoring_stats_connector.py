import logging
import re
import time
from abc import ABC
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from elasticsearch import Elasticsearch

logger = logging.getLogger(__name__)

DEFAULT_MONITORING_INDEX_PATTERN = ".monitoring-es-8-*,.monitoring-es-9-*"
DEFAULT_MONITORING_INDEX_PATTERN_V7 = ".monitoring-es-7-*"

# This will return datastream name and index date as groups 1 and 2 when it hits
DEFAULT_PARSING_REGEX_STR = r"(?:partial-)?(?:restored-)?(?:shrink-.{4}-)?(?:\.ds-)?(.*?)-?(\d{4}\.\d{2}\.\d{2})?(?:-\d+)?$"


class NoResultsError(Exception):
    pass


def _roles_to_tier(roles: List[str]) -> str:
    """
    Convert the roles of a node to a tier.
    We default to the "hotest" tier.
    """
    if "data_hot" in roles or "data_content" in roles:
        return "hot"
    elif "data_warm" in roles:
        return "warm"
    elif "data_cold" in roles:
        return "cold"
    elif "data_frozen" in roles:
        return "frozen"
    elif "data" in roles:
        return "hot"  # Generic data nodes default to hot tier (common in 7.x)
    else:
        return None


def range_filter(range_start: datetime, range_end: datetime, timestamp_field: str = "@timestamp") -> Dict:
    return {
        "range": {
            timestamp_field: {
                "gte": range_start.isoformat(),
                "lt": range_end.isoformat(),
            }
        }
    }


def elasticsearch_id_filter(elasticsearch_id: str, version: str = "8") -> Dict:
    if version == "7":
        return {"term": {"cluster_uuid": elasticsearch_id}}
    return {"term": {"elasticsearch.cluster.name": elasticsearch_id}}


def get_all_elasticsearch_ids(
    es: Elasticsearch, index: str, range_start: datetime, range_end: datetime
) -> List[str]:
    response = es.search(
        index=index,
        allow_no_indices=True,
        expand_wildcards=["open", "hidden"],
        query={
            "bool": {
                "filter": [
                    range_filter(range_start, range_end),
                    {"term": {"event.dataset": "elasticsearch.cluster.stats"}},
                ]
            }
        },
        size=0,
        aggs={
            "elasticsearch_ids": {
                "terms": {"field": "elasticsearch.cluster.name", "size": 10000}
            }
        },
    )
    # Check for response code
    status = response.get("status", 200)
    if status != 200:
        # Return error with the failed response code
        logger.error(f"Query failed with response code: {status}.")
        return []

    buckets = (
        response.get("aggregations", {}).get("elasticsearch_ids", {}).get("buckets", [])
    )
    if buckets:
        return [bucket["key"] for bucket in buckets]
    else:
        # Return data existence error
        logger.warning(
            f"No existing data found in the monitoring cluster between {range_start} and {range_end}."
        )
        return []


class Stats(ABC):
    # For gauge, what percentiles to compute
    # These need to be strings with 1 decimal, otherwise we won't read it properly
    # in the Elasticsearch response!
    percentiles = ["25.0", "50.0", "75.0", "95.0"]

    def __init__(
        self,
        es: Elasticsearch,
        key_field: str,
        counter_fields: List[str],
        static_fields: List[str],
        gauge_fields: List[str],
        static_filters: List[Dict[str, Any]] = [],
        sample_fields: List[str] = [],
        monitoring_index_pattern: Optional[str] = None,
        timestamp_field: str = "@timestamp",
    ):

        # Defines the behavior of the composite aggregation
        self.key_field = key_field
        self.counter_fields = counter_fields
        self.static_fields = static_fields
        self.gauge_fields = gauge_fields
        self.static_filters = static_filters
        self.sample_fields = sample_fields
        self.timestamp_field = timestamp_field

        self.es = es
        self.monitoring_index_pattern = monitoring_index_pattern

    def _build_composite(self) -> Callable[[Optional[str], Optional[int]], Dict[str, Any]]:
        def composite_fn(after_key: Optional[str], size: int = 100) -> Dict[str, Any]:
            composite_params: Dict[str, Any] = {
                "size": size,
                "sources": [
                    {"per_key": {"terms": {"field": self.key_field}}},
                ],
            }
            if after_key is not None:
                composite_params["after"] = {"per_key": after_key}
            return {
                "composite": {
                    "composite": composite_params,
                    "aggs": {
                        "per_10_minutes": {
                            "date_histogram": {
                                "field": self.timestamp_field,
                                "fixed_interval": "10m",
                                "min_doc_count": 0,
                            },
                            "aggs": dict(
                                **(
                                    {
                                        "sample": {
                                            "top_hits": {
                                                "size": 1,
                                                "_source": self.sample_fields,
                                            }
                                        }
                                    }
                                    if self.sample_fields
                                    else {}
                                ),
                                **{
                                    counter_name.replace(".", "__"): {
                                        "max": {"field": counter_name}
                                    }
                                    for counter_name in self.counter_fields
                                },
                                **{
                                    f"dv_{counter_name.replace('.', '__')}": {
                                        "derivative": {
                                            "buckets_path": counter_name.replace(
                                                ".", "__"
                                            )
                                        }
                                    }
                                    for counter_name in self.counter_fields
                                },
                                **{
                                    "static": {
                                        "top_metrics": {
                                            "metrics": [
                                                {"field": static_name}
                                                for static_name in self.static_fields
                                            ],
                                            "sort": {self.timestamp_field: "desc"},
                                        }
                                    }
                                },
                                **{
                                    f"g_{gauge_name.replace('.', '__')}": {
                                        "percentiles": {
                                            "field": gauge_name,
                                            "percents": self.percentiles,
                                        }
                                    }
                                    for gauge_name in self.gauge_fields
                                },
                            ),
                        }
                    },
                }
            }

        return composite_fn

    def search(self, filters: List[Dict[str, Any]] = []):
        composite_fn = self._build_composite()
        after_key = None
        count = 0

        # Time the querying if we're on debug level
        if logger.isEnabledFor(logging.DEBUG):
            start_time = time.time()

        while True:
            res = self.es.search(
                index=self.monitoring_index_pattern or DEFAULT_MONITORING_INDEX_PATTERN,
                size=0,
                allow_no_indices=True,
                expand_wildcards=["open", "hidden"],
                query={"bool": {"filter": self.static_filters + filters}},
                aggs=composite_fn(after_key),
                filter_path=[
                    "aggregations.composite.after_key",
                    "aggregations.composite.buckets.key",
                    "aggregations.composite.buckets.per_10_minutes.buckets.key_as_string",
                    "aggregations.composite.buckets.per_10_minutes.buckets.dv_*",
                    "aggregations.composite.buckets.per_10_minutes.buckets.g_*",
                    "aggregations.composite.buckets.per_10_minutes.buckets.static.top.metrics",
                    "aggregations.composite.buckets.per_10_minutes.buckets.sample",
                ],
            )

            # Newer ES versions may return a non-empty response with empty buckets
            # instead of the empty {} that older versions returned via filter_path.
            # Check explicitly so we don't hit a KeyError on missing after_key.
            composite_agg = (res or {}).get("aggregations", {}).get("composite", {})
            buckets = composite_agg.get("buckets") or []

            if not buckets:
                if count == 0:
                    raise NoResultsError(
                        f"Querying for stats {self.__class__.__name__} returned no results"
                    )

                # We've gone through all the entries
                break

            # Used for next loop
            after_key = composite_agg["after_key"]["per_key"]

            # Process the results
            for entity_bucket in buckets:
                # We skip the first entry as it will be missing the derivative values
                for ten_minute_bucket in entity_bucket["per_10_minutes"]["buckets"][1:]:
                    record = {
                        self.key_field: entity_bucket["key"]["per_key"],
                        "@timestamp": datetime.fromisoformat(  # If you crash here, you NEED python >=3.11
                            ten_minute_bucket["key_as_string"]
                        ),
                    }

                    # Static fields
                    if "static" in ten_minute_bucket:
                        record.update(ten_minute_bucket["static"]["top"][0]["metrics"])

                    # Sample fields
                    if (
                        "sample" in ten_minute_bucket
                        and ten_minute_bucket["sample"]["hits"]["hits"]
                    ):
                        record.update(
                            ten_minute_bucket["sample"]["hits"]["hits"][0]["_source"]
                        )

                    # Counters
                    # We force a None value if the derivative is negative
                    # is the counter got reset.
                    # TODO: consider using the original value
                    # This will require updating the dashboards
                    none_if_neg = lambda x: x if x and x >= 0 else None  # notest: E731
                    record.update(
                        {
                            f"dv_{counter_name}": none_if_neg(
                                ten_minute_bucket.get(
                                    f"dv_{counter_name.replace('.', '__')}", {}
                                ).get("value")
                            )
                            for counter_name in self.counter_fields
                        }
                    )

                    # Gauge
                    record.update(
                        {
                            f"g_{gauge_name}_{percent}": ten_minute_bucket.get(
                                f"g_{gauge_name.replace('.', '__')}", {}
                            ).get("values", {}).get(percent)
                            for gauge_name in self.gauge_fields
                            for percent in self.percentiles
                        }
                    )

                    count += 1
                    yield record

        # Time the querying if we're on debug level
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(
                f"Querying for {self.__class__.__name__} "
                f"took {time.time() - start_time:.2f} seconds "
                f"and yielded {count} records"
            )

    def search_as_dataframe(
        self,
        filters: List[Dict[str, Any]] = [],
    ):
        try:
            return pd.DataFrame(self.search(filters))
        except NoResultsError:
            logger.warning(
                f"No results for {self.__class__.__name__}, returning empty DataFrame"
            )
            return pd.DataFrame()


class ClusterStats(Stats):
    def __init__(
        self, es: Elasticsearch, monitoring_index_pattern: Optional[str] = None
    ):
        super().__init__(
            es=es,
            key_field="elasticsearch.cluster.name",
            counter_fields=[],
            static_fields=[],
            gauge_fields=[],
            static_filters=[
                {"term": {"event.dataset": {"value": "elasticsearch.cluster.stats"}}},
            ],
            sample_fields=[
                "cluster_settings.cluster.metadata.display_name",
                "elasticsearch.cluster.stats.state.nodes.*.roles",
                "elasticsearch.cluster.stats.state.nodes.*.version",
            ],
            monitoring_index_pattern=monitoring_index_pattern,
        )

    def search_as_dataframe(self, filters: List[Dict[str, Any]] = []):
        # We want to post-process the records
        try:
            records = list(self.search(filters))
        except NoResultsError:
            logger.warning(
                f"No results for {self.__class__.__name__}, returning empty DataFrame"
            )
            return pd.DataFrame()
        updated_records = []

        for record in records:
            # We want to split each node from the elasticsearch.cluster.stats.state.nodes
            # field into its own record
            for node_id, node_attributes in (
                # We use chained get() calls to avoid KeyError
                record.get("elasticsearch", {})
                .get("cluster", {})
                .get("stats", {})
                .get("state", {})
                .get("nodes", {})
                .items()
            ):
                updated_record = {
                    "@timestamp": record["@timestamp"],
                    "deployment_name": record.get("cluster_settings", {})
                    .get("cluster", {})
                    .get("metadata", {})
                    .get("display_name", "unknown")
                    .split(" (")[0],
                    "elasticsearch_id": record["elasticsearch.cluster.name"],
                    "id": node_id,
                }

                tier = _roles_to_tier(node_attributes["roles"])
                if not tier:
                    # A master or ML node, we're not interested
                    continue

                updated_record["tier"] = tier
                updated_record["version"] = node_attributes.get("version", "unknown")
                updated_records.append(updated_record)

        # From that point on, we apply the same logic as with other classes
        df = pd.DataFrame(updated_records)

        return df[
            [
                "@timestamp",
                "id",
                "deployment_name",
                "elasticsearch_id",
                "version",
                "tier",
            ]
        ]


class NodeStats(Stats):
    def __init__(
        self, es: Elasticsearch, monitoring_index_pattern: Optional[str] = None
    ):
        super().__init__(
            es=es,
            key_field="elasticsearch.node.id",
            counter_fields=[
                "elasticsearch.node.stats.os.cgroup.cpu.stat.times_throttled.count",
                "elasticsearch.node.stats.os.cgroup.cpu.stat.time_throttled.ns",
            ],
            static_fields=[
                "elasticsearch.node.name",
                "elasticsearch.node.stats.os.cgroup.cpu.cfs.quota.us",
                "elasticsearch.node.stats.os.cgroup.memory.limit.bytes",
                "elasticsearch.node.stats.fs.total.total_in_bytes",
                "elasticsearch.node.stats.fs.total.available_in_bytes",
            ],
            gauge_fields=[
                "elasticsearch.node.stats.process.cpu.pct",
            ],
            static_filters=[
                {"term": {"event.dataset": {"value": "elasticsearch.node.stats"}}}
            ],
            sample_fields=[],
            monitoring_index_pattern=monitoring_index_pattern,
        )

    def search_as_dataframe(self, filters: List[Dict[str, Any]] = []):
        df = super().search_as_dataframe(filters)

        if df.empty:
            return df

        column_mapping = {
            "elasticsearch.node.id": "id",
            "dv_elasticsearch.node.stats.os.cgroup.cpu.stat.times_throttled.count": "cpu_throttled_count_delta",
            "dv_elasticsearch.node.stats.os.cgroup.cpu.stat.time_throttled.ns": "cpu_throttled_ns_delta",
            "elasticsearch.node.name": "name",
            "elasticsearch.node.stats.os.cgroup.cpu.cfs.quota.us": "cpu_quota_us",
            "elasticsearch.node.stats.os.cgroup.memory.limit.bytes": "memory_limit_bytes",
            "elasticsearch.node.stats.fs.total.total_in_bytes": "fs_total_in_bytes",
            "elasticsearch.node.stats.fs.total.available_in_bytes": "fs_available_in_bytes",
        }

        column_mapping.update(
            {
                f"g_elasticsearch.node.stats.process.cpu.pct_{percent}": f"cpu_pct_{percent}"
                for percent in self.percentiles
            }
        )

        df.rename(columns=column_mapping, inplace=True)

        # Use the cfs quota to derive the number of cores
        # Empirically, cores = cfs quota / 1e5
        df["cores"] = df["cpu_quota_us"] / 1e5

        # Convert ns to seconds
        df["cpu_throttled_seconds_delta"] = df["cpu_throttled_ns_delta"] / 1e9

        # Convert the CPU percents to an actual 0 < 1 value
        for percent in self.percentiles:
            df[f"cpu_pct_{percent}"] = df[f"cpu_pct_{percent}"] / 100

        # This is a string in the Elasticsearch response, but we want to treat it as an int
        df["memory_limit_bytes"] = df["memory_limit_bytes"].astype("Int64")

        # TODO:
        # For now we just save the median CPU percent
        # In the future, we'll want to expose all chosen percentiles
        df["cpu_pct"] = df["cpu_pct_50.0"]

        return df[
            [
                "@timestamp",
                "id",
                "name",
                "cores",
                "cpu_throttled_count_delta",
                "cpu_throttled_seconds_delta",
                "cpu_pct",  # TODO: expose all percentiles
                "memory_limit_bytes",
                "fs_total_in_bytes",
                "fs_available_in_bytes",
            ]
        ]


class IndexStats(Stats):
    def __init__(
        self,
        es: Elasticsearch,
        monitoring_index_pattern: Optional[str] = None,
        parsing_regex_str: Optional[str] = None,
    ):
        super().__init__(
            es=es,
            key_field="elasticsearch.index.name",
            counter_fields=[
                "elasticsearch.index.total.docs.count",
                "elasticsearch.index.primaries.docs.count",
                "elasticsearch.index.total.store.size_in_bytes",
                "elasticsearch.index.total.store.total_data_set_size_in_bytes",  # >8.13 only
                "elasticsearch.index.primaries.store.size_in_bytes",
                "elasticsearch.index.primaries.store.total_data_set_size_in_bytes",  # >8.13 only
                "elasticsearch.index.total.search.query_total.count",
                "elasticsearch.index.total.search.query_time.ms",
                "elasticsearch.index.total.indexing.index_total.count",
                "elasticsearch.index.total.indexing.index_time.ms",
            ],
            static_fields=[
                "elasticsearch.index.shards.total",
                "elasticsearch.index.shards.primaries",
                "elasticsearch.index.total.store.size_in_bytes",
                "elasticsearch.index.total.store.total_data_set_size_in_bytes",  # >8.13 only
                "elasticsearch.index.total.docs.count",
                "elasticsearch.index.primaries.store.size_in_bytes",
                "elasticsearch.index.primaries.store.total_data_set_size_in_bytes",  # >8.13 only
                "elasticsearch.index.primaries.docs.count",
                "elasticsearch.index.status",
            ],
            gauge_fields=[],
            static_filters=[
                {"bool": {"should": [
                    {"term": {"event.dataset": {"value": "elasticsearch.index"}}},
                    {"term": {"event.dataset": {"value": "elasticsearch.index_summary"}}},
                ], "minimum_should_match": 1}},
            ],
            sample_fields=[],
            monitoring_index_pattern=monitoring_index_pattern,
        )

        self.parsing_regex = re.compile(parsing_regex_str or DEFAULT_PARSING_REGEX_STR)

    def _get_ds_and_date(self, df: pd.DataFrame) -> Tuple[str, Optional[datetime]]:
        """
        This helper function is used to extract the datastream name and index date
        from the index name.
        """
        name = df["name"]
        try:
            datastream, index_date = self.parsing_regex.search(name).groups()
        except AttributeError:
            return name, None

        if index_date:
            # Parse the index date into a datetime object
            try:
                index_date = datetime.strptime(index_date, "%Y.%m.%d")
            except ValueError:
                logger.warning(
                    f"Failed to parse index date {index_date} "
                    f"for datastream {datastream}"
                )
                return name, None

            # Put UTC tzinfo in the datetime object
            index_date = index_date.replace(tzinfo=timezone.utc)

        return datastream, index_date

    def search_as_dataframe(
        self,
        filters: List[Dict[str, Any]] = [],
    ):
        df = super().search_as_dataframe(filters)

        if df.empty:
            return df

        column_mapping = {
            "elasticsearch.index.name": "name",
            "dv_elasticsearch.index.total.docs.count": "total_docs_count_delta",
            "dv_elasticsearch.index.primaries.docs.count": "primary_docs_count_delta",
            "dv_elasticsearch.index.total.store.size_in_bytes": "total_store_size_in_bytes_delta",
            "dv_elasticsearch.index.total.store.total_data_set_size_in_bytes": "total_data_set_size_in_bytes_delta",
            "dv_elasticsearch.index.primaries.store.size_in_bytes": "primary_store_size_in_bytes_delta",
            "dv_elasticsearch.index.primaries.store.total_data_set_size_in_bytes": "primary_data_set_size_in_bytes_delta",
            "dv_elasticsearch.index.total.search.query_total.count": "search_query_total_delta",
            "dv_elasticsearch.index.total.search.query_time.ms": "search_query_time_ms_delta",
            "dv_elasticsearch.index.total.indexing.index_total.count": "index_total_delta",
            "dv_elasticsearch.index.total.indexing.index_time.ms": "index_time_ms_delta",
            "elasticsearch.index.shards.total": "shards_total",
            "elasticsearch.index.shards.primaries": "shards_primary",
            "elasticsearch.index.total.store.size_in_bytes": "total_store_size_in_bytes",
            "elasticsearch.index.total.store.total_data_set_size_in_bytes": "total_data_set_size_in_bytes",
            "elasticsearch.index.total.docs.count": "total_docs_count",
            "elasticsearch.index.primaries.store.size_in_bytes": "primary_store_size_in_bytes",
            "elasticsearch.index.primaries.store.total_data_set_size_in_bytes": "primary_data_set_size_in_bytes",
            "elasticsearch.index.primaries.docs.count": "primary_docs_count",
            "elasticsearch.index.status": "status",
        }

        df.rename(columns=column_mapping, inplace=True)

        # For >8.13, we can use dataset size instead of store size
        # We simply check for non-null values in the dataset size fields and override the store size
        # These columns may not exist if the field is absent from all documents
        if "total_data_set_size_in_bytes_delta" in df.columns:
            df.loc[
                ~df["total_data_set_size_in_bytes_delta"].isna(),
                "total_store_size_in_bytes_delta",
            ] = df["total_data_set_size_in_bytes_delta"]
        if "primary_data_set_size_in_bytes_delta" in df.columns:
            df.loc[
                ~df["primary_data_set_size_in_bytes_delta"].isna(),
                "primary_store_size_in_bytes_delta",
            ] = df["primary_data_set_size_in_bytes_delta"]
        if "total_data_set_size_in_bytes" in df.columns:
            df.loc[
                ~df["total_data_set_size_in_bytes"].isna(), "total_store_size_in_bytes"
            ] = df["total_data_set_size_in_bytes"]
        if "primary_data_set_size_in_bytes" in df.columns:
            df.loc[
                ~df["primary_data_set_size_in_bytes"].isna(), "primary_store_size_in_bytes"
            ] = df["primary_data_set_size_in_bytes"]

        # Convert the time deltas to seconds
        df["search_query_time_in_seconds_delta"] = (
            df["search_query_time_ms_delta"] / 1e3
        )
        df["index_time_in_seconds_delta"] = df["index_time_ms_delta"] / 1e3

        # Extract the datastream and index date from the index name
        df[["datastream", "index_date"]] = df[["name"]].apply(
            self._get_ds_and_date, axis=1, result_type="expand"
        )

        # For the indices where we have an index_date,
        # we compute their age in days.
        df.loc[~df["index_date"].isna(), "age_days"] = (
            df["@timestamp"] - df["index_date"]
        ).dt.days

        # Limit the columns we return
        return df[
            [
                "@timestamp",
                "name",
                "datastream",
                "total_docs_count_delta",
                "primary_docs_count_delta",
                "total_store_size_in_bytes_delta",
                "primary_store_size_in_bytes_delta",
                "search_query_total_delta",
                "search_query_time_in_seconds_delta",
                "index_total_delta",
                "index_time_in_seconds_delta",
                "shards_total",
                "shards_primary",
                "total_store_size_in_bytes",
                "total_docs_count",
                "primary_store_size_in_bytes",
                "primary_docs_count",
                "status",
                "age_days",
            ]
        ]


class ShardStats(Stats):
    def __init__(
        self, es: Elasticsearch, monitoring_index_pattern: Optional[str] = None
    ):
        super().__init__(
            es=es,
            key_field="elasticsearch.index.name",
            counter_fields=[],
            static_fields=["elasticsearch.shard.source_node.uuid"],
            gauge_fields=[],
            static_filters=[
                {"term": {"event.dataset": {"value": "elasticsearch.shard"}}},
                {"term": {"elasticsearch.shard.state": {"value": "STARTED"}}},
                {"term": {"elasticsearch.shard.primary": {"value": True}}},
            ],
            sample_fields=[],
            monitoring_index_pattern=monitoring_index_pattern,
        )

    def search_as_dataframe(
        self,
        filters: List[Dict[str, Any]] = [],
    ):
        df = super().search_as_dataframe(filters)

        if df.empty:
            return df

        column_mapping = {
            "elasticsearch.index.name": "index_name",
            "elasticsearch.shard.source_node.uuid": "node_uuid",
        }

        # Important to sort by index THEN @timestamp, so we don't "pollute" an index
        # with a tier coming from another index.
        # We'd also want to look far enough back.
        df = df.rename(columns=column_mapping).sort_values(
            ["index_name", "@timestamp"], ascending=True
        )

        # Fill the NaN forward, as we might not have data for every bucket.
        df["node_uuid"] = df["node_uuid"].ffill()

        return df[["@timestamp", "index_name", "node_uuid"]]


# =============================================================================
# V7 Stats classes for .monitoring-es-7-* (internal collection) support
# These produce the same output DataFrame columns as their V8 counterparts.
# =============================================================================


class ClusterStatsV7(Stats):
    def __init__(
        self, es: Elasticsearch, monitoring_index_pattern: Optional[str] = None
    ):
        super().__init__(
            es=es,
            key_field="cluster_uuid",
            counter_fields=[],
            static_fields=[],
            gauge_fields=[],
            static_filters=[
                {"term": {"type": "cluster_stats"}},
            ],
            sample_fields=[
                "cluster_state.nodes",
                "cluster_name",
            ],
            monitoring_index_pattern=monitoring_index_pattern or DEFAULT_MONITORING_INDEX_PATTERN_V7,
            timestamp_field="timestamp",
        )

    def search_as_dataframe(self, filters: List[Dict[str, Any]] = []):
        try:
            records = list(self.search(filters))
        except NoResultsError:
            logger.warning(
                f"No results for {self.__class__.__name__}, returning empty DataFrame"
            )
            return pd.DataFrame()
        updated_records = []

        for record in records:
            for node_id, node_attributes in (
                record.get("cluster_state", {})
                .get("nodes", {})
                .items()
            ):
                updated_record = {
                    "@timestamp": record["@timestamp"],
                    "deployment_name": record.get("cluster_name", "unknown"),
                    "elasticsearch_id": record["cluster_uuid"],
                    "id": node_id,
                }

                roles = node_attributes.get("roles", [])
                tier = _roles_to_tier(roles)
                if not tier:
                    continue

                updated_record["tier"] = tier
                updated_record["version"] = node_attributes.get("version", "unknown")
                updated_records.append(updated_record)

        df = pd.DataFrame(updated_records)

        return df[
            [
                "@timestamp",
                "id",
                "deployment_name",
                "elasticsearch_id",
                "version",
                "tier",
            ]
        ]


class NodeStatsV7(Stats):
    def __init__(
        self, es: Elasticsearch, monitoring_index_pattern: Optional[str] = None
    ):
        super().__init__(
            es=es,
            key_field="source_node.uuid",
            counter_fields=[
                "node_stats.os.cgroup.cpu.stat.number_of_times_throttled",
                "node_stats.os.cgroup.cpu.stat.time_throttled_nanos",
            ],
            static_fields=[
                "source_node.name",
                "node_stats.os.cgroup.cpu.cfs_quota_micros",
                "node_stats.os.cgroup.memory.limit_in_bytes",
                "node_stats.fs.total.total_in_bytes",
                "node_stats.fs.total.available_in_bytes",
            ],
            gauge_fields=[
                "node_stats.process.cpu.percent",
            ],
            static_filters=[
                {"term": {"type": "node_stats"}}
            ],
            sample_fields=[],
            monitoring_index_pattern=monitoring_index_pattern or DEFAULT_MONITORING_INDEX_PATTERN_V7,
            timestamp_field="timestamp",
        )

    def search_as_dataframe(self, filters: List[Dict[str, Any]] = []):
        df = super().search_as_dataframe(filters)

        if df.empty:
            return df

        column_mapping = {
            "source_node.uuid": "id",
            "dv_node_stats.os.cgroup.cpu.stat.number_of_times_throttled": "cpu_throttled_count_delta",
            "dv_node_stats.os.cgroup.cpu.stat.time_throttled_nanos": "cpu_throttled_ns_delta",
            "source_node.name": "name",
            "node_stats.os.cgroup.cpu.cfs_quota_micros": "cpu_quota_us",
            "node_stats.os.cgroup.memory.limit_in_bytes": "memory_limit_bytes",
            "node_stats.fs.total.total_in_bytes": "fs_total_in_bytes",
            "node_stats.fs.total.available_in_bytes": "fs_available_in_bytes",
        }

        column_mapping.update(
            {
                f"g_node_stats.process.cpu.percent_{percent}": f"cpu_pct_{percent}"
                for percent in self.percentiles
            }
        )

        df.rename(columns=column_mapping, inplace=True)

        df["cores"] = df["cpu_quota_us"] / 1e5
        df["cpu_throttled_seconds_delta"] = df["cpu_throttled_ns_delta"] / 1e9

        for percent in self.percentiles:
            df[f"cpu_pct_{percent}"] = df[f"cpu_pct_{percent}"] / 100

        df["memory_limit_bytes"] = df["memory_limit_bytes"].astype("Int64")
        df["cpu_pct"] = df["cpu_pct_50.0"]

        return df[
            [
                "@timestamp",
                "id",
                "name",
                "cores",
                "cpu_throttled_count_delta",
                "cpu_throttled_seconds_delta",
                "cpu_pct",
                "memory_limit_bytes",
                "fs_total_in_bytes",
                "fs_available_in_bytes",
            ]
        ]


class IndexStatsV7(Stats):
    def __init__(
        self,
        es: Elasticsearch,
        monitoring_index_pattern: Optional[str] = None,
        parsing_regex_str: Optional[str] = None,
    ):
        super().__init__(
            es=es,
            key_field="index_stats.index",
            counter_fields=[
                "index_stats.total.docs.count",
                "index_stats.primaries.docs.count",
                "index_stats.total.store.size_in_bytes",
                "index_stats.primaries.store.size_in_bytes",
                "index_stats.total.search.query_total",
                "index_stats.total.search.query_time_in_millis",
                "index_stats.total.indexing.index_total",
                "index_stats.total.indexing.index_time_in_millis",
            ],
            static_fields=[
                "index_stats.total.store.size_in_bytes",
                "index_stats.total.docs.count",
                "index_stats.primaries.store.size_in_bytes",
                "index_stats.primaries.docs.count",
            ],
            gauge_fields=[],
            static_filters=[
                {"term": {"type": "index_stats"}},
            ],
            sample_fields=[],
            monitoring_index_pattern=monitoring_index_pattern or DEFAULT_MONITORING_INDEX_PATTERN_V7,
            timestamp_field="timestamp",
        )

        self.parsing_regex = re.compile(parsing_regex_str or DEFAULT_PARSING_REGEX_STR)

    def _get_ds_and_date(self, df: pd.DataFrame) -> Tuple[str, Optional[datetime]]:
        name = df["name"]
        try:
            datastream, index_date = self.parsing_regex.search(name).groups()
        except AttributeError:
            return name, None

        if index_date:
            try:
                index_date = datetime.strptime(index_date, "%Y.%m.%d")
            except ValueError:
                logger.warning(
                    f"Failed to parse index date {index_date} "
                    f"for datastream {datastream}"
                )
                return name, None
            index_date = index_date.replace(tzinfo=timezone.utc)

        return datastream, index_date

    def search_as_dataframe(
        self,
        filters: List[Dict[str, Any]] = [],
    ):
        df = super().search_as_dataframe(filters)

        if df.empty:
            return df

        column_mapping = {
            "index_stats.index": "name",
            "dv_index_stats.total.docs.count": "total_docs_count_delta",
            "dv_index_stats.primaries.docs.count": "primary_docs_count_delta",
            "dv_index_stats.total.store.size_in_bytes": "total_store_size_in_bytes_delta",
            "dv_index_stats.primaries.store.size_in_bytes": "primary_store_size_in_bytes_delta",
            "dv_index_stats.total.search.query_total": "search_query_total_delta",
            "dv_index_stats.total.search.query_time_in_millis": "search_query_time_ms_delta",
            "dv_index_stats.total.indexing.index_total": "index_total_delta",
            "dv_index_stats.total.indexing.index_time_in_millis": "index_time_ms_delta",
            "index_stats.total.store.size_in_bytes": "total_store_size_in_bytes",
            "index_stats.total.docs.count": "total_docs_count",
            "index_stats.primaries.store.size_in_bytes": "primary_store_size_in_bytes",
            "index_stats.primaries.docs.count": "primary_docs_count",
        }

        df.rename(columns=column_mapping, inplace=True)

        # V7 doesn't have total_data_set_size fields - no override needed

        df["search_query_time_in_seconds_delta"] = (
            df["search_query_time_ms_delta"] / 1e3
        )
        df["index_time_in_seconds_delta"] = df["index_time_ms_delta"] / 1e3

        df[["datastream", "index_date"]] = df[["name"]].apply(
            self._get_ds_and_date, axis=1, result_type="expand"
        )

        df.loc[~df["index_date"].isna(), "age_days"] = (
            df["@timestamp"] - df["index_date"]
        ).dt.days

        # V7 doesn't have shards_total, shards_primary, status - add defaults
        df["shards_total"] = None
        df["shards_primary"] = None
        df["status"] = None

        return df[
            [
                "@timestamp",
                "name",
                "datastream",
                "total_docs_count_delta",
                "primary_docs_count_delta",
                "total_store_size_in_bytes_delta",
                "primary_store_size_in_bytes_delta",
                "search_query_total_delta",
                "search_query_time_in_seconds_delta",
                "index_total_delta",
                "index_time_in_seconds_delta",
                "shards_total",
                "shards_primary",
                "total_store_size_in_bytes",
                "total_docs_count",
                "primary_store_size_in_bytes",
                "primary_docs_count",
                "status",
                "age_days",
            ]
        ]


class ShardStatsV7(Stats):
    def __init__(
        self, es: Elasticsearch, monitoring_index_pattern: Optional[str] = None
    ):
        super().__init__(
            es=es,
            key_field="shard.index",
            counter_fields=[],
            static_fields=["shard.node"],
            gauge_fields=[],
            static_filters=[
                {"term": {"type": "shards"}},
                {"term": {"shard.state": "STARTED"}},
                {"term": {"shard.primary": True}},
            ],
            sample_fields=[],
            monitoring_index_pattern=monitoring_index_pattern or DEFAULT_MONITORING_INDEX_PATTERN_V7,
            timestamp_field="timestamp",
        )

    def search_as_dataframe(
        self,
        filters: List[Dict[str, Any]] = [],
    ):
        df = super().search_as_dataframe(filters)

        if df.empty:
            return df

        column_mapping = {
            "shard.index": "index_name",
            "shard.node": "node_uuid",
        }

        df = df.rename(columns=column_mapping).sort_values(
            ["index_name", "@timestamp"], ascending=True
        )

        df["node_uuid"] = df["node_uuid"].ffill()

        return df[["@timestamp", "index_name", "node_uuid"]]
