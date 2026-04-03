import logging
from collections import namedtuple
from datetime import datetime, timedelta
from typing import Dict, Iterable, Optional, Tuple

import pandas as pd
import pytz
from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk

from ..utils import (DepDataChecker, ESSBillingClient, ESSResource,
                     MultithreadingEngine)
from .monitoring_stats_connector import (DEFAULT_MONITORING_INDEX_PATTERN,
                                         DEFAULT_MONITORING_INDEX_PATTERN_V7)
from .on_prem_costs import get_on_prem_costs
from .processor import DeploymentDataProcessor

logger = logging.getLogger(__name__)

INDEX = "consumption"


class ESSBillingClientCostsProvider(ESSBillingClient):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.tier_prices = {}

    def get_elasticsearch_costs(
        self, deployment_id: str, from_ts: datetime
    ) -> pd.DataFrame:
        # Get the billing data for the given deployment_id
        billing_data = next(
            (
                d
                for d in self.get_billing_data(from_ts)["instances"]
                if d["id"] == deployment_id
            ),
            None,
        )

        if not billing_data:
            logger.error(
                f"No billing data found for {deployment_id} for {from_ts.isoformat()}"
            )

            # An empty dataframe ensures we can still compute some info
            return pd.DataFrame()

        # TODO: merge the below with organization processing
        for line_item in billing_data["product_line_items"]:
            if (
                line_item["name"] != "unknown"
                and line_item["sku"]
                and line_item["kind"] == "elasticsearch"
            ):
                resource = ESSResource(line_item)
                self.tier_prices[resource.tier] = resource._price_per_hour_per_gb

        # TODO: reuse the on_prem_costs function
        df = pd.DataFrame.from_dict(
            self.tier_prices, orient="index", columns=["price_per_hour_per_gb"]
        )
        df.index.name = "tier"

        return df


def _source_walk(
    source_es: Elasticsearch,
    index: str,
    range_start: datetime,
    range_end: datetime,
    size: int = 100,
    timestamp_field: str = "@timestamp",
    cluster_id_field: str = "elasticsearch.cluster.name",
    dataset_filter: Optional[Dict] = None,
) -> Iterable[Tuple[datetime, str]]:
    """
    Composite aggregation to walk through the monitoring source data.
    We yield the elasticsearch_id and start of the hour for each block
    where we have monitoring data.
    """
    if dataset_filter is None:
        dataset_filter = {
            "exists": {"field": "elasticsearch.cluster.name"}
        }

    after = None
    while True:
        composite_params = {
            "size": size,
            "sources": [
                {
                    "per_hour": {
                        "date_histogram": {
                            "field": timestamp_field,
                            "fixed_interval": "1h",
                            "order": "desc",
                        }
                    }
                },
                {
                    "per_elasticsearch_id": {
                        "terms": {"field": cluster_id_field}
                    }
                },
            ],
        }
        if after is not None:
            composite_params["after"] = {
                "per_hour": after[0],
                "per_elasticsearch_id": after[1],
            }

        response = source_es.search(
            index=index,
            size=0,
            allow_no_indices=True,
            expand_wildcards=["open", "hidden"],
            query={
                "bool": {
                    "filter": [
                        {
                            "range": {
                                timestamp_field: {
                                    "gte": int(range_start.timestamp() * 1000),
                                    "lt": int(min(
                                        range_end,
                                        datetime.now(pytz.utc).replace(
                                            minute=0, second=0, microsecond=0
                                        ),
                                    ).timestamp() * 1000),
                                    "format": "epoch_millis",
                                }
                            }
                        },
                        dataset_filter,
                    ]
                }
            },
            aggs={"composite": {"composite": composite_params}},
            filter_path=["aggregations"],
        )

        res = (response or {}).get("aggregations", {}).get("composite", {})

        logger.debug(
            f"_source_walk query: index={index!r} "
            f"range=[{range_start.isoformat()} → {min(range_end, datetime.now(pytz.utc).replace(minute=0, second=0, microsecond=0)).isoformat()}] "
            f"buckets={len(res.get('buckets', []))} "
            f"raw_agg_keys={list((response or {}).get('aggregations', {}).keys())}"
        )

        if not res.get("buckets"):
            # We've gone through all the data in scope
            break

        # Used for our next iteration
        after = tuple(res["after_key"].values())

        yield from (
            (
                datetime.fromtimestamp(b["key"]["per_hour"] / 1000, pytz.utc),
                b["key"]["per_elasticsearch_id"],
            )
            for b in res["buckets"]
        )


def _analyze_chunk(
    source_es: Elasticsearch,
    destination_es: Elasticsearch,
    organization_id: Optional[str],
    organization_name: str,
    elasticsearch_id: str,
    from_ts: datetime,
    price_df: pd.DataFrame,
    monitoring_index_pattern: Optional[str] = None,
    parsing_regex_str: Optional[str] = None,
    compute_usages: bool = False,
    monitoring_version: str = "8",
):
    def _as_elasticsearch_doc(tuple: namedtuple) -> dict:
        source = tuple._asdict()
        source["@timestamp"] = source.pop("timestamp").isoformat()
        del source["index"]

        source["organization_id"] = organization_id
        source["organization_name"] = organization_name

        # Necessary for compatibility with v1
        source["deployment_id"] = source["elasticsearch_id"]

        # Some parameters are shared between node and datastream, and we use them
        # as base for the resulting document ID.
        doc_id = (
            source["dataset"]
            + "|"
            # On-prem can have empty organization_id
            + (source["organization_id"] or "")
            + "|"
            + source["deployment_id"]
            + "|"
            + source["@timestamp"]
            + "|"
            + source["tier"]
            + "|"
        )

        # The id generation process is different for Node and Index
        if source["dataset"] == "node":
            doc_id += source["id"]
        elif source["dataset"] == "datastream":
            doc_id += source["datastream"]
        elif source["dataset"] == "datastream_usage":
            doc_id += source["datastream"] + "|" + str(source["age_days"])
        else:
            raise ValueError(f"Unknown dataset: {source['dataset']}")

        return {
            "_id": doc_id,
            "_index": "consumption",
            "_source": source,
            "_op_type": "index",
        }

    ok_count = 0

    for ok, action in streaming_bulk(
        client=destination_es,
        raise_on_error=False,
        actions=(
            _as_elasticsearch_doc(entry)
            for entry in DeploymentDataProcessor(
                es=source_es,
                elasticsearch_id=elasticsearch_id,
                from_ts=from_ts,
                price_df=price_df,
                monitoring_index_pattern=monitoring_index_pattern,
                parsing_regex_str=parsing_regex_str,
                monitoring_version=monitoring_version,
            ).process(compute_usages=compute_usages)
        ),
    ):
        if ok:
            ok_count += 1
        else:
            logger.error(f"Failed to index document: {action}")

    logger.info(f"Data upload completed: {ok_count} OK for {elasticsearch_id} (v{monitoring_version})")


def _iter_source_walks(
    source_es: Elasticsearch,
    range_start: datetime,
    range_end: datetime,
    monitoring_index_pattern: Optional[str] = None,
) -> Iterable[Tuple[datetime, str, str]]:
    """
    Yields (from_ts, elasticsearch_id, monitoring_version) tuples
    from both V8 and V7 monitoring indices.

    Both V8 and V7 source walks are always tried.
    Custom monitoring_index_pattern is used for both when provided.
    """
    v8_index = monitoring_index_pattern or DEFAULT_MONITORING_INDEX_PATTERN
    # V7 always uses its own pattern to avoid mapping conflicts with V8 indices
    v7_index = DEFAULT_MONITORING_INDEX_PATTERN_V7

    # V8/V9 source walk (metricbeat format: @timestamp, elasticsearch.cluster.name)
    for from_ts, elasticsearch_id in _source_walk(
        source_es, v8_index, range_start, range_end,
    ):
        yield from_ts, elasticsearch_id, "8"

    # V7 source walk (internal monitoring format: timestamp, cluster_uuid)
    logger.info(f"Starting V7 source walk on index={v7_index!r}")
    v7_count = 0
    try:
        for from_ts, elasticsearch_id in _source_walk(
            source_es,
            v7_index,
            range_start,
            range_end,
            timestamp_field="timestamp",
            cluster_id_field="cluster_uuid",
            dataset_filter={"term": {"type": "cluster_stats"}},
        ):
            v7_count += 1
            yield from_ts, elasticsearch_id, "7"
    except Exception as e:
        logger.error(f"V7 source walk failed: {e}", exc_info=True)
    logger.info(f"V7 source walk found {v7_count} chunks")


def monitoring_analyzer(
    source_es: Elasticsearch,
    destination_es: Elasticsearch,
    organization_id: Optional[str],
    organization_name: str,
    billing_api_key: Optional[str],
    range_start: datetime,
    range_end: datetime,
    threads: int,
    force: bool,
    compute_usages: bool,
    api_host: str,
    monitoring_index_pattern: Optional[str] = None,
    parsing_regex_str: Optional[str] = None,
    on_prem_costs_dict: Optional[Dict[str, float]] = None,
):
    cost_provider = (
        ESSBillingClientCostsProvider(api_host, billing_api_key, organization_id)
        if not on_prem_costs_dict
        else None
    )

    checker = DepDataChecker(destination_es, organization_id, force=force)

    # Collect all discovered chunks first so we can log a summary
    discovered = []
    for from_ts, elasticsearch_id, monitoring_version in _iter_source_walks(
        source_es, range_start, range_end, monitoring_index_pattern,
    ):
        discovered.append((from_ts, elasticsearch_id, monitoring_version))

    v7_count = sum(1 for _, _, v in discovered if v.startswith("7"))
    v8_count = sum(1 for _, _, v in discovered if v == "8")
    unique_clusters = set((eid, v) for _, eid, v in discovered)
    logger.info(
        f"Source walk discovered {len(discovered)} chunks "
        f"({v8_count} V8, {v7_count} V7) "
        f"across {len(unique_clusters)} unique cluster(s): "
        f"{[c[0] for c in unique_clusters]}"
    )

    if not discovered:
        logger.error("No monitoring data found in any format. Nothing to process.")
        return

    with MultithreadingEngine(workers=threads) as engine:
        submitted = 0
        for from_ts, elasticsearch_id, monitoring_version in discovered:
            if checker.is_in_cluster(
                from_ts,
                from_ts + timedelta(hours=1),
                filters=[{"term": {"elasticsearch_id": elasticsearch_id}}],
            ):
                continue

            params = {
                "source_es": source_es,
                "destination_es": destination_es,
                "organization_id": organization_id,
                "organization_name": organization_name,
                "elasticsearch_id": elasticsearch_id,
                "from_ts": from_ts,
                "price_df": (
                    get_on_prem_costs(on_prem_costs_dict)
                    if on_prem_costs_dict
                    else cost_provider.get_elasticsearch_costs(
                        elasticsearch_id, from_ts
                    )
                ),
                "monitoring_index_pattern": monitoring_index_pattern,
                "parsing_regex_str": parsing_regex_str,
                "compute_usages": compute_usages,
                "monitoring_version": monitoring_version,
            }
            engine.submit_task(_analyze_chunk, params)
            submitted += 1

        logger.info(f"Submitted {submitted} tasks for processing")


__all__ = ["monitoring_analyzer"]
