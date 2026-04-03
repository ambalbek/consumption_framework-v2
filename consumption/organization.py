import logging
from datetime import datetime, timedelta
from hashlib import sha1
from typing import Any, Dict

from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk

from .utils import (ESSDTS, ESSBillingClient, ESSResource,
                    MultithreadingEngine, OrgDataChecker)

logger = logging.getLogger(__name__)

INDEX = "consumption"


def _as_elasticsearch_doc(d: Dict[str, Any]) -> Dict[str, Any]:
    time = d.pop("time")

    return {
        "@timestamp": time.isoformat(timespec="microseconds"),
        "_op_type": "index",
        "_index": "consumption",
        "_id": sha1(
            (
                d["organization_id"] + d["deployment_id"] + d["sku"] + time.isoformat()
            ).encode("utf-8")
        ).hexdigest(),
        "dataset": "deployment",
        **d,
    }


def get_org_billing_data(
    destination_es: Elasticsearch,
    organization_id: str,
    organization_name: str,
    billing_api_key: str,
    from_ts: datetime,
    api_host: str,
):
    deployments = ESSBillingClient(
        api_host=api_host, api_key=billing_api_key, org_id=organization_id
    ).get_billing_data(from_ts)["instances"]

    meta_parameters = {
        "time": from_ts.replace(minute=0, second=0, microsecond=0),
        "organization_id": organization_id,
        "organization_name": organization_name,
    }

    ok_count = 0

    for ok, action in streaming_bulk(
        client=destination_es,
        raise_on_error=False,
        chunk_size=5000,
        actions=(
            _as_elasticsearch_doc(d)
            for d in [
                {
                    **ESSResource(line_item).asdict(),
                    **meta_parameters,
                    "deployment_id": deployment["id"],
                    "deployment_name": deployment["name"],
                }
                if line_item["type"] == "capacity"
                else {
                    **ESSDTS(line_item).asdict(),
                    **meta_parameters,
                    "deployment_id": deployment["id"],
                    "deployment_name": deployment["name"],
                }
                for deployment in deployments
                for line_item in deployment["product_line_items"]
                if line_item["name"] != "unknown"
                and line_item["sku"]  # Skip unknown line items
                and line_item["quantity"]["value"] > 0  # Skip zero quantity
            ]
        ),
    ):
        if ok:
            ok_count += 1
        else:
            logger.error(f"Failed to index document: {action}")

    logger.debug(f"Data upload completed: {ok_count} OK")


def organization_billing(
    destination_es: Elasticsearch,
    organization_id: str,
    organization_name: str,
    billing_api_key: str,
    range_start: datetime,
    range_end: datetime,
    threads: int,
    force: bool,
    api_host: str,
):
    # Round the time ranges to the full hour
    time_ranges = [
        range_start.replace(minute=0, second=0, microsecond=0) + timedelta(hours=i)
        for i in range(0, int((range_end - range_start).total_seconds() / 3600))
    ]

    # Reverse the time ranges to start with the most recent ones
    time_ranges.reverse()

    logger.debug(
        f"Identified {len(time_ranges)} ranges "
        f"between {range_start.strftime('%Y-%m-%d %H:%M:%S')} "
        f"and {range_end.strftime('%Y-%m-%d %H:%M:%S')}"
    )

    checker = OrgDataChecker(
        destination_es=destination_es,
        organization_id=organization_id,
        force=force,
    )

    all_params = (
        {
            "destination_es": destination_es,
            "organization_id": organization_id,
            "organization_name": organization_name,
            "billing_api_key": billing_api_key,
            "from_ts": from_ts,
            "api_host": api_host,
        }
        for from_ts in time_ranges
        if not checker.is_in_cluster(from_ts, from_ts + timedelta(hours=1))
    )

    with MultithreadingEngine(workers=threads) as engine:
        [engine.submit_task(get_org_billing_data, params) for params in all_params]


__all__ = ["organization_billing"]
