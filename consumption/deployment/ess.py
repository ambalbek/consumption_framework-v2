import logging
from datetime import datetime
from typing import Optional

import pandas as pd
import requests

from ..utils import ESSResource, ESSURLs

logger = logging.getLogger(__name__)


def get_elasticsearch_costs(
    organization_id: str,
    deployment_id: Optional[str],
    ess_api_key: str,
    from_ts: datetime,
    to_ts: datetime,
    api_host: str,
) -> pd.DataFrame:
    """
    Get cost of individual ES instances for a given deployment.
    Returns a dict of {instance_configuration: price}
    Since this can vary over time, we use the given time to fetch the costs.
    """

    if not deployment_id:
        logger.warning(
            "No deployment_id provided, skipping cost fetching - does the deployment still exist?"
        )
        # an empty dataframe will make the processing function exit gracefully
        return pd.DataFrame()

    logger.debug(
        f"Fetching costs for deployment {deployment_id} between {from_ts.isoformat()} and {to_ts.isoformat()}"
    )

    res = requests.get(
        url=ESSURLs(api_host).ESS_BILLING_URL % (organization_id, deployment_id),
        headers={"Authorization": "ApiKey " + ess_api_key},
        params={
            "from": from_ts.isoformat(timespec="microseconds"),
            "to": to_ts.isoformat(timespec="microseconds"),
        },
    )
    res.raise_for_status()

    resources = res.json().get("resources", [])

    prices = []
    for resource in resources:
        ess_resource = ESSResource(resource)

        if ess_resource.type != "elasticsearch" or ess_resource.tier == "system":
            continue

        # If we already have a price for this tier, log it
        if ess_resource.tier in [price["tier"] for price in prices]:
            logger.warning(
                f"Found multiple prices for tier {ess_resource.tier} for deployment {deployment_id} between {from_ts.isoformat()} and {to_ts.isoformat()}"
            )
            logger.warning(f"Full resources: {resources}")

        prices.append(
            {
                "tier": ess_resource.tier,
                "price_per_hour_per_gb": ess_resource.price_per_hour_per_gb,
            }
        )

    logger.debug(
        f"Found prices for deployment ID {deployment_id} between "
        f"{from_ts.strftime('%Y-%m-%d %H:%M:%S')} and "
        f"{to_ts.strftime('%Y-%m-%d %H:%M:%S')}: {prices}"
    )

    return pd.DataFrame(prices).set_index("tier") if prices else pd.DataFrame()
