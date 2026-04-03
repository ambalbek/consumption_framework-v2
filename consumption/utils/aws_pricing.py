import json
import logging
from typing import Dict, Optional

import requests

logger = logging.getLogger(__name__)

# AWS Pricing API endpoint (public, no auth needed)
AWS_PRICING_URL = (
    "https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonEC2/current"
)

# Cache: {(instance_type, region): hourly_price_usd}
_price_cache: Dict[tuple, float] = {}


def _region_to_pricing_region(region: str) -> str:
    """Convert AWS region code to the pricing API region name."""
    region_map = {
        "us-east-1": "US East (N. Virginia)",
        "us-east-2": "US East (Ohio)",
        "us-west-1": "US West (N. California)",
        "us-west-2": "US West (Oregon)",
        "eu-west-1": "EU (Ireland)",
        "eu-west-2": "EU (London)",
        "eu-west-3": "EU (Paris)",
        "eu-central-1": "EU (Frankfurt)",
        "eu-central-2": "EU (Zurich)",
        "eu-north-1": "EU (Stockholm)",
        "eu-south-1": "EU (Milan)",
        "ap-northeast-1": "Asia Pacific (Tokyo)",
        "ap-northeast-2": "Asia Pacific (Seoul)",
        "ap-northeast-3": "Asia Pacific (Osaka)",
        "ap-southeast-1": "Asia Pacific (Singapore)",
        "ap-southeast-2": "Asia Pacific (Sydney)",
        "ap-south-1": "Asia Pacific (Mumbai)",
        "sa-east-1": "South America (Sao Paulo)",
        "ca-central-1": "Canada (Central)",
        "me-south-1": "Middle East (Bahrain)",
        "af-south-1": "Africa (Cape Town)",
    }
    return region_map.get(region, region)


def get_ec2_hourly_price(
    instance_type: str, region: str = "us-east-1"
) -> Optional[float]:
    """
    Fetch the on-demand hourly price (USD) for an EC2 instance type in a region.
    Uses the AWS Pricing bulk JSON index (no auth required).
    Results are cached for the session.
    """
    cache_key = (instance_type, region)
    if cache_key in _price_cache:
        return _price_cache[cache_key]

    pricing_region = _region_to_pricing_region(region)

    try:
        # Use the pricing filter API
        url = (
            f"https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonEC2/current/"
            f"{region}/index.json"
        )
        logger.debug(f"Fetching EC2 pricing for {instance_type} in {region}")

        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        # Search through products for matching instance type
        for sku, product in data.get("products", {}).items():
            attrs = product.get("attributes", {})
            if (
                attrs.get("instanceType") == instance_type
                and attrs.get("tenancy") == "Shared"
                and attrs.get("operatingSystem") == "Linux"
                and attrs.get("preInstalledSw") == "NA"
                and attrs.get("capacitystatus") == "Used"
            ):
                # Find the on-demand price for this SKU
                terms = data.get("terms", {}).get("OnDemand", {}).get(sku, {})
                for term_key, term_data in terms.items():
                    for dim_key, dim_data in term_data.get(
                        "priceDimensions", {}
                    ).items():
                        price_str = dim_data.get("pricePerUnit", {}).get("USD", "0")
                        price = float(price_str)
                        if price > 0:
                            _price_cache[cache_key] = price
                            logger.info(
                                f"EC2 price for {instance_type} in {region}: "
                                f"${price:.4f}/hour"
                            )
                            return price

        logger.warning(
            f"No pricing found for {instance_type} in {region}"
        )
        _price_cache[cache_key] = None
        return None

    except Exception as e:
        logger.error(f"Failed to fetch EC2 pricing: {e}")
        _price_cache[cache_key] = None
        return None


def get_ec2_costs_dataframe(node_instances: Dict[str, dict]):
    """
    Given a dict of {node_id: {"instance_type": ..., "region": ...}},
    return a DataFrame with hourly cost per node.
    """
    import pandas as pd

    records = []
    for node_id, info in node_instances.items():
        price = get_ec2_hourly_price(
            info.get("instance_type", ""), info.get("region", "us-east-1")
        )
        records.append(
            {"id": node_id, "ec2_hourly_price_usd": price or 0.0}
        )

    return pd.DataFrame(records).set_index("id")
