import json
import logging
from datetime import datetime, timedelta
from typing import Dict, Optional

logger = logging.getLogger(__name__)

# Cache: {(instance_type, region): hourly_price_usd}
_price_cache: Dict[tuple, float] = {}
_account_costs_cache: Dict[str, Dict[str, float]] = {}


def get_account_costs(
    account_id: str, region: str = "us-east-1"
) -> Dict[str, float]:
    """
    Fetch actual EC2 costs per instance type from AWS Cost Explorer.
    Returns {instance_type: daily_cost_per_instance_usd} based on real billing.
    Includes reserved instance discounts, savings plans, etc.

    Requires: boto3, AWS credentials with ce:GetCostAndUsage permission.
    """
    cache_key = f"{account_id}:{region}"
    if cache_key in _account_costs_cache:
        return _account_costs_cache[cache_key]

    try:
        import boto3

        ce = boto3.client("ce", region_name="us-east-1")
        ec2 = boto3.client("ec2", region_name=region)

        # Get costs for last 30 days grouped by instance type
        end = datetime.utcnow().strftime("%Y-%m-%d")
        start = (datetime.utcnow() - timedelta(days=30)).strftime("%Y-%m-%d")

        logger.info(
            f"Fetching AWS Cost Explorer data for account {account_id} "
            f"({start} to {end})"
        )

        response = ce.get_cost_and_usage(
            TimePeriod={"Start": start, "End": end},
            Granularity="MONTHLY",
            Metrics=["UnblendedCost"],
            GroupBy=[
                {"Type": "DIMENSION", "Key": "INSTANCE_TYPE"},
            ],
            Filter={
                "And": [
                    {
                        "Dimensions": {
                            "Key": "SERVICE",
                            "Values": [
                                "Amazon Elastic Compute Cloud - Compute"
                            ],
                        }
                    },
                    {
                        "Dimensions": {
                            "Key": "REGION",
                            "Values": [region],
                        }
                    },
                ]
            },
        )

        # Parse: {instance_type: total_cost_for_period}
        type_costs = {}
        days_in_period = 0
        for result in response.get("ResultsByTime", []):
            period_start = datetime.strptime(result["TimePeriod"]["Start"], "%Y-%m-%d")
            period_end = datetime.strptime(result["TimePeriod"]["End"], "%Y-%m-%d")
            days_in_period = max(days_in_period, (period_end - period_start).days)

            for group in result.get("Groups", []):
                itype = group["Keys"][0]
                cost = float(group["Metrics"]["UnblendedCost"]["Amount"])
                type_costs[itype] = type_costs.get(itype, 0) + cost

        if not type_costs:
            logger.warning("No EC2 costs found in Cost Explorer")
            _account_costs_cache[cache_key] = {}
            return {}

        # Count running instances per type to get per-instance cost
        # Use EC2 describe_instances to get current count
        instance_counts = {}
        paginator = ec2.get_paginator("describe_instances")
        for page in paginator.paginate(
            Filters=[{"Name": "instance-state-name", "Values": ["running"]}]
        ):
            for reservation in page["Reservations"]:
                for instance in reservation["Instances"]:
                    itype = instance["InstanceType"]
                    instance_counts[itype] = instance_counts.get(itype, 0) + 1

        # Calculate per-instance hourly cost
        costs = {}
        for itype, total_cost in type_costs.items():
            count = instance_counts.get(itype, 1)  # fallback to 1 if not found
            hours = days_in_period * 24
            hourly = total_cost / hours / count if hours > 0 else 0
            costs[itype] = hourly
            logger.info(
                f"  {itype}: ${total_cost:.2f}/month total, "
                f"{count} instances, ${hourly:.4f}/hr/instance"
            )

        _account_costs_cache[cache_key] = costs
        return costs

    except ImportError:
        logger.error(
            "boto3 not installed. Run: pip install boto3. "
            "Falling back to on-demand pricing."
        )
        _account_costs_cache[cache_key] = {}
        return {}
    except Exception as e:
        logger.error(f"AWS Cost Explorer failed: {e}. Falling back to on-demand pricing.")
        _account_costs_cache[cache_key] = {}
        return {}


# Static on-demand fallback prices (USD/hour) for us-east-1
_ON_DEMAND_PRICES = {
    "r6i.large": 0.126, "r6i.xlarge": 0.252, "r6i.2xlarge": 0.504,
    "r6i.4xlarge": 1.008, "r6i.8xlarge": 2.016, "r6i.12xlarge": 3.024,
    "r6i.16xlarge": 4.032, "r6i.24xlarge": 6.048,
    "r6g.large": 0.1008, "r6g.xlarge": 0.2016, "r6g.2xlarge": 0.4032,
    "r6g.4xlarge": 0.8064, "r6g.8xlarge": 1.6128, "r6g.16xlarge": 3.2256,
    "r5.large": 0.126, "r5.xlarge": 0.252, "r5.2xlarge": 0.504,
    "r5.4xlarge": 1.008, "r5.8xlarge": 2.016, "r5.12xlarge": 3.024,
    "r5.16xlarge": 4.032, "r5.24xlarge": 6.048,
    "r7i.large": 0.1323, "r7i.xlarge": 0.2646, "r7i.2xlarge": 0.5292,
    "r7i.4xlarge": 1.0584, "r7i.8xlarge": 2.1168, "r7i.16xlarge": 4.2336,
    "m5.large": 0.096, "m5.xlarge": 0.192, "m5.2xlarge": 0.384,
    "m5.4xlarge": 0.768, "m5.12xlarge": 2.304,
    "m6i.large": 0.096, "m6i.xlarge": 0.192, "m6i.2xlarge": 0.384,
    "m6i.4xlarge": 0.768, "m6i.12xlarge": 2.304,
    "i3.large": 0.156, "i3.xlarge": 0.312, "i3.2xlarge": 0.624,
    "i3.4xlarge": 1.248, "i3.8xlarge": 2.496, "i3.16xlarge": 4.992,
    "i3en.large": 0.226, "i3en.xlarge": 0.452, "i3en.2xlarge": 0.904,
    "i3en.6xlarge": 2.712, "i3en.12xlarge": 5.424, "i3en.24xlarge": 10.848,
    "c5.large": 0.085, "c5.xlarge": 0.17, "c5.2xlarge": 0.34,
    "c5.4xlarge": 0.68, "c5.9xlarge": 1.53,
    "c6i.large": 0.085, "c6i.xlarge": 0.17, "c6i.2xlarge": 0.34,
    "c6i.4xlarge": 0.68, "c6i.12xlarge": 2.04,
}

_REGION_MULTIPLIERS = {
    "us-east-1": 1.0, "us-east-2": 1.0, "us-west-1": 1.08, "us-west-2": 1.0,
    "eu-west-1": 1.10, "eu-west-2": 1.15, "eu-central-1": 1.13,
    "eu-north-1": 1.08, "ap-northeast-1": 1.25, "ap-southeast-1": 1.12,
    "ap-southeast-2": 1.18, "ap-south-1": 1.05, "sa-east-1": 1.40,
    "ca-central-1": 1.05,
}


def get_ec2_hourly_price(
    instance_type: str,
    region: str = "us-east-1",
    account_id: Optional[str] = None,
) -> Optional[float]:
    """
    Get hourly price for an EC2 instance type.
    Priority:
      1. Real costs from AWS Cost Explorer (if account_id provided + boto3 available)
      2. Static on-demand pricing table (fallback)
    """
    if not instance_type:
        return None

    cache_key = (instance_type, region, account_id)
    if cache_key in _price_cache:
        return _price_cache[cache_key]

    # Try real account costs first
    if account_id:
        account_costs = get_account_costs(account_id, region)
        if instance_type in account_costs:
            price = account_costs[instance_type]
            _price_cache[cache_key] = price
            return price

    # Fallback to static on-demand pricing
    base_price = _ON_DEMAND_PRICES.get(instance_type)
    if base_price is not None:
        multiplier = _REGION_MULTIPLIERS.get(region, 1.10)
        price = base_price * multiplier
        _price_cache[cache_key] = price
        logger.info(f"Using on-demand price for {instance_type} in {region}: ${price:.4f}/hr")
        return price

    logger.warning(f"No pricing found for {instance_type}")
    _price_cache[cache_key] = None
    return None
