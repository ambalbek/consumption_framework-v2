import datetime
import logging
from typing import Dict, Optional

logger = logging.getLogger(__name__)

_cached_daily_cost: Optional[float] = None


def get_average_daily_cost(aws_config: Dict) -> float:
    """
    Fetch average daily cost from AWS Cost Explorer using NetUnblendedCost
    (after RI/savings plan discounts), filtered by Application Name tag.

    Replicates the original standalone script logic exactly:
    - Queries from first day of current month to yesterday
    - Filters by Application Name tags (OR logic)
    - Groups by Application Name
    - Calculates average daily cost across all days
    - Falls back to fallback_daily_cost if average < min_daily_cost

    aws_config keys:
        aws_access_key_id: str
        aws_secret_access_key: str
        region: str (default: us-east-1)
        tag_key: str (default: "Application Name")
        tag_values: list (default: ["ELMS - Enterprise Logging and Monitoring System", "elms"])
        min_daily_cost: float (default: 500)
        fallback_daily_cost: float (default: 887)
    """
    global _cached_daily_cost
    if _cached_daily_cost is not None:
        return _cached_daily_cost

    try:
        import boto3
    except ImportError:
        logger.error("boto3 not installed. Run: pip install boto3")
        fallback = aws_config.get("fallback_daily_cost", 887.0)
        _cached_daily_cost = fallback
        return fallback

    aws_access_key_id = aws_config["aws_access_key_id"]
    aws_secret_access_key = aws_config["aws_secret_access_key"]
    region = aws_config.get("region", "us-east-1")
    tag_key = aws_config.get("tag_key", "Application Name")
    tag_values = aws_config.get("tag_values", [
        "ELMS - Enterprise Logging and Monitoring System",
        "elms",
    ])
    min_daily_cost = aws_config.get("min_daily_cost", 500.0)
    fallback_daily_cost = aws_config.get("fallback_daily_cost", 887.0)

    today = datetime.datetime.utcnow().date()
    first_day_of_month = today.replace(day=1)
    start_date = first_day_of_month.isoformat()
    end_date = (today - datetime.timedelta(days=1)).isoformat()

    # Need at least 1 full day of data
    if first_day_of_month >= today - datetime.timedelta(days=1):
        logger.warning("Not enough days in current month for cost calculation, using fallback")
        _cached_daily_cost = fallback_daily_cost
        return fallback_daily_cost

    logger.info(f"Fetching AWS Cost Explorer data ({start_date} to {end_date})")

    try:
        client = boto3.client(
            "ce",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region,
        )

        # Build OR filter for multiple tag values (same as original script)
        tag_filters = [
            {"Tags": {"Key": tag_key, "Values": [v], "MatchOptions": ["EQUALS"]}}
            for v in tag_values
        ]
        cost_filter = {"Or": tag_filters} if len(tag_filters) > 1 else tag_filters[0]

        response = client.get_cost_and_usage(
            TimePeriod={"Start": start_date, "End": end_date},
            Granularity="DAILY",
            Metrics=["NetUnblendedCost"],
            Filter=cost_filter,
            GroupBy=[{"Type": "TAG", "Key": tag_key}],
        )

        total_cost = 0.0
        matching_group_count = len(response.get("ResultsByTime", []))

        for result in response.get("ResultsByTime", []):
            for group in result.get("Groups", []):
                cost_str = group["Metrics"]["NetUnblendedCost"]["Amount"]
                try:
                    cost = float(cost_str)
                    total_cost += cost
                    logger.debug(
                        f"Date: {result['TimePeriod']['Start']}, "
                        f"App: {group['Keys'][0]}, Cost: {cost:.2f}"
                    )
                except ValueError:
                    pass

        # Average daily cost (same logic as original script)
        _average_cost = (
            total_cost / matching_group_count
            if matching_group_count > 0
            else 0.0
        )
        average_cost = (
            _average_cost if _average_cost >= min_daily_cost else fallback_daily_cost
        )

        logger.info(
            f"AWS Cost Explorer: total=${total_cost:.2f} over {matching_group_count} days, "
            f"average=${average_cost:.2f}/day"
        )

        _cached_daily_cost = average_cost
        return average_cost

    except Exception as e:
        logger.error(f"AWS Cost Explorer failed: {e}")
        _cached_daily_cost = fallback_daily_cost
        return fallback_daily_cost
