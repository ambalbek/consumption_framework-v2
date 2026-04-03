import logging
from typing import Dict, Optional

import pandas as pd

logger = logging.getLogger(__name__)


# Tier weight ratios for distributing total cost when using total_monthly_cost_usd
TIER_WEIGHTS = {"hot": 1.0, "warm": 0.5, "cold": 0.25, "frozen": 0.1}


def get_on_prem_costs(cost_dict: Dict[str, float]) -> pd.DataFrame:
    """
    Dict shoud be of the form:
    {
        "hot": 0.1,
        "warm": 0.05,
        "cold": 0.01,
        "frozen": 0.005,
    }
    Values are the price per node GB RAM per hour.
    For compatibility with the ESS--version, this returns a Callable.
    """

    logger.debug(f"Loading on-prem costs from dict: {cost_dict}")

    try:
        df = pd.DataFrame.from_dict(
            cost_dict, orient="index", columns=["price_per_hour_per_gb"]
        )
    except ValueError:
        logger.error(
            f"Error loading on-prem costs from dict: {cost_dict}. "
            "Please check the format of the dict."
        )
        raise

    df.index.name = "tier"

    # Emit a warning if we don't have hot, warm, cold or frozen tiers
    for tier in ["hot", "warm", "cold", "frozen"]:
        if tier not in df.index:
            logger.warning(f"Tier {tier} not found in on-prem costs dict: {cost_dict}")

    return df


def get_costs_from_monthly_total(total_monthly_usd: float) -> pd.DataFrame:
    """
    Auto-calculate price_per_hour_per_gb for each tier from a total monthly USD cost.

    Distributes the total cost across tiers using fixed weight ratios:
      hot=1.0, warm=0.5, cold=0.25, frozen=0.1

    The actual per-node cost is: price_per_hour_per_gb * node_memory_gb * hours.
    We set prices so the weighted sum across all tiers ≈ total_monthly_usd.

    The price is per GB RAM per hour. With the tier weights:
      hot_price = total_monthly_usd / 720 hours (per GB, normalized by weight)
    """
    hourly_total = total_monthly_usd / 720.0  # 30 days * 24 hours

    costs = {}
    for tier, weight in TIER_WEIGHTS.items():
        costs[tier] = hourly_total * weight

    logger.info(
        f"Calculated USD costs from ${total_monthly_usd}/month: "
        f"hot=${costs['hot']:.4f}/hr/GB, warm=${costs['warm']:.4f}/hr/GB, "
        f"cold=${costs['cold']:.4f}/hr/GB, frozen=${costs['frozen']:.4f}/hr/GB"
    )

    df = pd.DataFrame.from_dict(
        costs, orient="index", columns=["price_per_hour_per_gb"]
    )
    df.index.name = "tier"
    return df
