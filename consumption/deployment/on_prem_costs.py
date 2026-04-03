import logging
from typing import Dict

import pandas as pd

logger = logging.getLogger(__name__)


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
