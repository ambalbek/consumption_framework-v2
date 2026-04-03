import logging
import re
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from time import sleep
from typing import Any, Dict

import requests

logger = logging.getLogger(__name__)


class ESSBillingClient:
    def __init__(self, api_host: str, api_key: str, org_id: str):
        client = requests.Session()
        client.headers.update({"Authorization": "ApiKey " + api_key})

        self.client = client
        self.org_id = org_id
        self.url = (
            f"https://{api_host}/api/v2/billing/organizations/{org_id}/costs/instances"
        )

        # This will ensure we don't query for the same data multiple times
        self.cache = {}

    def _perform_request_with_backoff(
        self, params: Dict[str, Any], backoff: int = 2
    ) -> Dict[str, Any]:
        response = self.client.get(self.url, params=params)

        # Check if we have a 429, apply exponential backoff and retry
        if response.status_code == 429:
            logger.warning(
                f"Got 429 from ESS API, sleeping for {backoff} seconds and retrying"
            )
            sleep(backoff)
            return self._perform_request_with_backoff(self.url, params, backoff * 2)

        # Raise for other status codes
        response.raise_for_status()

        return response.json()

    def get_billing_data(self, from_date: datetime) -> Dict[str, Any]:
        # Ensure we're only querying full hours
        assert (
            from_date.minute == 0
            and from_date.second == 0
            and from_date.microsecond == 0
        )

        to_date = from_date + timedelta(hours=1)

        if from_date not in self.cache:
            # We need to populate the cache, fetch the data.

            params = {
                "from": from_date.isoformat(timespec="microseconds"),
                "to": to_date.isoformat(timespec="microseconds"),
            }

            logger.debug(
                f"Fetching billing data between {params['from']} "
                f"and {params['to']} for organization {self.org_id}"
            )

            self.cache[from_date] = self._perform_request_with_backoff(params)

        return self.cache[from_date]


@dataclass(kw_only=True)
class ESSLineItem:
    category: str  # resource, dts
    type: str
    name: str  # human-readable name
    sku: str  # unique machine-readable identifier
    quantity: float
    rate: float  # per-hour rate
    cost: float  # total cost in ECUs

    def asdict(self) -> Dict[str, Any]:
        # Don't return "private" attributes
        # Note: doesn't work for nested attributes
        return {
            key: value for key, value in asdict(self).items() if not key.startswith("_")
        }


@dataclass(kw_only=True)
class ESSDTS(ESSLineItem):
    category: str = "dts"

    def __init__(self, api_response: Dict[str, Any]):
        self.type = api_response["type"]
        self.name = api_response["name"]
        self.sku = api_response["sku"]
        self.quantity = api_response["quantity"]["value"]
        self.rate = api_response["rate"]["value"]
        self.cost = api_response["total_ecu"]


@dataclass(kw_only=True)
class ESSResource(ESSLineItem):
    category: str = "resource"

    provider: str
    family: str
    region: str
    tier: str

    # A couple of private attributes that are used for calculations
    _ram_per_zone: int
    _zone_count: int

    _sku_regex = re.compile(
        r"(?P<provider>gcp|azure|aws)\.(?P<family>[^_]+)_(?P<region>[^_]+)_(?P<ram_per_zone>\d+)_(?P<zone_count>\d+)$"
    )

    def __init__(self, api_response: Dict[str, Any]):
        # Save the raw attributes we're interested in
        self.type = api_response["kind"]
        self.name = api_response["name"]
        self.sku = api_response["sku"]
        self.quantity = api_response["quantity"]["value"]
        self.rate = api_response["rate"]["value"]
        self.cost = api_response["total_ecu"]

        # Default values that will get overwritten if the SKU is known and parseable
        self.provider = "unknown"
        self.family = "unknown"
        self.region = "unknown"
        self.instance_count = 0

        # Identify the data tier based on name
        self.tier = "unknown"
        name_lower = self.name.lower()

        if "hot" in name_lower or "high i/o" in name_lower or "high cpu" in name_lower:
            self.tier = "hot"
        elif "warm" in name_lower:
            self.tier = "warm"
        elif "cold" in name_lower:
            self.tier = "cold"
        elif "frozen" in name_lower:
            self.tier = "frozen"
        elif "highstorage" in name_lower or "high storage" in name_lower:
            self.tier = "warm_or_cold"
        elif "coordinating" in name_lower:
            self.tier = "coordinating"
        elif "master" in name_lower:
            self.tier = "master"
        elif "ml" in name_lower or "machine learning" in name_lower:
            self.tier = "ml"

        sku_match = self._sku_regex.search(self.sku)
        if sku_match:
            # Extract the named groups from the regex match
            self.provider = sku_match.group("provider")
            self.family = sku_match.group("family")
            self.region = sku_match.group("region")
            self._ram_per_zone = int(sku_match.group("ram_per_zone")) / 1024
            self._zone_count = int(sku_match.group("zone_count"))
        else:
            logger.error(
                f'Could not parse "{self.sku}" to get RAM per zone and AZ count, skipping'
            )
            return

        self.quantity = self._ram_per_zone * self._zone_count
        self._price_per_hour_per_gb = self.rate / self.quantity
