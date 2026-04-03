import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

from .deployment import monitoring_analyzer
from .organization import organization_billing
from .utils import ElasticsearchClient

logger = logging.getLogger(__name__)


# TODO: we'll probably want to clean this up, as calls are separate from one another
class Consumption:
    def __init__(
        self,
        organization_id: str,
        organization_name: str,
        billing_api_key: str,
        destination_config: Dict[str, Any],
        monitoring_index_pattern: Optional[str] = None,
        parsing_regex_str: Optional[str] = None,
        api_host: Optional[str] = None,
        source_config: Optional[Dict[str, Any]] = None,
        threads: int = 5,
        on_prem_costs_dict: Optional[Dict[str, float]] = None,
        force: bool = False,
        compute_usages: bool = False,
    ):
        self.organization_id = organization_id
        self.organization_name = organization_name
        self.billing_api_key = billing_api_key
        self.on_prem_costs_dict = on_prem_costs_dict
        self.destination_es = ElasticsearchClient(**destination_config)
        self.threads = threads
        self.force = force
        self.compute_usages = compute_usages
        self.api_host = api_host
        self.monitoring_index_pattern = monitoring_index_pattern
        self.parsing_regex_str = parsing_regex_str

        if source_config:
            self.source_es = ElasticsearchClient(**source_config)

        logger.info(f"Created consumption object for {self.organization_name}")

    def init(self):
        """
        Uploads all assets (index templates, ILM policies, ingest pipelines...)
        Necessary to the consumption package.
        """

        logger.info("Initializing consumption package Elasticsearch assets")

        # Read local meta files,
        # and create the corresponding assets on the cluster
        base_path = Path(__file__).parent

        # Load the ingest pipeline
        logger.debug("Pushing ingest pipeline")
        with open(
            base_path / "_meta" / "ingest_pipeline.json", "r"
        ) as ingest_pipeline_file:
            self.destination_es.ingest.put_pipeline(**json.load(ingest_pipeline_file))

        # ILM policy
        logger.debug("Pushing ILM policy")
        with open(base_path / "_meta" / "ilm_policy.json", "r") as ilm_policy_file:
            self.destination_es.ilm.put_lifecycle(**json.load(ilm_policy_file))

        # Index template
        logger.debug("Pushing solution index template")
        with open(
            base_path / "_meta" / "index_template.json", "r"
        ) as index_template_file:
            self.destination_es.indices.put_index_template(
                **json.load(index_template_file)
            )

    def get_billing_data(
        self,
        range_start: datetime,
        range_end: datetime,
    ):
        organization_billing(
            destination_es=self.destination_es,
            organization_id=self.organization_id,
            organization_name=self.organization_name,
            billing_api_key=self.billing_api_key,
            range_start=range_start,
            range_end=range_end,
            threads=self.threads,
            force=self.force,
            api_host=self.api_host,
        )

    def consume_monitoring(
        self,
        range_start: datetime,
        range_end: datetime,
    ):
        monitoring_analyzer(
            source_es=self.source_es,
            destination_es=self.destination_es,
            organization_id=self.organization_id,
            organization_name=self.organization_name,
            billing_api_key=self.billing_api_key,
            range_start=range_start,
            range_end=range_end,
            threads=self.threads,
            force=self.force,
            compute_usages=self.compute_usages,
            api_host=self.api_host,
            monitoring_index_pattern=self.monitoring_index_pattern,
            parsing_regex_str=self.parsing_regex_str,
            on_prem_costs_dict=self.on_prem_costs_dict,
        )


__all__ = ["Consumption"]
