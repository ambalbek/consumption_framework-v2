from datetime import datetime
from typing import Any, Dict, List

from elasticsearch import Elasticsearch

INDEX = "consumption"


class DataChecker:
    def __init__(
        self,
        destination_es: Elasticsearch,
        organization_id: str,
        filters: List[Dict[str, Any]] = [],
        force: bool = False,
    ):
        self.es = destination_es
        self.filters = filters + [{"term": {"organization_id": organization_id}}]
        self.force = force

    def is_in_cluster(self, from_ts: datetime, to_ts: datetime, filters=[]) -> bool:
        return not self.force and (
            self.es.count(
                index=INDEX + "*",
                allow_no_indices=True,
                query={
                    "bool": {
                        "filter": self.filters
                        + [
                            {
                                "range": {
                                    "timestamp": {
                                        "gte": from_ts.isoformat(),
                                        "lt": to_ts.isoformat(),
                                    }
                                }
                            }
                        ]
                        + filters
                    }
                },
            )["count"]
            > 0
        )


class OrgDataChecker(DataChecker):
    def __init__(
        self,
        destination_es: Elasticsearch,
        organization_id: str,
        filters: List[Dict[str, Any]] = [],
        force: bool = False,
    ):
        super().__init__(
            destination_es,
            organization_id,
            filters + [{"term": {"dataset": "deployment"}}],
            force,
        )


class DepDataChecker(DataChecker):
    def __init__(
        self,
        destination_es: Elasticsearch,
        organization_id: str,
        filters: List[Dict[str, Any]] = [],
        force: bool = False,
    ):
        super().__init__(
            destination_es,
            organization_id,
            filters + [{"term": {"dataset": "node"}}],
            force,
        )
