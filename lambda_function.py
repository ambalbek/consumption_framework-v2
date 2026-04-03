import json
import logging
import os
import sys
from datetime import datetime, timedelta

import pytz
import requests

from consumption import Consumption

logger = logging.getLogger(__name__)


def _unpack_flat_dict(d):
    """
    Transforms a flat dictionary into a nested dictionary.
    {
        "a.b": 1,
        "a.c": 2,
        "a.d.foo": 3
    }

    into

    {
        "a": {
            "b": 1,
            "c": 2,
            "d": {
                "foo": 3
            }
        }
    }
    """

    result = {}
    for key, value in d.items():
        keys = key.split(".")
        current = result
        for k in keys[:-1]:
            current = current.setdefault(k, {})
        current[keys[-1]] = value
    return result


def handler(event, context):
    env = os.environ

    logging.basicConfig(
        stream=sys.stdout,
        format="%(asctime)s [%(threadName)s] - %(levelname)s (%(name)s): %(message)s",
        level=logging.DEBUG if "debug" in env else logging.INFO,
    )

    if "config_arn" not in env:
        logger.error("config_arn is required in environment variables")
        sys.exit(1)

    if "command" not in env:
        logger.error("command is required in environment variables")
        sys.exit(1)

    # The config key/values are recovered from the Secret Manager
    # See https://docs.aws.amazon.com/secretsmanager/latest/userguide/retrieving-secrets_lambda.html
    headers = {"X-Aws-Parameters-Secrets-Token": os.environ.get("AWS_SESSION_TOKEN")}
    secrets_extension_endpoint = (
        "http://localhost:"
        + os.environ.get("PARAMETERS_SECRETS_EXTENSION_HTTP_PORT", "2773")
        + "/secretsmanager/get?secretId="
        + env["config_arn"]
    )
    r = requests.get(secrets_extension_endpoint, headers=headers)
    config = _unpack_flat_dict(json.loads(r.json()["SecretString"]))
    
    # Convert the string values to the correct types
    config['monitoring_source']['retry_on_timeout'] = config['monitoring_source']['retry_on_timeout'].lower() == 'true'
    config['monitoring_source']['request_timeout'] = int(config['monitoring_source']['request_timeout'])

    command = env["command"]
    threads = int(env.get("threads", 5))
    lookbehind = int(env.get("lookbehind", 24))
    force = bool(env.get("force", False))
    compute_usages = bool(env.get("compute_usages", False))

    to_ts = datetime.now(tz=pytz.UTC)
    from_ts = to_ts - timedelta(hours=lookbehind)
    
    Consumption(
        organization_id=None,
        organization_name=None,
        billing_api_key=None,
        destination_config=config["consumption_destination"],
        api_host="api.elastic-cloud.com",
        monitoring_index_pattern=config.get(
                "monitoring_index_pattern", ".monitoring-es-8-*"
        ),
    ).init()

    if command == "consume-monitoring":
        Consumption(
            organization_id=config.get("organization_id"),
            organization_name=config["organization_name"],
            billing_api_key=config.get("billing_api_key"),
            on_prem_costs_dict=config.get("on_prem_costs"),
            destination_config=config["consumption_destination"],
            source_config=config["monitoring_source"],
            threads=threads,
            force=force,
            compute_usages=compute_usages,
            api_host="api.elastic-cloud.com",
            monitoring_index_pattern=config.get(
                "monitoring_index_pattern", ".monitoring-es-8-*"
            ),
        ).consume_monitoring(from_ts, to_ts)
    elif command == "get-billing-data":
        Consumption(
            organization_id=config["organization_id"],
            organization_name=config["organization_name"],
            billing_api_key=config["billing_api_key"],
            destination_config=config["consumption_destination"],
            threads=threads,
            force=force,
            api_host="api.elastic-cloud.com",
            monitoring_index_pattern=config.get(
                "monitoring_index_pattern", ".monitoring-es-8-*"
            ),
        ).get_billing_data(from_ts, to_ts)
    elif command == "both":
        Consumption(
            organization_id=config.get("organization_id"),
            organization_name=config["organization_name"],
            billing_api_key=config.get("billing_api_key"),
            on_prem_costs_dict=config.get("on_prem_costs"),
            destination_config=config["consumption_destination"],
            source_config=config["monitoring_source"],
            threads=threads,
            force=force,
            compute_usages=compute_usages,
            api_host="api.elastic-cloud.com",
            monitoring_index_pattern=config.get(
                "monitoring_index_pattern", ".monitoring-es-8-*"
            ),
        ).consume_monitoring(from_ts, to_ts)

        Consumption(
            organization_id=config["organization_id"],
            organization_name=config["organization_name"],
            billing_api_key=config["billing_api_key"],
            destination_config=config["consumption_destination"],
            threads=threads,
            force=force,
            api_host="api.elastic-cloud.com",
            monitoring_index_pattern=config.get(
                "monitoring_index_pattern", ".monitoring-es-8-*"
            ),
        ).get_billing_data(from_ts, to_ts)
    else:
        logger.error(f"Unknown command: {command}")
        sys.exit(1)
