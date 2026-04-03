import json
import logging
import os
import sys
from datetime import datetime, timedelta

import click
import pytz
import urllib3
import yaml

from consumption import Consumption

logger = logging.getLogger(__name__)

# Disable warnings from urllib3
urllib3.disable_warnings()


def with_click_options(func):
    @click.option(
        "--config-file",
        required=False,
        help="Path to config file",
    )
    @click.option("--inline-config", required=False, help="Inline config as a json")
    @click.option(
        "--config",
        required=False,
        multiple=True,
        help="Config entry as key=value, will override both other config options",
    )
    @click.option(
        "--lookbehind", default=24, help="Number of hours to look behind (default 24)"
    )
    @click.option("--threads", default=5, help="Number of threads to use (default 5)")
    @click.option(
        "--force",
        is_flag=True,
        help="Force data computation, even if it already exists",
    )
    @click.option("--debug", is_flag=True, help="Enable debug logging")
    @click.option(
        "--vpn", is_flag=True, default=False, help="Whether to use the VPN or not"
    )
    def wrapper(**kwargs):
        # Pop some parameters from kwargs to avoid passing them to the function
        config_file = kwargs.pop("config_file", False)
        inline_config = kwargs.pop("inline_config", False)
        config = kwargs.pop("config", [])
        debug = kwargs.pop("debug", False)

        logging.basicConfig(
            stream=sys.stdout,
            format="%(asctime)s [%(threadName)s] - %(levelname)s (%(name)s): %(message)s",
            level=logging.DEBUG if debug else logging.INFO,
        )
        if not debug:
            logging.getLogger("urllib3").setLevel(logging.WARNING)
            logging.getLogger("elastic_transport").setLevel(logging.WARNING)

        config_dict = {}

        # Inline config takes precedence over config file,
        # since the config_file has a default value and could otherwise lead to conflicts.
        if inline_config:
            config_dict = json.loads(inline_config)
        elif config_file and os.path.exists(config_file):
            with open(config_file, "r") as f:
                config_dict = yaml.safe_load(f)

        # Override config_dict with individually passed config parameters
        for c in config:
            if "=" not in c:
                if c.endswith((".yml", ".yaml")) or c.startswith(("/", "./", "../")):
                    logger.error(
                        f"Invalid --config entry '{c}': it looks like a file path. "
                        f"Use --config-file {c} instead."
                    )
                else:
                    logger.error(
                        f"Invalid --config entry '{c}': expected format key=value "
                        f"(e.g. --config consumption_destination.cloud_id=MY_CLOUD_ID)"
                    )
                sys.exit(1)
            key, value = c.split("=", 1)

            # key is a nested key, e.g. "consumption_destination.cloud_id"
            path = key.split(".")

            # Traverse the nested dict to set the value
            d = config_dict
            for p in path[:-1]:
                d = d.setdefault(p, {})
            d[path[-1]] = value

        if not config_dict:
            logger.error(
                "No configuration found. Please provide a config file, inline config or individual config parameters"
            )
            sys.exit(1)

        func(config_dict, **kwargs)

    return wrapper


@click.group()
def cli():
    pass


@cli.command("init", help="Initialize the target cluster")
@with_click_options
def init(config, vpn, *args, **kwargs):
    Consumption(
        organization_id=None,
        organization_name=None,
        billing_api_key=None,
        destination_config=config["consumption_destination"],
        api_host=get_api_host(config, vpn),
        monitoring_index_pattern=config.get("monitoring_index_pattern"),
    ).init()


@cli.command(
    "consume-monitoring", help="Consume monitoring data from an existing cluster"
)
@click.option(
    "--compute-usages",
    is_flag=True,
    help="Compute index usages by age, this will create a significant amount of documents",
)
@with_click_options
def consume_monitoring(config, lookbehind, threads, force, vpn, compute_usages):
    range_end = datetime.now(tz=pytz.UTC)
    range_start = range_end - timedelta(hours=lookbehind)

    Consumption(
        organization_id=str(config.get("organization_id")),
        organization_name=str(config["organization_name"]),
        billing_api_key=str(config.get("billing_api_key")),
        on_prem_costs_dict=config.get("on_prem_costs"),
        destination_config=config["consumption_destination"],
        source_config=config["monitoring_source"],
        threads=threads,
        force=force,
        compute_usages=compute_usages,
        api_host=get_api_host(config, vpn),
        monitoring_index_pattern=config.get("monitoring_index_pattern"),
        parsing_regex_str=config.get("parsing_regex_str"),
    ).consume_monitoring(range_start, range_end)


@cli.command("get-billing-data", help="Recover org-level billing data from ESS")
@with_click_options
def get_billing_data(
    config,
    lookbehind,
    threads,
    force,
    vpn,
):
    if "on_prem_costs" in config:
        logger.warning(
            "on_prem_costs isn't supported for get-billing-data, "
            "are you sure you're using the right command?"
        )

    range_end = datetime.now(tz=pytz.UTC)
    range_start = range_end - timedelta(hours=lookbehind)

    Consumption(
        organization_id=str(config["organization_id"]),
        organization_name=str(config["organization_name"]),
        billing_api_key=str(config["billing_api_key"]),
        destination_config=config["consumption_destination"],
        threads=threads,
        force=force,
        api_host=get_api_host(config, vpn),
        monitoring_index_pattern=config.get("monitoring_index_pattern"),
    ).get_billing_data(range_start, range_end)


def get_api_host(config, vpn):
    return config.get("api_host", "admin.found.no" if vpn else "api.elastic-cloud.com")


if __name__ == "__main__":
    cli()
