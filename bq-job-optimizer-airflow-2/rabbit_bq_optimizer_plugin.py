import logging

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.log.logging_mixin import LoggingMixin
from rabbit_bq_job_optimizer import OptimizationConfig, RabbitBQJobOptimizer

RABBIT_PATCHED_MARKER = "_rabbit_bq_job_optimizer_patched"
RABBIT_API_CONN_ID = "rabbit_api"
RABBIT_API_BASE_URL_EXTRA_KEY = "api_base_url"


def _load_rabbit_credentials():
    try:
        connection = BaseHook.get_connection(RABBIT_API_CONN_ID)
    except AirflowException as exc:
        raise RuntimeError(
            f"Airflow connection '{RABBIT_API_CONN_ID}' could not be loaded"
        ) from exc

    api_key = (connection.password or "").strip()
    if not api_key:
        raise RuntimeError(
            f"Airflow connection '{RABBIT_API_CONN_ID}' is missing the password field "
            "which must contain the Rabbit API key"
        )

    extras = connection.extra_dejson or {}
    base_url = extras.get(RABBIT_API_BASE_URL_EXTRA_KEY)

    if base_url:
        base_url = base_url.strip()
        if not base_url:
            base_url = None

    return {
        "api_key": api_key,
        "base_url": base_url,
    }


def patch_bigquery_hook():
    from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook, BigQueryJob

    if not hasattr(BigQueryHook, RABBIT_PATCHED_MARKER):
        logging.info("Patching BigQueryHook to optimize job configs via Rabbit API")
        setattr(BigQueryHook, RABBIT_PATCHED_MARKER, True)
        original_insert_job = BigQueryHook.insert_job

        def insert_job(self, *, configuration: dict, **kwargs) -> BigQueryJob:
            try:
                # Try to get the configuration
                try:
                    config = Variable.get(
                        "rabbit_bq_optimizer_config", deserialize_json=True
                    )
                    if not config:
                        raise KeyError("rabbit_bq_optimizer_config is empty")
                    logging.debug(
                        "Rabbit BQ Optimizer: Successfully loaded configuration: %s",
                        config,
                    )
                except (KeyError, ValueError) as e:
                    logging.warning(
                        "Rabbit BQ Optimizer: Configuration error: %s. Proceeding with "
                        "original job configuration.",
                        str(e),
                    )
                    return original_insert_job(
                        self, configuration=configuration, **kwargs
                    )

                # Validate required fields
                required_fields = ["reservation_ids", "default_pricing_mode"]
                missing_fields = [
                    field for field in required_fields if field not in config
                ]
                if missing_fields:
                    logging.warning(
                        "Rabbit BQ Optimizer: Missing required configuration fields: "
                        "%s. Proceeding with original job configuration.",
                        ", ".join(missing_fields),
                    )
                    return original_insert_job(
                        self, configuration=configuration, **kwargs
                    )

                # Validate default_pricing_mode
                valid_pricing_modes = ["on_demand", "slot_based"]
                if config["default_pricing_mode"] not in valid_pricing_modes:
                    logging.warning(
                        "Rabbit BQ Optimizer: Invalid default_pricing_mode '%s'. Must "
                        "be one of: %s. Proceeding with original job configuration.",
                        config["default_pricing_mode"],
                        ", ".join(valid_pricing_modes),
                    )
                    return original_insert_job(
                        self, configuration=configuration, **kwargs
                    )

                if not config["reservation_ids"]:
                    logging.warning(
                        "Rabbit BQ Optimizer: No reservation IDs configured. Proceeding "
                        "with original job configuration."
                    )
                    return original_insert_job(
                        self, configuration=configuration, **kwargs
                    )

                logging.debug(
                    "Rabbit BQ Optimizer: Original job configuration: %s", configuration
                )

                try:
                    credentials = _load_rabbit_credentials()
                except Exception as e:
                    logging.warning(
                        "Rabbit BQ Optimizer: Failed to load Rabbit API connection '%s':"
                        " %s. Proceeding with original job configuration.",
                        RABBIT_API_CONN_ID,
                        str(e),
                    )
                    return original_insert_job(
                        self, configuration=configuration, **kwargs
                    )

                client = RabbitBQJobOptimizer(
                    api_key=credentials["api_key"], base_url=credentials["base_url"]
                )
                logging.debug("Rabbit BQ Optimizer: Client initialized successfully")

                optimizationConfig = OptimizationConfig(
                    type="reservation_assignment",
                    config={
                        "defaultPricingMode": config.get("default_pricing_mode"),
                        "reservationIds": config["reservation_ids"],
                    },
                )
                logging.debug(
                    "Rabbit BQ Optimizer: Optimization config created with pricing "
                    "mode: %s and %d reservation IDs",
                    config.get("default_pricing_mode"),
                    len(config["reservation_ids"]),
                )

                result = client.optimize_job(
                    configuration={"configuration": configuration},
                    enabledOptimizations=[optimizationConfig],
                )
                logging.info(
                    "Rabbit BQ Optimizer: Received optimization result: %s", result
                )

                optimizedJobConfiguration = result.optimizedJob["configuration"]

            except Exception as e:
                logging.warning(
                    "Rabbit BQ Optimizer: Optimization failed due to error: %s. "
                    "Proceeding with original job configuration.",
                    str(e),
                )
                return original_insert_job(self, configuration=configuration, **kwargs)

            try:
                result = original_insert_job(
                    self, configuration=optimizedJobConfiguration, **kwargs
                )
                return result
            except Exception as e:
                logging.warning(
                    "Rabbit BQ Optimizer: Optimization job failed due to error: %s. "
                    "Proceeding with original job configuration.",
                    str(e),
                )
                return original_insert_job(self, configuration=configuration, **kwargs)

        BigQueryHook.insert_job = insert_job


class RabbitBQOptimizerPlugin(AirflowPlugin, LoggingMixin):
    name = "rabbit_bq_job_optimizer_plugin"

    def on_load(self, *args, **kwargs):
        patch_bigquery_hook()
