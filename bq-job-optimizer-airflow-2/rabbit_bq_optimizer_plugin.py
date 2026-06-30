"""
Rabbit BQ job optimizer — Airflow 2 plugin.

Patches ``BigQueryHook.insert_job`` so every BQ submit is optimized via the Rabbit API
before ``jobs.insert``. Patches ``BigQueryInsertJobOperator._submit_job`` with a
short-lived hook bridge so operator-driven submits can also route ``project_id`` to an
on-demand pool when the optimizer sets ``optimizedJob.jobReference.projectId``.

The ``rabbit_bq_optimizer_config`` variable is read once per job submit at the hook
boundary; optimization runs only when ``enabled`` is explicitly ``true``. Otherwise the
original ``insert_job`` runs with no API call.
"""

from __future__ import annotations

import logging
import os
from typing import Any

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.log.logging_mixin import LoggingMixin
from rabbit_bq_job_optimizer import OptimizationConfig, RabbitBQJobOptimizer

RABBIT_HOOK_PATCHED_MARKER = "_rabbit_bq_job_optimizer_hook_patched"
RABBIT_OPERATOR_PATCHED_MARKER = "_rabbit_bq_job_optimizer_operator_patched"
RABBIT_OPERATOR_BRIDGE_ATTR = "_rabbit_operator"
SOURCE_PROJECT_LABEL = "rabbit-source-project"
POOL_PROJECT_LABEL = "rabbit-pool-project"
POOL_ROUTING_LABEL = "rabbit-pool-routing"
POOL_ROUTING_APPLIED = "applied"
POOL_ROUTING_SKIPPED = "skipped"
POOL_ROUTING_NONE = "none"
OPTIMIZER_VARIABLE = "rabbit_bq_optimizer_config"
RABBIT_API_CONN_ID = "rabbit_api"
RABBIT_API_BASE_URL_EXTRA_KEY = "api_base_url"


def _load_rabbit_credentials() -> dict[str, str | None]:
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
        base_url = base_url.strip() or None

    return {"api_key": api_key, "base_url": base_url}


def _resolve_source_project(*, project_id: str | None, hook) -> str | None:
    from airflow.providers.google.common.hooks.base_google import PROVIDE_PROJECT_ID

    if project_id and project_id not in (None, "", PROVIDE_PROJECT_ID, "PROVIDE_PROJECT_ID"):
        return project_id
    if hook.project_id:
        return hook.project_id
    try:
        import google.auth

        _, adc = google.auth.default()
        return adc or os.environ.get("GOOGLE_CLOUD_PROJECT")
    except Exception:
        return os.environ.get("GOOGLE_CLOUD_PROJECT")


def _load_optimizer_config() -> dict[str, Any] | None:
    try:
        raw = Variable.get(OPTIMIZER_VARIABLE, deserialize_json=True)
        # Variable missing, empty, or not a JSON object.
        if not raw or not isinstance(raw, dict):
            raise ValueError(f"{OPTIMIZER_VARIABLE} not set or invalid")

        # Optimization is opt-in; enabled must be present and true.
        if "enabled" not in raw:
            logging.info(
                "Rabbit BQ Optimizer: %s missing enabled field. Using original job.",
                OPTIMIZER_VARIABLE,
            )
            return None

        enabled = raw["enabled"]
        if isinstance(enabled, str):
            enabled = enabled.strip().lower() in ("true", "1", "yes", "on")
        if not enabled:
            logging.info(
                "Rabbit BQ Optimizer: disabled via %s (enabled=false). Using original job.",
                OPTIMIZER_VARIABLE,
            )
            return None

        # Required optimizer input.
        if "default_pricing_mode" not in raw:
            raise ValueError("missing default_pricing_mode")

        # Only supported pricing modes.
        if raw["default_pricing_mode"] not in ("on_demand", "slot_based"):
            raise ValueError(f"bad default_pricing_mode: {raw['default_pricing_mode']!r}")

        config = dict(raw)
        reservation_ids = config.get("reservation_ids") or []
        # On-demand routing needs at least one reservation to compare against.
        if config["default_pricing_mode"] == "on_demand" and not reservation_ids:
            raise ValueError("on_demand default with no reservation_ids")
        config["reservation_ids"] = reservation_ids
        return config
    except (KeyError, ValueError) as exc:
        # Missing variable, bad JSON, or failed validation above.
        logging.warning("Rabbit BQ Optimizer: config error: %s. Using original job.", exc)
        return None


def _should_optimize_for_current_dag(config: dict[str, Any]) -> bool:
    """Honor the optional ``dag_whitelist``: only optimize jobs from listed DAGs.

    Returns True when optimization should proceed (no whitelist configured, or the
    current DAG is in it). Returns False — leaving the job untouched — when the
    whitelist is empty, malformed, the current DAG cannot be determined, or the DAG
    is not listed.

    An absent (or ``null``) ``dag_whitelist`` means "no restriction" and optimizes
    every DAG. An explicit empty list means "nothing is whitelisted" and optimizes
    no DAG.
    """
    dag_whitelist = config.get("dag_whitelist")
    if dag_whitelist is None:
        return True

    if not isinstance(dag_whitelist, list):
        logging.warning(
            "Rabbit BQ Optimizer: dag_whitelist must be a list, got %s. Using original job.",
            type(dag_whitelist).__name__,
        )
        return False

    if not dag_whitelist:
        logging.info(
            "Rabbit BQ Optimizer: dag_whitelist is empty; no DAGs are whitelisted. "
            "Skipping optimization."
        )
        return False

    try:
        from airflow.operators.python import get_current_context

        dag_id = get_current_context()["dag"].dag_id
    except Exception:
        dag_id = None

    if dag_id is None:
        logging.warning(
            "Rabbit BQ Optimizer: dag_whitelist is set but the current DAG could not be "
            "determined. Using original job."
        )
        return False

    if dag_id not in dag_whitelist:
        logging.info(
            "Rabbit BQ Optimizer: DAG '%s' is not in dag_whitelist. Skipping optimization.",
            dag_id,
        )
        return False

    return True


def _optimize(
    *,
    config: dict[str, Any],
    configuration: dict[str, Any],
    hook,
    project_id: str | None,
) -> tuple[dict[str, Any], str | None] | None:
    """Returns (optimized_configuration, pool_billing_project) or None."""
    try:
        credentials = _load_rabbit_credentials()
    except Exception as exc:
        logging.warning(
            "Rabbit BQ Optimizer: failed to load Rabbit API connection '%s': %s. "
            "Using original job.",
            RABBIT_API_CONN_ID,
            exc,
        )
        return None

    source_project = _resolve_source_project(project_id=project_id, hook=hook)
    client_kwargs = {"api_key": credentials["api_key"]}
    if credentials["base_url"]:
        client_kwargs["base_url"] = credentials["base_url"]
    client = RabbitBQJobOptimizer(**client_kwargs)

    optimize_kwargs: dict[str, Any] = {
        "configuration": {"configuration": configuration},
        "enabledOptimizations": [
            OptimizationConfig(
                type="reservation_assignment",
                config={
                    "defaultPricingMode": config.get("default_pricing_mode"),
                    "reservationIds": config["reservation_ids"],
                },
            )
        ],
    }
    if source_project:
        optimize_kwargs["project_id"] = source_project

    try:
        result = client.optimize_job(**optimize_kwargs)
    except Exception as exc:
        logging.warning("Rabbit BQ Optimizer: optimize_job failed: %s. Using original job.", exc)
        return None

    logging.debug("Rabbit BQ Optimizer: optimization result=%s", result)

    optimized = dict((result.optimizedJob or {}).get("configuration") or {})
    if source_project:
        labels = dict(optimized.get("labels") or {})
        labels.setdefault(SOURCE_PROJECT_LABEL, source_project)
        optimized["labels"] = labels

    job_ref = (result.optimizedJob or {}).get("jobReference") or {}
    pool_billing_project = job_ref.get("projectId")
    return optimized, pool_billing_project


def _stamp_pool_routing_labels(
    optimized: dict[str, Any],
    *,
    pool_billing_project: str | None,
    operator_bridge_active: bool,
) -> None:
    """Record pool-routing outcome on the BQ job config for post-run verification."""
    labels = dict(optimized.get("labels") or {})
    if pool_billing_project:
        labels[POOL_PROJECT_LABEL] = pool_billing_project
        labels[POOL_ROUTING_LABEL] = (
            POOL_ROUTING_APPLIED if operator_bridge_active else POOL_ROUTING_SKIPPED
        )
    else:
        labels[POOL_ROUTING_LABEL] = POOL_ROUTING_NONE
    optimized["labels"] = labels


def patch_bigquery_insert_job_operator() -> None:
    from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

    if hasattr(BigQueryInsertJobOperator, RABBIT_OPERATOR_PATCHED_MARKER):
        return

    logging.info("Rabbit BQ Optimizer: patching BigQueryInsertJobOperator._submit_job")
    setattr(BigQueryInsertJobOperator, RABBIT_OPERATOR_PATCHED_MARKER, True)
    original = BigQueryInsertJobOperator._submit_job

    def patched_submit_job(self, hook, job_id: str):
        # BigQueryInsertJobOperator path: lets insert_job align project_id with poll/defer.
        setattr(hook, RABBIT_OPERATOR_BRIDGE_ATTR, self)
        try:
            return original(self, hook, job_id)
        finally:
            if getattr(hook, RABBIT_OPERATOR_BRIDGE_ATTR, None) is self:
                delattr(hook, RABBIT_OPERATOR_BRIDGE_ATTR)

    BigQueryInsertJobOperator._submit_job = patched_submit_job


def patch_bigquery_hook() -> None:
    from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook, BigQueryJob

    if hasattr(BigQueryHook, RABBIT_HOOK_PATCHED_MARKER):
        return

    logging.info("Rabbit BQ Optimizer: patching BigQueryHook.insert_job")
    setattr(BigQueryHook, RABBIT_HOOK_PATCHED_MARKER, True)
    original_insert_job = BigQueryHook.insert_job

    def patched_insert_job(self, *, configuration: dict, **kwargs) -> BigQueryJob:
        config = _load_optimizer_config()
        if not config:
            return original_insert_job(self, configuration=configuration, **kwargs)

        if not _should_optimize_for_current_dag(config):
            return original_insert_job(self, configuration=configuration, **kwargs)

        operator = getattr(self, RABBIT_OPERATOR_BRIDGE_ATTR, None)
        outcome = _optimize(
            config=config,
            configuration=configuration,
            hook=self,
            project_id=kwargs.get("project_id"),
        )
        if not outcome:
            return original_insert_job(self, configuration=configuration, **kwargs)

        optimized, pool_billing_project = outcome
        _stamp_pool_routing_labels(
            optimized,
            pool_billing_project=pool_billing_project,
            operator_bridge_active=operator is not None,
        )
        submit_kwargs = dict(kwargs)
        original_operator_config = operator.configuration if operator is not None else None
        original_operator_project = operator.project_id if operator is not None else None
        if operator is not None:
            operator.configuration = optimized
            if pool_billing_project:
                operator.project_id = pool_billing_project
                submit_kwargs["project_id"] = pool_billing_project

        try:
            return original_insert_job(self, configuration=optimized, **submit_kwargs)
        except Exception as exc:
            logging.warning(
                "Rabbit BQ Optimizer: optimized submit failed: %s. Using original job.", exc
            )
            # Restore operator state so poll/defer track the source project, not the pool.
            if operator is not None:
                operator.configuration = original_operator_config
                operator.project_id = original_operator_project
            return original_insert_job(self, configuration=configuration, **kwargs)

    BigQueryHook.insert_job = patched_insert_job


LEGACY_PLUGIN_FILENAME = "rabbit_bq_optimizer_plugin.py"


def _warn_or_remove_legacy_plugin_file() -> None:
    """Drop the pre-PyPI copy-to-plugins deploy file when upgrading."""
    airflow_home = os.environ.get("AIRFLOW_HOME")
    if not airflow_home:
        return

    legacy = os.path.join(airflow_home, "plugins", LEGACY_PLUGIN_FILENAME)
    if not os.path.isfile(legacy):
        return

    msg = (
        "Rabbit BQ Optimizer: found legacy plugin file at %s. "
        "Remove it when using rabbit-bq-optimizer-airflow-plugin from PyPI "
        "to avoid duplicate plugin loading."
    )
    auto_remove = os.environ.get("RABBIT_BQ_OPTIMIZER_REMOVE_LEGACY_PLUGIN", "").lower() in (
        "1",
        "true",
        "yes",
    )
    if auto_remove:
        try:
            os.remove(legacy)
            logging.warning(
                "Rabbit BQ Optimizer: removed legacy plugin file %s "
                "(RABBIT_BQ_OPTIMIZER_REMOVE_LEGACY_PLUGIN enabled).",
                legacy,
            )
        except OSError as exc:
            logging.error(
                "Rabbit BQ Optimizer: could not remove legacy plugin file %s: %s",
                legacy,
                exc,
            )
    else:
        logging.error(
            "%s Set env RABBIT_BQ_OPTIMIZER_REMOVE_LEGACY_PLUGIN=true to remove automatically.",
            msg % legacy,
        )


class RabbitBQOptimizerPlugin(AirflowPlugin, LoggingMixin):
    name = "rabbit_bq_job_optimizer_plugin"

    def on_load(self, *args, **kwargs):
        _warn_or_remove_legacy_plugin_file()
        patch_bigquery_hook()
        patch_bigquery_insert_job_operator()
