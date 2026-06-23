"""
Rabbit BQ job optimizer — Airflow 2 plugin.

Patches ``BigQueryHook.insert_job`` so every BQ submit is optimized via the Rabbit API
before ``jobs.insert``. Patches ``BigQueryInsertJobOperator._submit_job`` with a
short-lived hook bridge so operator-driven submits can also route ``project_id`` to an
on-demand pool when the optimizer sets ``optimizedJob.jobReference.projectId``.
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
        config = Variable.get(OPTIMIZER_VARIABLE, deserialize_json=True)
        if not config:
            raise KeyError(f"{OPTIMIZER_VARIABLE} is empty")
    except (KeyError, ValueError) as exc:
        logging.warning("Rabbit BQ Optimizer: config error: %s. Using original job.", exc)
        return None

    for field in ("reservation_ids", "default_pricing_mode"):
        if field not in config:
            logging.warning("Rabbit BQ Optimizer: missing %s. Using original job.", field)
            return None

    if config["default_pricing_mode"] not in ("on_demand", "slot_based"):
        logging.warning("Rabbit BQ Optimizer: bad default_pricing_mode. Using original job.")
        return None

    if not config["reservation_ids"]:
        logging.warning("Rabbit BQ Optimizer: no reservation_ids. Using original job.")
        return None

    return config


def _optimize(
    *,
    configuration: dict[str, Any],
    hook,
    project_id: str | None,
) -> tuple[dict[str, Any], str | None] | None:
    """Returns (optimized_configuration, pool_billing_project) or None."""
    config = _load_optimizer_config()
    if not config:
        return None

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

    try:
        result = client.optimize_job(
            configuration={"configuration": configuration},
            enabledOptimizations=[
                OptimizationConfig(
                    type="reservation_assignment",
                    config={
                        "defaultPricingMode": config.get("default_pricing_mode"),
                        "reservationIds": config["reservation_ids"],
                    },
                )
            ],
        )
    except Exception as exc:
        logging.warning("Rabbit BQ Optimizer: optimize_job failed: %s. Using original job.", exc)
        return None

    logging.debug("Rabbit BQ Optimizer: optimization result=%s", result)

    optimized = dict(result.optimizedJob.get("configuration") or {})
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
        operator = getattr(self, RABBIT_OPERATOR_BRIDGE_ATTR, None)
        outcome = _optimize(
            configuration=configuration, hook=self, project_id=kwargs.get("project_id")
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
            return original_insert_job(self, configuration=configuration, **kwargs)

    BigQueryHook.insert_job = patched_insert_job


class RabbitBQOptimizerPlugin(AirflowPlugin, LoggingMixin):
    name = "rabbit_bq_job_optimizer_plugin"

    def on_load(self, *args, **kwargs):
        patch_bigquery_hook()
        patch_bigquery_insert_job_operator()
