"""
Rabbit BQ job optimizer — Airflow plugin (Airflow 2.9+ and Airflow 3).

Patches ``BigQueryHook.insert_job`` so every BQ submit is optimized via the Rabbit API
before ``jobs.insert``. Patches ``BigQueryInsertJobOperator._submit_job`` with a
short-lived hook bridge so operator-driven submits can also route ``project_id`` to an
on-demand pool when the optimizer sets ``optimizedJob.jobReference.projectId``.

The ``rabbit_bq_optimizer_config`` variable is read once per job submit at the hook
boundary; optimization runs only when ``enabled`` is explicitly ``true``. Otherwise the
original ``insert_job`` runs with no API call.
"""

from __future__ import annotations

import functools
import importlib.metadata
import json
import logging
import os
from dataclasses import asdict
from typing import Any

from airflow.exceptions import AirflowException
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.log.logging_mixin import LoggingMixin
from rabbit_bq_job_optimizer import OptimizationConfig, RabbitBQJobOptimizer

try:  # Airflow 3: the Task SDK is the supported import path
    from airflow.sdk import BaseHook, Variable, get_current_context
    from airflow.sdk.exceptions import TaskDeferred
except ImportError:  # Airflow 2 (and early Airflow 3.0, where the old paths still work)
    from airflow.exceptions import TaskDeferred
    from airflow.hooks.base import BaseHook
    from airflow.models import Variable
    from airflow.operators.python import get_current_context

RABBIT_HOOK_PATCHED_MARKER = "_rabbit_bq_job_optimizer_hook_patched"
RABBIT_OPERATOR_PATCHED_MARKER = "_rabbit_bq_job_optimizer_operator_patched"
RABBIT_OPERATOR_BRIDGE_ATTR = "_rabbit_operator"
SOURCE_PROJECT_LABEL = "rabbit-source-project"
POOL_PROJECT_LABEL = "rabbit-pool-project"
POOL_ROUTING_LABEL = "rabbit-pool-routing"
POOL_ROUTING_APPLIED = "applied"
POOL_ROUTING_SKIPPED = "skipped"
POOL_ROUTING_NONE = "none"
ROUTING_RETRY_LABEL = "rabbit-routing-retry"
ROUTING_RETRY_APPLIED = "applied"
# Operator attributes / deferral kwarg used by the routing retry (see _retry_on_original_config).
RABBIT_MOVED_ATTR = "_rabbit_moved_job"
RABBIT_NO_OPTIMIZE_ATTR = "_rabbit_no_optimize"
RABBIT_DEFER_KWARG = "rabbit_moved_job"
# BigQuery limits that only apply to on-demand jobs. A job Rabbit moved off a reservation can hit
# them although the original placement would not: the custom query-usage quotas
# (QueryUsagePerDay / QueryUsagePerUserPerDay) and the on-demand CPU-per-byte limit. Matched as
# lowercase substrings of the error text: a script parent reports the quota as `invalidQuery`, so
# the message is the only reliable signal there.
ON_DEMAND_LIMIT_MARKERS = ("custom quota exceeded", "billingtierlimitexceeded")
OPTIMIZER_VARIABLE = "rabbit_bq_optimizer_config"
RABBIT_API_CONN_ID = "rabbit_api"
RABBIT_API_BASE_URL_EXTRA_KEY = "api_base_url"

# Identifies this plugin (name + version) to the optimizer via the client library's x-rabbit-client
# header, so optimizer-side diagnostics can attribute traffic per client and version. The version
# comes from the installed package metadata (setup.py is the single source of truth); "unknown"
# covers the file being copied into the plugins folder without a pip install.
try:
    PLUGIN_VERSION = importlib.metadata.version("rabbit-bq-optimizer-airflow-plugin")
except importlib.metadata.PackageNotFoundError:
    PLUGIN_VERSION = "unknown"
RABBIT_CLIENT_INFO = f"rabbit-bq-optimizer-airflow-plugin/{PLUGIN_VERSION}"


def _as_bool(value: Any) -> bool:
    """Parse config booleans; accept JSON bools and common true/false strings."""
    if isinstance(value, str):
        return value.strip().lower() in ("true", "1", "yes", "on")
    return bool(value)


def _mask_secret(value: str | None) -> str:
    """Mask a secret for logs: first/last 3 chars only (or fully masked if short)."""
    if value is None:
        return "<unset>"
    if not value:
        return "<empty>"
    length = len(value)
    if length <= 6:
        masked = "*" * length
    else:
        masked = f"{value[:3]}...{value[-3:]}"
    return f"{masked} (len={length})"


def _failure_context(
    *,
    config: dict[str, Any] | None = None,
    api_key: str | None = None,
    base_url: str | None = None,
) -> str:
    """Compact, safe context for fail-open warnings (no full secrets)."""
    parts: list[str] = []
    if config is not None:
        # Prefer the Airflow Variable payload as loaded (not our normalized copy).
        raw = config.get("_raw", config)
        parts.append(f"config={json.dumps(raw, default=str)}")
    if api_key is not None:
        parts.append(f"rabbit_api_key={_mask_secret(api_key)}")
        parts.append(f"api_base_url={base_url!r}")
    return " ".join(parts)


def _log_failure(
    message: str,
    exc: BaseException,
    *,
    debug: bool,
    config: dict[str, Any] | None = None,
    api_key: str | None = None,
    base_url: str | None = None,
) -> None:
    """Log a fail-open warning; include traceback when debug is on."""
    context = _failure_context(config=config, api_key=api_key, base_url=base_url)
    if context:
        message = f"{message} {context}"
    logging.warning(message, exc, exc_info=debug)


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
    if not api_key.isascii():
        # Non-ASCII keys blow up later with an opaque UnicodeEncodeError when the key is
        # sent as an HTTP header; fail fast with an actionable message instead.
        raise RuntimeError(
            f"Airflow connection '{RABBIT_API_CONN_ID}' password contains non-ASCII "
            f"characters ({_mask_secret(api_key)}) — this is not a valid Rabbit API key. "
            "Re-paste the API key into the connection's password field"
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

        if not _as_bool(raw["enabled"]):
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
        # Statement-level routing is opt-in. The plugin submits the server-rewritten query verbatim,
        # so enabling it here is safe; it also requires the tenant's
        # bq_dynamic_pricing_statement_level feature flag.
        config["statement_level"] = _as_bool(config.get("statement_level"))
        config["debug"] = _as_bool(config.get("debug"))
        # Keep the Variable payload as loaded for fail-open diagnostics.
        config["_raw"] = raw
        return config
    except Exception as exc:
        # Missing variable (KeyError on Airflow 2, AirflowRuntimeError on Airflow 3), bad JSON,
        # or failed validation above.
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
    debug = config["debug"]
    try:
        credentials = _load_rabbit_credentials()
    except Exception as exc:
        _log_failure(
            "Rabbit BQ Optimizer: failed to load Rabbit API connection "
            f"'{RABBIT_API_CONN_ID}': %s. Using original job.",
            exc,
            debug=debug,
            config=config,
        )
        return None

    source_project = _resolve_source_project(project_id=project_id, hook=hook)
    client_kwargs = {"api_key": credentials["api_key"], "client_info": RABBIT_CLIENT_INFO}
    if credentials["base_url"]:
        client_kwargs["base_url"] = credentials["base_url"]
    try:
        client = RabbitBQJobOptimizer(**client_kwargs)
    except TypeError:
        # Older client library without client_info support — keep working without the identity
        # header.
        client = RabbitBQJobOptimizer(
            **{k: v for k, v in client_kwargs.items() if k != "client_info"}
        )

    optimize_kwargs: dict[str, Any] = {
        "configuration": {"configuration": configuration},
        "enabledOptimizations": [
            OptimizationConfig(
                type="reservation_assignment",
                config={
                    "defaultPricingMode": config.get("default_pricing_mode"),
                    "reservationIds": config["reservation_ids"],
                    **({"statementLevel": True} if config.get("statement_level") else {}),
                },
            )
        ],
    }
    if source_project:
        optimize_kwargs["project_id"] = source_project

    try:
        result = client.optimize_job(**optimize_kwargs)
    except Exception as exc:
        _log_failure(
            "Rabbit BQ Optimizer: optimize_job failed: %s. Using original job.",
            exc,
            debug=debug,
            config=config,
            api_key=credentials["api_key"],
            base_url=credentials.get("base_url"),
        )
        return None

    if debug:
        logging.info(
            "Rabbit BQ Optimizer: optimization result=%s",
            json.dumps(asdict(result), default=str),
        )
    else:
        logging.debug("Rabbit BQ Optimizer: optimization result=%s", result)

    optimized = dict((result.optimizedJob or {}).get("configuration") or {})
    if source_project:
        labels = dict(optimized.get("labels") or {})
        labels.setdefault(SOURCE_PROJECT_LABEL, source_project)
        optimized["labels"] = labels

    job_ref = (result.optimizedJob or {}).get("jobReference") or {}
    pool_billing_project = job_ref.get("projectId")
    return optimized, pool_billing_project


def _optimized_query_was_rewritten(original: dict[str, Any], optimized: dict[str, Any]) -> bool:
    """True when the optimizer changed the query TEXT (statement-level routing injects per-statement
    ``SET @@reservation``). A whole-job reservation change leaves the query text untouched, so there
    is nothing to validate in that case."""
    new_sql = ((optimized or {}).get("query") or {}).get("query")
    old_sql = ((original or {}).get("query") or {}).get("query")
    return isinstance(new_sql, str) and new_sql != old_sql


def _rewritten_query_dry_run_ok(
    hook, optimized: dict[str, Any], source_project: str | None
) -> bool:
    """Validate a server-rewritten query with the CUSTOMER's own credentials before submitting it.

    The optimizer rewrites SQL for statement-level routing but cannot dry-run against the customer's
    tables. We do it here — the client holds the full query text (no INFORMATION_SCHEMA truncation)
    and the customer's BigQuery credentials — via a dry-run. On any failure we return False so the
    caller submits the ORIGINAL query, which is exactly the un-optimized baseline: worst case is a
    missed optimization, never a broken job.
    """
    try:
        from google.cloud import bigquery

        sql = (optimized.get("query") or {}).get("query")
        project = source_project or getattr(hook, "project_id", None)
        client = hook.get_client(project_id=project)
        client.query(
            sql,
            project=project,
            job_config=bigquery.QueryJobConfig(dry_run=True, use_query_cache=False),
        )
        return True
    except Exception as exc:
        logging.warning(
            "Rabbit BQ Optimizer: dry-run of the rewritten query failed (%s); submitting the "
            "original job.",
            exc,
        )
        return False


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


def _placement_moved(
    original: dict[str, Any], optimized: dict[str, Any], pool_billing_project: str | None
) -> bool:
    """True when the optimizer moved the job towards on-demand: a pool billing project, a whole-job
    ``reservation: none``, or a statement-level rewrite (per-statement ``SET @@reservation``)."""
    reservation = optimized.get("reservation")
    return (
        bool(pool_billing_project)
        or (str(reservation).lower() == "none" and reservation != original.get("reservation"))
        or _optimized_query_was_rewritten(original, optimized)
    )


def _is_on_demand_limit_error(*parts: Any) -> bool:
    """True when the error text names a limit that only applies to on-demand jobs."""
    text = " ".join(str(part) for part in parts if part).lower()
    return any(marker in text for marker in ON_DEMAND_LIMIT_MARKERS)


def _failed_job_errors(operator, moved_job: dict[str, Any]) -> Any:
    """Error details of the failed job. The deferrable trigger event carries only the message,
    and ``billingTierLimitExceeded`` is identifiable only by its reason."""
    try:
        from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

        hook = BigQueryHook(
            gcp_conn_id=operator.gcp_conn_id, impersonation_chain=operator.impersonation_chain
        )
        job = hook.get_job(
            project_id=moved_job.get("project_id"),
            location=moved_job.get("location"),
            job_id=moved_job.get("job_id"),
        )
        return [job.error_result, job.errors]
    except Exception as exc:
        logging.warning(
            "Rabbit BQ Optimizer: could not read the failed job %s: %s",
            moved_job.get("job_id"),
            exc,
        )
        return None


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

    # Routing retry. The operator submits with nowait=True and waits for the job itself, so a job
    # that is accepted and then fails never reaches the fail-open in patched_insert_job. When a
    # job Rabbit moved fails on an on-demand-only limit, run the task once more on the original
    # configuration, where the job would have run without Rabbit.
    original_execute = BigQueryInsertJobOperator.execute
    original_execute_complete = BigQueryInsertJobOperator.execute_complete
    # Airflow wraps execute in ExecutorSafeguard, which expects a token only TaskInstance passes
    # (and only to a callable named "execute"). The first run forwards that token; the retry is
    # a continuation of the same task run, so it calls the provider's execute directly.
    unguarded_execute = getattr(original_execute, "__wrapped__", original_execute)

    def _retry_on_original_config(self, context, error):
        """Re-run on the operator's original config, unoptimized. Runs at most once per task try:
        the retry clears RABBIT_MOVED_ATTR and attaches no deferral kwarg, so a second failure
        surfaces as usual."""
        logging.warning(
            "Rabbit BQ Optimizer: re-routed job failed on an on-demand limit (%s). "
            "Retrying once with the original configuration.",
            error,
        )
        self.configuration = {
            **self.configuration,
            "labels": {
                **(self.configuration.get("labels") or {}),
                ROUTING_RETRY_LABEL: ROUTING_RETRY_APPLIED,
            },
        }
        setattr(self, RABBIT_MOVED_ATTR, False)
        setattr(self, RABBIT_NO_OPTIMIZE_ATTR, True)
        try:
            return unguarded_execute(self, context)
        finally:
            setattr(self, RABBIT_NO_OPTIMIZE_ATTR, False)

    @functools.wraps(original_execute)  # keep the name "execute": TaskInstance checks it
    def patched_execute(self, context, **kwargs):
        original_state = (self.configuration, self.project_id, self.job_id)
        setattr(self, RABBIT_MOVED_ATTR, False)
        try:
            return original_execute(self, context, **kwargs)
        except TaskDeferred as deferred:
            # A subclass with its own execute_complete may not accept the extra kwarg; leave
            # its deferral untouched (no routing retry on that path) rather than break resume.
            resumes_here = type(self).execute_complete is patched_execute_complete
            if resumes_here and getattr(self, RABBIT_MOVED_ATTR, False):
                # execute_complete runs later (normally on a fresh operator instance) and its
                # failure event carries only a message: hand it the moved job's coordinates.
                deferred.kwargs = {
                    **(deferred.kwargs or {}),
                    RABBIT_DEFER_KWARG: {
                        "project_id": self.project_id,
                        "location": deferred.trigger.location,
                        "job_id": self.job_id,
                    },
                }
                # Leave the operator as a fresh instance would be, for runners that resume on
                # the same object (dag.test()).
                self.configuration, self.project_id, self.job_id = original_state
                setattr(self, RABBIT_MOVED_ATTR, False)
            raise
        except Exception as exc:
            if not (
                getattr(self, RABBIT_MOVED_ATTR, False)
                and _is_on_demand_limit_error(exc, getattr(exc, "errors", None))
            ):
                raise
            self.configuration, self.project_id, self.job_id = original_state
            return _retry_on_original_config(self, context, exc)

    def patched_execute_complete(self, context, event, **kwargs):
        moved_job = kwargs.pop(RABBIT_DEFER_KWARG, None)
        if (
            moved_job
            and event.get("status") == "error"
            and (
                _is_on_demand_limit_error(event.get("message"))
                or _is_on_demand_limit_error(_failed_job_errors(self, moved_job))
            )
        ):
            return _retry_on_original_config(self, context, event.get("message"))
        return original_execute_complete(self, context, event, **kwargs)

    BigQueryInsertJobOperator.execute = patched_execute
    BigQueryInsertJobOperator.execute_complete = patched_execute_complete


def patch_bigquery_hook() -> None:
    from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook, BigQueryJob

    if hasattr(BigQueryHook, RABBIT_HOOK_PATCHED_MARKER):
        return

    logging.info("Rabbit BQ Optimizer: patching BigQueryHook.insert_job")
    setattr(BigQueryHook, RABBIT_HOOK_PATCHED_MARKER, True)
    original_insert_job = BigQueryHook.insert_job

    def patched_insert_job(self, *, configuration: dict, **kwargs) -> BigQueryJob:
        operator = getattr(self, RABBIT_OPERATOR_BRIDGE_ATTR, None)
        if operator is not None and getattr(operator, RABBIT_NO_OPTIMIZE_ATTR, False):
            # Routing retry: submit the original job as-is.
            return original_insert_job(self, configuration=configuration, **kwargs)

        config = _load_optimizer_config()
        if not config:
            return original_insert_job(self, configuration=configuration, **kwargs)

        if not _should_optimize_for_current_dag(config):
            return original_insert_job(self, configuration=configuration, **kwargs)

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

        # If the optimizer rewrote the query text (statement-level routing), dry-run the rewrite
        # with the customer's own credentials before submitting. On failure, fall back to the
        # original job here — before any operator state is mutated below.
        if _optimized_query_was_rewritten(configuration, optimized):
            source_project = _resolve_source_project(project_id=kwargs.get("project_id"), hook=self)
            if not _rewritten_query_dry_run_ok(self, optimized, source_project):
                return original_insert_job(self, configuration=configuration, **kwargs)

        submit_kwargs = dict(kwargs)
        original_operator_config = operator.configuration if operator is not None else None
        original_operator_project = operator.project_id if operator is not None else None
        if operator is not None:
            operator.configuration = optimized
            if pool_billing_project:
                operator.project_id = pool_billing_project
                submit_kwargs["project_id"] = pool_billing_project
            setattr(
                operator,
                RABBIT_MOVED_ATTR,
                _placement_moved(configuration, optimized, pool_billing_project),
            )

        try:
            return original_insert_job(self, configuration=optimized, **submit_kwargs)
        except Exception as exc:
            _log_failure(
                "Rabbit BQ Optimizer: optimized submit failed: %s. Using original job.",
                exc,
                debug=config["debug"],
                config=config,
            )
            # Restore operator state so poll/defer track the source project, not the pool.
            if operator is not None:
                operator.configuration = original_operator_config
                operator.project_id = original_operator_project
                setattr(operator, RABBIT_MOVED_ATTR, False)
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
