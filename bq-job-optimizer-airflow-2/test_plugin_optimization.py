#!/usr/bin/env python3
"""Unit tests for plugin optimization: pool routing, fail-open, hook bridge, routing retry."""

from __future__ import annotations

import os
import unittest
from contextlib import contextmanager
from datetime import datetime
from unittest.mock import MagicMock, patch

script_dir = os.path.dirname(os.path.abspath(__file__))
repo_root = os.path.dirname(script_dir)
os.environ.setdefault("AIRFLOW_HOME", os.path.join(repo_root, "airflow_home"))

from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook  # noqa: E402
from airflow.providers.google.cloud.operators.bigquery import (  # noqa: E402
    BigQueryInsertJobOperator,
)

# The provider's own methods, captured before any test patches them.
_PROVIDER_ORIGINALS = {
    (cls.__name__, name): getattr(cls, name)
    for cls, names in (
        (BigQueryInsertJobOperator, ("execute", "execute_complete", "_submit_job")),
        (BigQueryHook, ("insert_job", "get_job")),
    )
    for name in names
}

# Generic example GCP project IDs — no customer-specific names in public tests.
SOURCE_PROJECT = "example-analytics-prod"
POOL_BILLING_PROJECT = "example-bq-pool-us-1"


class TestPluginOptimization(unittest.TestCase):
    def setUp(self):
        from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
        from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
        from rabbit_bq_optimizer_plugin import (
            RABBIT_HOOK_PATCHED_MARKER,
            RABBIT_OPERATOR_PATCHED_MARKER,
        )

        for cls, marker in (
            (BigQueryHook, RABBIT_HOOK_PATCHED_MARKER),
            (BigQueryInsertJobOperator, RABBIT_OPERATOR_PATCHED_MARKER),
        ):
            if hasattr(cls, marker):
                delattr(cls, marker)

    @staticmethod
    def _execute_context(op):
        return {
            "logical_date": datetime(2026, 1, 1),
            # providers>=20 read context["task"] in BigQueryJobDetailLink.persist
            "task": op,
            # try_number must be a real int; submit_job compares it (> 2)
            "ti": MagicMock(try_number=1),
        }

    def _mock_optimizer_response(self, *, pool_project: str | None):
        from rabbit_bq_job_optimizer.models import OptimizationResponse

        optimized_job = {"configuration": {"query": {"query": "SELECT 1", "useLegacySql": False}}}
        if pool_project:
            optimized_job["jobReference"] = {"projectId": pool_project}
        return OptimizationResponse(
            optimizedJob=optimized_job,
            optimizationResults=[],
            estimatedSavings=0.0,
            optimizationPerformed=True,
        )

    @contextmanager
    def _patch_plugin(self, plugin_module, *, mock_client):
        from airflow.models import Variable

        with (
            patch.object(
                Variable,
                "get",
                return_value={
                    "enabled": True,
                    "default_pricing_mode": "on_demand",
                    "reservation_ids": ["p:US.r"],
                },
            ),
            patch.object(
                plugin_module,
                "_load_rabbit_credentials",
                return_value={"api_key": "k", "base_url": None},
            ),
            patch.object(plugin_module, "RabbitBQJobOptimizer", return_value=mock_client),
        ):
            yield

    def test_direct_hook_insert_keeps_source_project_id(self):
        import importlib

        import rabbit_bq_optimizer_plugin as plugin_module
        from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

        importlib.reload(plugin_module)

        submitted: dict = {}

        def fake_bq_submit(self, *, configuration, **kwargs):
            submitted["configuration"] = configuration
            submitted["kwargs"] = dict(kwargs)
            return MagicMock(job_id="mock-job-1")

        BigQueryHook.insert_job = fake_bq_submit

        mock_client = MagicMock()
        mock_client.optimize_job.return_value = self._mock_optimizer_response(
            pool_project=POOL_BILLING_PROJECT
        )

        with self._patch_plugin(plugin_module, mock_client=mock_client):
            plugin_module.patch_bigquery_hook()
            plugin_module.patch_bigquery_insert_job_operator()

            hook = BigQueryHook(gcp_conn_id="google_cloud_default")
            hook.insert_job(
                configuration={"query": {"query": "SELECT 1", "useLegacySql": False}},
                project_id=SOURCE_PROJECT,
                nowait=True,
            )

        self.assertEqual(submitted["kwargs"]["project_id"], SOURCE_PROJECT)
        self.assertEqual(
            submitted["configuration"].get("labels", {}).get("rabbit-source-project"),
            SOURCE_PROJECT,
        )
        labels = submitted["configuration"].get("labels", {})
        self.assertEqual(labels.get("rabbit-pool-project"), POOL_BILLING_PROJECT)
        self.assertEqual(labels.get("rabbit-pool-routing"), "skipped")
        payload = mock_client.optimize_job.call_args.kwargs
        self.assertEqual(payload.get("project_id"), SOURCE_PROJECT)

    def test_insert_job_operator_routes_billing_project(self):
        import importlib

        import rabbit_bq_optimizer_plugin as plugin_module
        from airflow import DAG
        from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
        from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

        importlib.reload(plugin_module)

        submitted: dict = {}

        def fake_bq_submit(self, *, configuration, **kwargs):
            submitted["configuration"] = configuration
            submitted["kwargs"] = dict(kwargs)
            submitted["bridge_active"] = getattr(self, "_rabbit_operator", None) is not None
            job = MagicMock()
            job.job_id = kwargs.get("job_id") or "mock-op-job-1"
            job.running.return_value = False
            job.state = "DONE"
            job.error_result = None
            job.errors = []
            job.result.return_value = None
            job.to_api_repr.return_value = {"configuration": {}}
            return job

        BigQueryHook.insert_job = fake_bq_submit

        mock_client = MagicMock()
        mock_client.optimize_job.return_value = self._mock_optimizer_response(
            pool_project=POOL_BILLING_PROJECT
        )

        dag = DAG("plugin_optimization_test", start_date=datetime(2026, 1, 1), schedule=None)
        op = BigQueryInsertJobOperator(
            task_id="run_query",
            configuration={"query": {"query": "SELECT 1", "useLegacySql": False}},
            project_id=SOURCE_PROJECT,
            location="US",
            deferrable=False,
            dag=dag,
        )

        with self._patch_plugin(plugin_module, mock_client=mock_client):
            plugin_module.patch_bigquery_hook()
            plugin_module.patch_bigquery_insert_job_operator()
            op.execute(context=self._execute_context(op))

        self.assertTrue(submitted.get("bridge_active"))
        self.assertEqual(op.project_id, POOL_BILLING_PROJECT)
        self.assertEqual(submitted["kwargs"]["project_id"], POOL_BILLING_PROJECT)
        self.assertEqual(
            submitted["configuration"].get("labels", {}).get("rabbit-source-project"),
            SOURCE_PROJECT,
        )
        labels = submitted["configuration"].get("labels", {})
        self.assertEqual(labels.get("rabbit-pool-project"), POOL_BILLING_PROJECT)
        self.assertEqual(labels.get("rabbit-pool-routing"), "applied")
        self.assertEqual(
            mock_client.optimize_job.call_args.kwargs.get("project_id"), SOURCE_PROJECT
        )

    def test_operator_failopen_restores_source_project(self):
        """If the pool submit fails, fail-open must reset operator.project_id to the source."""
        import importlib

        import rabbit_bq_optimizer_plugin as plugin_module
        from airflow import DAG
        from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
        from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

        importlib.reload(plugin_module)

        submitted: dict = {}
        calls = {"n": 0}

        def fake_bq_submit(self, *, configuration, **kwargs):
            calls["n"] += 1
            if calls["n"] == 1:
                # First (optimized/pool) submit fails → plugin must fail open.
                raise RuntimeError("pool submit denied")
            submitted["configuration"] = configuration
            submitted["kwargs"] = dict(kwargs)
            job = MagicMock()
            job.job_id = "mock-op-job-failopen"
            job.running.return_value = False
            job.state = "DONE"
            job.error_result = None
            job.errors = []
            job.result.return_value = None
            job.to_api_repr.return_value = {"configuration": {}}
            return job

        BigQueryHook.insert_job = fake_bq_submit

        mock_client = MagicMock()
        mock_client.optimize_job.return_value = self._mock_optimizer_response(
            pool_project=POOL_BILLING_PROJECT
        )

        original_config = {"query": {"query": "SELECT 1", "useLegacySql": False}}
        dag = DAG("plugin_optimization_failopen", start_date=datetime(2026, 1, 1), schedule=None)
        op = BigQueryInsertJobOperator(
            task_id="run_query",
            configuration=dict(original_config),
            project_id=SOURCE_PROJECT,
            location="US",
            deferrable=False,
            dag=dag,
        )

        with self._patch_plugin(plugin_module, mock_client=mock_client):
            plugin_module.patch_bigquery_hook()
            plugin_module.patch_bigquery_insert_job_operator()
            op.execute(context=self._execute_context(op))

        # Job actually landed on the source project; operator must agree so poll/defer match.
        self.assertEqual(submitted["kwargs"]["project_id"], SOURCE_PROJECT)
        self.assertEqual(op.project_id, SOURCE_PROJECT)
        # Fail-open must drop the optimized config (and its rabbit-pool routing labels).
        submitted_labels = submitted["configuration"].get("labels", {})
        self.assertFalse([k for k in submitted_labels if k.startswith("rabbit-")])
        op_labels = (op.configuration or {}).get("labels", {})
        self.assertFalse([k for k in op_labels if k.startswith("rabbit-")])

    def test_operator_without_job_reference_keeps_source_project(self):
        """Reservation-only optimization must not rewrite operator.project_id."""
        import importlib

        import rabbit_bq_optimizer_plugin as plugin_module
        from airflow import DAG
        from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
        from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

        importlib.reload(plugin_module)

        submitted: dict = {}

        def fake_bq_submit(self, *, configuration, **kwargs):
            submitted["configuration"] = configuration
            submitted["kwargs"] = dict(kwargs)
            job = MagicMock()
            job.job_id = "mock-op-job-2"
            job.running.return_value = False
            job.state = "DONE"
            job.error_result = None
            job.errors = []
            job.result.return_value = None
            job.to_api_repr.return_value = {"configuration": {}}
            return job

        BigQueryHook.insert_job = fake_bq_submit

        mock_client = MagicMock()
        mock_client.optimize_job.return_value = self._mock_optimizer_response(pool_project=None)

        dag = DAG("plugin_optimization_no_ref", start_date=datetime(2026, 1, 1), schedule=None)
        op = BigQueryInsertJobOperator(
            task_id="run_query",
            configuration={"query": {"query": "SELECT 1", "useLegacySql": False}},
            project_id=SOURCE_PROJECT,
            location="US",
            deferrable=False,
            dag=dag,
        )

        with self._patch_plugin(plugin_module, mock_client=mock_client):
            plugin_module.patch_bigquery_hook()
            plugin_module.patch_bigquery_insert_job_operator()
            op.execute(context=self._execute_context(op))

        self.assertEqual(op.project_id, SOURCE_PROJECT)
        self.assertEqual(submitted["kwargs"]["project_id"], SOURCE_PROJECT)
        labels = submitted["configuration"].get("labels", {})
        self.assertEqual(labels.get("rabbit-pool-routing"), "none")
        self.assertNotIn("rabbit-pool-project", labels)
        mock_client.optimize_job.assert_called_once()

    def test_on_demand_missing_reservation_ids_skips_optimizer(self):
        import importlib

        import rabbit_bq_optimizer_plugin as plugin_module
        from airflow.models import Variable
        from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

        importlib.reload(plugin_module)

        original_config = {"query": {"query": "SELECT 1", "useLegacySql": False}}
        submitted: dict = {}

        def fake_bq_submit(self, *, configuration, **kwargs):
            submitted["configuration"] = configuration
            return MagicMock(job_id="mock-job-empty-res")

        BigQueryHook.insert_job = fake_bq_submit

        mock_client = MagicMock()

        with (
            patch.object(
                Variable,
                "get",
                return_value={
                    "enabled": True,
                    "default_pricing_mode": "on_demand",
                },
            ),
            patch.object(
                plugin_module,
                "_load_rabbit_credentials",
                return_value={"api_key": "k", "base_url": None},
            ),
            patch.object(plugin_module, "RabbitBQJobOptimizer", return_value=mock_client),
        ):
            plugin_module.patch_bigquery_hook()

            hook = BigQueryHook(gcp_conn_id="google_cloud_default")
            hook.insert_job(
                configuration=dict(original_config),
                project_id=SOURCE_PROJECT,
            )

        mock_client.optimize_job.assert_not_called()
        self.assertEqual(submitted["configuration"], original_config)

    def test_slot_based_missing_reservation_ids_calls_optimizer_with_empty_list(self):
        import importlib

        import rabbit_bq_optimizer_plugin as plugin_module
        from airflow.models import Variable
        from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

        importlib.reload(plugin_module)

        def fake_bq_submit(self, *, configuration, **kwargs):
            return MagicMock(job_id="mock-job-slot-empty-res")

        BigQueryHook.insert_job = fake_bq_submit

        mock_client = MagicMock()
        mock_client.optimize_job.return_value = self._mock_optimizer_response(pool_project=None)

        with (
            patch.object(
                Variable,
                "get",
                return_value={"enabled": True, "default_pricing_mode": "slot_based"},
            ),
            patch.object(
                plugin_module,
                "_load_rabbit_credentials",
                return_value={"api_key": "k", "base_url": None},
            ),
            patch.object(plugin_module, "RabbitBQJobOptimizer", return_value=mock_client),
        ):
            plugin_module.patch_bigquery_hook()

            hook = BigQueryHook(gcp_conn_id="google_cloud_default")
            hook.insert_job(
                configuration={"query": {"query": "SELECT 1", "useLegacySql": False}},
                project_id=SOURCE_PROJECT,
            )

        mock_client.optimize_job.assert_called_once()
        optimization_config = mock_client.optimize_job.call_args.kwargs["enabledOptimizations"][0]
        self.assertEqual(optimization_config.config["reservationIds"], [])

    def test_statement_level_flag_forwarded_to_optimizer(self):
        import importlib

        import rabbit_bq_optimizer_plugin as plugin_module
        from airflow.models import Variable
        from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

        importlib.reload(plugin_module)

        def fake_bq_submit(self, *, configuration, **kwargs):
            return MagicMock(job_id="mock-job-stmt-level")

        BigQueryHook.insert_job = fake_bq_submit

        mock_client = MagicMock()
        mock_client.optimize_job.return_value = self._mock_optimizer_response(pool_project=None)

        with (
            patch.object(
                Variable,
                "get",
                return_value={
                    "enabled": True,
                    "default_pricing_mode": "slot_based",
                    "statement_level": True,
                },
            ),
            patch.object(
                plugin_module,
                "_load_rabbit_credentials",
                return_value={"api_key": "k", "base_url": None},
            ),
            patch.object(plugin_module, "RabbitBQJobOptimizer", return_value=mock_client),
        ):
            plugin_module.patch_bigquery_hook()

            hook = BigQueryHook(gcp_conn_id="google_cloud_default")
            hook.insert_job(
                configuration={"query": {"query": "SELECT 1; SELECT 2", "useLegacySql": False}},
                project_id=SOURCE_PROJECT,
            )

        optimization_config = mock_client.optimize_job.call_args.kwargs["enabledOptimizations"][0]
        self.assertTrue(optimization_config.config.get("statementLevel"))

    def test_statement_level_absent_when_not_configured(self):
        import importlib

        import rabbit_bq_optimizer_plugin as plugin_module
        from airflow.models import Variable
        from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

        importlib.reload(plugin_module)

        def fake_bq_submit(self, *, configuration, **kwargs):
            return MagicMock(job_id="mock-job-no-stmt-level")

        BigQueryHook.insert_job = fake_bq_submit

        mock_client = MagicMock()
        mock_client.optimize_job.return_value = self._mock_optimizer_response(pool_project=None)

        with (
            patch.object(
                Variable,
                "get",
                return_value={"enabled": True, "default_pricing_mode": "slot_based"},
            ),
            patch.object(
                plugin_module,
                "_load_rabbit_credentials",
                return_value={"api_key": "k", "base_url": None},
            ),
            patch.object(plugin_module, "RabbitBQJobOptimizer", return_value=mock_client),
        ):
            plugin_module.patch_bigquery_hook()

            hook = BigQueryHook(gcp_conn_id="google_cloud_default")
            hook.insert_job(
                configuration={"query": {"query": "SELECT 1", "useLegacySql": False}},
                project_id=SOURCE_PROJECT,
            )

        optimization_config = mock_client.optimize_job.call_args.kwargs["enabledOptimizations"][0]
        self.assertNotIn("statementLevel", optimization_config.config)

    def test_disabled_via_enabled_false_skips_optimizer(self):
        import importlib

        import rabbit_bq_optimizer_plugin as plugin_module
        from airflow.models import Variable
        from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

        importlib.reload(plugin_module)

        original_config = {"query": {"query": "SELECT 1", "useLegacySql": False}}
        submitted: dict = {}

        def fake_bq_submit(self, *, configuration, **kwargs):
            submitted["configuration"] = configuration
            return MagicMock(job_id="mock-job-disabled")

        BigQueryHook.insert_job = fake_bq_submit

        mock_client = MagicMock()

        with (
            patch.object(
                Variable,
                "get",
                return_value={
                    "enabled": False,
                    "default_pricing_mode": "on_demand",
                    "reservation_ids": ["p:US.r"],
                },
            ),
            patch.object(
                plugin_module,
                "_load_rabbit_credentials",
                return_value={"api_key": "k", "base_url": None},
            ),
            patch.object(plugin_module, "RabbitBQJobOptimizer", return_value=mock_client),
        ):
            plugin_module.patch_bigquery_hook()

            hook = BigQueryHook(gcp_conn_id="google_cloud_default")
            hook.insert_job(configuration=dict(original_config), project_id=SOURCE_PROJECT)

        mock_client.optimize_job.assert_not_called()
        self.assertEqual(submitted["configuration"], original_config)

    def test_missing_variable_skips_optimizer(self):
        import importlib

        import rabbit_bq_optimizer_plugin as plugin_module
        from airflow.models import Variable
        from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

        importlib.reload(plugin_module)

        original_config = {"query": {"query": "SELECT 1", "useLegacySql": False}}
        submitted: dict = {}

        def fake_bq_submit(self, *, configuration, **kwargs):
            submitted["configuration"] = configuration
            return MagicMock(job_id="mock-job-missing-var")

        BigQueryHook.insert_job = fake_bq_submit

        mock_client = MagicMock()

        with (
            patch.object(
                Variable,
                "get",
                side_effect=KeyError("Variable rabbit_bq_optimizer_config does not exist"),
            ),
            patch.object(plugin_module, "RabbitBQJobOptimizer", return_value=mock_client),
        ):
            plugin_module.patch_bigquery_hook()

            hook = BigQueryHook(gcp_conn_id="google_cloud_default")
            hook.insert_job(configuration=dict(original_config), project_id=SOURCE_PROJECT)

        mock_client.optimize_job.assert_not_called()
        self.assertEqual(submitted["configuration"], original_config)

    def test_missing_enabled_field_skips_optimizer(self):
        import importlib

        import rabbit_bq_optimizer_plugin as plugin_module
        from airflow.models import Variable
        from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

        importlib.reload(plugin_module)

        original_config = {"query": {"query": "SELECT 1", "useLegacySql": False}}
        submitted: dict = {}

        def fake_bq_submit(self, *, configuration, **kwargs):
            submitted["configuration"] = configuration
            return MagicMock(job_id="mock-job-no-enabled")

        BigQueryHook.insert_job = fake_bq_submit

        mock_client = MagicMock()

        with (
            patch.object(
                Variable,
                "get",
                return_value={
                    "default_pricing_mode": "on_demand",
                    "reservation_ids": ["p:US.r"],
                },
            ),
            patch.object(plugin_module, "RabbitBQJobOptimizer", return_value=mock_client),
        ):
            plugin_module.patch_bigquery_hook()

            hook = BigQueryHook(gcp_conn_id="google_cloud_default")
            hook.insert_job(configuration=dict(original_config), project_id=SOURCE_PROJECT)

        mock_client.optimize_job.assert_not_called()
        self.assertEqual(submitted["configuration"], original_config)

    def test_optimize_failure_fails_open(self):
        import importlib

        import rabbit_bq_optimizer_plugin as plugin_module
        from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

        importlib.reload(plugin_module)

        original_config = {"query": {"query": "SELECT 1", "useLegacySql": False}}
        submitted: dict = {}

        def fake_bq_submit(self, *, configuration, **kwargs):
            submitted["configuration"] = configuration
            return MagicMock(job_id="mock-job-failopen")

        BigQueryHook.insert_job = fake_bq_submit

        mock_client = MagicMock()
        mock_client.optimize_job.side_effect = RuntimeError("optimizer down")

        with self._patch_plugin(plugin_module, mock_client=mock_client):
            plugin_module.patch_bigquery_hook()

            hook = BigQueryHook(gcp_conn_id="google_cloud_default")
            hook.insert_job(configuration=original_config, project_id=SOURCE_PROJECT)

        self.assertEqual(submitted["configuration"], original_config)
        mock_client.optimize_job.assert_called_once()


QUOTA_MESSAGE = (
    "Custom quota exceeded: Your usage exceeded the custom quota for QueryUsagePerDay, "
    "which is set by your administrator. at [40:1]"
)
QUOTA_ERROR = {"reason": "invalidQuery", "message": QUOTA_MESSAGE}  # as a script parent reports it
BILLING_TIER_ERROR = {
    "reason": "billingTierLimitExceeded",
    "message": "Query exceeded resource limits. 2147.3 CPU seconds were used.",
}
SYNTAX_ERROR = {"reason": "invalidQuery", "message": "Syntax error: Unexpected identifier"}
ORIGINAL_CONFIG = {"query": {"query": "SELECT 1", "useLegacySql": False}}


class TestRoutingRetry(unittest.TestCase):
    """A job Rabbit moved that fails on an on-demand-only limit is retried once, unoptimized."""

    def setUp(self):
        import importlib

        import rabbit_bq_optimizer_plugin as plugin_module
        from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
        from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

        self.plugin = importlib.reload(plugin_module)
        self.hook_cls, self.op_cls = BigQueryHook, BigQueryInsertJobOperator
        # Start every test from the provider's own methods so the patches never stack.
        for cls, names in (
            (BigQueryInsertJobOperator, ("execute", "execute_complete", "_submit_job")),
            (BigQueryHook, ("insert_job", "get_job")),
        ):
            for name in names:
                setattr(cls, name, _PROVIDER_ORIGINALS[cls.__name__, name])
        for cls, marker in (
            (BigQueryHook, self.plugin.RABBIT_HOOK_PATCHED_MARKER),
            (BigQueryInsertJobOperator, self.plugin.RABBIT_OPERATOR_PATCHED_MARKER),
        ):
            if hasattr(cls, marker):
                delattr(cls, marker)
        self.addCleanup(self._restore)
        self.submits: list[dict] = []

    def _restore(self):
        for cls in (self.hook_cls, self.op_cls):
            for (cls_name, name), original in _PROVIDER_ORIGINALS.items():
                if cls_name == cls.__name__:
                    setattr(cls, name, original)

    def _run(self, fn, *, errors, pool_project=None, reservation="none", running=False):
        """Run ``fn`` with the plugin patched in. ``errors[i]`` is the error_result of the i-th
        submitted job (None = success). Jobs fail the way BigQuery does: ``result()`` raises and
        ``error_result`` is set."""
        from google.api_core.exceptions import BadRequest
        from rabbit_bq_job_optimizer.models import OptimizationResponse

        def fake_bq_submit(hook, *, configuration, **kwargs):
            error = errors[len(self.submits)]
            self.submits.append({"configuration": configuration, "kwargs": dict(kwargs)})
            job = MagicMock()
            job.job_id = kwargs.get("job_id") or "mock-job"
            job.project = kwargs.get("project_id")
            job.location = "US"
            job.running.return_value = running
            job.state = "DONE"
            job.error_result = error
            job.errors = [error] if error else []
            if error:
                job.result.side_effect = BadRequest(error["message"], errors=[error])
            job.to_api_repr.return_value = {"configuration": {}}
            return job

        self.hook_cls.insert_job = fake_bq_submit
        # The failed job as get_job reads it back: a single statement unless a test sets a script.
        failed = getattr(self, "failed_job", None) or MagicMock(
            statement_type="SELECT", error_result=errors[0], errors=[errors[0]]
        )

        def fake_get_job(hook, *, project_id, location, job_id):
            failed.project, failed.location, failed.job_id = project_id, location, job_id
            return failed

        self.hook_cls.get_job = fake_get_job

        optimized_job: dict = {"configuration": {**ORIGINAL_CONFIG}}
        if reservation:
            optimized_job["configuration"]["reservation"] = reservation
        if pool_project:
            optimized_job["jobReference"] = {"projectId": pool_project}
        self.optimizer = MagicMock()
        self.optimizer.optimize_job.return_value = OptimizationResponse(
            optimizedJob=optimized_job,
            optimizationResults=[],
            estimatedSavings=0.0,
            optimizationPerformed=True,
        )

        from airflow.models import Variable

        with (
            patch.object(
                Variable,
                "get",
                return_value={"enabled": True, "default_pricing_mode": "slot_based"},
            ),
            patch.object(
                self.plugin,
                "_load_rabbit_credentials",
                return_value={"api_key": "k", "base_url": None},
            ),
            patch.object(self.plugin, "RabbitBQJobOptimizer", return_value=self.optimizer),
        ):
            self.plugin.patch_bigquery_hook()
            self.plugin.patch_bigquery_insert_job_operator()
            return fn()

    def _operator(self, *, deferrable=False, cls=None, **kwargs):
        from airflow import DAG

        dag = DAG(f"routing_retry_{self.id().rsplit('.', 1)[-1]}", start_date=datetime(2026, 1, 1))
        return (cls or self.op_cls)(
            task_id="run_query",
            configuration={"query": dict(ORIGINAL_CONFIG["query"])},
            project_id=SOURCE_PROJECT,
            location="US",
            deferrable=deferrable,
            dag=dag,
            **kwargs,
        )

    def _execute(self, op):
        return op.execute(context=TestPluginOptimization._execute_context(op))

    def _assert_retried_on_original(self, op):
        self.assertEqual(len(self.submits), 2)
        first, retry = self.submits
        self.assertEqual(first["configuration"].get("reservation"), "none")
        self.assertNotIn("reservation", retry["configuration"])
        self.assertEqual(retry["configuration"]["query"], ORIGINAL_CONFIG["query"])
        self.assertEqual(retry["configuration"]["labels"].get("rabbit-routing-retry"), "applied")
        self.assertNotIn("rabbit-pool-routing", retry["configuration"]["labels"])
        self.assertEqual(retry["kwargs"]["project_id"], SOURCE_PROJECT)
        self.assertNotEqual(first["kwargs"]["job_id"], retry["kwargs"]["job_id"])
        # Operator state points at the retried job, on the source project.
        self.assertEqual(op.project_id, SOURCE_PROJECT)
        self.assertEqual(op.job_id, retry["kwargs"]["job_id"])
        self.assertIs(op.configuration, retry["configuration"])
        self.optimizer.optimize_job.assert_called_once()

    def test_quota_error_on_script_parent_is_retried_on_original_config(self):
        op = self._operator()
        self._run(lambda: self._execute(op), errors=[QUOTA_ERROR, None])
        self._assert_retried_on_original(op)

    def test_billing_tier_error_on_pool_job_is_retried_on_source_project(self):
        op = self._operator()
        self._run(
            lambda: self._execute(op),
            errors=[BILLING_TIER_ERROR, None],
            pool_project=POOL_BILLING_PROJECT,
        )
        self.assertEqual(self.submits[0]["kwargs"]["project_id"], POOL_BILLING_PROJECT)
        self._assert_retried_on_original(op)

    def test_non_routing_error_is_not_retried(self):
        from google.api_core.exceptions import BadRequest

        op = self._operator()
        with self.assertRaises(BadRequest):
            self._run(lambda: self._execute(op), errors=[SYNTAX_ERROR])
        self.assertEqual(len(self.submits), 1)

    def test_failed_retry_surfaces_without_second_retry(self):
        from google.api_core.exceptions import BadRequest

        op = self._operator()
        with self.assertRaises(BadRequest):
            self._run(lambda: self._execute(op), errors=[QUOTA_ERROR, QUOTA_ERROR])
        self.assertEqual(len(self.submits), 2)

    def test_job_rabbit_did_not_move_is_not_retried(self):
        from google.api_core.exceptions import BadRequest

        op = self._operator()
        with self.assertRaises(BadRequest):
            self._run(
                lambda: self._execute(op),
                errors=[QUOTA_ERROR],
                reservation="projects/p/locations/US/reservations/r",
            )
        self.assertEqual(len(self.submits), 1)

    def _defer(self, op, **run_kwargs):
        from airflow.exceptions import TaskDeferred

        with self.assertRaises(TaskDeferred) as ctx:
            self._run(lambda: self._execute(op), running=True, **run_kwargs)
        return ctx.exception

    def test_deferrable_quota_error_is_retried_and_redeferred(self):
        from airflow.exceptions import AirflowException, TaskDeferred

        deferred = self._defer(self._operator(deferrable=True), errors=[None, None])
        moved_job = deferred.kwargs["rabbit_moved_job"]
        self.assertEqual(moved_job["job_id"], self.submits[0]["kwargs"]["job_id"])
        self.assertEqual(moved_job["project_id"], SOURCE_PROJECT)

        # The task resumes on a fresh operator instance with the trigger's failure event.
        resumed = self._operator(deferrable=True)
        event = {"status": "error", "message": QUOTA_MESSAGE}
        context = TestPluginOptimization._execute_context(resumed)
        with self.assertRaises(TaskDeferred) as ctx:
            resumed.execute_complete(context, event, **deferred.kwargs)
        self._assert_retried_on_original(resumed)
        # The retry carries no rabbit kwarg, so its failure surfaces instead of retrying again.
        self.assertFalse(ctx.exception.kwargs)
        with self.assertRaises(AirflowException):
            self._operator(deferrable=True).execute_complete(context, event)
        self.assertEqual(len(self.submits), 2)

    def test_deferrable_billing_tier_error_is_identified_by_job_reason(self):
        from airflow.exceptions import TaskDeferred

        deferred = self._defer(
            self._operator(deferrable=True), errors=[None, None], pool_project=POOL_BILLING_PROJECT
        )
        self.assertEqual(deferred.kwargs["rabbit_moved_job"]["project_id"], POOL_BILLING_PROJECT)

        failed_job = MagicMock(error_result=BILLING_TIER_ERROR, errors=[BILLING_TIER_ERROR])
        resumed = self._operator(deferrable=True)
        with patch.object(self.hook_cls, "get_job", return_value=failed_job) as get_job:
            with self.assertRaises(TaskDeferred):
                resumed.execute_complete(
                    TestPluginOptimization._execute_context(resumed),
                    {"status": "error", "message": BILLING_TIER_ERROR["message"]},
                    **deferred.kwargs,
                )
        self.assertEqual(get_job.call_args.kwargs["project_id"], POOL_BILLING_PROJECT)
        self._assert_retried_on_original(resumed)

    def test_executor_safeguard_token_is_forwarded(self):
        """Airflow passes its ExecutorSafeguard token only to a callable named "execute"; without
        it, operators with allow_nested_operators=False refuse to run."""
        from airflow.exceptions import TaskDeferred
        from airflow.models.base import _sentinel

        token = {"BigQueryInsertJobOperator__sentinel": _sentinel}
        strict = {"allow_nested_operators": False}
        op = self._operator(**strict)
        context = TestPluginOptimization._execute_context(op)

        def scenario():
            # As TaskInstance calls it: token passed, and the retry runs too.
            op.execute(context, **token)
            self.assertEqual(self.op_cls.execute.__name__, "execute")
            self._assert_retried_on_original(op)

            # The retry after a deferral runs from execute_complete, which gets no token.
            with self.assertRaises(TaskDeferred) as first:
                self._operator(deferrable=True, **strict).execute(context, **token)
            with self.assertRaises(TaskDeferred):
                self._operator(deferrable=True, **strict).execute_complete(
                    context,
                    {"status": "error", "message": QUOTA_MESSAGE},
                    **first.exception.kwargs,
                )
            self.assertEqual(len(self.submits), 4)

        self._run(scenario, errors=[QUOTA_ERROR, None, None, None], running=True)

    def test_subclass_with_own_execute_complete_defers_without_rabbit_kwarg(self):
        class CustomOperator(self.op_cls):
            def execute_complete(self, context, event):
                return super().execute_complete(context, event)

        op = self._operator(deferrable=True, cls=CustomOperator)
        deferred = self._defer(op, errors=[None])
        self.assertFalse(deferred.kwargs)

    def test_deferrable_non_routing_error_and_success_are_untouched(self):
        from airflow.exceptions import AirflowException

        deferred = self._defer(self._operator(deferrable=True), errors=[None])
        resumed = self._operator(deferrable=True)
        context = TestPluginOptimization._execute_context(resumed)
        failed_job = MagicMock(error_result=SYNTAX_ERROR, errors=[SYNTAX_ERROR])
        with patch.object(self.hook_cls, "get_job", return_value=failed_job):
            with self.assertRaises(AirflowException):
                resumed.execute_complete(
                    context,
                    {"status": "error", "message": SYNTAX_ERROR["message"]},
                    **deferred.kwargs,
                )
        success = {"status": "success", "message": "Job completed", "job_id": "j1"}
        resumed.execute_complete(context, success, **deferred.kwargs)
        self.assertEqual(len(self.submits), 1)

    # --- Scripts: retried only when no statement before the failure wrote persistent data. ---
    # Child-job shapes as BigQuery lists them (checked on real scripts, AIT-166).

    def _script(self, *children, num_child_jobs=None, session=None):
        """A failed script parent and a get_client patch listing ``children`` as its statements."""
        self.failed_job = MagicMock(
            statement_type="SCRIPT",
            state="DONE",
            num_child_jobs=len(children) if num_child_jobs is None else num_child_jobs,
            session_info=session,
            error_result=QUOTA_ERROR,
            errors=[QUOTA_ERROR],
        )
        client = MagicMock()
        if children and isinstance(children[0], Exception):
            client.list_jobs.side_effect = children[0]
        else:
            client.list_jobs.return_value = iter(children)
        return patch.object(self.hook_cls, "get_client", return_value=client)

    @staticmethod
    def _child(statement_type, table=None, error=None, txn=None):
        from google.cloud.bigquery import TableReference

        return MagicMock(
            job_id=f"child-{statement_type}",
            statement_type=statement_type,
            destination=TableReference.from_string(table) if table else None,
            error_result=error,
            transaction_info=MagicMock(transaction_id=txn) if txn else None,
        )

    def _transaction(self, txn, *statements, end="COMMIT_TRANSACTION"):
        """BEGIN, ``(statement_type, table)`` statements and ``end`` (None: still open)."""
        children = [self._child("BEGIN_TRANSACTION", txn=txn)]
        children += [self._child(kind, table, txn=txn) for kind, table in statements]
        return children + ([self._child(end, txn=txn)] if end else [])

    def _failing_in(self, txn):
        """The rejected statement, then the ROLLBACK BigQuery adds (not counted in numChildJobs)."""
        return [
            self._child("DELETE", "p.ds.events", error=QUOTA_ERROR, txn=txn),
            self._child("ROLLBACK_TRANSACTION", txn=txn),
        ]

    def _scripts(self):
        """name -> (retried?, script patch factory)."""
        temp, persistent = "p._script0d5616fb3da5ea88.t", "p.ds.log"
        failed = self._child("DELETE", "p.ds.events", error=QUOTA_ERROR)

        def script(*children, **kwargs):
            return lambda: self._script(*children, **kwargs)

        def failing_after(*children):  # the script failed inside the last, open transaction
            listed = [*children, *self._failing_in("t9")]
            return script(*listed, num_child_jobs=len(listed) - 1)

        return {
            "temp writes and reads": (
                True,
                script(
                    self._child("CREATE_TABLE_AS_SELECT", temp),
                    self._child("INSERT", temp),
                    self._child("SELECT", "p._5f515d049d096bd0.anon1f3c"),  # SET v = (SELECT)
                    failed,
                ),
            ),
            "uncommitted transaction": (
                True,
                failing_after(*self._transaction("t9", ("INSERT", persistent), end=None)),
            ),
            "explicit rollback": (
                True,
                script(
                    *self._transaction("t1", ("INSERT", persistent), end="ROLLBACK_TRANSACTION"),
                    failed,
                ),
            ),
            "committed temp transaction, then failing one": (
                True,
                failing_after(
                    *self._transaction("t1", ("INSERT", temp)),
                    *self._transaction("t9", ("MERGE", persistent), end=None),
                ),
            ),
            "autocommitted write": (False, script(self._child("INSERT", persistent), failed)),
            "committed transaction": (
                False,
                script(*self._transaction("t1", ("INSERT", persistent)), failed),
            ),
            "committed persistent transaction, then failing one": (
                False,
                failing_after(
                    *self._transaction("t1", ("INSERT", persistent)),
                    *self._transaction("t9", ("INSERT", temp), end=None),
                ),
            ),
            "exception handler commits": (
                False,
                script(
                    self._child("BEGIN_TRANSACTION", txn="t1"),
                    self._child("INSERT", persistent, txn="t1"),
                    self._child("SELECT", error=QUOTA_ERROR, txn="t1"),
                    self._child("COMMIT_TRANSACTION", txn="t1"),
                ),
            ),
            "statement a rollback can't undo": (
                False,
                failing_after(*self._transaction("t9", ("EXPORT_DATA", None), end=None)),
            ),
            "session": (False, script(failed, session=MagicMock(session_id="s1"))),
            "listing denied": (False, script(Exception("403 Access Denied: jobs.list"))),
            "incomplete listing": (False, script(failed, num_child_jobs=3)),
            # e.g. a listed child that is not a query job: reading it raises AttributeError.
            "statement the check can't read": (False, script(object(), failed)),
        }

    def test_script_is_retried_only_when_no_persistent_write_was_applied(self):
        from google.api_core.exceptions import BadRequest

        for name, (retried, script) in self._scripts().items():
            self.setUp()  # _run patches the classes once per test
            with self.subTest(name), script() as get_client:
                op = self._operator()
                if retried:
                    self._run(lambda op=op: self._execute(op), errors=[QUOTA_ERROR, None])
                    self._assert_retried_on_original(op)
                    get_client.return_value.list_jobs.assert_called_once_with(
                        project=SOURCE_PROJECT, parent_job=self.submits[0]["kwargs"]["job_id"]
                    )
                    continue
                with self.assertRaises(BadRequest) as ctx:
                    with self.assertLogs(level="WARNING") as logs:
                        self._run(lambda op=op: self._execute(op), errors=[QUOTA_ERROR])
                self.assertIn(QUOTA_MESSAGE, str(ctx.exception))  # the original error surfaces
                self.assertEqual(len(self.submits), 1)
                self.assertTrue(any("not retrying" in line for line in logs.output))

    def test_deferrable_script_is_retried_only_when_no_persistent_write_was_applied(self):
        from airflow.exceptions import AirflowException, TaskDeferred

        event = {"status": "error", "message": QUOTA_MESSAGE}
        for name, (retried, script) in self._scripts().items():
            self.setUp()
            with self.subTest(name), script():
                deferred = self._defer(self._operator(deferrable=True), errors=[None, None])
                resumed = self._operator(deferrable=True)
                context = TestPluginOptimization._execute_context(resumed)
                if retried:
                    with self.assertRaises(TaskDeferred):
                        resumed.execute_complete(context, event, **deferred.kwargs)
                    self._assert_retried_on_original(resumed)
                    continue
                with self.assertRaises(AirflowException) as ctx:
                    resumed.execute_complete(context, event, **deferred.kwargs)
                self.assertIn(QUOTA_MESSAGE, str(ctx.exception))
                self.assertEqual(len(self.submits), 1)


if __name__ == "__main__":
    unittest.main()
