#!/usr/bin/env python3
"""Unit tests for plugin optimization: pool routing, fail-open, and hook bridge."""

from __future__ import annotations

import os
import sys
import unittest
from contextlib import contextmanager
from datetime import datetime
from unittest.mock import MagicMock, patch

script_dir = os.path.dirname(os.path.abspath(__file__))
repo_root = os.path.dirname(script_dir)
os.environ.setdefault("AIRFLOW_HOME", os.path.join(repo_root, "airflow_home"))
sys.path.insert(0, os.path.join(os.environ["AIRFLOW_HOME"], "plugins"))

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
                return_value={"default_pricing_mode": "on_demand", "reservation_ids": ["p:US.r"]},
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
        self.assertNotIn("project_id", payload)

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
            op.execute(context={"logical_date": datetime(2026, 1, 1), "ti": MagicMock()})

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
            op.execute(context={"logical_date": datetime(2026, 1, 1), "ti": MagicMock()})

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
            op.execute(context={"logical_date": datetime(2026, 1, 1), "ti": MagicMock()})

        self.assertEqual(op.project_id, SOURCE_PROJECT)
        self.assertEqual(submitted["kwargs"]["project_id"], SOURCE_PROJECT)
        labels = submitted["configuration"].get("labels", {})
        self.assertEqual(labels.get("rabbit-pool-routing"), "none")
        self.assertNotIn("rabbit-pool-project", labels)
        mock_client.optimize_job.assert_called_once()

    def test_missing_reservation_ids_calls_optimizer_with_empty_list(self):
        import importlib

        import rabbit_bq_optimizer_plugin as plugin_module
        from airflow.models import Variable
        from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

        importlib.reload(plugin_module)

        def fake_bq_submit(self, *, configuration, **kwargs):
            return MagicMock(job_id="mock-job-empty-res")

        BigQueryHook.insert_job = fake_bq_submit

        mock_client = MagicMock()
        mock_client.optimize_job.return_value = self._mock_optimizer_response(pool_project=None)

        with (
            patch.object(
                Variable,
                "get",
                return_value={"default_pricing_mode": "on_demand"},
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


if __name__ == "__main__":
    unittest.main()
