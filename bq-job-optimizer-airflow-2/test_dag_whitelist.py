#!/usr/bin/env python3
"""
Test script that verifies the dag_whitelist feature of the Rabbit BQ Optimizer plugin.

Scenarios covered:
  1. No dag_whitelist → all DAGs optimized (backward compatible)
  2. Empty dag_whitelist → all DAGs optimized
  3. DAG in whitelist → optimized
  4. DAG not in whitelist → skipped
  5. get_current_context unavailable with whitelist set → skipped (safe fallback)
"""

import os
import sys
from unittest.mock import MagicMock, patch

# Set up Airflow environment
script_dir = os.path.dirname(os.path.abspath(__file__))
repo_root = os.path.dirname(script_dir)
os.environ["AIRFLOW_HOME"] = os.path.join(repo_root, "airflow_home")
sys.path.insert(0, os.path.join(os.environ["AIRFLOW_HOME"], "plugins"))

from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook, BigQueryJob  # noqa: E402

VALID_CONFIG_BASE = {
    "default_pricing_mode": "on_demand",
    "reservation_ids": ["project:us-central1.test-reservation"],
}

DUMMY_CONNECTION = MagicMock()
DUMMY_CONNECTION.password = "test-api-key"
DUMMY_CONNECTION.extra_dejson = {}


def _make_mock_optimizer_response():
    resp = MagicMock()
    resp.optimizedJob = {"configuration": {"query": {"query": "SELECT 1", "useLegacySql": False}}}
    return resp


def _reset_patch():
    """Remove the patch marker so patch_bigquery_hook() can be applied again."""
    if hasattr(BigQueryHook, "_rabbit_bq_job_optimizer_patched"):
        delattr(BigQueryHook, "_rabbit_bq_job_optimizer_patched")


def _run_insert_job(config, context_side_effect=None):
    """
    Apply a fresh patch and call insert_job once.

    Returns (optimize_job_called: bool, original_called_with_original: bool).
    """
    _reset_patch()

    original_config = {"query": {"query": "SELECT 1", "useLegacySql": False}}
    original_called_configs = []

    def mock_original(self, *, configuration, **kwargs):
        original_called_configs.append(configuration)
        job = MagicMock(spec=BigQueryJob)
        job.job_id = "test-job-123"
        return job

    BigQueryHook.insert_job = mock_original

    from rabbit_bq_optimizer_plugin import patch_bigquery_hook

    patch_bigquery_hook()

    mock_client = MagicMock()
    mock_client.optimize_job.return_value = _make_mock_optimizer_response()

    patches = {
        "rabbit_bq_optimizer_plugin.Variable.get": MagicMock(return_value=config),
        "rabbit_bq_optimizer_plugin.BaseHook.get_connection": MagicMock(
            return_value=DUMMY_CONNECTION
        ),
        "rabbit_bq_optimizer_plugin.RabbitBQJobOptimizer": MagicMock(return_value=mock_client),
    }

    if context_side_effect is not None:
        patches["rabbit_bq_optimizer_plugin.get_current_context"] = context_side_effect

    with (
        patch.dict("sys.modules", {}),
        patch(list(patches.keys())[0], patches[list(patches.keys())[0]]),
        patch(list(patches.keys())[1], patches[list(patches.keys())[1]]),
        patch(list(patches.keys())[2], patches[list(patches.keys())[2]]),
    ):

        if len(patches) == 4:
            ctx_key = list(patches.keys())[3]
            with patch(ctx_key, patches[ctx_key]):
                hook = BigQueryHook()
                hook.insert_job(configuration=original_config)
        else:
            hook = BigQueryHook()
            hook.insert_job(configuration=original_config)

    optimized = mock_client.optimize_job.called
    return optimized


def test_no_whitelist_optimizes_all():
    """Without dag_whitelist, all DAGs should be optimized."""
    config = {**VALID_CONFIG_BASE}
    optimized = _run_insert_job(config)

    if optimized:
        print("✓ No dag_whitelist → optimization ran")
        return True
    else:
        print("✗ No dag_whitelist → expected optimization to run, but it didn't")
        return False


def test_empty_whitelist_optimizes_all():
    """Empty dag_whitelist should behave the same as absent."""
    config = {**VALID_CONFIG_BASE, "dag_whitelist": []}
    optimized = _run_insert_job(config)

    if optimized:
        print("✓ Empty dag_whitelist → optimization ran")
        return True
    else:
        print("✗ Empty dag_whitelist → expected optimization to run, but it didn't")
        return False


def test_dag_in_whitelist_optimized():
    """DAG that appears in the whitelist should be optimized."""
    config = {**VALID_CONFIG_BASE, "dag_whitelist": ["my_dag", "other_dag"]}

    mock_context = {"dag": MagicMock(dag_id="my_dag")}

    with patch("airflow.operators.python.get_current_context", return_value=mock_context):
        optimized = _run_insert_job(config)

    if optimized:
        print("✓ DAG in whitelist → optimization ran")
        return True
    else:
        print("✗ DAG in whitelist → expected optimization to run, but it didn't")
        return False


def test_dag_not_in_whitelist_skipped():
    """DAG not in the whitelist should skip optimization."""
    config = {**VALID_CONFIG_BASE, "dag_whitelist": ["allowed_dag"]}

    mock_context = {"dag": MagicMock(dag_id="other_dag")}

    with patch("airflow.operators.python.get_current_context", return_value=mock_context):
        optimized = _run_insert_job(config)

    if not optimized:
        print("✓ DAG not in whitelist → optimization skipped")
        return True
    else:
        print("✗ DAG not in whitelist → expected optimization to be skipped, but it ran")
        return False


def test_context_unavailable_skips_when_whitelist_set():
    """If dag_whitelist is set but get_current_context fails, optimization should be skipped."""
    config = {**VALID_CONFIG_BASE, "dag_whitelist": ["some_dag"]}

    with patch(
        "airflow.operators.python.get_current_context",
        side_effect=RuntimeError("No context"),
    ):
        optimized = _run_insert_job(config)

    if not optimized:
        print("✓ Context unavailable with whitelist → optimization skipped (safe fallback)")
        return True
    else:
        print(
            "✗ Context unavailable with whitelist → expected optimization to be skipped, "
            "but it ran"
        )
        return False


if __name__ == "__main__":
    print("=" * 70)
    print("Rabbit BQ Optimizer Plugin — DAG Whitelist Tests")
    print("=" * 70)

    tests = [
        test_no_whitelist_optimizes_all,
        test_empty_whitelist_optimizes_all,
        test_dag_in_whitelist_optimized,
        test_dag_not_in_whitelist_skipped,
        test_context_unavailable_skips_when_whitelist_set,
    ]

    results = []
    for test_fn in tests:
        print(f"\n→ {test_fn.__doc__.strip()}")
        try:
            results.append(test_fn())
        except Exception as e:
            print(f"✗ {test_fn.__name__} raised an exception: {e}")
            import traceback

            traceback.print_exc()
            results.append(False)

    passed = sum(results)
    total = len(results)

    print("\n" + "=" * 70)
    if all(results):
        print(f"✓ All {total} dag_whitelist tests PASSED")
    else:
        print(f"✗ {total - passed}/{total} dag_whitelist tests FAILED")
    print("=" * 70)

    sys.exit(0 if all(results) else 1)
