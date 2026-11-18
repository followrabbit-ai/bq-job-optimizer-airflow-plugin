"""End-to-end test DAG: one sync and one deferrable task per scenario (see fake_servers.py)."""

from datetime import datetime

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

SCENARIOS = ("OK", "QUOTA", "TIER", "BOTHFAIL", "SYNTAX", "KEEP", "APIDOWN", "POOL")
SCRIPT = "-- {marker} task={task_id}\nBEGIN\n  SELECT 1;\n  DELETE FROM ds.t WHERE id = 1;\nEND"

with DAG(
    "rabbit_e2e",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    # allow_nested_operators=False turns a missing executor-safeguard token into a task failure.
    default_args={"retries": 0, "allow_nested_operators": False},
) as dag:
    for marker in SCENARIOS:
        for deferrable in (False, True):
            task_id = f"{marker.lower()}_{'deferrable' if deferrable else 'sync'}"
            BigQueryInsertJobOperator(
                task_id=task_id,
                configuration={
                    "query": {
                        "query": SCRIPT.format(marker=marker, task_id=task_id),
                        "useLegacySql": False,
                    }
                },
                project_id="e2e-source-project",
                location="US",
                deferrable=deferrable,
                poll_interval=1,
            )
