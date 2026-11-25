"""End-to-end test DAG: one sync and one deferrable task per scenario (see fake_servers.py)."""

from datetime import datetime

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

SCENARIOS = ("OK", "QUOTA", "TIER", "BOTHFAIL", "SYNTAX", "KEEP", "APIDOWN", "POOL")
# Scripts whose moved run fails on the quota; the fake BigQuery lists each one's statements.
SCRIPT_SCENARIOS = (
    "SCRIPT_TEMP",
    "SCRIPT_WRITE",
    "SCRIPT_OPEN_TXN",
    "SCRIPT_COMMITTED",
    "SCRIPT_TEMP_TXN_THEN_OPEN",
    "SCRIPT_WRITE_TXN_THEN_OPEN",
    "SCRIPT_SESSION",
    "SCRIPT_NOLIST",
    "SCRIPT_UNREADABLE",
)
STATEMENT = "-- {marker} task={task_id}\nDELETE FROM ds.t WHERE id = 1"
SCRIPT = "-- {marker} task={task_id}\nBEGIN\n  SELECT 1;\n  DELETE FROM ds.t WHERE id = 1;\nEND"

with DAG(
    "rabbit_e2e",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_tasks=8,  # keeps task start-up fast on a small CI runner (see RUN_SECONDS)
    # allow_nested_operators=False turns a missing executor-safeguard token into a task failure.
    default_args={"retries": 0, "allow_nested_operators": False},
) as dag:
    for marker in SCENARIOS + SCRIPT_SCENARIOS:
        sql = SCRIPT if marker in SCRIPT_SCENARIOS else STATEMENT
        for deferrable in (False, True):
            task_id = f"{marker.lower()}_{'deferrable' if deferrable else 'sync'}"
            BigQueryInsertJobOperator(
                task_id=task_id,
                configuration={
                    "query": {
                        "query": sql.format(marker=marker, task_id=task_id),
                        "useLegacySql": False,
                    }
                },
                project_id="e2e-source-project",
                location="US",
                deferrable=deferrable,
                poll_interval=1,
            )
