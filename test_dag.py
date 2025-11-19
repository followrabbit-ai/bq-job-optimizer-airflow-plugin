from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

with DAG(
    'rabbit_optimizer_test',
    default_args=default_args,
    description='Test DAG for Rabbit BQ Optimizer Plugin',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    # Simple query job that should be intercepted
    insert_query_job = BigQueryInsertJobOperator(
        task_id="test_optimization",
        configuration={
            "query": {
                "query": "SELECT 1",
                "useLegacySql": False,
            }
        },
        # We don't actually want to hit BQ in this test, just verify the hook patch
        # The plugin should log before the operator tries to execute
    )

    insert_query_job

