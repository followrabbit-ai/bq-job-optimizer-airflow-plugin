#!/bin/bash
# Runs the plugin's test suites against the Airflow installed in the current environment.
# CI and ../test_in_docker.sh both call this, so they cannot drift apart.
# usage: run_tests.sh [unit|e2e|all]
set -euo pipefail
cd "$(dirname "$0")/.."
DIR=bq-job-optimizer-airflow
WHAT="${1:-all}"

unit() {
  (
    # Unit tests always use a throwaway SQLite home; connections and variables come from env.
    unset AIRFLOW__DATABASE__SQL_ALCHEMY_CONN
    export AIRFLOW_HOME="$(pwd)/airflow_home" AIRFLOW__CORE__LOAD_EXAMPLES=False
    export AIRFLOW_CONN_RABBIT_API='{"conn_type":"generic","password":"test-key","extra":{"api_base_url":"https://api.followrabbit.ai/bq-job-optimizer"}}'
    export AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT='{"conn_type":"google_cloud_platform","extra":{"project":"test-source-project"}}'
    export AIRFLOW_VAR_RABBIT_BQ_OPTIMIZER_CONFIG='{"enabled":true,"default_pricing_mode":"on_demand","reservation_ids":["project:us-central1.test-reservation"]}'
    rm -rf "$AIRFLOW_HOME" && mkdir -p "$AIRFLOW_HOME"
    airflow db migrate >/dev/null
    for t in test_plugin_connection.py test_dag_execution.py test_dag_whitelist.py; do
      echo "== $t" && python "$DIR/$t"
    done
    for t in test_plugin_optimization.py test_dry_run_guard.py; do
      echo "== $t" && python -m unittest "$DIR/$t"
    done
  )
}

e2e() {
  echo "== e2e" && python "$DIR/e2e/run_e2e.py"
}

case "$WHAT" in
  unit) unit ;;
  e2e) e2e ;;
  all) unit; e2e ;;
  *) echo "usage: $0 [unit|e2e|all]" >&2; exit 2 ;;
esac
