#!/bin/bash
# Runs the full test suite (unit + end-to-end) for one Airflow version in the official image,
# with a Postgres metadata database, like CI does.
# usage: test_in_docker.sh <airflow-version> [python-version] [unit|e2e|all]
#   e.g. test_in_docker.sh 3.4.0 3.12
set -euo pipefail
cd "$(dirname "$0")"
AF="${1:?airflow version, e.g. 2.11.2 or 3.4.0}"; PY="${2:-3.12}"; WHAT="${3:-all}"
NAME="rabbit-plugin-test-$AF-py$PY"; NET="$NAME-net"
cleanup() { docker rm -f "$NAME" "$NAME-db" >/dev/null 2>&1 || true; docker network rm "$NET" >/dev/null 2>&1 || true; }
trap cleanup EXIT; cleanup
docker network create "$NET" >/dev/null
docker run -d --name "$NAME-db" --network "$NET" -e POSTGRES_USER=airflow -e POSTGRES_PASSWORD=airflow \
  -e POSTGRES_DB=airflow postgres:16 >/dev/null
docker run --rm --name "$NAME" --network "$NET" -v "$(pwd)":/src:ro \
  -e AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="postgresql+psycopg2://airflow:airflow@$NAME-db/airflow" \
  -e AIRFLOW__CORE__EXECUTOR=LocalExecutor \
  --entrypoint bash "apache/airflow:$AF-python$PY" -c "
    set -e
    cp -r /src /tmp/plugin && cd /tmp/plugin && rm -rf airflow_home *.egg-info
    pip install -q rabbit-bq-job-optimizer && pip install -q --no-deps -e .
    bash bq-job-optimizer-airflow/run_tests.sh $WHAT"
