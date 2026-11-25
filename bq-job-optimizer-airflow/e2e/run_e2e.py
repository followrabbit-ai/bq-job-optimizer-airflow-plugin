#!/usr/bin/env python3
"""End-to-end test: run a DAG on a real Airflow (``airflow standalone``: scheduler, triggerer,
and on Airflow 3 the API server and DAG processor) with the plugin installed, against a fake
BigQuery and a fake Rabbit optimizer API, then check every task's final state and the exact
jobs each task submitted.

Uses the Airflow installed in the current environment. Set AIRFLOW__DATABASE__SQL_ALCHEMY_CONN
to a Postgres database for parallel task execution; the default SQLite database also works.
"""

from __future__ import annotations

import http.client
import json
import os
import re
import signal
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import fake_servers

DAG_ID = "rabbit_e2e"
SOURCE = "e2e-source-project"
TIMEOUT_SECONDS = 600
API_SERVER_PORT = 8080
TERMINAL = {"success", "failed", "upstream_failed", "skipped", "removed"}
NOT_RETRYING = "not retrying it on the original configuration"  # the plugin's warning

# scenario -> (final task state, [(project, on_demand, retry_label, job fails)] in order)
MOVED_THEN_RETRIED = [(SOURCE, True, None, True), (SOURCE, False, "applied", False)]
EXPECTED = {
    # Moved to on-demand and succeeds there.
    "ok": ("success", [(SOURCE, True, None, False)]),
    # Moved job hits an on-demand-only limit: retried once on the original config.
    "quota": ("success", MOVED_THEN_RETRIED),
    "tier": ("success", MOVED_THEN_RETRIED),
    # The retry fails too: the task fails after exactly two jobs.
    "bothfail": ("failed", [(SOURCE, True, None, True), (SOURCE, False, "applied", True)]),
    # An error unrelated to routing is not retried.
    "syntax": ("failed", [(SOURCE, True, None, True)]),
    # A job Rabbit left in place is not retried, even on a quota error.
    "keep": ("failed", [(SOURCE, False, None, True)]),
    # Optimizer API down: the original job is submitted (fail-open).
    "apidown": ("success", [(SOURCE, False, None, False)]),
    # Routed to a pool project.
    "pool": ("success", [(fake_servers.POOL_PROJECT, False, None, False)]),
    # Scripts (fake_servers.SCRIPTS) are retried only if no write of the failed run was applied:
    # temp tables only, a write in a transaction left open, the same after a committed
    # transaction that touched only temp tables.
    "script_temp": ("success", MOVED_THEN_RETRIED),
    "script_open_txn": ("success", MOVED_THEN_RETRIED),
    "script_temp_txn_then_open": ("success", MOVED_THEN_RETRIED),
}
# Script scenarios the plugin must refuse to retry -> the reason its warning gives in the task
# log: a write outside a transaction, a committed write (also when a later transaction is the one
# that failed), a session, a listing that is denied or unreadable. A crash inside the check
# would also fail the task after one job, but without the warning.
REFUSED = {
    "script_write": "may have written to",
    "script_committed": "may have written to",
    "script_write_txn_then_open": "may have written to",
    "script_session": "it ran in a session",
    "script_nolist": "could not list the script's statements",
    "script_unreadable": "could not check the script's statements",
}
EXPECTED.update({scenario: ("failed", [(SOURCE, True, None, True)]) for scenario in REFUSED})


def _service_account_key(path: Path) -> None:
    """Throwaway key whose token endpoint is the fake server; works on every provider version."""
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import rsa

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048).private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    )
    path.write_text(
        json.dumps(
            {
                "type": "service_account",
                "project_id": SOURCE,
                "private_key_id": "e2e",
                "private_key": key.decode(),
                "client_email": f"e2e@{SOURCE}.iam.gserviceaccount.com",
                "client_id": "1",
                "token_uri": f"http://127.0.0.1:{fake_servers.BIGQUERY_PORT}/token",
            }
        )
    )


def _airflow(*args: str, check: bool = True) -> str:
    proc = subprocess.run(["airflow", *args], capture_output=True, text=True, timeout=300)
    if check and proc.returncode != 0:
        raise RuntimeError(f"airflow {' '.join(args)} failed:\n{proc.stdout}\n{proc.stderr}")
    return proc.stdout


def _task_states(run_id: str) -> dict[str, str]:
    out = _airflow("tasks", "states-for-dag-run", DAG_ID, run_id, "-o", "json")
    match = re.search(r"\[\s*\{", out)  # Airflow 3 may print log lines before the JSON
    if not match:
        return {}
    rows, _ = json.JSONDecoder().raw_decode(out[match.start() :])
    return {row["task_id"]: str(row["state"]) for row in rows}


def _task_log(home: Path, task: str) -> str:
    logs = [p for d in (home / "logs").rglob(f"task_id={task}") for p in d.rglob("*")]
    return "".join(p.read_text(errors="replace") for p in logs if p.is_file())


def _wait_for_api_server(standalone: subprocess.Popen) -> None:
    deadline = time.time() + 300
    while time.time() < deadline:
        if standalone.poll() is not None:
            raise RuntimeError("airflow standalone exited early")
        try:
            connection = http.client.HTTPConnection("localhost", API_SERVER_PORT, timeout=5)
            connection.request("GET", "/api/v2/monitor/health")
            if connection.getresponse().status == 200:
                return
        except OSError:
            pass
        time.sleep(2)
    raise RuntimeError("the Airflow API server did not become healthy in 300 seconds")


def main() -> int:
    import airflow

    airflow_3 = int(airflow.__version__.split(".")[0]) >= 3
    # RABBIT_E2E_HOME lets CI keep the Airflow home (logs) as an artifact.
    home = Path(os.environ.get("RABBIT_E2E_HOME") or tempfile.mkdtemp(prefix="rabbit-e2e-"))
    home.mkdir(parents=True, exist_ok=True)
    key_path = home / "sa.json"
    _service_account_key(key_path)
    os.environ.update(
        {
            "AIRFLOW_HOME": str(home),
            "AIRFLOW__CORE__DAGS_FOLDER": str(HERE / "dags"),
            "AIRFLOW__CORE__LOAD_EXAMPLES": "False",
            "AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION": "False",
            "BIGQUERY_EMULATOR_HOST": f"http://127.0.0.1:{fake_servers.BIGQUERY_PORT}",
            "AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT": json.dumps(
                {
                    "conn_type": "google_cloud_platform",
                    "extra": {"key_path": str(key_path), "project": SOURCE},
                }
            ),
        }
    )
    fake_servers.start()
    _airflow("db", "migrate")
    # The plugin's connection and variable live in the metadata database, as in production,
    # so tasks read them the way real deployments do (on Airflow 3: through the API server).
    _airflow(
        "connections",
        "add",
        "rabbit_api",
        "--conn-json",
        json.dumps(
            {
                "conn_type": "generic",
                "password": "e2e-key",
                "extra": {"api_base_url": f"http://127.0.0.1:{fake_servers.OPTIMIZER_PORT}"},
            }
        ),
    )
    _airflow(
        "variables",
        "set",
        "rabbit_bq_optimizer_config",
        json.dumps(
            {
                "enabled": True,
                "default_pricing_mode": "slot_based",
                "reservation_ids": [f"{SOURCE}:US.prod"],
            }
        ),
    )
    _airflow("dags", "reserialize")

    log_path = home / "standalone.log"
    with open(log_path, "w") as log:
        standalone = subprocess.Popen(
            ["airflow", "standalone"], stdout=log, stderr=subprocess.STDOUT, start_new_session=True
        )
    states: dict[str, str] = {}
    try:
        if airflow_3:
            # Task processes talk to the API server; tasks started before it is up fail.
            _wait_for_api_server(standalone)
        run_id = f"e2e_{int(time.time())}"
        trigger = ["dags", "trigger", DAG_ID, "-r", run_id]
        if airflow_3:  # some Airflow 3 providers need an explicit logical date
            trigger += ["--logical-date", datetime.now(timezone.utc).isoformat(timespec="seconds")]
        _airflow(*trigger)

        deadline = time.time() + TIMEOUT_SECONDS
        while time.time() < deadline:
            states = _task_states(run_id)
            if states and all(state in TERMINAL for state in states.values()):
                break
            if standalone.poll() is not None:
                raise RuntimeError("airflow standalone exited early")
            time.sleep(5)
    finally:
        try:
            os.killpg(standalone.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass

    jobs_by_task: dict[str, list] = {}
    for job in fake_servers.submitted_jobs():
        jobs_by_task.setdefault(job["task"], []).append(
            (job["project"], job["on_demand"], job["retry_label"], job["fails"])
        )

    failures = []
    for scenario, (want_state, want_jobs) in EXPECTED.items():
        for mode in ("sync", "deferrable"):
            task = f"{scenario}_{mode}"
            got = (states.get(task), jobs_by_task.get(task, []))
            ok = got == (want_state, want_jobs)
            reason = REFUSED.get(scenario)
            refused = not reason or any(
                NOT_RETRYING in line and reason in line
                for line in _task_log(home, task).splitlines()
            )
            print(
                f"{'PASS' if ok and refused else 'FAIL'} {task:38} "
                f"state={got[0]} jobs={len(got[1])}"
            )
            if not ok:
                failures.append(f"{task}: expected {(want_state, want_jobs)}, got {got}")
            if not refused:
                failures.append(f"{task}: no '{NOT_RETRYING}: ... {reason}' in the task log")

    print(f"airflow {airflow.__version__}: {'OK' if not failures else 'FAILED'}")
    if failures:
        print("\n".join(failures))
        print(f"--- last lines of {log_path}")
        print("\n".join(log_path.read_text(errors="replace").splitlines()[-60:]))
        print(f"task logs kept in {home / 'logs'}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
