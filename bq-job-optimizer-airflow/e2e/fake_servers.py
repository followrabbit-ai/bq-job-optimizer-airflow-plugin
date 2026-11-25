"""Fake Rabbit optimizer API and fake BigQuery REST API for the end-to-end test.

The SQL of every job carries ``-- <MARKER> task=<task_id>``. The marker picks the behaviour:

  optimizer   default: move the job to on-demand (``reservation: 'none'``)
              KEEP: return the job unchanged      APIDOWN: answer HTTP 500
              POOL: route the job to POOL_PROJECT
  BigQuery    QUOTA / TIER: fail on-demand jobs with the matching on-demand-only error
              BOTHFAIL / KEEP: fail with the custom quota error wherever the job runs
              SYNTAX: fail with a plain invalidQuery
              SCRIPT_*: a script (see SCRIPTS) that fails on-demand with the custom quota error
              anything else: succeed

A job stays RUNNING for RUN_SECONDS so deferrable operators really defer.
"""

from __future__ import annotations

import json
import re
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread
from urllib.parse import parse_qs, urlsplit

BIGQUERY_PORT = 9050
OPTIMIZER_PORT = 9051
POOL_PROJECT = "e2e-pool-project"
# Longer than a loaded runner needs to finish the task process that deferred: on Airflow 3.1 - 3.3
# the scheduler fails a task whose trigger fires before that process has exited.
RUN_SECONDS = 10
JOBS: dict[str, dict] = {}  # job id -> job; insertion order is submission order
QUOTA_MSG = (
    "Custom quota exceeded: Your usage exceeded the custom quota for QueryUsagePerDay, "
    "which is set by your administrator. For more information, see "
    "https://cloud.google.com/bigquery/cost-controls at [40:1]"
)
TIER_MSG = (
    "Query exceeded resource limits. 2147.3 CPU seconds were used, and this query must use "
    "less than 1800.0 CPU seconds."
)
TEMP = "_script0d5616fb3da5ea88"  # the hidden dataset BigQuery gives a script's TEMP tables
_COMMITTED_WRITE = [
    ("BEGIN_TRANSACTION", None, "t1"),
    ("INSERT", "ds", "t1"),
    ("COMMIT_TRANSACTION", None, "t1"),
]
_COMMITTED_TEMP = [
    ("BEGIN_TRANSACTION", None, "t1"),
    ("INSERT", TEMP, "t1"),
    ("COMMIT_TRANSACTION", None, "t1"),
]
# marker -> the statements that succeeded before the one that hit the quota, as
# (statementType, destination dataset, transaction id). The fake lists them as the failed job's
# children in the shape real BigQuery reports (checked on real scripts, AIT-166); see _children.
SCRIPTS: dict[str, list] = {
    "SCRIPT_TEMP": [("CREATE_TABLE_AS_SELECT", TEMP, None), ("SELECT", "_anon", None)],
    "SCRIPT_WRITE": [("INSERT", "ds", None)],
    "SCRIPT_OPEN_TXN": [("BEGIN_TRANSACTION", None, "t1"), ("INSERT", "ds", "t1")],
    "SCRIPT_COMMITTED": _COMMITTED_WRITE,
    "SCRIPT_TEMP_TXN_THEN_OPEN": [
        *_COMMITTED_TEMP,
        ("BEGIN_TRANSACTION", None, "t2"),
        ("MERGE", "ds", "t2"),
    ],
    "SCRIPT_WRITE_TXN_THEN_OPEN": [
        *_COMMITTED_WRITE,
        ("BEGIN_TRANSACTION", None, "t2"),
        ("INSERT", TEMP, "t2"),
    ],
    "SCRIPT_SESSION": [],  # the job ran in a session
    "SCRIPT_NOLIST": [],  # listing the children is denied
    "SCRIPT_UNREADABLE": [(None, None, None)],  # a listed child that is not a query job
}


def _sql(configuration: dict) -> str:
    return (configuration.get("query") or {}).get("query", "")


def _script(configuration: dict) -> str | None:
    marker = re.search(r"SCRIPT_[A-Z_]+", _sql(configuration))
    return marker.group() if marker else None


def _error_for(configuration: dict) -> dict | None:
    sql = _sql(configuration)
    on_demand = configuration.get("reservation") == "none"
    if "SYNTAX" in sql:
        return {"reason": "invalidQuery", "message": "Syntax error: Unexpected identifier at [1:1]"}
    if "BOTHFAIL" in sql or "KEEP" in sql or (on_demand and ("QUOTA" in sql or "SCRIPT_" in sql)):
        return {"reason": "invalidQuery", "message": QUOTA_MSG}
    if on_demand and "TIER" in sql:
        return {"reason": "billingTierLimitExceeded", "message": TIER_MSG}
    return None


def _resource(job: dict) -> dict:
    done = time.time() - job["created"] >= RUN_SECONDS
    status: dict = {"state": "DONE" if done else "RUNNING"}
    if done and job["error"]:
        status["errorResult"] = job["error"]
        status["errors"] = [job["error"]]
    ref = job["ref"]
    statistics: dict = {"creationTime": str(int(job["created"] * 1000))}
    marker = _script(job["configuration"])
    if marker:
        statistics["query"] = {"statementType": "SCRIPT"}
        statistics["numChildJobs"] = str(len(SCRIPTS[marker]) + 1)  # + the failing statement
        if marker == "SCRIPT_SESSION":
            statistics["sessionInfo"] = {"sessionId": "e2e-session"}
    return {
        "kind": "bigquery#job",
        "id": f"{ref['projectId']}:US.{ref['jobId']}",
        "jobReference": ref,
        "configuration": job["configuration"],
        "status": status,
        "statistics": statistics,
    }


def _children(parent: dict) -> list[dict]:
    """A failed script's statements as jobs.list returns them: the ones that succeeded, the one
    that hit the quota and, if that left a transaction open, the ROLLBACK BigQuery adds (which
    numChildJobs does not count)."""
    succeeded = SCRIPTS[_script(parent["configuration"])]
    open_txn = succeeded[-1][2] if succeeded and succeeded[-1][0] != "COMMIT_TRANSACTION" else None
    statements = [*succeeded, ("DELETE", "ds", open_txn)]
    if open_txn:
        statements.append(("ROLLBACK_TRANSACTION", None, open_txn))
    ref = parent["ref"]
    jobs = []
    for n, (kind, dataset, txn) in enumerate(statements):
        query: dict = {"query": "-- one statement of the script"}
        if dataset:
            query["destinationTable"] = {
                "projectId": ref["projectId"],
                "datasetId": dataset,
                "tableId": "t",
            }
        statistics: dict = {"parentJobId": ref["jobId"], "query": {"statementType": kind}}
        if txn:
            statistics["transactionInfo"] = {"transactionId": txn}
        status: dict = {"state": "DONE"}
        if n == len(succeeded):
            status["errorResult"] = parent["error"]
        jobs.append(
            {
                "jobReference": {**ref, "jobId": f"{ref['jobId']}_{n}"},
                "configuration": {"query": query} if kind else {},
                "status": status,
                "statistics": statistics,
            }
        )
    return jobs


def submitted_jobs() -> list[dict]:
    """One summary per submitted job, in submission order."""
    out = []
    for job in list(JOBS.values()):
        cfg = job["configuration"]
        task = re.search(r"task=(\w+)", _sql(cfg))
        out.append(
            {
                "task": task.group(1) if task else None,
                "project": job["ref"]["projectId"],
                "on_demand": cfg.get("reservation") == "none",
                "retry_label": (cfg.get("labels") or {}).get("rabbit-routing-retry"),
                "fails": bool(job["error"]),
            }
        )
    return out


class _Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def _send(self, body: dict, code: int = 200):
        data = json.dumps(body).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_POST(self):
        raw = self.rfile.read(int(self.headers.get("Content-Length", 0) or 0))
        if self.path.endswith("/token"):  # OAuth token endpoint for the throwaway service account
            return self._send({"access_token": "fake", "expires_in": 3600, "token_type": "Bearer"})
        body = json.loads(raw or b"{}")
        if self.path.endswith("/v1/optimize-job"):
            cfg = dict(body["job"]["configuration"])
            sql = _sql(cfg)
            if "APIDOWN" in sql:
                return self._send({"error": "unavailable"}, 500)
            optimized: dict = {"configuration": cfg}
            if "POOL" in sql:
                optimized["jobReference"] = {"projectId": POOL_PROJECT}
            elif "KEEP" not in sql:
                cfg["reservation"] = "none"
            return self._send(
                {
                    "optimizedJob": optimized,
                    "optimizationResults": [],
                    "estimatedSavings": 1.0,
                    "optimizationPerformed": True,
                }
            )
        match = re.search(r"/projects/([^/]+)/jobs", self.path)
        if match:
            ref = dict(body.get("jobReference") or {})
            ref.setdefault("projectId", match.group(1))
            ref.setdefault("location", "US")
            job = {
                "ref": ref,
                "configuration": body["configuration"],
                "created": time.time(),
                "error": _error_for(body["configuration"]),
            }
            JOBS[ref["jobId"]] = job
            return self._send(_resource(job))
        self._send({"error": {"code": 404, "message": "not found " + self.path}}, 404)

    def do_GET(self):
        parent_id = parse_qs(urlsplit(self.path).query).get("parentJobId")
        if parent_id:  # jobs.list for a script's statements
            parent = JOBS[parent_id[0]]
            if _script(parent["configuration"]) == "SCRIPT_NOLIST":
                denied = {"code": 403, "message": "Access Denied: jobs.list"}
                return self._send({"error": denied}, 403)
            return self._send({"kind": "bigquery#jobList", "jobs": _children(parent)})
        match = re.search(r"/projects/([^/]+)/(jobs|queries)/([^/?]+)", self.path)
        job = JOBS.get(match.group(3)) if match else None
        if not job:
            return self._send({"error": {"code": 404, "message": "Not found: Job"}}, 404)
        res = _resource(job)
        if match.group(2) == "jobs":
            return self._send(res)
        if res["status"]["state"] != "DONE":
            return self._send({"jobComplete": False, "jobReference": job["ref"]})
        if job["error"]:
            error = {
                "code": 400,
                "message": job["error"]["message"],
                "errors": [dict(job["error"], domain="global")],
                "status": "INVALID_ARGUMENT",
            }
            return self._send({"error": error}, 400)
        self._send(
            {
                "jobComplete": True,
                "jobReference": job["ref"],
                "totalRows": "0",
                "schema": {"fields": [{"name": "f0_", "type": "INTEGER", "mode": "NULLABLE"}]},
                "rows": [],
            }
        )


def start() -> None:
    for port in (BIGQUERY_PORT, OPTIMIZER_PORT):
        server = ThreadingHTTPServer(("127.0.0.1", port), _Handler)
        Thread(target=server.serve_forever, daemon=True).start()
