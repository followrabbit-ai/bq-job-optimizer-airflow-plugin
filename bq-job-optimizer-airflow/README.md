# Rabbit BigQuery Job Optimizer Airflow Plugin

This Airflow plugin automatically optimizes BigQuery job configurations using the Rabbit API. It patches the BigQueryHook to intercept job configurations and optimize them before execution.

## Features

- Automatically intercepts all BigQuery job submissions
- Optimizes job configurations via the Rabbit API
- Routes `BigQueryInsertJobOperator` submits to an on-demand pool billing project when the optimizer sets `optimizedJob.jobReference.projectId`
- Graceful fallback to original configuration if optimization fails
- Comprehensive error handling and logging

Already running an older version? See [Updating](#updating).

## Supported versions

One package supports Airflow 2 and Airflow 3. CI runs the unit tests and an end-to-end test (a real Airflow running a DAG) on every version in this table:

| Airflow | Python | `apache-airflow-providers-google` tested |
| --- | --- | --- |
| 2.9, 2.10, 2.11 | 3.9 – 3.12 | 10.17, 12.0, 20.0 |
| 3.1, 3.2, 3.3, 3.4 | 3.10 – 3.13 | 20.0 – 22.8 |

The plugin behaves the same on both major versions; installation and configuration do not differ. Airflow 3.0 is not supported: see [Airflow 3 notes](#airflow-3-notes).

## Installation

### Option A: PyPI (recommended)

1. Add the plugin and client to your Airflow environment:
   - If using `requirements.txt`:
     ```txt
     rabbit-bq-job-optimizer==0.1.19
     rabbit-bq-optimizer-airflow-plugin==1.1.5
     ```
   - If using `constraints.txt`:
     ```txt
     rabbit-bq-job-optimizer==0.1.19
     rabbit-bq-optimizer-airflow-plugin==1.1.5
     ```
   - If using a custom Docker image, add to your Dockerfile:
     ```dockerfile
     RUN pip install rabbit-bq-job-optimizer==0.1.19 rabbit-bq-optimizer-airflow-plugin==1.1.5
     ```

   The plugin registers via Airflow's plugin entry point — no file copy into `plugins/` is required.

2. Restart your Airflow scheduler, workers, and webserver (on Airflow 3: the API server and DAG processor) to load the plugin. If you use deferrable `BigQueryInsertJobOperator`, restart the triggerer as well.

### Option B: Copy into `plugins/`

1. Add the Python client to your Airflow environment dependencies:

   ```txt
   rabbit-bq-job-optimizer==0.1.19
   ```

2. Copy the plugin file into your Airflow plugins directory:
   ```bash
   cp bq-job-optimizer-airflow/rabbit_bq_optimizer_plugin.py "$AIRFLOW_HOME/plugins/"
   ```

3. Restart your Airflow scheduler, workers, and webserver (and triggerer if using deferrable operators).

## Updating

### PyPI install

Update to the new package versions in your environment dependencies:

```txt
rabbit-bq-job-optimizer==0.1.19
rabbit-bq-optimizer-airflow-plugin==1.1.5
```

Apply the update using your platform's usual process, then restart Airflow components (including the triggerer if you use deferrable `BigQueryInsertJobOperator`). Your `rabbit_api` connection, `rabbit_bq_optimizer_config` variable, and DAGs are unchanged. If optimization fails, the plugin still submits the original job (fail-open).

### Copy into `plugins/` (optional)

If you installed via [Option B](#option-b-copy-into-plugins) and want to stay on the copy method, bump the client version in your environment dependencies and replace `rabbit_bq_optimizer_plugin.py` in your plugins location with the new file from this repo. Then restart Airflow components as above.

### Switching from copy to PyPI

If you previously deployed `rabbit_bq_optimizer_plugin.py` to your plugins location and are switching to the PyPI package, **remove that file** — the PyPI install loads via an entry point and the copied file causes duplicate plugin registration. On startup the plugin logs an error if both are present. When the file is writable, you can set `RABBIT_BQ_OPTIMIZER_REMOVE_LEGACY_PLUGIN=true` to remove it automatically on load.

For pool billing routing, the Airflow service account also needs `roles/bigquery.jobUser` on the pool billing projects — contact Rabbit support for your project list. See [Pool billing project routing](#pool-billing-project-routing).

## Configuration

### 1. Create the Rabbit API connection

For security reasons the Rabbit API key must be stored in an Airflow connection instead of a variable. The plugin expects a connection with ID `rabbit_api`:

- `Conn Type`: `Generic` (or `HTTP`)
- `Conn ID`: `rabbit_api`
- `Password`: Rabbit API key (required)
- `Extra` (optional):
  ```json
  {
    "api_base_url": "https://api.followrabbit.ai/bq-job-optimizer"
  }
  ```
  Only include `api_base_url` if you need to override the default Rabbit endpoint; otherwise it will fall back automatically.

CLI example:

```bash
airflow connections add rabbit_api \
    --conn-type generic \
    --conn-password "<your-rabbit-api-key>" \
    --conn-extra '{"api_base_url": "https://api.followrabbit.ai/bq-job-optimizer"}'
```

### 2. Set the optimization parameters

The remaining optimizer configuration stays in an Airflow variable. Create a JSON configuration with the following structure:

```json
{
    "enabled": true,
    "default_pricing_mode": "on_demand",
    "reservation_ids": [
        "project:region.reservation-name1",
        "project:region.reservation-name2"
    ]
}
```

### Configuration Fields

- `enabled` (required): Must be `true` to run optimization. If omitted, `false`, or the variable is not set, each BigQuery submit bypasses optimization (no API call). Takes effect on the next job without restarting Airflow.
- `default_pricing_mode` (required when `enabled` is `true`): The default pricing mode for jobs. Must be one of: `"on_demand"` or `"slot_based"`
- `reservation_ids` (required when `default_pricing_mode` is `"on_demand"`): List of reservation IDs in the format "project:region.reservation-name". On-demand routing needs at least one reservation to compare against; missing or empty fails validation and the job submits unoptimized. When `default_pricing_mode` is `"slot_based"`, this field is optional (defaults to `[]`).
- `dag_whitelist` (optional): List of DAG IDs to optimize. When omitted (or `null`), all DAGs are optimized. When set to a list, only BigQuery jobs from matching DAGs are optimized; all others pass through unmodified — and an **empty list (`[]`) means nothing is whitelisted, so no DAG is optimized**. Must be a JSON list; any other type is ignored and optimization is skipped as a safe fallback.
- `statement_level` (optional): When `true`, opts multi-statement SCRIPT jobs into **per-statement reservation routing** — each statement of the script is routed to whichever is cheaper (a reservation or pay-as-you-go) instead of a single setting for the whole script. Also requires the tenant's `bq_dynamic_pricing_statement_level` feature flag, which is enabled by Rabbit; until it is on, setting this is a safe no-op. Before submitting a rewritten script, the plugin dry-runs it with your own BigQuery credentials and, on any error, submits the original job unchanged — worst case is a missed optimization, never a broken job. Default: off.
- `debug` (optional): When `true`, fail-open paths include a full traceback, and successful optimizer calls log the API response JSON at INFO (`Rabbit BQ Optimizer: optimization result=...`). Default: off.

### Setting the Configuration

You can set the configuration in two ways:

1. Using the Airflow CLI:
```bash
airflow variables set rabbit_bq_optimizer_config '{
    "enabled": true,
    "default_pricing_mode": "on_demand",
    "reservation_ids": [
        "project:region.reservation-name1",
        "project:region.reservation-name2"
    ],
    "dag_whitelist": [
        "my_etl_dag",
        "my_analytics_dag"
    ],
    "statement_level": true
}'
```

   Omit `dag_whitelist` to optimize all DAGs. Setting it to an empty list (`[]`) whitelists nothing and disables optimization for every DAG.

2. Through the Airflow UI:
   - Go to Admin -> Variables
   - Add a new variable named `rabbit_bq_optimizer_config`
   - Paste the JSON configuration

## End-to-End Test Plan

### Local Testing

A setup script `setup_local_test.sh` is provided in the root directory to help you spin up a local Airflow environment for testing.

**Prerequisites:**
- Python 3.8-3.12 (Airflow 2.9.1 requirement; the script will automatically detect and use a compatible version)
- `pip`

**Steps:**

1. **Run the setup script:**
   ```bash
   ./setup_local_test.sh
   ```
   This script will:
   - Create a virtual environment (if not present)
  - Install Airflow using the official constraints file plus dependencies from `test_requirements.txt`
   - Initialize the Airflow database
   - Create an admin user
   - Deploy the plugin and test DAG
   - Configure the `rabbit_api` connection (with a dummy key)
   - Configure the `rabbit_bq_optimizer_config` variable

  *Note: The script installs Airflow using the official constraints file for your Python version so that binary wheels (including `google-re2`) are used. Additional dependencies (`rabbit-bq-job-optimizer`, `apache-airflow-providers-google`, `google-cloud-bigquery`, etc.) are installed with the same constraints to match the Airflow release.*

2. **Run the test DAG:**
   The setup script automatically deploys `test_dag.py` to the DAGs directory.

   Execute the test DAG:
   ```bash
   source venv/bin/activate
   export AIRFLOW_HOME=$(pwd)/airflow_home
   airflow dags test rabbit_optimizer_test $(date +%Y-%m-%d)
   ```

3. **Verify Results:**
   Check the logs for the following:
   - `Rabbit BQ Optimizer: patching BigQueryHook.insert_job` (scheduler logs at startup)
   - `Rabbit BQ Optimizer: patching BigQueryInsertJobOperator._submit_job` (scheduler logs at startup)
   - `Rabbit BQ Optimizer: optimization result=...` on job submit when `debug` is true (INFO), or at DEBUG when `debug` is off (or failure if using dummy key/URL)

4. **Run automated tests:**
   See [Automated tests](#automated-tests).

5. **Cleanup (optional):**
   To remove the test environment (venv and airflow_home directories):
   ```bash
   ./setup_local_test.sh --clean
   ```
   Or use the short form:
   ```bash
   ./setup_local_test.sh -c
   ```

### Automated tests

The same script runs locally and in CI, against whichever Airflow is installed in the current environment:

```bash
bq-job-optimizer-airflow/run_tests.sh unit   # unit tests, no network
bq-job-optimizer-airflow/run_tests.sh e2e    # end-to-end test
bq-job-optimizer-airflow/run_tests.sh        # both
```

To test a specific Airflow version without installing it, run the suite in the official Airflow image (needs Docker):

```bash
./test_in_docker.sh 3.4.0 3.12     # <airflow version> [python version] [unit|e2e|all]
./test_in_docker.sh 2.9.1
```

**Unit tests** (`test_*.py`) mock BigQuery and the Rabbit API and cover connection loading, the DAG whitelist, routing, fail-open, the dry-run guard and the routing retry.

**The end-to-end test** (`e2e/run_e2e.py`) starts a real Airflow with `airflow standalone` (scheduler and triggerer; on Airflow 3 also the API server and DAG processor), with the plugin installed from the package. It runs the DAG `e2e/dags/rabbit_e2e.py` against a fake BigQuery API and a fake Rabbit API (`e2e/fake_servers.py`), then checks the final state of every task and the exact jobs each task submitted. Each scenario runs once as a normal task and once as a deferrable task:

| Scenario | Expected |
| --- | --- |
| Job moved to on-demand | One job, on-demand, task succeeds |
| Moved job fails on a custom quota or billing tier limit | Retried once on the original configuration with `rabbit-routing-retry: applied`, task succeeds |
| The retry fails too | Task fails after exactly two jobs |
| SQL error | One job, no retry, task fails |
| Job Rabbit left in place fails on a quota | One job, no retry, task fails |
| Rabbit API returns an error | The original job is submitted, task succeeds |
| Job routed to a pool project | One job, in the pool project, task succeeds |

The DAG sets `allow_nested_operators=False`, so a task also fails if the plugin breaks Airflow's executor safeguard. Set `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` to a Postgres database to run the tasks in parallel, as CI and `test_in_docker.sh` do; the default SQLite database also works.

When you add behaviour, add a scenario: a marker in `e2e/fake_servers.py`, the marker in `SCENARIOS` in the DAG, and the expected result in `EXPECTED` in `e2e/run_e2e.py`.

### Manual Validation Steps

1. **Connection setup**
   - Create the `rabbit_api` connection as described above.
   - Verify the password is populated and (optionally) the base URL override is correct by running `airflow connections get rabbit_api`.

2. **Variable setup**
   - Set the `rabbit_bq_optimizer_config` variable using the sample JSON (without the API key).
   - Confirm `default_pricing_mode` is valid. For `"on_demand"`, include at least one reservation ID.

3. **DAG validation**
   - Deploy the plugin file to `$AIRFLOW_HOME/plugins`.
   - Restart the scheduler/webserver/workers so the patch is applied.
   - Trigger a DAG that uses `BigQueryInsertJobOperator` (or any BigQuery operator).

4. **Positive path (connection + variable present)**
   - Confirm in the scheduler logs at startup that the plugin logs `patching BigQueryHook.insert_job` and `patching BigQueryInsertJobOperator._submit_job`.
   - Validate that the BigQuery job request reaches Rabbit (check Rabbit logs, or set `"debug": true` in `rabbit_bq_optimizer_config` to see `Rabbit BQ Optimizer: optimization result=...` in the task log).
   - For a query with historical data/reservations, verify the resulting BigQuery job uses the optimized configuration.

5. **Missing connection safety net**
   - Temporarily rename/delete the Airflow connection and re-run the DAG.
   - The scheduler logs should show `Failed to load Rabbit API connection` and the job should proceed without optimization (ensures graceful fallback).

6. **Custom base URL regression**
   - Set `api_base_url` in the connection extras to a staging endpoint (or use schema/host).
   - Trigger a DAG and confirm via Rabbit logs/network traces that the request hits the custom endpoint.

7. **Compatibility check**
   - Ensure the environment installs the latest `rabbit-bq-job-optimizer` package (e.g., `pip install -U rabbit-bq-job-optimizer`).
   - Run unit tests or a smoke DAG run to confirm no API contract regressions with the service.

## Usage

The plugin works automatically with any BigQuery operator. No additional configuration is needed in your DAGs:

```python
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

# The plugin will automatically optimize this job
insert_job = BigQueryInsertJobOperator(
    task_id="insert_query_job",
    configuration={
        "query": {
            "query": "SELECT * FROM `project.dataset.table`",
            "useLegacySql": False
        }
    }
)
```

### Pool billing project routing

When the optimizer returns a pool billing project in
`optimizedJob.jobReference.projectId`, jobs submitted through
`BigQueryInsertJobOperator` run and poll on that project. The plugin briefly
links the operator to the hook during `_submit_job` so `operator.project_id`
and the BigQuery `jobs.insert` call use the same billing project.

Direct `BigQueryHook.insert_job` calls still receive the optimized
configuration but keep the original `project_id`. Use
`BigQueryInsertJobOperator` in DAGs when you need pool billing project
routing with deferrable tasks.

If the optimizer response has no `jobReference.projectId` (for example
reservation-only assignment), the operator keeps its configured source
`project_id`.

### Job labels

Optimized jobs include labels you can query in BigQuery
(`INFORMATION_SCHEMA.JOBS_BY_*`) to confirm how each submit was handled:

| Label | Values | Meaning |
|-------|--------|---------|
| `rabbit-source-project` | GCP project id | Project the job was submitted from (your DAG / operator `project_id`) |
| `rabbit-pool-project` | GCP project id | Pool billing project recommended by the optimizer (present only when a pool was assigned) |
| `rabbit-pool-routing` | `applied` \| `skipped` \| `none` | `applied`: `BigQueryInsertJobOperator` submitted on `rabbit-pool-project`; `skipped`: pool recommended but submit stayed on the source project (typically a direct hook call); `none`: no pool billing project in the optimizer response |
| `rabbit-routing-retry` | `applied` | The job is the routing retry: the first, Rabbit-routed job failed on an on-demand-only limit and this one runs on the original configuration (see [Routing retry](#routing-retry)) |

## Error Handling

The plugin includes comprehensive error handling for:
- Missing or invalid configuration
- Missing required fields
- Empty reservation IDs list when `default_pricing_mode` is `"on_demand"`
- API errors

In all error cases, the plugin will log a warning and proceed with the original job configuration.

### Routing retry

Some BigQuery limits apply only to on-demand jobs, so a job that Rabbit moved from a
reservation to on-demand can be accepted and then fail, although it would have succeeded
where it was. When that happens in a `BigQueryInsertJobOperator` task, the plugin runs the
job **once more with the original configuration** (original project, original reservation,
no optimizer call) and the task continues with that job. This works for both deferrable and
non-deferrable operators.

The retry happens only when both conditions hold:

- **Rabbit moved the job**: the optimized job has `reservation: none`, runs on a pool billing
  project, or was rewritten for statement-level routing.
- **The job failed on an on-demand-only limit**:
  - a custom query-usage quota (`QueryUsagePerDay`, `QueryUsagePerUserPerDay`), reported as
    `Custom quota exceeded`. BigQuery checks this quota against the up-front estimate, which
    can be far above what the job actually reads.
  - `billingTierLimitExceeded`, the on-demand CPU-per-byte limit.

Every other failure (SQL errors, permission errors, `backendError`, ...) surfaces as before,
and so does a failure of the retry itself: there is no second retry. Airflow task retries
are unaffected and go through the optimizer again.

The retry is logged as a warning (`Rabbit BQ Optimizer: re-routed job failed on an on-demand
limit ... Retrying once with the original configuration.`) and the retried job carries the
label `rabbit-routing-retry: applied`.

**Partial execution.** A multi-statement script can fail on statement N after statements 1
to N-1 already ran. The retry runs the whole script again from the start, exactly as an
Airflow task retry would, so scripts should be safe to re-run (idempotent).

Jobs submitted by calling `BigQueryHook.insert_job` directly, or by other operators, are not
covered: the plugin does not see their result. A direct call with `nowait=False` falls back
to the original configuration on any failure, as before.

## Troubleshooting

```
Broken plugin: `No module named 'rabbit_bq_job_optimizer'` — install `rabbit-bq-optimizer-airflow-plugin` (or `rabbit-bq-job-optimizer`) in the Airflow environment and restart.
```

If you see this error, it means the rabbit-bq-job-optimizer package is missing from your Airflow environment. Install it by adding the package (https://pypi.org/project/rabbit-bq-job-optimizer/) to your Airflow requirements or environment.

```
[2025-06-15, 17:25:20 UTC] {rabbit_bq_optimizer_plugin.py} WARNING - Rabbit BQ Optimizer: config error: rabbit_bq_optimizer_config not set or invalid. Using original job.
```

If you see this on job submit, the Airflow variable `rabbit_bq_optimizer_config` is not set or has invalid content. Make sure you have:

1. Created the variable named exactly `rabbit_bq_optimizer_config`
2. Set valid JSON content that includes all required fields:
   ```json
   {
       "enabled": true,
       "default_pricing_mode": "on_demand",
       "reservation_ids": [
           "project:us-central1.reservation-name1",
           "project:us-east1.reservation-name2"
       ]
   }
   ```
3. Verified the JSON is properly formatted without any syntax errors

## Development

### Pre-commit Hooks

This repository uses pre-commit hooks to ensure code quality. To set them up:

```bash
# Install pre-commit
pip install pre-commit

# Install the hooks
pre-commit install

# (Optional) Run hooks on all files
pre-commit run --all-files
```

The hooks will automatically:
- **Auto-fix** formatting with Black (CI checks formatting)
- **Auto-fix** linting issues with Ruff (CI checks linting)
- Check for common issues (trailing whitespace, large files, etc.)

**Note**: Pre-commit hooks auto-fix issues for convenience, while CI runs in check-only mode to ensure nothing slips through.

### CI/CD

GitHub Actions automatically runs on every push and pull request:
- **Linting and Formatting**: Checks code style with Black and Ruff
- **Tests**: Runs the unit tests and the end-to-end test on every supported Airflow version (see [Supported versions](#supported-versions)), on the oldest and newest Python of each major version
- **Latest Airflow**: One extra, non-blocking row installs the newest Airflow on PyPI. The workflow also runs weekly, so a new Airflow release that breaks the plugin shows up without a pull request

To support a new Airflow version, add a row to the matrix in `.github/workflows/ci.yml` and to the table above.

### Airflow 3 notes

- **Airflow 3.0 is not supported.** With the Google provider that Airflow 3.0 pins (17.1), deferrable `BigQueryInsertJobOperator` tasks never complete, with or without the plugin. Use Airflow 3.1 or later.
- **Task retries can reconnect to a running job.** With recent Google providers on Airflow 3, a retried task can reconnect to a BigQuery job that is still running instead of submitting a new one. The plugin only sees jobs at submit, so a reconnected job is not optimized again and the [routing retry](#routing-retry) does not apply to it.

## Uninstalling

1. Remove the plugin file from your Airflow plugins directory
2. Restart Airflow

## Support

For questions, API endpoint details, API key, or any issues, contact the Rabbit support team at success@followrabbit.ai
