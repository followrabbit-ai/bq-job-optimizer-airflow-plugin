# Rabbit BigQuery Job Optimizer Airflow Plugin

This Airflow plugin automatically optimizes BigQuery job configurations using the Rabbit API. It patches the BigQueryHook to intercept job configurations and optimize them before execution.

## Features

- Automatically intercepts all BigQuery job submissions
- Optimizes job configurations via the Rabbit API
- Routes `BigQueryInsertJobOperator` submits to an on-demand pool billing project when the optimizer sets `optimizedJob.jobReference.projectId`
- Graceful fallback to original configuration if optimization fails
- Comprehensive error handling and logging

Already running an older version? See [Updating](#updating).

## Installation

### Option A: PyPI (recommended)

1. Add the plugin and client to your Airflow environment:
   - If using `requirements.txt`:
     ```txt
     rabbit-bq-job-optimizer==0.1.18
     rabbit-bq-optimizer-airflow-plugin==1.0.0
     ```
   - If using `constraints.txt`:
     ```txt
     rabbit-bq-job-optimizer==0.1.18
     rabbit-bq-optimizer-airflow-plugin==1.0.0
     ```
   - If using a custom Docker image, add to your Dockerfile:
     ```dockerfile
     RUN pip install rabbit-bq-job-optimizer==0.1.18 rabbit-bq-optimizer-airflow-plugin==1.0.0
     ```

   The plugin registers via Airflow's plugin entry point — no file copy into `plugins/` is required.

2. Restart your Airflow scheduler, workers, and webserver to load the plugin. If you use deferrable `BigQueryInsertJobOperator`, restart the triggerer as well.

### Option B: Copy into `plugins/`

1. Add the Python client to your Airflow environment dependencies:

   ```txt
   rabbit-bq-job-optimizer==0.1.18
   ```

2. Copy the plugin file into your Airflow plugins directory:
   ```bash
   cp bq-job-optimizer-airflow-2/rabbit_bq_optimizer_plugin.py "$AIRFLOW_HOME/plugins/"
   ```

3. Restart your Airflow scheduler, workers, and webserver (and triggerer if using deferrable operators).

## Updating

### PyPI install

Update to the new package versions in your environment dependencies:

```txt
rabbit-bq-job-optimizer==0.1.18
rabbit-bq-optimizer-airflow-plugin==1.0.0
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
    "default_pricing_mode": "on_demand",
    "reservation_ids": [
        "project:region.reservation-name1",
        "project:region.reservation-name2"
    ]
}
```

### Configuration Fields

- `default_pricing_mode` (required): The default pricing mode for jobs. Must be one of: `"on_demand"` or `"slot_based"`
- `reservation_ids` (required): List of reservation IDs in the format "project:region.reservation-name"

### Setting the Configuration

You can set the configuration in two ways:

1. Using the Airflow CLI:
```bash
airflow variables set rabbit_bq_optimizer_config '{
    "default_pricing_mode": "on_demand",
    "reservation_ids": [
        "project:region.reservation-name1",
        "project:region.reservation-name2"
    ]
}'
```

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
   - `Rabbit BQ Optimizer: optimization result=...` at DEBUG on job submit (or failure if using dummy key/URL)

4. **Run automated tests:**
   After setup, you can run automated tests to validate the plugin:

   **Test connection loading:**
   ```bash
   source venv/bin/activate
   export AIRFLOW_HOME=$(pwd)/airflow_home
   cd bq-job-optimizer-airflow-2
   python test_plugin_connection.py
   ```
   This verifies that the plugin can load credentials from the Airflow connection.

   **Test DAG execution (full integration):**
   ```bash
   source venv/bin/activate
   export AIRFLOW_HOME=$(pwd)/airflow_home
   cd bq-job-optimizer-airflow-2
   python test_dag_execution.py
   ```
   This simulates a DAG execution and verifies that:
   - The plugin intercepts BigQuery jobs
   - The Rabbit optimization API is called with connection credentials
   - The optimized configuration is used

   **Test plugin optimization (unit, no real BQ):**
   ```bash
   source venv/bin/activate
   export AIRFLOW_HOME=$(pwd)/airflow_home
   python -m unittest bq-job-optimizer-airflow-2/test_plugin_optimization.py -v
   ```

5. **Cleanup (optional):**
   To remove the test environment (venv and airflow_home directories):
   ```bash
   ./setup_local_test.sh --clean
   ```
   Or use the short form:
   ```bash
   ./setup_local_test.sh -c
   ```

### Manual Validation Steps

1. **Connection setup**
   - Create the `rabbit_api` connection as described above.
   - Verify the password is populated and (optionally) the base URL override is correct by running `airflow connections get rabbit_api`.

2. **Variable setup**
   - Set the `rabbit_bq_optimizer_config` variable using the sample JSON (without the API key).
   - Include at least one reservation ID and confirm the `default_pricing_mode` is valid.

3. **DAG validation**
   - Deploy the plugin file to `$AIRFLOW_HOME/plugins`.
   - Restart the scheduler/webserver/workers so the patch is applied.
   - Trigger a DAG that uses `BigQueryInsertJobOperator` (or any BigQuery operator).

4. **Positive path (connection + variable present)**
   - Confirm in the scheduler logs at startup that the plugin logs `patching BigQueryHook.insert_job` and `patching BigQueryInsertJobOperator._submit_job`.
   - Validate that the BigQuery job request reaches Rabbit (check Rabbit logs or enable DEBUG for `Rabbit BQ Optimizer: optimization result=...`).
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

## Error Handling

The plugin includes comprehensive error handling for:
- Missing or invalid configuration
- Missing required fields
- Empty reservation IDs list
- API errors

In all error cases, the plugin will log a warning and proceed with the original job configuration.

## Troubleshooting

```
Broken plugin: `No module named 'rabbit_bq_job_optimizer'` — install `rabbit-bq-optimizer-airflow-plugin` (or `rabbit-bq-job-optimizer`) in the Airflow environment and restart.
```

If you see this error, it means the rabbit-bq-job-optimizer package is missing from your Airflow environment. Install it by adding the package (https://pypi.org/project/rabbit-bq-job-optimizer/) to your Airflow requirements or environment.

```
[2025-06-15, 17:25:20 UTC] {rabbit_bq_optimizer_plugin.py:82} WARNING - Rabbit BQ Optimizer: config error: 'Variable rabbit_bq_optimizer_config does not exist'. Using original job.
```

If you see this error, it means the Airflow variable `rabbit_bq_optimizer_config` is not set or has invalid content. Make sure you have:

1. Created the variable named exactly `rabbit_bq_optimizer_config`
2. Set valid JSON content that includes all required fields:
   ```json
   {
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
- **Tests**: Runs test suite on Python 3.9, 3.10, 3.11, and 3.12

## Uninstalling

1. Remove the plugin file from your Airflow plugins directory
2. Restart Airflow

## Support

For questions, API endpoint details, API key, or any issues, contact the Rabbit support team at success@followrabbit.ai
