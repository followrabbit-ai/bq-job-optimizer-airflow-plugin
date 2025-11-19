# Rabbit BigQuery Job Optimizer Airflow Plugin

This Airflow plugin automatically optimizes BigQuery job configurations using the Rabbit API. It patches the BigQueryHook to intercept job configurations and optimize them before execution.

## Features

- Automatically intercepts all BigQuery job submissions
- Optimizes job configurations via the Rabbit API
- Graceful fallback to original configuration if optimization fails
- Comprehensive error handling and logging

## Installation

1. Add the dependency to your Airflow environment:
   - If using `requirements.txt`:
     ```bash
     echo "rabbit-bq-job-optimizer" >> requirements.txt
     pip install -r requirements.txt
     ```
   - If using `constraints.txt`:
     ```bash
     echo "rabbit-bq-job-optimizer" >> constraints.txt
     pip install -r constraints.txt
     ```
   - If using a custom Docker image, add to your Dockerfile:
     ```dockerfile
     RUN pip install rabbit-bq-job-optimizer
     ```

2. Add the plugin to your Airflow plugins directory:
```bash
cp rabbit_bq_optimizer_plugin.py $AIRFLOW_HOME/plugins/
```

3. Restart your Airflow webserver, scheduler, and workers to load the plugin.

## Configuration

### 1. Create the Rabbit API connection

For security reasons the Rabbit API key must be stored in an Airflow connection instead of a variable. The plugin expects a connection with ID `rabbit_api`:

- `Conn Type`: `Generic` (or `HTTP`)
- `Conn ID`: `rabbit_api`
- `Password`: Rabbit API key (required)
- `Extra` (optional):
  ```json
  {
    "api_base_url": "https://api.followrabbit.ai/bq-job-optimizer",
    "api_base_path": "v1" // only needed when host does not include the path
  }
  ```

If you prefer using the host/schema fields, set them to build the base URL (e.g., `schema=https`, `host=api.followrabbit.ai/bq-job-optimizer`). The plugin automatically falls back to the production endpoint when no override is provided.

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
   - Install Airflow and dependencies from `test_requirements.txt`
   - Initialize the Airflow database
   - Create an admin user
   - Deploy the plugin and test DAG
   - Configure the `rabbit_api` connection (with a dummy key)
   - Configure the `rabbit_bq_optimizer_config` variable

   *Note: The script installs Airflow with all its dependencies (including `google-re2`). The Google provider is installed with `--no-deps` to avoid pulling in unnecessary Google service libraries. Core packages installed: `apache-airflow`, `rabbit-bq-job-optimizer`, `apache-airflow-providers-google`, and `google-cloud-bigquery`.*

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
   - `Rabbit BQ Optimizer: Client initialized successfully`
   - `Rabbit BQ Optimizer: Received optimization result` (or failure if using dummy key/URL)

4. **Run automated tests:**
   After setup, you can run automated tests to validate the plugin:
   
   **Test connection loading:**
   ```bash
   source venv/bin/activate
   export AIRFLOW_HOME=$(pwd)/airflow_home
   python test_plugin_connection.py
   ```
   This verifies that the plugin can load credentials from the Airflow connection.
   
   **Test DAG execution (full integration):**
   ```bash
   source venv/bin/activate
   export AIRFLOW_HOME=$(pwd)/airflow_home
   python test_dag_execution.py
   ```
   This simulates a DAG execution and verifies that:
   - The plugin intercepts BigQuery jobs
   - The Rabbit optimization API is called with connection credentials
   - The optimized configuration is used

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
   - Confirm in the scheduler logs that the plugin logs `Client initialized successfully`.
   - Validate that the BigQuery job request reaches Rabbit (check Rabbit logs or look for the `Rabbit BQ Optimizer: Received optimization result` log line).
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

## Error Handling

The plugin includes comprehensive error handling for:
- Missing or invalid configuration
- Missing required fields
- Empty reservation IDs list
- API errors

In all error cases, the plugin will log a warning and proceed with the original job configuration.

## Troubleshooting

```
Broken plugin: [/home/airflow/gcs/plugins/rabbit_bq_optimizer_plugin.py] No module named 'rabbit_bq_job_optimizer'
```

If you see this error, it means the rabbit-bq-job-optimizer package is missing from your Airflow environment. Install it by adding the package (https://pypi.org/project/rabbit-bq-job-optimizer/) to your Airflow requirements or environment.

```
[2025-06-15, 17:25:20 UTC] {rabbit_bq_optimizer_plugin.py:26} WARNING - Rabbit BQ Optimizer: Configuration error: 'Variable rabbit_bq_optimizer_config does not exist'. Proceeding with original job configuration.
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


## Uninstalling

1. Remove the plugin file from your Airflow plugins directory
2. Restart Airflow

## Support

For questions, API endpoint details, API key, or any issues, contact the Rabbit support team at success@followrabbit.ai
