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

The plugin uses a single Airflow variable for configuration. Create a JSON configuration with the following structure:

```json
{
    "api_key": "your-rabbit-api-key",
    "default_pricing_mode": "on_demand",
    "reservation_ids": [
        "project:region.reservation-name1",
        "project:region.reservation-name2"
    ]
}
```

### Configuration Fields

- `api_key` (required): Your Rabbit API key
- `default_pricing_mode` (required): The default pricing mode for jobs. Must be one of: `"on_demand"` or `"slot_based"`
- `reservation_ids` (required): List of reservation IDs in the format "project:region.reservation-name"

### Setting the Configuration

You can set the configuration in two ways:

1. Using the Airflow CLI:
```bash
airflow variables set rabbit_bq_optimizer_config '{
    "api_key": "your-rabbit-api-key",
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
       "api_key": "your-rabbit-api-key", 
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
