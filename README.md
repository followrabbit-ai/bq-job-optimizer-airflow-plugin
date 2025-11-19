This repository contains the source code for Rabbit's Airflow plugins, specifically focusing on BigQuery job optimization. These plugins are designed to enhance Apache Airflow's capabilities when working with BigQuery, providing optimized job execution and cost saving.

## Airflow 2 Plugin Highlights

- Patches the BigQuery hook to transparently send job configurations to the Rabbit Job Optimizer service.
- Authenticates via the official `rabbit-bq-job-optimizer` Python client (install the latest version for compatibility).
- Retrieves the Rabbit API key (and optional base URL override) from the Airflow connection `rabbit_api` so secrets stay out of plain-text variables.
- Reads optimization parameters (pricing mode, reservation IDs, etc.) from the `rabbit_bq_optimizer_config` Airflow variable.

Refer to `bq-job-optimizer-airflow-2/README.md` for installation, configuration, and the end-to-end test plan.

For more information about Rabbit and our data platform, visit [https://followrabbit.ai/](https://followrabbit.ai/).