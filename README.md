This repository contains the source code for Rabbit's Airflow plugins, specifically focusing on BigQuery job optimization. These plugins are designed to enhance Apache Airflow's capabilities when working with BigQuery, providing optimized job execution and cost saving.

## Airflow 2 Plugin Highlights

- Patches the BigQuery hook to transparently send job configurations to the Rabbit Job Optimizer service.
- Authenticates via the official `rabbit-bq-job-optimizer` Python client (install the latest version for compatibility).
- Retrieves the Rabbit API key (and optional base URL override) from the Airflow connection `rabbit_api` so secrets stay out of plain-text variables.
- Reads optimization parameters (pricing mode, reservation IDs, etc.) from the `rabbit_bq_optimizer_config` Airflow variable. Optimization runs only when `"enabled": true` is set explicitly; otherwise BigQuery jobs are unchanged on each submit. Set `"statement_level": true` to opt into per-statement routing for multi-statement SCRIPTs (also requires the tenant's `bq_dynamic_pricing_statement_level` feature flag).

Refer to `bq-job-optimizer-airflow-2/README.md` for installation, configuration, and the end-to-end test plan.

## PyPI

The Airflow 2 plugin is published as [`rabbit-bq-optimizer-airflow-plugin`](https://pypi.org/project/rabbit-bq-optimizer-airflow-plugin/). Release with `./publish_to_pypi.sh` (same flow as the [python-bq-job-optimizer](https://github.com/followrabbit-ai/python-bq-job-optimizer) client).

For more information about Rabbit and our data platform, visit [https://followrabbit.ai/](https://followrabbit.ai/).
