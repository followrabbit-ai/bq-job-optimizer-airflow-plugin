# Changelog

All notable changes to the Rabbit BigQuery Job Optimizer Airflow plugin will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.0.1] - 2026-06-26

### Added
- `enabled` field on `rabbit_bq_optimizer_config` — optimization runs only when `"enabled": true` is set explicitly; otherwise jobs bypass the optimizer (no API call). Checked at the hook boundary before each optimize call (no restart required).

## [1.0.0] - 2026-06-24

### Added
- PyPI package `rabbit-bq-optimizer-airflow-plugin` with Airflow plugin entry point (no manual copy to `plugins/`).
- Pass resolved source `project_id` to the optimizer API for pool billing routing.

### Changed
- Pool billing routing for `BigQueryInsertJobOperator` via operator hook bridge.
- Empty or missing `reservation_ids` in config defaults to `[]` instead of skipping optimization.
