# Changelog

All notable changes to the Rabbit BigQuery Job Optimizer Airflow plugin will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.0.0] - 2026-06-24

### Added
- PyPI package `rabbit-bq-optimizer-airflow-plugin` with Airflow plugin entry point (no manual copy to `plugins/`).
- Pass resolved source `project_id` to the optimizer API for pool billing routing.

### Changed
- Pool billing routing for `BigQueryInsertJobOperator` via operator hook bridge.
- Empty or missing `reservation_ids` in config defaults to `[]` instead of skipping optimization.
