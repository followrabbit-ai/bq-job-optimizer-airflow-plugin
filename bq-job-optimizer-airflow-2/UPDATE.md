# Updating the Rabbit BQ Optimizer Plugin

You already have the Rabbit BigQuery Job Optimizer Airflow plugin installed and need the version with **pool billing routing**.

> [!NOTE]
> Full install and configuration: [README.md](README.md).

## What changed

- Jobs submitted through `BigQueryInsertJobOperator` can run on an on-demand pool billing project (including deferrable tasks).
- Optimized jobs get labels: `rabbit-source-project`, `rabbit-pool-project`, `rabbit-pool-routing`.

## Download the plugin

```bash
curl -fsSL -o rabbit_bq_optimizer_plugin.py \
  https://raw.githubusercontent.com/followrabbit-ai/bq-job-optimizer-airflow-plugin/master/bq-job-optimizer-airflow-2/rabbit_bq_optimizer_plugin.py
```

Or browse the [plugin repository](https://github.com/followrabbit-ai/bq-job-optimizer-airflow-plugin/tree/master/bq-job-optimizer-airflow-2).

## Cloud Composer

### 1. Upload the new plugin

Find your bucket:

```bash
gcloud composer environments describe YOUR_ENV \
  --location=YOUR_REGION \
  --project=YOUR_PROJECT \
  --format="value(config.dagGcsPrefix)"

# Example: `gs://europe-west3-composer-xxxxx-bucket/dags`
# → bucket is `gs://europe-west3-composer-xxxxx-bucket`.
```

Upload:

```bash
gsutil cp rabbit_bq_optimizer_plugin.py \
  gs://YOUR_COMPOSER_BUCKET/plugins/rabbit_bq_optimizer_plugin.py
```

> [!NOTE]
> If you deploy Composer via Terraform or CI, update `airflow/plugins/rabbit_bq_optimizer_plugin.py` in that repo instead.

### 2. Wait for sync

Wait about **2–5 minutes**. A full environment rebuild is not required.

```bash
gcloud composer environments describe YOUR_ENV \
  --location=YOUR_REGION \
  --project=YOUR_PROJECT \
  --format="value(state)"

# State should be `RUNNING`.
```

### 3. Verify

**Plugin loaded** — in Cloud Logging (scheduler or worker), look for at startup:

```text
Rabbit BQ Optimizer: patching BigQueryHook.insert_job
Rabbit BQ Optimizer: patching BigQueryInsertJobOperator._submit_job
```

**Connection and variable** — should still be present:

```bash
gcloud composer environments run YOUR_ENV \
  --location=YOUR_REGION --project=YOUR_PROJECT \
  connections -- get rabbit_api

gcloud composer environments run YOUR_ENV \
  --location=YOUR_REGION --project=YOUR_PROJECT \
  variables -- get rabbit_bq_optimizer_config
```

**Smoke test** — trigger a DAG with `BigQueryInsertJobOperator`:

```bash
gcloud composer environments run YOUR_ENV \
  --location=YOUR_REGION --project=YOUR_PROJECT \
  dags trigger -- YOUR_DAG_ID
```

The task should succeed as before. If optimization fails, the plugin still submits the original job (fail-open).

If logs show `No module named 'rabbit_bq_job_optimizer'`, install `rabbit-bq-job-optimizer` per the [README](README.md#installation).

---

## Airflow

### 1. Replace the plugin file

```bash
cp rabbit_bq_optimizer_plugin.py "$AIRFLOW_HOME/plugins/rabbit_bq_optimizer_plugin.py"
```

> [!NOTE]
> If plugins live in a Docker image or Kubernetes volume, update the file there and redeploy.

### 2. Restart components

Restart scheduler and workers. Restart the triggerer too if you use deferrable `BigQueryInsertJobOperator`.

```bash
docker compose restart airflow-scheduler airflow-worker airflow-triggerer
```

### 3. Verify

**Plugin loaded** — in scheduler, worker, or triggerer logs at startup:

```text
Rabbit BQ Optimizer: patching BigQueryHook.insert_job
Rabbit BQ Optimizer: patching BigQueryInsertJobOperator._submit_job
```

**Connection and variable:**

```bash
airflow connections get rabbit_api
airflow variables get rabbit_bq_optimizer_config
```

**Smoke test:**

```bash
airflow dags trigger YOUR_DAG_ID
```

The task should succeed as before. If optimization fails, the plugin still submits the original job (fail-open).

If logs show `No module named 'rabbit_bq_job_optimizer'`, install `rabbit-bq-job-optimizer` per the [README](README.md#installation).

---

## Pool routing (optional)

Skip this section if your account does not use on-demand pools.

**Requirements**

- DAGs must use `BigQueryInsertJobOperator` (not direct `BigQueryHook` calls) — [details](README.md#pool-billing-project-routing)
- Airflow service account needs `roles/bigquery.jobUser` on pool billing projects — contact Rabbit support for your project list
- Reservation IAM is unchanged

**Check routed jobs** in BigQuery (replace `YOUR_POOL_PROJECT` and region, e.g. `region-eu`):

```sql
SELECT
  job_id,
  project_id,
  (SELECT value FROM UNNEST(labels) WHERE key = 'rabbit-pool-routing') AS pool_routing,
  (SELECT value FROM UNNEST(labels) WHERE key = 'rabbit-source-project') AS source_project,
  (SELECT value FROM UNNEST(labels) WHERE key = 'rabbit-pool-project') AS pool_project
FROM `YOUR_POOL_PROJECT.region-eu.INFORMATION_SCHEMA.JOBS_BY_PROJECT`
WHERE creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
  AND EXISTS (
    SELECT 1 FROM UNNEST(labels) AS l
    WHERE l.key = 'rabbit-pool-routing' AND l.value = 'applied'
  )
ORDER BY creation_time DESC
LIMIT 10;
```

| Label value | Meaning |
| --- | --- |
| `applied` | Job submitted on the pool billing project |
| `skipped` | Pool recommended; job stayed on source project |
| `none` | No pool assigned |

More on labels: [README](README.md#job-labels).

---

## Rollback

Put back your previous `rabbit_bq_optimizer_plugin.py` and reload (Composer: re-upload and wait; Airflow: restart components). Connection, variable, and DAGs are unchanged.

## Support

**success@followrabbit.ai**
