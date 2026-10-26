# Batch pipelines (Airflow + dbt)

A batch pipeline (Airflow → dbt → warehouse) uses the same quickstart as a CDC pipeline
([existing pipeline](existing-pipeline.md)). There is no `--kind` flag:
the kind is `kind: batch` on the pipeline in `owners.yaml` (`internal/owners/kind.go`). `discover`
only drafts Debezium pipelines (`internal/discover`), so write the batch `owners.yaml` by hand.
`make quickstart` keeps an existing file and skips `discover` (`Makefile`, target `quickstart`).
Save this as `owners.yaml`, with your user id (any id; the server writes a one-user users file
for it) and your DAG and model names:

```yaml
pipelines:
  - id: daily-revenue
    title: Daily revenue (Airflow + dbt)
    kind: batch
    dags: [revenue_daily]                 # Airflow dag_ids the tools may read
    models: [stg_orders, fct_revenue]     # dbt models the tools may read
    sink_tables: [analytics.fct_revenue]  # the only tables warehouse_row may read
    sink_ts_column: loaded_at             # required with sink_tables
layers:                                   # the three batch layers
  orchestrator: me
  transform: me
  warehouse: me
```

Then point Piperoom at Airflow and dbt, and start as above:

```sh
export AIRFLOW_URL=https://airflow.example.com       # webserver root, without /api/v1
export AIRFLOW_TOKEN=...                             # or AIRFLOW_USERNAME + AIRFLOW_PASSWORD; https only
export DBT_ARTIFACTS_DIR=/abs/path/to/dbt/target     # run_results.json + manifest.json; absolute path
export WAREHOUSE_DB_URL=postgres://...               # WAREHOUSE_KIND picks snowflake, bigquery, databricks, clickhouse
make quickstart
```

- A batch-only owners file opens no Kafka, Connect, Schema Registry or source Postgres client, but the
  warehouse is still pinged at start (`internal/wiring/wiring.go`, `openClientsWith`; checked by starting
  `--quickstart` on this file with an unreachable warehouse: it stops at the warehouse ping).
- Instead of `DBT_ARTIFACTS_DIR`, dbt Cloud: `DBT_CLOUD_URL`, `DBT_CLOUD_TOKEN`, `DBT_CLOUD_ACCOUNT_ID`,
  `DBT_CLOUD_JOB_ID`; not both ([`docs/pilot/setup.md`](../pilot/setup.md), "Batch pipeline readers"). An unset
  reader leaves its tools answering "not configured".
- Airflow needs only a `Viewer` role and `GET` calls (Airflow 2.1+, REST API v1); the dbt, BigQuery and
  Databricks roles are in [`docs/pilot/permissions.md`](../pilot/permissions.md). Task logs and compiled SQL reach
  the model, redacted: [`docs/pilot/data-handling.md`](../pilot/data-handling.md).
- Send a test alert with `"pipeline_id":"daily-revenue"` (`curl` in [`docs/pilot/quickstart.md`](../pilot/quickstart.md)).
- No Airflow yet? `docker compose -f dev/docker-compose.yml --profile batch up -d --build --wait` starts one
  with a dbt project, and `go run ./cmd/devctl break dag-failed --owner <tag>` (also `dbt-model-broken`, `late-batch`;
  take the lock first, see [the dev stack](../development.md#the-dev-stack)) breaks it: [`dev/batch.md`](../../dev/batch.md).
