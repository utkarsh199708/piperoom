# P4 — Pipeline kinds: a room for the alerts data engineers actually get (Airflow, dbt, BigQuery, Databricks)

Owners: P4a A3 · P4b A4 · P4c C15 (new, cloud) · P4d C16 (new, cloud) · P4e C3 · P4f A6 · P4g A1 · P4h C7 + C6 · P4i C14 + C1.
Model: **Opus 5.5** for P4b (tool registration by kind and the per-kind prompt decide what the agent can see)
and P4c (the first non-CDC client and tools set the pattern); **Sonnet 5.5** for the rest.
Plan: `docs/product/fit-2026-10.md` §1.1, §2 row 4. Uses P0 items 3, 6, 8.

## Goal
`owners.yaml` can say

```yaml
pipelines:
  - id: daily-revenue
    kind: batch
    dags: [revenue_daily]
    dbt_project: /srv/dbt/analytics      # or dbt Cloud (config)
    models: [fct_revenue, dim_customer]
    sink_tables: [analytics.fct_revenue]
    sink_ts_column: loaded_at
layers:
  orchestrator: priya
  transform: priya
  warehouse: meera
```
and a "DAG `revenue_daily` failed" alert from Alertmanager or the Airflow callback webhook opens a room where the
agent reads the DAG run, the failed task's log tail, the dbt `run_results.json`, the model's lineage and the
warehouse freshness, and says which model broke and why. The lane shows **DAG → task → model → sink table**
instead of connector → topic → group. CDC rooms are unchanged.

## Evidence
- `internal/tools/**`: every tool is `pg_*`, `kafka_*`, `connect_*`, `schema_registry_*`, `debezium_*`,
  `warehouse_*`; `grep -rliE "airflow|dbt|spark|flink|bigquery|databricks"` over `*.go` finds nothing.
- `internal/wiring/wiring.go:312-343`: tools register per package from one `Wired` client set, unconditionally.
- `internal/topology/graph.go:19-38`: node kinds are the CDC ones; `events.Layers` (`events.go:850`) is the CDC six.
- `internal/wiring/warehouse.go`: `WAREHOUSE_KIND` selects Postgres or Snowflake; `WarehouseReader` is small.
- `internal/alertin/parse.go`: sources are Alertmanager, Grafana, PagerDuty, Datadog, generic; a DAG failure
  arrives today as a generic alert with no `pipeline_id` mapping for DAG names.

## Scope — what to build

1. **P4a (A3): kinds in owners and topology.** `internal/owners`: `Pipeline.Kind` (default `cdc`), the batch
   fields of P0 item 3, `Load` refuses fields of the other kind, `Pipeline.Layers() []string` =
   `events.LayersOf(kind)`. `internal/topology` (not `graph.go`): the builder draws a `batch` pipeline as
   `KindDAG` → `KindTask` (one per task instance of the latest run, state on the node) → `KindModel` (from
   `TransformReader.ModelLineage`, parents before children) → `KindSinkTable`; `EdgeData` throughout; readers
   that are nil or answer `ErrNotSupported` leave their nodes out and mark the graph `partial`. `internal/tools/scope.go`:
   a batch pipeline's scope is its `dags`, `models` and `sink_tables`; `pipeline_id` outside the room's is refused
   as today. `internal/alertin/pipeline.go` is C5's: ask in thread for a DAG-id → pipeline mapping
   (`dag_id` label or payload field → the pipeline whose `dags` lists it).
2. **P4b (A4): wiring and prompt by kind.** `wiring.RegisterTools` registers `dbtools`/`kafkatools`/
   `debeziumtools` only when some pipeline is `cdc`, `batchtools`/`dbttools` only when some is `batch`;
   `runbooks`, `similarincidents`, `changes` always. The agent's tool list per room is filtered by the room's
   pipeline kind (`Alert.PipelineKind`, P0 item 6) so a batch room never offers `kafka_read`. The system prompt
   gets a per-kind section: layers, what "healthy" means (last run success, freshness within SLA), the first
   three checks (DAG run state → failed task log → dbt result for the model), and the hypothesis layer names.
   `verify.go`: after a fix (none in this wave: the agent says what a person should run) verification for batch
   is freshness + last run state. `cmd/investigate` takes `-kind`.
3. **P4c (C15, new cloud agent): Airflow.** `internal/clients/airflow/**`: REST API v1 client (`/api/v1/dags/{id}/dagRuns`,
   `/dagRuns/{run}/taskInstances`, `/taskInstances/{task}/logs/{try}`), basic auth or token, one `*http.Client` with
   10 s timeout, 4 MB cap, `netsec.CheckPath`, 404 → `ErrNotFound`, log tail capped at `maxBytes` **from the end**
   (the API returns whole logs; read through `io.LimitReader` and keep the tail). Implements `OrchestratorReader`.
   `internal/tools/batchtools/**`: `dag_runs` (last N runs with state and duration), `task_instances` (one run),
   `task_log_tail` (untrusted: wrapped, redacted, injection-scanned like every read; Summary ≤ `MaxSummaryBytes`,
   full tail in `Data` under the Data cap), `dag_schedule_gap` (expected vs last successful run from the DAG's
   schedule). Scope: `dag_id` must be in the pipeline's `dags`.
4. **P4d (C16, new cloud agent): dbt.** `internal/clients/dbt/**`: artifacts reader (`run_results.json`,
   `manifest.json` from `DBT_ARTIFACTS_DIR`, re-read on each call, 16 MB cap, schema version checked) and dbt
   Cloud reader (`/api/v2/accounts/{a}/runs/?job_definition_id=`, then the run's artifacts) behind one
   `TransformReader`. `internal/tools/dbttools/**`: `dbt_run_results` (status per model, errors as untrusted
   text), `dbt_model_lineage`, `dbt_model_sql` (the compiled SQL of one model, ≤ 64 KiB, untrusted, read-only).
   Scope: `model` must be in `models` or a parent/child of one.
5. **P4e (C3): BigQuery and Databricks warehouse readers.** `internal/clients/bigquery/**` and
   `internal/clients/databricks/**` implementing `WarehouseReader` (+ `LoadErrorReader`, `CountReader` where the
   platform has it: BigQuery `INFORMATION_SCHEMA.JOBS` errors; Databricks `system.query` is optional) behind
   `WAREHOUSE_KIND=bigquery|databricks`; `WAREHOUSE_DB_URL` is the driver DSN; read-only comes from the
   credential (document it); identifiers quoted per dialect with the same `^[a-zA-Z_][a-zA-Z0-9_]*$` check, no
   raw SQL from tools. New modules: `cloud.google.com/go/bigquery` and `github.com/databricks/databricks-sql-go`,
   listed with versions and why in the PR (rule 10); unit tests with fakes, `integration` tests skipped unless
   the credentials env is set.
6. **P4f (A6): the lane and badges for batch.** `LaneTrace`/topology view renders `dag`, `task`, `model` nodes
   with their states; layer badges and the login card know `orchestrator` and `transform`; the new-room form
   shows the pipeline's kind; the "what changed" lane (F8) lists DAG and model changes when the feed has them.
   Nothing CDC-specific is removed.
7. **P4g (A1): dev stack `batch` profile.** `dev/docker-compose.yml` profile `batch`: Airflow (standalone image,
   one example DAG that writes `analytics.fct_revenue` in the warehouse Postgres) and a dbt project
   (`dbt-postgres`, two models, artifacts volume mounted at `DBT_ARTIFACTS_DIR`); `devctl break dag-failed`,
   `dbt-model-broken`, `late-batch` and their `fix`; `dev/owners.yaml` gains the `daily-revenue` pipeline;
   `internal/testenv` can start the profile.
8. **P4h (C7, C6): scenarios and evals.** C7: `evals/scenarios/dag-failed.yaml`, `dbt-model-broken.yaml`,
   `late-batch.yaml`, `backfill-overlap.yaml` with recorded tool outputs from P4g; C6: replay clients for the
   two new readers, `kind` in the scenario header, per-kind pass rate in the report; `evals/README.md`.
9. **P4i (C14, C1): docs.** C14: `docs/pilot/permissions.md` (Airflow read-only user, dbt Cloud read token,
   BigQuery `roles/bigquery.dataViewer` + `jobUser`, Databricks SQL warehouse `CAN USE` on a read-only
   catalog), `setup.md` reference rows, `data-handling.md` (task logs and compiled SQL reach the model, redacted);
   C1: `README.md` §Quickstart mentions `--kind batch` with Airflow, CI runs the unit tests of the new packages.

## Out of scope
- Write actions for batch (clear a task, re-run a DAG, `dbt run --select`): the next wave, after P3's pattern
  has run in a pilot. Spark, Flink, Fivetran, Airbyte, Iceberg: later kinds, same mechanism.
- A pipeline that is both kinds: one pipeline id per kind; a CDC pipeline feeding a batch one is two pipelines.

## Ownership and files
| Sub-task | Agent | Files |
|---|---|---|
| P4a | A3 | `internal/owners/**`, `internal/topology/**` (not `graph.go`), `internal/tools/scope.go` |
| P4b | A4 | `internal/wiring/**`, `internal/agent/**` (not `contract.go`), `cmd/investigate/**` |
| P4c | C15 | `internal/clients/airflow/**`, `internal/tools/batchtools/**` (new; C15 owns both) |
| P4d | C16 | `internal/clients/dbt/**`, `internal/tools/dbttools/**` (new; C16 owns both) |
| P4e | C3 | `internal/clients/bigquery/**`, `internal/clients/databricks/**` (new; C3 owns both) |
| P4f | A6 | `web/**` |
| P4g | A1 | `dev/**`, `cmd/devctl/**`, `internal/testenv/**` |
| P4h | C7, C6 | `evals/scenarios/*.yaml` (C7); `evals/**` else (C6) |
| P4i | C14, C1 | `docs/pilot/**` (C14); `README.md`, `.github/**` (C1) |
| registration, config reads | A4 | `wiring.go` lines for the new packages and `WAREHOUSE_KIND` cases: C15, C16, C3 ask in thread |

## Dependencies and merge order
P0 → **P4a** → P4b (needs `Kind`) → P4c, P4d, P4e in parallel (each against P0's interfaces with fakes; A4
adds the `Register` line when each lands) → P4g (needs real clients to record against) → P4f, P4h → P4i.
P1, P2, P3 are not dependencies, but P4 merges after them (vision order): a batch room gets the quickstart,
the ceremony and the driver-authored proposal for free.

## Done when
- [ ] `owners.yaml` with one `cdc` and one `batch` pipeline loads; a CDC room offers no `dag_*`/`dbt_*` tool and a
      batch room no `kafka_*`/`pg_*`/`connect_*` tool (`cmd/investigate -kind batch -list-tools`).
- [ ] Against the `batch` dev profile: `devctl break dbt-model-broken` → alert → room → the agent's hypothesis
      names layer `transform` and the model, with receipts (F3) on `dbt_run_results` and `task_log_tail`; the
      lane shows DAG → task → model → sink; `dag-failed` and `late-batch` likewise, layer `orchestrator`.
- [ ] `WAREHOUSE_KIND=bigquery` and `databricks` pass the `WarehouseReader` contract test suite (A3's, in
      `internal/clients/warehouse`) with fakes; the integration test runs when credentials are present.
- [ ] Evals: the four batch scenarios score with the same scorer; the CDC pass rate is unchanged.
- [ ] A task log containing `[verification]` text or an instruction to the agent is shown wrapped and flagged,
      never acted on (reuse the N5/N6 tests with a batch fixture).

## Tests
- C15, C16, C3: `httptest`/fixture-file unit tests including 404, timeout, oversized body, bad JSON, schema
  version mismatch; `integration` tests behind env credentials.
- A3: table tests for `Load` kind validation; topology builder tests with nil readers and `ErrNotSupported`.
- A4: tool-list-by-kind test; prompt snapshot per kind; evals green.
- A1: the `batch` profile starts in CI's integration job within the existing time budget or is marked optional.

## Effort
P4a 2 days · P4b 3 days · P4c 5 days · P4d 4 days · P4e 5 days · P4f 3 days · P4g 4 days · P4h 3 days · P4i 1.5 days.
