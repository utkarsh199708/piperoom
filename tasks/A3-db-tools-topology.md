# A3 — Postgres + warehouse clients, DB tools, pipeline topology, record trace

Read `AGENTS.md` first. You own: `internal/clients/postgres/**`, `internal/clients/warehouse/**`,
`internal/tools/dbtools/**`, `internal/topology/**` (not `graph.go`), `internal/owners/**`.

## Goal

Facts about both ends of the pipeline (source Postgres, warehouse), plus the **pipeline map**
that ties all layers together and shows owners and health.

## 1. Clients (implement `internal/clients/clients.go`)

### `postgres` → `clients.SourceDB`
`New(ctx, url string) (*DB, error)` with `pgxpool`; `Close()`. Use a read-only session
(`SET default_transaction_read_only = on` in `AfterConnect`).
- `ReplicationSlots`: from `pg_replication_slots`; `RetainedWALBytes` =
  `pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)`.
- `RecentDDL`: from `piperoom_audit.ddl_log` (A1 creates it). If the table does not exist,
  return an error that says the DDL audit trigger is not installed (the tool shows this to the model).
- `LongTransactions`: `pg_stat_activity` where `xact_start < now() - $1`, excluding this backend;
  truncate `query` to 500 chars.
- `Row`: one row by primary key; identifiers validated + `pgx.Identifier.Sanitize()` (AGENTS.md rule 6).
  Values converted to JSON-friendly types (numeric → string, time → RFC 3339).

### `warehouse` → `clients.Warehouse`
Same style, Postgres-backed (the dev warehouse). Keep the SQL in one file so a Snowflake
implementation can be added later behind the same interface.

## 2. Owners (`internal/owners`)

Parse `dev/owners.yaml` (format in `tasks/A1-devstack.md` §2) with `gopkg.in/yaml.v3`:
`Load(path string) (*Owners, error)`, `Pipeline(id) (Pipeline, bool)`, `LayerOwner(layer) (string, bool)`.
Validate: known layers only (`events.Layers`), every pipeline has at least one source table and connector.

## 3. Topology (`internal/topology`) — implement `topology.Builder` from `graph.go`

`NewBuilder(o *owners.Owners, connect clients.ConnectClient, kafka clients.KafkaAdmin,
src clients.SourceDB, wh clients.Warehouse) *Builder`

`Build(ctx, pipelineID)`:
1. Nodes from `owners.yaml`: source tables, connectors, consumer groups, sink tables.
2. Topics derived from connector config: Debezium topic = `<topic.prefix>.<schema>.<table>` for each
   table in `table.include.list` that is in the pipeline.
3. Edges: table → connector → topic → each group → sink table.
4. Health + facts, gathered **in parallel** with a 5 s budget; a failing system makes its nodes
   `unknown`, never fails the whole build:
   - source table: slot for that connector (`slot.name` in config): `warn` if retained WAL > 1 GiB
     or slot inactive; facts `slot_retained`, `slot_active`.
   - connector: `fail` if connector or any task FAILED; `warn` if PAUSED/UNASSIGNED.
   - topic: fact `newest_record` ("N min ago"); health `ok` unless Kafka is unreachable
     (a quiet topic is not an error by itself — the connector and sink nodes show the problem).
   - group: `warn` if lag > 1000 or state Empty; facts `lag`, `state`.
   - sink table: freshness from `sink_ts_column`; `warn` > 10 min, `fail` > 30 min; fact `stale_for`.
   Put thresholds in one `Thresholds` struct with these defaults.

`Render(g)` — compact text for the model, one line per node in flow order, e.g.
```
pipeline orders
1 pg:public.orders [postgres, owner priya] WARN slot_retained=1.3 GiB slot_active=false
2 connect:pg-orders-connector [kafka-connect, owner arjun] FAIL tasks_failed=1
...
```

`Trace(ctx, pipelineID, table, key)` → `[]events.TraceStage`, one per stage:
source row (exists? `updated_at`), connector (task state; "stopped_here" if FAILED and the row is
newer than the failure), topic (`KafkaAdmin.FindByKey` since the row's `updated_at` − 1 min),
consumer group (committed offset vs found record offset → consumed or not), sink row (exists? values match?).
`status`: `seen | not_seen | stopped_here | unknown`. Exactly one stage is `stopped_here` when
the record is stuck. Never put full row values in `Facts` — only key, timestamps, offsets, and
the names of columns that differ.

## 4. Tools (`internal/tools/dbtools`) — all `tools.KindRead`

`func Register(reg *tools.Registry, d Deps) error`.

| Name | Input | Summary must say |
|---|---|---|
| `pg_replication_slots` | – | each slot: active?, retained WAL (human units), wal_status |
| `pg_recent_ddl` | `since` (RFC 3339), `table?` | DDL changes newest first: time, command, object, the SQL (≤ 300 chars), user |
| `pg_long_transactions` | `older_than_minutes` (default 5) | pid, user, state, age, query start |
| `pg_row` | `table`, `pk_column`, `key` | found or not, `updated_at`, column names (values only for the key and timestamps) |
| `warehouse_freshness` | `table`, `ts_column` | newest timestamp and "stale for N min" |
| `warehouse_row` | `table`, `pk_column`, `key` | found or not + timestamps |
| `pipeline_map` | `pipeline_id` | `Render` output |
| `trace_record` | `pipeline_id`, `table`, `key` | one line per stage with status, and which stage it stopped at |

## Tests

- Unit: fakes for every client interface; table tests for health rules, `Render`, topic derivation, `Trace` logic.
- Integration (tag `integration`): against the dev stack — slot exists for `pg-orders-connector`;
  `RecentDDL` sees an `ALTER TABLE` you run on a temp table; `Build("orders")` returns 7+ nodes
  with no `unknown`.

## Done when

On the `schema-change` scenario, `pipeline_map` shows the connector as FAIL and the warehouse as stale,
`pg_recent_ddl` shows the `ALTER TABLE ... amount TYPE numeric(12,2)`, and `trace_record` for a
new order says `stopped_here` at the connector.
