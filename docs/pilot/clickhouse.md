# ClickHouse warehouse (pilot)

Piperoom can point the warehouse at ClickHouse instead of the default Postgres (or
Snowflake). This page is the pilot guide: how to configure it, the least-privilege reader to
create, what the agent can read, and the limits you will hit.

The ClickHouse client is read-only by design: it issues only `SELECT` queries and forces a
read-only session (`internal/clients/clickhouse/client.go`; `internal/clients/clickhouse/README.md`).

## Configure

Set two variables. `WAREHOUSE_KIND` selects the warehouse client
(`internal/wiring/warehouse.go`), and `WAREHOUSE_DB_URL` is then a clickhouse-go/v2 DSN
(`internal/config/config.go`):

    WAREHOUSE_KIND=clickhouse
    WAREHOUSE_DB_URL=clickhouse://piperoom_reader:<password>@clickhouse.example:9000/analytics?secure=true

`WAREHOUSE_DB_URL` uses the clickhouse-go/v2 DSN format when `WAREHOUSE_KIND=clickhouse`
(`internal/config/config.go`; `internal/wiring/warehouse.go`). The client parses it with the
clickhouse-go/v2 driver and, whatever the DSN says, forces these settings
(`internal/clients/clickhouse/client.go`):

- `readonly = 2`, so writes and DDL are blocked while the driver can still apply its per-query
  `max_execution_time` from the query's context deadline.
- `max_result_bytes = 4 MiB`, so one oversized row cannot turn a warehouse lookup into an
  unbounded result handed to the agent.
- The driver's debug logger is forced off, so bound query bodies (record keys and values) are
  never printed.

The client pings the server when it opens, so a wrong DSN stops the start, and it uses a
10-second deadline on connect, read, ping and each query (`internal/clients/clickhouse/client.go`).

The DSN holds the password. It is never logged, and errors are scrubbed of the DSN and of its
password or token (`internal/clients/clickhouse/redact.go`; `internal/wiring/warehouse.go`).
Use a placeholder or your secret manager for the password; do not put a real one in a config file.

## Least-privilege reader

Create a dedicated user whose settings profile is `readonly = 2`, and grant it only `SELECT` on
the sink tables. The reference SQL is `dev/clickhouse/init/01-reader.sql`:

```sql
CREATE SETTINGS PROFILE piperoom_reader SETTINGS readonly = 2;
CREATE USER piperoom_reader IDENTIFIED WITH sha256_password BY '<generated>'
    SETTINGS PROFILE piperoom_reader;
GRANT SELECT ON analytics.* TO piperoom_reader;          -- the pipeline's sink tables
GRANT SELECT ON system.query_log TO piperoom_reader;      -- optional load-error history
```

**Use `readonly = 2`, never `readonly = 1`.** The client sends `readonly = 2` and the driver's
per-query `max_execution_time` with every query (`internal/clients/clickhouse/client.go`). A
profile with `readonly = 1` rejects those per-query settings, so every real query fails with
ClickHouse error code 164 (`READONLY`), while the start-up ping still succeeds because it sends
no settings. The client reports code 164 as a `readonly=1` profile error
(`internal/clients/clickhouse/errors.go`; `internal/clients/clickhouse/README.md`). `readonly = 2`
blocks writes and DDL and still allows the timeout setting.

The session setting is defence in depth, not the real boundary: give the DSN's user only the
`SELECT` grants above, matching the pipeline's `sink_tables` in `owners.yaml`
(`dev/clickhouse/init/01-reader.sql`; `internal/clients/clickhouse/README.md`).

For the optional load-error tool, `system.query_log` is enough for query-log history. Also grant
`SELECT` on `system.kafka_consumers` if you want the Kafka-engine consumer error history
(`internal/clients/clickhouse/loads.go`). A missing `system.kafka_consumers` grant, or a build
without Kafka support, skips only that history; query-log results are still returned
(`loads.go`).

## What works

The client implements the full read-only warehouse interface (`clients.Warehouse`) plus the two
optional readers (`internal/clients/clickhouse/client.go` asserts all three):

- **`warehouse_freshness`** runs `maxOrNull(<ts_column>)` and returns the value in UTC.
  `DateTime` and `DateTime64` keep their instant and fractional precision; an empty table returns
  the zero time (`internal/clients/clickhouse/warehouse.go`; `sql.go`).
- **`warehouse_row`** selects the row whose primary-key column matches, using `FINAL` first so a
  `ReplacingMergeTree` sink shows the current deduplicated row before background merges run. If
  the engine rejects `FINAL`, the client retries a plain `SELECT`, which supports engines such as
  `MergeTree` but does not deduplicate pending versions (`internal/clients/clickhouse/warehouse.go`;
  `sql.go`).
- **`warehouse_load_errors`** reads `system.query_log` for failed or partial `INSERT`s into one
  sink table and `system.kafka_consumers` for Kafka-engine consumer exceptions
  (`internal/clients/clickhouse/loads.go`).
- **`reconcile`** uses `CountByBucket`, which counts rows in `toStartOfInterval` buckets anchored
  to the requested `since` time, at most 200 buckets, and never reads row values
  (`internal/clients/clickhouse/counts.go`; `sql.go`).

Table names are `table` or `database.table`; each part must match `^[A-Za-z_][A-Za-z0-9_]*$`.
Names are quoted with backticks and values use bound parameters
(`internal/clients/clickhouse/identifier.go`; `query.go`).

## Known limits

- **The client only issues `SELECT`.** Identifiers that need special-character quoting,
  three-part names, and subsecond count bucket widths are rejected
  (`internal/clients/clickhouse/identifier.go`; `counts.go`).
- **A `MergeTree`-family sink is required.** `FINAL` row lookup needs a `ReplacingMergeTree` (or
  another engine that accepts `FINAL`); the plain-`SELECT` fallback supports `MergeTree` but does
  not deduplicate pending versions (`internal/clients/clickhouse/warehouse.go`; `README.md`).
- **Load history needs a database in the DSN or a `db.table` input.** With a bare table name and
  no database in the DSN, `LoadErrors` returns an error (`internal/clients/clickhouse/loads.go`).
- **Load history is local to the queried node and bounded by query-log retention.** Query-log
  results come only from the server node that answers the query, and the history is limited by
  the server's query-log retention (`internal/clients/clickhouse/README.md`).
- **Kafka-engine errors describe the Kafka engine table, not the downstream sink.** In a
  Kafka engine → materialized view → `MergeTree` hop, the consumer exception belongs to the Kafka
  engine table; the client does not map it through the view to the sink table
  (`internal/clients/clickhouse/README.md`; `dev/clickhouse/README.md`).
- **Raw query and exception text are never returned.** `LoadErrors` selects only exception codes
  and write counts from `query_log`; raw query text and exception text can contain record values,
  so neither is returned. `first_error` holds only a generic category or a numeric ClickHouse
  error code (`internal/clients/clickhouse/loads.go`).
- **Primary keys on numeric columns must be supplied as valid numeric strings**
  (`internal/clients/clickhouse/README.md`).

## Try it locally

The optional dev stack (`dev/clickhouse/README.md`) starts a ClickHouse profile with the
`piperoom_reader` user and `readonly = 2` profile above, and builds the Kafka engine table and
materialized view for the demo `orders` sink. Its native port is `localhost:9001`, bound to
loopback, and its credentials are fixed local test credentials that must not be reused outside
the dev stack (`dev/clickhouse/README.md`; `dev/clickhouse/init/01-reader.sql`).
