# Snowflake warehouse client

Implements `clients.Warehouse` for Snowflake. Piperoom uses it for
`warehouse_freshness`, `warehouse_row`, the sink health in `pipeline_map`, and the
warehouse leg of `trace_record`.

## Turn it on

```
WAREHOUSE_KIND=snowflake
WAREHOUSE_DB_URL='svc_piperoom:<password>@myorg-myaccount/ANALYTICS/PUBLIC?warehouse=PIPEROOM_WH&role=PIPEROOM_RO'
```

`WAREHOUSE_DB_URL` is a [gosnowflake DSN](https://pkg.go.dev/github.com/snowflakedb/gosnowflake#hdr-Connection_String):
`user:password@account/database/schema?warehouse=...&role=...`. `WAREHOUSE_KIND`
defaults to `postgres`. Any value other than `postgres` or `snowflake` stops the
server at startup. The DSN holds the password: it is never logged, and startup errors
are scrubbed of it (`snowflake.Redact`).

If the password has `@`, `/` or `:` in it, URL-encode it.

## Grants

The client does not enforce read-only. The role in the DSN does. Give it only
`USAGE` on the warehouse, database and schema, and `SELECT` on each sink table. The
exact SQL is in [docs/pilot/permissions.md](../../../docs/pilot/permissions.md)
(Warehouse, Snowflake section).

## How names and queries work

- `sink_tables` and `sink_ts_column` in `owners.yaml` are the only inputs. Use
  `schema.table`, or `db.schema.table` for a table outside the DSN's database.
- Every part must match `^[A-Za-z_][A-Za-z0-9_$]*$` (AGENTS.md section 4, rule 6). Anything else is
  refused before a query runs.
- Snowflake stores unquoted names in upper case. The client folds each part to upper
  case and then double-quotes it, so `analytics.orders` runs as
  `"ANALYTICS"."ORDERS"`. Reserved words such as `ORDER` or `TIMESTAMP` work.
  A table or column that was created with quotes and a lower- or mixed-case name
  cannot be reached.
- Freshness is `SELECT MAX("TS") FROM "SCHEMA"."TABLE"`, returned in UTC.
  `TIMESTAMP_NTZ` is read as UTC wall time. `TIMESTAMP_LTZ` and `TIMESTAMP_TZ` are exact instants.
  The column must be a timestamp type.
- Row lookups use a `?` bind parameter for the key and return column names in lower
  case, like the Postgres client.
- Every call has a 10 second deadline. A missing table, or one the role cannot see,
  maps to `clients.ErrNotFound`.

Not supported: key-pair or OAuth setup helpers (put those options in the DSN as the driver
documents), and tables whose names need quoting.
