# Databricks warehouse client

Implements `clients.Warehouse` (`Freshness`, `Row`), `clients.CountReader`
(`CountByBucket`) and `PrimaryKeyColumn` against a Databricks SQL warehouse on Unity
Catalog, with [`databricks-sql-go`](https://github.com/databricks/databricks-sql-go).
Piperoom uses it for `warehouse_freshness`, `warehouse_row`, reconcile counts, the sink
health in `pipeline_map` and the warehouse leg of `trace_record`. `LoadErrorReader` is not
implemented (the task file makes `system.query` optional); `warehouse_load_errors` answers
`ErrNotSupported`.

## Turn it on

```
WAREHOUSE_KIND=databricks
WAREHOUSE_DB_URL='token:<personal-access-token>@dbc-1234abcd-5678.cloud.databricks.com:443/sql/1.0/warehouses/1234567890abcdef?catalog=main'
```

`WAREHOUSE_DB_URL` is the driver's DSN form, restricted to what a read-only reader needs:

```
token:<personal-access-token>@<host>[:<port>]/<http-path>?catalog=<catalog>[&schema=<schema>]
<host>[:<port>]/<http-path>?catalog=<catalog>&authType=OAuthM2M&clientId=<application-id>&clientSecret=<secret>
```

| Part | Meaning |
|---|---|
| `token:<pat>@` | Personal access token of a user or service principal (`accessToken=<pat>` works too). URL-escape it if it holds `/`, `@` or `%`. |
| `<host>` | Workspace host, e.g. `dbc-....cloud.databricks.com` or `adb-....azuredatabricks.net`. An `https://` prefix is allowed; `http://` is refused. |
| `<port>` | Default 443. |
| `<http-path>` | The SQL warehouse's HTTP path (Connection details tab), e.g. `/sql/1.0/warehouses/<id>`. |
| `catalog` | **Required.** Unity Catalog catalog holding the sink tables. `sink_tables` in `owners.yaml` are `schema.table` in it. |
| `schema` | Optional initial schema; tables are always schema-qualified, so it changes nothing. |
| `authType=OAuthM2M`, `clientId`, `clientSecret` | OAuth machine-to-machine login of a service principal instead of a token. |

Anything else is refused at startup: other parameters (the driver would pass them on as
session parameters), a user name other than `token`, OAuth U2M (it opens a browser), a
repeated parameter, a `#fragment`. Error messages never quote the DSN or any part of it;
`databricks.Redact(err, dsn)` scrubs the token and client secret from any error text.

The DSN holds the credential. Keep it in a secret store, not in a file in the repository.

## Grants (read-only comes from these)

The driver has no read-only session switch: read-only is enforced by the principal the
token or service principal belongs to. Give it exactly:

| Grant | On | Why |
|---|---|---|
| `CAN USE` | the SQL warehouse | run statements (without it every call is HTTP 403, `ErrPermissionDenied`) |
| `USE CATALOG` | the catalog | resolve `catalog.schema.table` |
| `USE SCHEMA` | each sink schema | resolve the tables |
| `SELECT` | each sink table (or the schema) | `Freshness`, `Row`, `CountByBucket`; also makes the table's constraints visible in `information_schema` for `PrimaryKeyColumn` |

```sql
GRANT USE CATALOG ON CATALOG main TO `piperoom-reader`;
GRANT USE SCHEMA ON SCHEMA main.sales TO `piperoom-reader`;
GRANT SELECT ON SCHEMA main.sales TO `piperoom-reader`;
```

Do not grant `MODIFY`, `ALL PRIVILEGES`, `CREATE TABLE` or ownership, and do not use a
workspace admin's token. `CAN USE` cannot change the warehouse; `CAN MANAGE` can, so do not
grant it.

## How names and queries work

- Tables are `schema.table`; every part, and every column, must match
  `^[a-zA-Z_][a-zA-Z0-9_]*$` (AGENTS.md §4 rule 6) and is quoted with backticks:
  `` `main`.`sales`.`orders` ``. A catalog prefix in the table name is refused, so a tool
  cannot reach another catalog the principal can see.
- Values (the row key, time bounds, names compared in `information_schema`) are named
  parameters (`:key`, `:since`, ...), which need DBR 14.1 or later (every current SQL
  warehouse). No SQL text from a tool reaches a query.
- Sessions run in UTC (`timezone=UTC`), so `TIMESTAMP_NTZ` is read as UTC wall time.
- `Freshness` is `SELECT MAX(col)`. The column must be `TIMESTAMP` or `TIMESTAMP_NTZ`; any
  other type is `ErrNotTimestamp`, decided from the result schema, so the error never
  carries a value.
- `Row` compares `CAST(pk AS STRING) = :key`, so `BIGINT`, `DECIMAL` and `STRING` keys all
  work. Column names come back lower-cased; numbers as strings, times as RFC 3339,
  `BINARY` as its length, `ARRAY`/`MAP`/`STRUCT` as JSON text.
- `PrimaryKeyColumn` reads the declared (informational, not enforced) primary key from
  `<catalog>.information_schema`. None declared, or a table the principal cannot see, is
  `clients.ErrNotFound`; a composite key is `clients.ErrNotSupported`. The name is
  lower-cased.
- `CountByBucket` counts rows per bucket (at least one second wide, at most 200 buckets)
  over `[since, until)`; only counts are selected.
- Every call has a 10 second deadline (context and server-side statement timeout). Cloud
  fetch is off, so results come inline over one HTTP client whose responses are capped at
  4 MB (`ErrResponseTooLarge`).
- Errors: SQLSTATE `42P01` / `42704` / `42P08` / `42703` (table, schema, catalog, column
  missing) map to `clients.ErrNotFound`; `42501` and HTTP 401/403 to `ErrPermissionDenied`;
  HTTP 404 (wrong http path) to `ErrEndpointNotFound`. Classification uses the SQLSTATE and
  the HTTP status, never message text. A data exception (class `22`) keeps only its
  SQLSTATE, since its text can quote a value; other server text is redacted, folded onto
  one line and cut to 300 runes.
- Every query is recorded as a provenance call (`tools.Record`) with its parameters; the
  target is `databricks <host>/<catalog>`, never the credential.
- The driver's own logger (zerolog, to stderr) is switched off; failures reach the caller
  as redacted errors.

## Tests

Unit tests use a fake runner, a fake `database/sql` driver, and the real driver against an
`httptest` server (401, 403, 404, timeout, cancel, oversized response); no network. The
integration tests use their own tag (`go test -tags databricks_integration ./internal/clients/databricks/`,
not the dev-stack `integration` tag, which CI runs without cloud credentials) and are
skipped unless `PIPEROOM_IT_DATABRICKS_DSN` is set; `PIPEROOM_IT_DATABRICKS_TABLE` and
`PIPEROOM_IT_DATABRICKS_TS_COLUMN` name an existing table to read.
