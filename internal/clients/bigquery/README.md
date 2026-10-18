# BigQuery warehouse client

Implements `clients.Warehouse` (`Freshness`, `Row`), `clients.LoadErrorReader`
(`LoadErrors` over `INFORMATION_SCHEMA.JOBS`) and `PrimaryKeyColumn` for Google
BigQuery. Piperoom uses it for `warehouse_freshness`, `warehouse_row`,
`warehouse_load_errors`, the sink health in `pipeline_map` and the warehouse leg of
`trace_record`. `CountReader` is not implemented (a later row may add it).

## Turn it on

```
WAREHOUSE_KIND=bigquery
WAREHOUSE_DB_URL='bigquery://acme-data/us?billing_project=acme-billing&max_bytes_billed=10737418240'
GOOGLE_APPLICATION_CREDENTIALS=/var/run/secrets/piperoom/bq-reader.json
```

`WAREHOUSE_DB_URL` shape:

```
bigquery://<project>/<location>[?billing_project=<project>][&max_bytes_billed=<bytes>]
```

| Part | Meaning |
|---|---|
| `<project>` | Project that holds the sink datasets. `sink_tables` in `owners.yaml` are `dataset.table` in this project. Must be a plain project id (`^[a-z][a-z0-9-]{4,28}[a-z0-9]$`); domain-scoped ids are not supported. |
| `<location>` | Location of the datasets and jobs: `us`, `eu`, or a region such as `europe-west2`. Load history is read from `` `region-<location>`.INFORMATION_SCHEMA.JOBS ``. |
| `billing_project` | Project the query jobs run in and are billed to. Default: `<project>`. |
| `max_bytes_billed` | Cap per query, in bytes. Default 10 GiB; `0` uses the project default. A query over the cap fails instead of billing. |

Any other parameter, a repeated one, or `user:password@` is refused at startup. The DSN holds
no secret: credentials come from [Application Default Credentials](https://cloud.google.com/docs/authentication/application-default-credentials)
(`GOOGLE_APPLICATION_CREDENTIALS`, workload identity on GKE, or the metadata server).

## IAM roles (read-only comes from these)

The client sends only fixed `SELECT` statements, but read-only is enforced by the
credential. Give the service account exactly:

| Role | On | Why |
|---|---|---|
| `roles/bigquery.dataViewer` | each sink dataset (or the project) | read table data and `INFORMATION_SCHEMA.TABLE_CONSTRAINTS` / `KEY_COLUMN_USAGE` |
| `roles/bigquery.jobUser` | the billing project | run query jobs (`bigquery.jobs.create`) |
| `roles/bigquery.resourceViewer` | the data project | `bigquery.jobs.listAll`, which `INFORMATION_SCHEMA.JOBS` (all users' jobs) needs; without it `LoadErrors` returns `ErrPermissionDenied` |

None of these can modify table data. Do not grant `dataEditor`, `dataOwner` or `admin`.
Query results go to an anonymous temporary table the service manages; no user dataset is written.

## How names and queries work

- Tables are `dataset.table`; every part, and every column, must match
  `^[a-zA-Z_][a-zA-Z0-9_]*$` (AGENTS.md §4 rule 6) and is quoted with backticks:
  `` `acme-data`.`analytics`.`orders` ``. A project prefix in the table name is refused, so
  a tool cannot reach another project the credential can see.
- Values (the row key, the cut-off time, names compared in `INFORMATION_SCHEMA`) are named
  query parameters (`@key`, `@since`, ...). No SQL text from a tool reaches a query.
- `Freshness` is `SELECT MAX(col)`. The column must be `TIMESTAMP` or `DATETIME`
  (read as UTC wall time); any other type is `ErrNotTimestamp`, decided from the result
  schema, so the error never carries a value.
- `Row` compares `CAST(pk AS STRING) = @key`, so `INT64`, `NUMERIC` and `STRING` keys all
  work. Column names come back lower-cased; numbers as strings, times as RFC 3339.
- `PrimaryKeyColumn` reads the declared (unenforced) primary key. None declared is
  `clients.ErrNotFound`; a composite key is `clients.ErrNotSupported`.
- `LoadErrors` lists `LOAD` and `QUERY` jobs (dbt `INSERT`/`MERGE`) into the table that
  failed (`LOAD_FAILED`) or finished with row errors (`PARTIALLY_LOADED`), newest first,
  at most 100, back to 180 days (JOBS retention). `File` is empty (JOBS has no source
  URIs); `Rows` is the DML inserted count, 0 for load jobs. Job error text is untrusted:
  it is redacted, folded onto one line and cut to 300 runes.
- Every call has a 10 second deadline. 404 / reason `notFound` maps to
  `clients.ErrNotFound`; 403 / reason `accessDenied` to `ErrPermissionDenied` (a 403 for
  quota is neither). Server error text in an error message is redacted and cut too.
- Every query is recorded as a provenance call (`tools.Record`) with its parameters.

## Tests

Unit tests use a fake runner and a fake REST endpoint (`httptest`); no network. The
integration tests use their own tag (`go test -tags bigquery_integration ./internal/clients/bigquery/`,
not the dev-stack `integration` tag, which CI runs without cloud credentials) and are
skipped unless `GOOGLE_APPLICATION_CREDENTIALS` and `PIPEROOM_IT_BIGQUERY_DSN` are set; `PIPEROOM_IT_BIGQUERY_TABLE` and
`PIPEROOM_IT_BIGQUERY_TS_COLUMN` name an existing table to read.
