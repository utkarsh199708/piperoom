# ClickHouse warehouse client

Implements the read-only warehouse interfaces for ClickHouse. Piperoom uses it
for warehouse freshness, row lookup, reconciliation counts, and load errors.

## Configure

    WAREHOUSE_KIND=clickhouse
    WAREHOUSE_DB_URL=clickhouse://piperoom:<password>@clickhouse.example:9000/analytics?secure=true

WAREHOUSE_DB_URL uses the clickhouse-go v2 DSN format. The client sets the
session's readonly=1 setting and uses a 10 second connect, read, ping, and
query deadline. Give the service account only SELECT on the sink tables; the
session setting is defense in depth and does not replace least-privilege grants.

For the optional load-error tool, also grant SELECT on system.query_log and
system.kafka_consumers. Some ClickHouse builds do not include Kafka support; if
system.kafka_consumers is absent, query-log results are still returned.
Query-log and Kafka exception text are intentionally not returned because they
can contain record values. FirstError contains only a generic error category or
a numeric ClickHouse error code.

## Tables and behavior

- Table names are table or database.table; column names must match
  ^[A-Za-z_][A-Za-z0-9_]*$. Names are quoted with backticks and values use
  bound parameters.
- Freshness runs max(timestamp_column) and returns UTC. DateTime and DateTime64
  values retain their instant and fractional precision; empty tables return the
  zero time.
- Row lookup uses FINAL before filtering by the key. This returns the current
  deduplicated row from ReplacingMergeTree before background merges run. Row
  lookups therefore require a MergeTree-family sink table.
- Counts use toStartOfInterval buckets anchored to the requested since time,
  return at most 200 buckets, and never read row values. Widths must be whole
  seconds.
- Load-error history is limited by the server's query-log retention. Kafka
  consumer exception history contains at most the server's recent ten entries
  per consumer.
- The DSN contains credentials. The client never logs it and scrubs the DSN
  password and token from returned errors.

## Not supported

The client issues only SELECT queries. Row lookups require a MergeTree-family
table because they use FINAL. Identifiers needing special-character quoting,
three-part names, and subsecond count bucket widths are rejected.
## Integration test

The integration test creates and removes a unique it_ table. Set
CLICKHOUSE_INTEGRATION_DSN to the read-only client DSN and
CLICKHOUSE_INTEGRATION_ADMIN_DSN to a test-only DSN that can create/drop the
table and flush system logs. Run go test -tags integration
./internal/clients/clickhouse.
