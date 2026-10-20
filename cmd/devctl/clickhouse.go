package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	osexec "os/exec"
	"path/filepath"
	"strings"
	"time"
)

const clickhouseInitSQL = `CREATE DATABASE IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.orders
(
    id UInt64,
    customer_id Int64,
    amount Decimal(12, 2),
    status String,
    updated_at DateTime64(6, 'UTC'),
    version UInt64,
    is_deleted UInt8,
    loaded_at DateTime64(6, 'UTC') DEFAULT now64(6)
)
ENGINE = ReplacingMergeTree(version, is_deleted)
ORDER BY id;

-- These columns mirror the Debezium Avro envelope; null unions become defaults.
-- amount is read as Int64 because the source column is INTEGER (Avro int):
-- ClickHouse stores an Avro int into a Decimal column unscaled, so 4969 would
-- land as 49.69. The view converts it. After the schema-change scenario the
-- field becomes an Avro decimal (bytes) and this read fails on purpose: the
-- sink breaks on the schema change (dev/clickhouse/README.md).
-- There is no offset-reset table setting: ClickHouse's Kafka consumer already
-- defaults auto.offset.reset to earliest, so a new group reads the snapshot.
CREATE TABLE IF NOT EXISTS analytics.orders_kafka
(
    "before" Tuple(
        id Int64,
        customer_id Int64,
        amount Int64,
        status String,
        updated_at String
    ),
    "after" Tuple(
        id Int64,
        customer_id Int64,
        amount Int64,
        status String,
        updated_at String
    ),
    source Tuple(
        version String,
        connector String,
        name String,
        ts_ms Int64,
        snapshot Nullable(String),
        db String,
        sequence Nullable(String),
        ts_us Nullable(Int64),
        ts_ns Nullable(Int64),
        schema String,
        "table" String,
        txId Nullable(Int64),
        lsn Nullable(Int64),
        xmin Nullable(Int64)
    ),
    "transaction" Tuple(
        id String,
        total_order Int64,
        data_collection_order Int64
    ),
    op String,
    ts_ms Nullable(Int64),
    ts_us Nullable(Int64),
    ts_ns Nullable(Int64)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'pg.public.orders',
    kafka_group_name = 'piperoom-clickhouse-orders',
    kafka_format = 'AvroConfluent',
    format_avro_schema_registry_url = 'http://schema-registry:8081',
    input_format_avro_null_as_default = 1,
    input_format_avro_allow_missing_fields = 1;

-- A delete carries the row only in "before" (REPLICA IDENTITY FULL), so the
-- view reads that side for op='d' and marks it is_deleted; FINAL then hides it.
CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.orders_mv
TO analytics.orders
AS
SELECT
    toUInt64(tupleElement(if(op = 'd', "before", "after"), 'id')) AS id,
    tupleElement(if(op = 'd', "before", "after"), 'customer_id') AS customer_id,
    toDecimal64(tupleElement(if(op = 'd', "before", "after"), 'amount'), 2) AS amount,
    tupleElement(if(op = 'd', "before", "after"), 'status') AS status,
    parseDateTime64BestEffort(tupleElement(if(op = 'd', "before", "after"), 'updated_at'), 6, 'UTC') AS updated_at,
    toUInt64(ifNull(ts_ms, 0)) AS version,
    toUInt8(op = 'd') AS is_deleted
FROM analytics.orders_kafka;`

// clickhouseInit starts the optional ClickHouse profile and creates its
// orders sink, Kafka engine source and materialized view.
func (s *stack) clickhouseInit(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	if err := s.compose(ctx, nil, "--profile", "clickhouse", "up", "-d", "--wait", "clickhouse"); err != nil {
		return err
	}
	exec := s.clickhouseExecFn
	if exec == nil {
		exec = s.clickhouseExec
	}
	every, limit := s.clickhouseRetryEvery, s.clickhouseRetryFor
	if every <= 0 {
		every = 2 * time.Second
	}
	if limit <= 0 {
		limit = 60 * time.Second
	}
	if err := retryClickHouse(ctx, every, limit, func(ctx context.Context) error {
		return exec(ctx, clickhouseInitSQL)
	}); err != nil {
		return fmt.Errorf("devctl: initialize ClickHouse orders tables: %w", err)
	}
	return nil
}

// errClickHouseNotReady marks a clickhouse-client failure to reach the server.
// The image's entrypoint first runs a temporary server on 127.0.0.1 for the
// init scripts, which also answers the healthcheck, then stops it and starts
// the real one; `up --wait` can return in that gap.
var errClickHouseNotReady = errors.New("ClickHouse server not accepting connections")

// clickhouseConnExitCodes are clickhouse-client exit codes (the server error
// code, mod 256) that mean the connection failed or dropped, not that the SQL
// was rejected: NETWORK_ERROR (210), SOCKET_TIMEOUT (209) and
// ATTEMPT_TO_READ_AFTER_EOF (32, the temporary server stopping mid-query).
// The init SQL is idempotent, so a retry after any of them is safe.
var clickhouseConnExitCodes = map[int]bool{210: true, 209: true, 32: true}

// clickhouseExec pipes query into clickhouse-client inside the container.
func (s *stack) clickhouseExec(ctx context.Context, query string) error {
	args := []string{
		"compose", "-f", filepath.Join(s.root, "dev", "docker-compose.yml"),
		"exec", "-T", "clickhouse", "bash", "-ceu",
		"exec clickhouse-client --multiquery --user \"$CLICKHOUSE_USER\" --password \"$CLICKHOUSE_PASSWORD\"",
	}
	cmd := osexec.CommandContext(ctx, "docker", args...)
	cmd.Stdin = strings.NewReader(query)
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	cmd.Env = os.Environ()
	return classifyClickHouseExit(cmd.Run())
}

// classifyClickHouseExit marks connection-type clickhouse-client exits with
// errClickHouseNotReady so the caller retries them.
func classifyClickHouseExit(err error) error {
	if err == nil {
		return nil
	}
	var ee *osexec.ExitError
	if errors.As(err, &ee) && clickhouseConnExitCodes[ee.ExitCode()] {
		return fmt.Errorf("%w (clickhouse-client exit %d): %w", errClickHouseNotReady, ee.ExitCode(), err)
	}
	return fmt.Errorf("devctl: clickhouse-client: %w", err)
}

// retryClickHouse runs fn, and again every `every` while it fails with
// errClickHouseNotReady, until `limit` has passed or ctx ends. Any other error
// is returned at once.
func retryClickHouse(ctx context.Context, every, limit time.Duration, fn func(context.Context) error) error {
	giveUp := time.NewTimer(limit)
	defer giveUp.Stop()
	for attempt := 1; ; attempt++ {
		err := fn(ctx)
		if err == nil || !errors.Is(err, errClickHouseNotReady) {
			return err
		}
		wait := time.NewTimer(every)
		select {
		case <-ctx.Done():
			wait.Stop()
			return fmt.Errorf("devctl: waiting for ClickHouse: %w (last error: %w)", ctx.Err(), err)
		case <-giveUp.C:
			wait.Stop()
			return fmt.Errorf("devctl: ClickHouse still not accepting connections after %d attempts in %s: %w", attempt, limit, err)
		case <-wait.C:
		}
	}
}
