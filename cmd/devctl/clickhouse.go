package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
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
    loaded_at DateTime64(6, 'UTC') DEFAULT now64(6)
)
ENGINE = ReplacingMergeTree(version)
ORDER BY id;

-- These columns mirror the Debezium Avro envelope; null unions become defaults.
CREATE TABLE IF NOT EXISTS analytics.orders_kafka
(
    "before" Tuple(
        id Int64,
        customer_id Int64,
        amount Decimal(12, 2),
        status String,
        updated_at String
    ),
    "after" Tuple(
        id Int64,
        customer_id Int64,
        amount Decimal(12, 2),
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
    kafka_auto_offset_reset = 'earliest',
    format_avro_schema_registry_url = 'http://schema-registry:8081',
    input_format_avro_null_as_default = 1,
    input_format_avro_allow_missing_fields = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.orders_mv
TO analytics.orders
AS
SELECT
    toUInt64(tupleElement("after", 'id')) AS id,
    tupleElement("after", 'customer_id') AS customer_id,
    tupleElement("after", 'amount') AS amount,
    tupleElement("after", 'status') AS status,
    parseDateTime64BestEffort(tupleElement("after", 'updated_at'), 6, 'UTC') AS updated_at,
    toUInt64(ifNull(ts_ms, 0)) AS version
FROM analytics.orders_kafka
WHERE op != 'd';`

// clickhouseInit starts the optional ClickHouse profile and creates its
// orders sink, Kafka engine source and materialized view.
func (s *stack) clickhouseInit(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	if err := s.compose(ctx, nil, "--profile", "clickhouse", "up", "-d", "--wait", "clickhouse"); err != nil {
		return err
	}
	if s.clickhouseExecFn != nil {
		if err := s.clickhouseExecFn(ctx, clickhouseInitSQL); err != nil {
			return fmt.Errorf("devctl: initialize ClickHouse orders tables: %w", err)
		}
		return nil
	}

	args := []string{
		"compose", "-f", filepath.Join(s.root, "dev", "docker-compose.yml"),
		"exec", "-T", "clickhouse", "bash", "-ceu",
		"exec clickhouse-client --multiquery --user \"$CLICKHOUSE_USER\" --password \"$CLICKHOUSE_PASSWORD\"",
	}
	cmd := exec.CommandContext(ctx, "docker", args...)
	cmd.Stdin = strings.NewReader(clickhouseInitSQL)
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	cmd.Env = os.Environ()
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("devctl: initialize ClickHouse orders tables: %w", err)
	}
	return nil
}
