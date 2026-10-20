package wiring

import (
	"context"
	"fmt"
	"strings"

	"piperoom/internal/clients"
	"piperoom/internal/clients/bigquery"
	"piperoom/internal/clients/clickhouse"
	"piperoom/internal/clients/databricks"
	"piperoom/internal/clients/snowflake"
	"piperoom/internal/clients/warehouse"
	"piperoom/internal/config"
	"piperoom/internal/redact"
)

// Warehouse kinds accepted in WAREHOUSE_KIND (and in a pipeline's
// connections.warehouse_kind). The set matches the one config.go documents
// for WAREHOUSE_KIND; warehouse_kinds_test.go reads that comment and fails
// when the two drift apart.
const (
	WarehousePostgres   = "postgres"
	WarehouseSnowflake  = "snowflake"
	WarehouseBigQuery   = "bigquery"
	WarehouseDatabricks = "databricks"
	WarehouseClickHouse = "clickhouse"
)

// warehouseKinds lists every kind wiring can open, the default first. Error
// messages and the owners.yaml literal check are built from it.
var warehouseKinds = []string{WarehousePostgres, WarehouseSnowflake, WarehouseBigQuery, WarehouseDatabricks, WarehouseClickHouse}

// warehouseOpener opens one warehouse client from WAREHOUSE_DB_URL and returns
// it with its closer.
type warehouseOpener func(ctx context.Context, url string) (clients.Warehouse, func(), error)

// warehouseOpeners maps a kind to its opener. It is a parameter of
// openWarehouse so tests can pick a kind without a database.
type warehouseOpeners struct {
	postgres   warehouseOpener
	snowflake  warehouseOpener
	bigquery   warehouseOpener
	databricks warehouseOpener
	clickhouse warehouseOpener
}

// The openers return the concrete client as clients.Warehouse, so the
// optional readers it implements (clients.CountReader, clients.LoadErrorReader,
// PrimaryKeyColumn) stay visible to the tools' type assertions:
// snowflake and clickhouse have both readers, bigquery LoadErrorReader only,
// databricks CountReader only.
var defaultWarehouseOpeners = warehouseOpeners{
	postgres: func(ctx context.Context, url string) (clients.Warehouse, func(), error) {
		wh, err := warehouse.New(ctx, url)
		if err != nil {
			return nil, nil, err
		}
		return wh, wh.Close, nil
	},
	snowflake: func(ctx context.Context, dsn string) (clients.Warehouse, func(), error) {
		wh, err := snowflake.New(ctx, dsn)
		if err != nil {
			return nil, nil, err
		}
		return wh, func() { _ = wh.Close() }, nil
	},
	bigquery: func(ctx context.Context, dsn string) (clients.Warehouse, func(), error) {
		// Credentials come from Application Default Credentials, never from
		// the DSN (bigquery.ParseDSN refuses user:password@).
		wh, err := bigquery.New(ctx, dsn)
		if err != nil {
			return nil, nil, err
		}
		return wh, func() { _ = wh.Close() }, nil
	},
	databricks: func(ctx context.Context, dsn string) (clients.Warehouse, func(), error) {
		wh, err := databricks.New(ctx, dsn)
		if err != nil {
			return nil, nil, err
		}
		return wh, func() { _ = wh.Close() }, nil
	},
	clickhouse: func(ctx context.Context, dsn string) (clients.Warehouse, func(), error) {
		wh, err := clickhouse.New(ctx, dsn)
		if err != nil {
			return nil, nil, err
		}
		return wh, func() { _ = wh.Close() }, nil
	},
}

// warehouseKind normalises cfg.WarehouseKind. An empty value means the
// default, "postgres", so a Config built without config.Load keeps working.
func warehouseKind(cfg config.Config) (string, error) {
	kind := strings.ToLower(strings.TrimSpace(cfg.WarehouseKind))
	if kind == "" {
		return WarehousePostgres, nil
	}
	for _, k := range warehouseKinds {
		if kind == k {
			return k, nil
		}
	}
	return "", fmt.Errorf("wiring: unknown WAREHOUSE_KIND %q (allowed: %s)", cfg.WarehouseKind, strings.Join(warehouseKinds, ", "))
}

// openWarehouse opens the warehouse client chosen by cfg.WarehouseKind.
// WAREHOUSE_DB_URL is a Postgres URL for "postgres" and the selected client's
// DSN for every other kind (see each client's README). The snowflake,
// databricks and clickhouse DSNs hold a password or token, so errors from
// those paths go through the client's Redact; the bigquery DSN holds no
// secret, and its errors are still scrubbed of the DSN and of anything that
// reads as a credential. The DSN is never logged.
func openWarehouse(ctx context.Context, cfg config.Config, o warehouseOpeners) (clients.Warehouse, func(), error) {
	kind, err := warehouseKind(cfg)
	if err != nil {
		return nil, nil, err
	}
	dsn := cfg.WarehouseDBURL
	var open warehouseOpener
	var scrub func(error) error
	switch kind {
	case WarehouseSnowflake:
		open, scrub = o.snowflake, func(err error) error { return snowflake.Redact(err, dsn) }
	case WarehouseBigQuery:
		open, scrub = o.bigquery, func(err error) error { return redactDSN(err, dsn) }
	case WarehouseDatabricks:
		open, scrub = o.databricks, func(err error) error { return databricks.Redact(err, dsn) }
	case WarehouseClickHouse:
		open, scrub = o.clickhouse, func(err error) error { return clickhouse.Redact(err, dsn) }
	default:
		if o.postgres == nil {
			return nil, nil, fmt.Errorf("wiring: warehouse: no opener for kind %q", kind)
		}
		wh, closeFn, err := o.postgres(ctx, dsn)
		if err != nil {
			return nil, nil, fmt.Errorf("wiring: warehouse: %w", err)
		}
		return wh, closeFn, nil
	}
	if open == nil {
		return nil, nil, fmt.Errorf("wiring: warehouse (%s): no opener for this kind", kind)
	}
	wh, closeFn, err := open(ctx, dsn)
	if err != nil {
		return nil, nil, fmt.Errorf("wiring: warehouse (%s): %w", kind, scrub(err))
	}
	return wh, closeFn, nil
}

// redactedErr carries a scrubbed message and still unwraps to the original
// error, so errors.Is and errors.As keep working.
type redactedErr struct {
	msg   string
	cause error
}

func (e *redactedErr) Error() string { return e.msg }
func (e *redactedErr) Unwrap() error { return e.cause }

// redactDSN removes the DSN text from err's message and masks anything else
// in it that reads as a credential. It is for clients with no Redact of
// their own (bigquery).
func redactDSN(err error, dsn string) error {
	if err == nil {
		return nil
	}
	msg := err.Error()
	if dsn != "" {
		msg = strings.ReplaceAll(msg, dsn, "[dsn redacted]")
	}
	msg, _ = redact.Text(msg)
	return &redactedErr{msg: msg, cause: err}
}
