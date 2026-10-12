package wiring

import (
	"context"
	"fmt"
	"strings"

	"piperoom/internal/clients"
	"piperoom/internal/clients/snowflake"
	"piperoom/internal/clients/warehouse"
	"piperoom/internal/config"
)

// Warehouse kinds accepted in WAREHOUSE_KIND.
const (
	WarehousePostgres  = "postgres"
	WarehouseSnowflake = "snowflake"
)

// warehouseOpener opens one warehouse client from WAREHOUSE_DB_URL and returns
// it with its closer.
type warehouseOpener func(ctx context.Context, url string) (clients.Warehouse, func(), error)

// warehouseOpeners maps a kind to its opener. It is a parameter of
// openWarehouse so tests can pick a kind without a database.
type warehouseOpeners struct {
	postgres  warehouseOpener
	snowflake warehouseOpener
}

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
}

// warehouseKind normalises cfg.WarehouseKind. An empty value means the
// default, "postgres", so a Config built without config.Load keeps working.
func warehouseKind(cfg config.Config) (string, error) {
	kind := strings.ToLower(strings.TrimSpace(cfg.WarehouseKind))
	switch kind {
	case "", WarehousePostgres:
		return WarehousePostgres, nil
	case WarehouseSnowflake:
		return WarehouseSnowflake, nil
	}
	return "", fmt.Errorf("wiring: unknown WAREHOUSE_KIND %q (allowed: %s, %s)", cfg.WarehouseKind, WarehousePostgres, WarehouseSnowflake)
}

// openWarehouse opens the warehouse client chosen by cfg.WarehouseKind.
// WAREHOUSE_DB_URL is a Postgres URL for "postgres" and a gosnowflake DSN for
// "snowflake". A Snowflake DSN holds the password, so errors from that path
// are scrubbed of it and the DSN is never logged.
func openWarehouse(ctx context.Context, cfg config.Config, o warehouseOpeners) (clients.Warehouse, func(), error) {
	kind, err := warehouseKind(cfg)
	if err != nil {
		return nil, nil, err
	}
	switch kind {
	case WarehouseSnowflake:
		wh, closeFn, err := o.snowflake(ctx, cfg.WarehouseDBURL)
		if err != nil {
			return nil, nil, fmt.Errorf("wiring: warehouse (snowflake): %w", snowflake.Redact(err, cfg.WarehouseDBURL))
		}
		return wh, closeFn, nil
	default:
		wh, closeFn, err := o.postgres(ctx, cfg.WarehouseDBURL)
		if err != nil {
			return nil, nil, fmt.Errorf("wiring: warehouse: %w", err)
		}
		return wh, closeFn, nil
	}
}
