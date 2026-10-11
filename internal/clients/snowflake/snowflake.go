// Package snowflake implements clients.Warehouse against Snowflake, for
// production use. internal/clients/warehouse implements the same interface
// against the dev Postgres warehouse; cmd/server picks whichever one its
// config asks for.
//
// Read-only access is enforced by the role in the DSN, not by this package:
// grant the connecting role only USAGE on the warehouse, database and
// schema, and SELECT on the tables it reads (see the PR description for the
// exact grants). The driver has no session-level read-only switch, so a
// role with write grants could still write; keep this role SELECT-only.
package snowflake

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/snowflakedb/gosnowflake" // registers the "snowflake" sql.DB driver on import

	"piperoom/internal/clients"
)

const defaultTimeout = 10 * time.Second

// DB is a connection to Snowflake, implementing clients.Warehouse.
type DB struct {
	db *sql.DB
	q  querier
}

// New opens a connection pool for dsn (see gosnowflake's DSN syntax,
// typically "user:password@account/db/schema?warehouse=...&role=...") and
// verifies it with a ping.
func New(ctx context.Context, dsn string) (*DB, error) {
	db, err := sql.Open("snowflake", dsn)
	if err != nil {
		return nil, fmt.Errorf("snowflake: open: %w", err)
	}
	pctx, cancel := context.WithTimeout(ctx, defaultTimeout)
	defer cancel()
	if err := db.PingContext(pctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("snowflake: ping: %w", err)
	}
	return &DB{db: db, q: dbQuerier{db: db}}, nil
}

// Close releases the underlying connection pool.
func (d *DB) Close() error {
	if d == nil || d.db == nil {
		return nil
	}
	return d.db.Close()
}

// Freshness returns max(tsColumn) of table ("schema.table" or
// "db.schema.table"). A zero time means the table has no rows.
func (d *DB) Freshness(ctx context.Context, table, tsColumn string) (time.Time, error) {
	ctx, cancel := context.WithTimeout(ctx, defaultTimeout)
	defer cancel()

	qualified, err := validateTable(table)
	if err != nil {
		return time.Time{}, err
	}
	col, err := validateIdent(tsColumn)
	if err != nil {
		return time.Time{}, err
	}

	q := fmt.Sprintf("SELECT MAX(%s) FROM %s", col, qualified)
	rows, err := d.q.QueryContext(gosnowflake.WithHigherPrecision(ctx), q)
	if err != nil {
		return time.Time{}, wrapQueryErr("freshness of", table, err)
	}
	defer rows.Close()

	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return time.Time{}, wrapQueryErr("freshness of", table, err)
		}
		return time.Time{}, nil
	}
	var ts *time.Time
	if err := rows.Scan(&ts); err != nil {
		return time.Time{}, fmt.Errorf("snowflake: freshness of %s: decode: %w", table, err)
	}
	if ts == nil {
		return time.Time{}, nil
	}
	return *ts, nil
}

// Row reads one row by primary key. table is "schema.table" or
// "db.schema.table". Values are converted to JSON-friendly types (numbers
// and high-precision values -> strings, times -> RFC 3339).
//
// Column names are lower-cased, matching internal/clients/warehouse (the
// Postgres implementation of this same interface): Snowflake returns an
// unquoted column's name upper-cased, e.g. "AMOUNT", and both the topology
// builder's trace diff and the DB tools' timestamp-column detection assume
// lower-case keys like "amount" or "updated_at". A column created WITH
// quotes, preserving mixed/lower case, would have its name folded here too
// — there is no way to tell that case apart from the normal unquoted one
// once the driver has already returned the name as a plain string.
func (d *DB) Row(ctx context.Context, table, pkColumn, pk string) (map[string]any, bool, error) {
	ctx, cancel := context.WithTimeout(ctx, defaultTimeout)
	defer cancel()

	qualified, err := validateTable(table)
	if err != nil {
		return nil, false, err
	}
	col, err := validateIdent(pkColumn)
	if err != nil {
		return nil, false, err
	}

	q := fmt.Sprintf("SELECT * FROM %s WHERE %s = ? LIMIT 1", qualified, col)
	rows, err := d.q.QueryContext(gosnowflake.WithHigherPrecision(ctx), q, pk)
	if err != nil {
		return nil, false, wrapQueryErr("query row from", table, err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, false, fmt.Errorf("snowflake: row from %s: columns: %w", table, err)
	}
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return nil, false, wrapQueryErr("query row from", table, err)
		}
		return nil, false, nil
	}

	vals := make([]any, len(cols))
	ptrs := make([]any, len(cols))
	for i := range vals {
		ptrs[i] = &vals[i]
	}
	if err := rows.Scan(ptrs...); err != nil {
		return nil, false, fmt.Errorf("snowflake: row from %s: decode: %w", table, err)
	}

	m := make(map[string]any, len(cols))
	for i, name := range cols {
		m[strings.ToLower(name)] = toJSON(vals[i])
	}
	return m, true, nil
}

var _ clients.Warehouse = (*DB)(nil)
