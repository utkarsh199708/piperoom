// Package warehouse implements clients.Warehouse against a Postgres-backed
// dev warehouse. All SQL lives in this one file so a Snowflake implementation
// can replace the whole file behind the same interface later.
package warehouse

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"piperoom/internal/clients"
	"piperoom/internal/clients/pgconv"
)

// DB is a read-only connection pool to the warehouse.
type DB struct {
	pool *pgxpool.Pool
}

// New opens a read-only connection pool and verifies it with a ping.
func New(ctx context.Context, url string) (*DB, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf("warehouse: parse config: %w", err)
	}
	cfg.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		_, err := conn.Exec(ctx, "SET default_transaction_read_only = on")
		if err != nil {
			return fmt.Errorf("warehouse: set read-only: %w", err)
		}
		return nil
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("warehouse: connect: %w", err)
	}
	pctx, cancel := pgconv.WithDeadline(ctx, 10*time.Second)
	defer cancel()
	if err := pool.Ping(pctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("warehouse: ping: %w", err)
	}
	return &DB{pool: pool}, nil
}

// Close releases the pool.
func (d *DB) Close() {
	if d != nil && d.pool != nil {
		d.pool.Close()
	}
}

// Freshness returns max(tsColumn) of table. A zero time means the table has
// no rows.
func (d *DB) Freshness(ctx context.Context, table, tsColumn string) (time.Time, error) {
	ctx, cancel := pgconv.WithDeadline(ctx, 10*time.Second)
	defer cancel()

	schema, tbl, col, err := validateTableColumn(table, tsColumn)
	if err != nil {
		return time.Time{}, err
	}
	q := fmt.Sprintf("SELECT max(%s) FROM %s", pgx.Identifier{col}.Sanitize(), pgx.Identifier{schema, tbl}.Sanitize())
	var ts *time.Time
	if err := d.pool.QueryRow(ctx, q).Scan(&ts); err != nil {
		return time.Time{}, fmt.Errorf("warehouse: freshness of %s: %w", table, err)
	}
	if ts == nil {
		return time.Time{}, nil
	}
	return *ts, nil
}

// Row reads one row by primary key. table is "schema.table". Values are
// converted to JSON-friendly types (numbers -> strings, times -> RFC 3339).
func (d *DB) Row(ctx context.Context, table, pkColumn, pk string) (map[string]any, bool, error) {
	ctx, cancel := pgconv.WithDeadline(ctx, 10*time.Second)
	defer cancel()

	schema, tbl, col, err := validateTableColumn(table, pkColumn)
	if err != nil {
		return nil, false, err
	}
	q := fmt.Sprintf("SELECT * FROM %s WHERE %s = $1 LIMIT 1", pgx.Identifier{schema, tbl}.Sanitize(), pgx.Identifier{col}.Sanitize())

	// DescribeExec re-describes the statement on every call, so a concurrent
	// schema change cannot break a cached plan.
	rows, err := d.pool.Query(ctx, q, pgx.QueryExecModeDescribeExec, pk)
	if err != nil {
		return nil, false, fmt.Errorf("warehouse: query row: %w", err)
	}
	defer rows.Close()

	cols := rows.FieldDescriptions()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return nil, false, fmt.Errorf("warehouse: read row: %w", err)
		}
		return nil, false, nil
	}
	vals, err := rows.Values()
	if err != nil {
		return nil, false, fmt.Errorf("warehouse: decode row: %w", err)
	}
	m := make(map[string]any, len(cols))
	for i, fd := range cols {
		if i < len(vals) {
			m[fd.Name] = pgconv.ToJSON(vals[i])
		}
	}
	return m, true, nil
}

var identRe = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

// validateTableColumn validates a "schema.table" identifier and a column name,
// returning the raw parts for safe interpolation via pgx.Identifier.
func validateTableColumn(table, column string) (schema, tbl, col string, err error) {
	parts := strings.Split(table, ".")
	if len(parts) != 2 {
		return "", "", "", fmt.Errorf("warehouse: table must be \"schema.table\", got %q", table)
	}
	if !identRe.MatchString(parts[0]) || !identRe.MatchString(parts[1]) {
		return "", "", "", fmt.Errorf("warehouse: invalid table identifier %q", table)
	}
	if !identRe.MatchString(column) {
		return "", "", "", fmt.Errorf("warehouse: invalid column identifier %q", column)
	}
	return parts[0], parts[1], column, nil
}

var _ = clients.Warehouse((*DB)(nil))
