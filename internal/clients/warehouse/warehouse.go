// Package warehouse implements clients.Warehouse against a Postgres-backed
// dev warehouse. All SQL lives in this one file so a Snowflake implementation
// can replace the whole file behind the same interface later.
package warehouse

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
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

// maxRowBytes caps the text size of one row Row loads. Row feeds a trace diff
// and a tool summary, so a multi-megabyte JSON or text column is never needed
// and must not be pulled into memory whole.
const maxRowBytes = 1 << 20

// ErrRowTooLarge is returned by Row when the row's text form exceeds
// maxRowBytes. The row exists; it is not loaded.
var ErrRowTooLarge = errors.New("warehouse: row too large")

// wrapErr wraps a query error, mapping a missing table, schema or column
// (SQLSTATE 42P01 undefined_table, 3F000 invalid_schema_name, 42703
// undefined_column) to clients.ErrNotFound, as the Snowflake client does, so
// callers tell "does not exist" from "warehouse unreachable" with errors.Is.
func wrapErr(op, table string, err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "42P01", "3F000", "42703":
			return fmt.Errorf("warehouse: %s %s: %w: %w", op, table, clients.ErrNotFound, err)
		}
	}
	return fmt.Errorf("warehouse: %s %s: %w", op, table, err)
}

// Freshness returns max(tsColumn) of table. A zero time means the table has
// no rows. The caller computes an absolute age (now minus this time): how long
// nothing newer was written, not replication lag.
//
// A date column has no time of day, so its newest value is read as the end of
// that day (UTC), not its midnight: a row dated today is fresh. The end of
// today is clamped to now so it never reads as a future timestamp; a date
// after today is returned as its midnight, which callers flag as future. The
// age of a date column is therefore only accurate to a day.
func (d *DB) Freshness(ctx context.Context, table, tsColumn string) (time.Time, error) {
	ctx, cancel := pgconv.WithDeadline(ctx, 10*time.Second)
	defer cancel()

	schema, tbl, col, err := validateTableColumn(table, tsColumn)
	if err != nil {
		return time.Time{}, err
	}
	q := fmt.Sprintf("SELECT max(%s) FROM %s", pgx.Identifier{col}.Sanitize(), pgx.Identifier{schema, tbl}.Sanitize())
	rows, err := d.pool.Query(ctx, q)
	if err != nil {
		return time.Time{}, wrapErr("freshness of", table, err)
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return time.Time{}, wrapErr("freshness of", table, err)
		}
		return time.Time{}, nil
	}
	isDate := len(rows.FieldDescriptions()) > 0 && rows.FieldDescriptions()[0].DataTypeOID == pgtype.DateOID
	var ts *time.Time
	if err := rows.Scan(&ts); err != nil {
		return time.Time{}, wrapErr("freshness of", table, err)
	}
	if ts == nil {
		return time.Time{}, nil
	}
	if isDate {
		return endOfDay(*ts, time.Now()), nil
	}
	return *ts, nil
}

// endOfDay reads a date (midnight UTC) as the end of that day, clamped to
// now; a date after today keeps its midnight so it still reads as future.
func endOfDay(day, now time.Time) time.Time {
	if day.After(now) {
		return day
	}
	end := day.AddDate(0, 0, 1)
	if end.After(now) {
		return now
	}
	return end
}

// Row reads one row by primary key. table is "schema.table". Values are
// converted to JSON-friendly types (numbers -> strings, times -> RFC 3339).
// A row whose text form is larger than maxRowBytes is not loaded:
// Row returns ErrRowTooLarge.
func (d *DB) Row(ctx context.Context, table, pkColumn, pk string) (map[string]any, bool, error) {
	ctx, cancel := pgconv.WithDeadline(ctx, 10*time.Second)
	defer cancel()

	schema, tbl, col, err := validateTableColumn(table, pkColumn)
	if err != nil {
		return nil, false, err
	}
	ident := pgx.Identifier{schema, tbl}.Sanitize()
	pkIdent := pgx.Identifier{col}.Sanitize()
	// The size check runs in the database so an oversized row never crosses
	// the wire. (r.*)::text is the whole row, whatever its column names.
	q := fmt.Sprintf("SELECT r.* FROM %s AS r WHERE r.%s = $1 AND octet_length((r.*)::text) <= $2 LIMIT 1", ident, pkIdent)

	// DescribeExec re-describes the statement on every call, so a concurrent
	// schema change cannot break a cached plan.
	rows, err := d.pool.Query(ctx, q, pgx.QueryExecModeDescribeExec, pk, maxRowBytes)
	if err != nil {
		return nil, false, wrapErr("query row from", table, err)
	}
	defer rows.Close()

	cols := rows.FieldDescriptions()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return nil, false, wrapErr("read row from", table, err)
		}
		rows.Close()
		return nil, false, d.tooLarge(ctx, ident, pkIdent, table, pk)
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

// tooLarge tells "no such row" (nil) from "a row too large to load"
// (ErrRowTooLarge) after the size-capped read found nothing.
func (d *DB) tooLarge(ctx context.Context, ident, pkIdent, table, pk string) error {
	q := fmt.Sprintf("SELECT true FROM %s AS r WHERE r.%s = $1 LIMIT 1", ident, pkIdent)
	var exists bool
	err := d.pool.QueryRow(ctx, q, pgx.QueryExecModeDescribeExec, pk).Scan(&exists)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return nil
	case err != nil:
		return wrapErr("read row from", table, err)
	default:
		return fmt.Errorf("%w: %s row over %d bytes", ErrRowTooLarge, table, maxRowBytes)
	}
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
