// Package warehouse implements clients.Warehouse against a Postgres-backed
// dev warehouse. All SQL lives in this one file so a Snowflake implementation
// can replace the whole file behind the same interface later.
package warehouse

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
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
	pool   *pgxpool.Pool
	q      querier // the pool; a fake in tests
	target string  // receipt target: database@host:port, never credentials
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
	return &DB{pool: pool, q: pgconv.ReadOnlyPool{Pool: pool}, target: targetOf(pool)}, nil
}

// Close releases the pool.
func (d *DB) Close() {
	if d != nil && d.pool != nil {
		d.pool.Close()
	}
}

// Freshness returns max(tsColumn) of table. A zero time means the table has
// no rows (or only '-infinity'); 'infinity' returns a year-9999 time.
//
// tsColumn must be a timestamp or timestamptz column. Its type is checked
// from the result's row description before the value is decoded, because a
// failed decode (pgx's scan error) quotes the value: max() of an email or a
// card number column would reach the agent and the room log (#564). No error
// from this method carries a value of the column.
func (d *DB) Freshness(ctx context.Context, table, tsColumn string) (time.Time, error) {
	ctx, cancel := pgconv.WithDeadline(ctx, 10*time.Second)
	defer cancel()

	schema, tbl, col, err := validateTableColumn(table, tsColumn)
	if err != nil {
		return time.Time{}, err
	}
	q := fmt.Sprintf("SELECT max(%s) FROM %s", pgx.Identifier{col}.Sanitize(), pgx.Identifier{schema, tbl}.Sanitize())
	rows, err := d.query(ctx, q)
	if err != nil {
		return time.Time{}, freshnessErr(ctx, table, tsColumn, err)
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return time.Time{}, freshnessErr(ctx, table, tsColumn, err)
		}
		return time.Time{}, nil // max() always returns a row; none is no data
	}
	if fds := rows.FieldDescriptions(); len(fds) != 1 || !isTimestampOID(fds[0].DataTypeOID) {
		return time.Time{}, fmt.Errorf("warehouse: freshness of %s: column %q is not a timestamp or timestamptz column: %w", table, tsColumn, ErrNotTimestamp)
	}
	var ts pgtype.Timestamptz
	if err := rows.Scan(&ts); err != nil {
		return time.Time{}, freshnessErr(ctx, table, tsColumn, err)
	}
	if err := rows.Err(); err != nil {
		return time.Time{}, freshnessErr(ctx, table, tsColumn, err)
	}
	if !ts.Valid {
		return time.Time{}, nil
	}
	switch ts.InfinityModifier {
	case pgtype.Infinity:
		// time.Time cannot hold it. A far-future value takes the caller's
		// "in the future / bad data" path instead of failing the scan, which
		// would blame the warehouse connection.
		return infinityTime, nil
	case pgtype.NegativeInfinity:
		// No usable newest value: the same answer as an empty table.
		return time.Time{}, nil
	}
	return ts.Time, nil
}

// ErrNotTimestamp is wrapped by Freshness when the column is not a timestamp
// or timestamptz column.
var ErrNotTimestamp = errors.New("not a timestamp column")

func isTimestampOID(oid uint32) bool {
	return oid == pgtype.TimestamptzOID || oid == pgtype.TimestampOID
}

// freshnessErr turns a Freshness failure into an error that never carries a
// value of the column. A missing table or column keeps its server message
// (it names only identifiers that passed validation) and gains
// clients.ErrNotFound. A decode error, or a server error of class 22 (data
// exception: its message can quote the value), becomes a fixed phrase; the
// raw error is not logged, only its type and SQLSTATE at debug.
func freshnessErr(ctx context.Context, table, col string, err error) error {
	var pgErr *pgconn.PgError
	var scanErr pgx.ScanArgError
	switch {
	case errors.As(err, &pgErr) && strings.HasPrefix(pgErr.Code, "22"):
		slog.DebugContext(ctx, "warehouse: freshness: data exception", "table", table, "column", col, "sqlstate", pgErr.Code)
		return fmt.Errorf("warehouse: freshness of %s: max(%s) could not be read as a timestamp (SQLSTATE %s)", table, col, pgErr.Code)
	case errors.As(err, &pgErr):
		return fmt.Errorf("warehouse: freshness of %s: %w", table, notFound(err))
	case errors.As(err, &scanErr):
		slog.DebugContext(ctx, "warehouse: freshness: decode failed", "table", table, "column", col, "error_type", fmt.Sprintf("%T", scanErr.Err))
		return fmt.Errorf("warehouse: freshness of %s: max(%s) could not be read as a timestamp: %w", table, col, ErrNotTimestamp)
	}
	return fmt.Errorf("warehouse: freshness of %s: %w", table, err)
}

// infinityTime stands in for a Postgres 'infinity' timestamp.
var infinityTime = time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC)

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
	rows, err := d.query(ctx, q, pgx.QueryExecModeDescribeExec, pk)
	if err != nil {
		return nil, false, fmt.Errorf("warehouse: query row: %w", notFound(err))
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

// PrimaryKeyColumn returns the first primary key column of table
// ("schema.table"). A table without a primary key is clients.ErrNotFound.
func (d *DB) PrimaryKeyColumn(ctx context.Context, table string) (string, error) {
	ctx, cancel := pgconv.WithDeadline(ctx, 10*time.Second)
	defer cancel()

	parts := strings.Split(table, ".")
	if len(parts) != 2 || !identRe.MatchString(parts[0]) || !identRe.MatchString(parts[1]) {
		return "", fmt.Errorf("warehouse: table must be \"schema.table\", got %q", table)
	}
	var col string
	var keyCols int
	err := d.queryRow(ctx, `
		SELECT a.attname, i.indnkeyatts
		FROM pg_index i
		JOIN pg_class c ON c.oid = i.indrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = i.indkey[0]
		WHERE i.indisprimary AND n.nspname = $1 AND c.relname = $2`, parts[0], parts[1]).Scan(&col, &keyCols)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", fmt.Errorf("warehouse: primary key of %s: %w", table, clients.ErrNotFound)
	}
	if err != nil {
		return "", fmt.Errorf("warehouse: primary key of %s: %w", table, err)
	}
	// A lookup by the first column of a composite key reads whichever row
	// shares it (another tenant's, with its timestamps): refuse it (#483).
	if keyCols > 1 {
		return "", fmt.Errorf("warehouse: primary key of %s is composite (%d columns): a lookup by %q alone is not one row: %w", table, keyCols, col, clients.ErrNotSupported)
	}
	return col, nil
}

// notFound adds clients.ErrNotFound to an error for a schema, table or
// column that does not exist, so callers can tell a dropped sink table from
// an unreachable warehouse. Other errors are returned unchanged.
func notFound(err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "3F000", "42P01", "42703": // invalid_schema_name, undefined_table, undefined_column
			return fmt.Errorf("%w: %w", clients.ErrNotFound, err)
		}
	}
	return err
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
