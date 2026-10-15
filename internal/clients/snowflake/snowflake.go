// Package snowflake implements clients.Warehouse against Snowflake, for
// production use. internal/clients/warehouse implements the same interface
// against the dev Postgres warehouse; internal/wiring picks whichever one
// WAREHOUSE_KIND asks for. See README.md in this folder for the DSN and grants.
//
// Read-only access is enforced by the role in the DSN, not by this package:
// grant the connecting role only USAGE on the warehouse, database and
// schema, and SELECT on the tables it reads (see README.md in this folder and
// docs/pilot/permissions.md for the exact grants). The driver has no session-level read-only switch, so a
// role with write grants could still write; keep this role SELECT-only.
//
// Driver logging (GO-2026-6665): gosnowflake v1 logs session tokens when its
// level is raised, and New pins it to error and refuses the tracing and
// clientConfigFile DSN parameters. The driver's easy logging can still read
// a client config file on the first connection, so deployments must keep
// SF_CLIENT_CONFIG_FILE unset and must not place sf_client_config.json in the
// executable's or the home directory. Remove this once on gosnowflake v2.
package snowflake

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
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
	// target is the receipt target, "snowflake <account>/<database>".
	target string
}

// New opens a connection pool for dsn (see gosnowflake's DSN syntax,
// typically "user:password@account/db/schema?warehouse=...&role=...") and
// verifies it with a ping.
func New(ctx context.Context, dsn string) (*DB, error) {
	// The DSN carries the password and driver errors can echo parts of it,
	// so every error leaving New goes through Redact.
	cfg, err := hardenedConfig(dsn)
	if err != nil {
		return nil, Redact(err, dsn)
	}
	db := sql.OpenDB(gosnowflake.NewConnector(gosnowflake.SnowflakeDriver{}, *cfg))
	pctx, cancel := context.WithTimeout(ctx, defaultTimeout)
	defer cancel()
	if err := db.PingContext(pctx); err != nil {
		db.Close()
		return nil, Redact(fmt.Errorf("snowflake: ping: %w", err), dsn)
	}
	// A Snowflake session token expires after about 4 hours. Retire idle and
	// old pooled connections well before that, so a quiet server does not
	// hand the next query a dead session.
	db.SetConnMaxIdleTime(30 * time.Minute)
	db.SetConnMaxLifetime(time.Hour)
	return &DB{db: db, q: dbQuerier{db: db}, target: targetFromDSN(dsn)}, nil
}

// hardenedConfig is the mitigation for GO-2026-6665 (gosnowflake v1 logs
// session tokens and response bodies once its log level is raised; the fix is
// only in v2). It pins the driver logger to error, refuses the DSN parameters
// that raise the level or load a client config file (easy logging, which can
// also raise it on the first connection), and returns the parsed config to
// open through NewConnector. The logger is pinned even when the DSN is
// refused. See reviews/vuln-report-2026-10.md.
func hardenedConfig(dsn string) (*gosnowflake.Config, error) {
	if err := gosnowflake.GetLogger().SetLogLevel("error"); err != nil {
		return nil, fmt.Errorf("snowflake: set driver log level: %w", err)
	}
	cfg, err := gosnowflake.ParseDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("snowflake: parse dsn: %w", err)
	}
	if cfg.Tracing != "" {
		return nil, errors.New("snowflake: dsn parameter tracing is not allowed: it raises the driver log level, which logs session tokens")
	}
	if cfg.ClientConfigFile != "" {
		return nil, errors.New("snowflake: dsn parameter clientConfigFile is not allowed: it can raise the driver log level, which logs session tokens")
	}
	return cfg, nil
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

	q, err := freshnessSQL(table, tsColumn)
	if err != nil {
		return time.Time{}, err
	}
	rows, err := d.query(gosnowflake.WithHigherPrecision(ctx), q)
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
	// Compare in UTC: the driver returns TIMESTAMP_NTZ as UTC wall time and
	// TIMESTAMP_LTZ in the session zone, so normalise before the caller does
	// any age arithmetic or formatting.
	return ts.UTC(), nil
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

	q, err := rowSQL(table, pkColumn)
	if err != nil {
		return nil, false, err
	}
	rows, err := d.query(gosnowflake.WithHigherPrecision(ctx), q, pk)
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

// PrimaryKeyColumn returns the first primary key column of table
// ("schema.table" or "db.schema.table"), lower-cased like Row's column names.
// A table without a primary key is clients.ErrNotFound. warehouse_row uses it
// to refuse lookups by any other column, which would make the tool an
// existence oracle for personal data. Snowflake does not enforce primary
// keys, but the declared key is what the pipeline owners maintain.
func (d *DB) PrimaryKeyColumn(ctx context.Context, table string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, defaultTimeout)
	defer cancel()

	qualified, err := quoteTable(table)
	if err != nil {
		return "", err
	}
	rows, err := d.query(ctx, "SHOW PRIMARY KEYS IN TABLE "+qualified)
	if err != nil {
		return "", wrapQueryErr("primary key of", table, err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return "", fmt.Errorf("snowflake: primary key of %s: columns: %w", table, err)
	}
	nameIdx, seqIdx := -1, -1
	for i, c := range cols {
		switch strings.ToLower(c) {
		case "column_name":
			nameIdx = i
		case "key_sequence":
			seqIdx = i
		}
	}
	if nameIdx < 0 || seqIdx < 0 {
		return "", fmt.Errorf("snowflake: primary key of %s: SHOW PRIMARY KEYS result has no column_name/key_sequence (got %v)", table, cols)
	}

	best, bestSeq := "", -1
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return "", fmt.Errorf("snowflake: primary key of %s: decode: %w", table, err)
		}
		seq, err := strconv.Atoi(fmt.Sprint(vals[seqIdx]))
		if err != nil {
			return "", fmt.Errorf("snowflake: primary key of %s: key_sequence %v: %w", table, vals[seqIdx], err)
		}
		if name := fmt.Sprint(vals[nameIdx]); bestSeq < 0 || seq < bestSeq {
			best, bestSeq = name, seq
		}
	}
	if err := rows.Err(); err != nil {
		return "", wrapQueryErr("primary key of", table, err)
	}
	if bestSeq < 0 {
		return "", fmt.Errorf("snowflake: primary key of %s: table has no declared primary key (it may exist): %w", table, clients.ErrNotFound)
	}
	return strings.ToLower(best), nil
}
