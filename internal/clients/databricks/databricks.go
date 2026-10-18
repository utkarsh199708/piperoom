// Package databricks implements clients.Warehouse and clients.CountReader
// against a Databricks SQL warehouse (Unity Catalog), selected with
// WAREHOUSE_KIND=databricks. See README.md in this folder for the DSN shape
// and the grants.
//
// Read-only access comes from the credential, not from this package: give
// the token's user or service principal CAN USE on the SQL warehouse and,
// in Unity Catalog, USE CATALOG on the catalog, USE SCHEMA on the sink
// schemas and SELECT on the sink tables (or the schemas). None of those can
// write. The driver has no read-only session switch, so a principal with
// MODIFY could still write if this package sent a write; it never does: it
// only sends the fixed SELECT statements below, identifiers are checked
// against ^[a-zA-Z_][a-zA-Z0-9_]*$ and backtick-quoted, values are named
// parameters (:name, DBR 14.1 or later), and no SQL text from a tool
// reaches a query.
//
// Driver logging: databricks-sql-go logs through its own zerolog logger to
// stderr, outside log/slog, and its messages can carry server error text.
// New switches it off (process-wide, like the snowflake client pins its
// driver logger); every failure reaches the caller as a redacted error.
package databricks

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	dbsql "github.com/databricks/databricks-sql-go"
	dbsqllog "github.com/databricks/databricks-sql-go/logger"

	"piperoom/internal/clients"
)

const defaultTimeout = 10 * time.Second

// DB is a Databricks SQL client implementing clients.Warehouse and
// clients.CountReader.
type DB struct {
	cfg     Config
	r       runner
	close   func() error
	secrets []string // credential strings scrubbed from every error text
}

var (
	_ clients.Warehouse   = (*DB)(nil)
	_ clients.CountReader = (*DB)(nil)
)

// New parses dsn (see ParseDSN), opens a connection pool and verifies it
// with a SELECT 1 under a 10 s deadline. Every error is scrubbed of the
// DSN's credentials; internal/wiring may still pass it through Redact.
func New(ctx context.Context, dsn string) (*DB, error) {
	cfg, err := ParseDSN(dsn)
	if err != nil {
		return nil, Redact(err, dsn)
	}
	d, err := open(ctx, cfg, "https://"+cfg.Host, http.DefaultTransport)
	if err != nil {
		return nil, Redact(err, dsn)
	}
	return d, nil
}

// open builds the connector from cfg. host carries the scheme: always
// https:// in New; tests pass an httptest http:// URL.
func open(ctx context.Context, cfg Config, host string, base http.RoundTripper) (*DB, error) {
	if err := dbsqllog.SetLogLevel("disabled"); err != nil {
		return nil, fmt.Errorf("databricks: set driver log level: %w", err)
	}
	opts := []dbsql.ConnOption{
		dbsql.WithServerHostname(host),
		dbsql.WithPort(cfg.Port),
		dbsql.WithHTTPPath(cfg.HTTPPath),
		dbsql.WithInitialNamespace(cfg.Catalog, cfg.Schema),
		// UTC sessions: TIMESTAMP values come back in the session zone and
		// TIMESTAMP_NTZ compares against it, so pin it.
		dbsql.WithSessionParams(map[string]string{"timezone": "UTC"}),
		// Server-side bound as well as the context deadline.
		dbsql.WithTimeout(defaultTimeout),
		// Results inline over the guarded transport, never staged in cloud
		// storage and downloaded by a second, uncapped client.
		dbsql.WithCloudFetch(false),
		dbsql.WithMaxRows(maxCountBuckets + 1),
		dbsql.WithRetries(2, 250*time.Millisecond, 2*time.Second),
		dbsql.WithUserAgentEntry("piperoom"),
		dbsql.WithTransport(guardTransport{base: base}),
	}
	switch cfg.Auth {
	case authM2M:
		opts = append(opts, dbsql.WithClientCredentials(cfg.ClientID, cfg.ClientSecret))
	default:
		opts = append(opts, dbsql.WithAccessToken(cfg.Token))
	}
	connector, err := dbsql.NewConnector(opts...)
	if err != nil {
		return nil, fmt.Errorf("databricks: connector: %w", err)
	}
	db := sql.OpenDB(connector)
	// Sessions expire on the server after idle time; retire pooled ones
	// well before.
	db.SetConnMaxIdleTime(15 * time.Minute)
	db.SetConnMaxLifetime(time.Hour)
	db.SetMaxOpenConns(8)

	d := &DB{cfg: cfg, r: sqlRunner{db: db}, close: db.Close, secrets: credentialStrings(cfg)}
	pctx, cancel := context.WithTimeout(ctx, defaultTimeout)
	defer cancel()
	if _, st, err := d.run(pctx, query{SQL: "SELECT 1", MaxRows: 1}); err != nil {
		_ = db.Close()
		return nil, d.wrapQueryErr(pctx, st, "ping", cfg.Host, err)
	}
	return d, nil
}

// newWithRunner builds a DB over r, for tests.
func newWithRunner(cfg Config, r runner) *DB {
	return &DB{cfg: cfg, r: r, secrets: credentialStrings(cfg)}
}

// credentialStrings are the secrets of cfg, as they could appear in text.
func credentialStrings(cfg Config) []string {
	var out []string
	for _, s := range []string{cfg.Token, cfg.ClientSecret} {
		if len(s) >= minSecretLen {
			out = append(out, s)
		}
	}
	return out
}

// Close releases the connection pool.
func (d *DB) Close() error {
	if d == nil || d.close == nil {
		return nil
	}
	return d.close()
}

// Freshness returns max(tsColumn) of table ("schema.table") in UTC. A zero
// time means the table has no rows (or only NULLs). tsColumn must be a
// TIMESTAMP or TIMESTAMP_NTZ column (NTZ is read as UTC wall time); any other
// type is ErrNotTimestamp, decided from the result schema before the value
// is looked at, so no error carries a value of the column.
func (d *DB) Freshness(ctx context.Context, table, tsColumn string) (time.Time, error) {
	ctx, cancel := context.WithTimeout(ctx, defaultTimeout)
	defer cancel()

	q, err := freshnessQuery(d.cfg.Catalog, table, tsColumn)
	if err != nil {
		return time.Time{}, err
	}
	res, st, err := d.run(ctx, q)
	if err != nil {
		return time.Time{}, d.wrapQueryErr(ctx, st, "freshness of", table, err)
	}
	if len(res.Fields) != 1 || !isTimestampType(res.Fields[0].Type) {
		return time.Time{}, fmt.Errorf("databricks: freshness of %s: column %q is not a TIMESTAMP column: %w", table, tsColumn, ErrNotTimestamp)
	}
	if len(res.Rows) == 0 || len(res.Rows[0]) == 0 {
		return time.Time{}, nil
	}
	switch v := res.Rows[0][0].(type) {
	case nil:
		return time.Time{}, nil
	case time.Time:
		return v.UTC(), nil
	default:
		return time.Time{}, fmt.Errorf("databricks: freshness of %s: column %q decoded as %T: %w", table, tsColumn, v, ErrNotTimestamp)
	}
}

func isTimestampType(t string) bool {
	return t == "TIMESTAMP" || t == "TIMESTAMP_NTZ"
}

func freshnessQuery(catalog, table, tsColumn string) (query, error) {
	t, err := parseTable(catalog, table)
	if err != nil {
		return query{}, err
	}
	if err := validateIdent(tsColumn); err != nil {
		return query{}, err
	}
	return query{SQL: "SELECT MAX(" + quote(tsColumn) + ") AS newest FROM " + t.sql(), MaxRows: 1}, nil
}

// Row reads one row of table ("schema.table") whose pkColumn equals pk. The
// key is compared as STRING (CAST(col AS STRING) = :key), so a BIGINT,
// DECIMAL or STRING key all match the string a caller has. Column names are
// lower-cased (Databricks names are case-insensitive), matching the Postgres
// warehouse client; values become JSON-friendly (numbers as strings, times
// as RFC 3339, binary as its length).
func (d *DB) Row(ctx context.Context, table, pkColumn, pk string) (map[string]any, bool, error) {
	ctx, cancel := context.WithTimeout(ctx, defaultTimeout)
	defer cancel()

	q, err := rowQuery(d.cfg.Catalog, table, pkColumn, pk)
	if err != nil {
		return nil, false, err
	}
	res, st, err := d.run(ctx, q)
	if err != nil {
		return nil, false, d.wrapQueryErr(ctx, st, "query row from", table, err)
	}
	if len(res.Rows) == 0 {
		return nil, false, nil
	}
	row := res.Rows[0]
	if len(row) != len(res.Fields) {
		return nil, false, fmt.Errorf("databricks: row from %s: %d values for %d columns", table, len(row), len(res.Fields))
	}
	m := make(map[string]any, len(row))
	for i, f := range res.Fields {
		m[strings.ToLower(f.Name)] = toJSON(f.Type, row[i])
	}
	return m, true, nil
}

func rowQuery(catalog, table, pkColumn, pk string) (query, error) {
	t, err := parseTable(catalog, table)
	if err != nil {
		return query{}, err
	}
	if err := validateIdent(pkColumn); err != nil {
		return query{}, err
	}
	return query{
		SQL:     "SELECT * FROM " + t.sql() + " WHERE CAST(" + quote(pkColumn) + " AS STRING) = :key LIMIT 1",
		Params:  []sql.NamedArg{sql.Named("key", pk)},
		MaxRows: 1,
	}, nil
}

// PrimaryKeyColumn returns the declared primary key column of table
// ("schema.table") from the catalog's information_schema. Unity Catalog
// primary keys are informational (not enforced), but a declared one is what
// the pipeline owners maintain. No declared key, or a table the credential
// cannot see, is clients.ErrNotFound; a composite key is
// clients.ErrNotSupported (no single column names one row). warehouse_row
// uses it to refuse lookups by any other column. The name comes back
// lower-cased, as Row's column names do.
func (d *DB) PrimaryKeyColumn(ctx context.Context, table string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, defaultTimeout)
	defer cancel()

	q, err := pkQuery(d.cfg.Catalog, table)
	if err != nil {
		return "", err
	}
	res, st, err := d.run(ctx, q)
	if err != nil {
		return "", d.wrapQueryErr(ctx, st, "primary key of", table, err)
	}
	var cols []string
	for _, r := range res.Rows {
		if len(r) > 0 {
			if s, ok := r[0].(string); ok && s != "" {
				cols = append(cols, strings.ToLower(s))
			}
		}
	}
	switch {
	case len(cols) == 0:
		return "", fmt.Errorf("databricks: primary key of %s: table has no declared primary key (it may exist): %w", table, clients.ErrNotFound)
	case len(cols) > 1:
		return "", fmt.Errorf("databricks: primary key of %s is composite (%d columns): %w", table, len(cols), clients.ErrNotSupported)
	}
	return cols[0], nil
}

func pkQuery(catalog, table string) (query, error) {
	t, err := parseTable(catalog, table)
	if err != nil {
		return query{}, err
	}
	is := quote(t.catalog) + ".information_schema"
	// Unity Catalog stores schema and table names lower-case; compare
	// lower-cased so "Sales.Orders" finds them too.
	return query{
		SQL: "SELECT k.column_name FROM " + is + ".table_constraints AS c" +
			" JOIN " + is + ".key_column_usage AS k" +
			" ON k.constraint_catalog = c.constraint_catalog AND k.constraint_schema = c.constraint_schema" +
			" AND k.constraint_name = c.constraint_name" +
			" WHERE lower(c.table_schema) = lower(:schema) AND lower(c.table_name) = lower(:table)" +
			" AND c.constraint_type = 'PRIMARY KEY'" +
			" ORDER BY k.ordinal_position",
		Params:  []sql.NamedArg{sql.Named("schema", t.schema), sql.Named("table", t.table)},
		MaxRows: 16,
	}, nil
}

// toJSON converts a scanned Databricks value into a JSON-friendly one.
// Numbers become strings (BIGINT beyond 2^53 and DECIMAL must not pass
// through float64); BINARY becomes its length; ARRAY, MAP and STRUCT arrive
// as JSON text and stay text.
func toJSON(dbType string, v any) any {
	switch t := v.(type) {
	case nil:
		return nil
	case bool:
		return t
	case string:
		return t
	case []byte:
		if dbType == "BINARY" {
			return fmt.Sprintf("<%d bytes>", len(t))
		}
		return string(t)
	case int8:
		return strconv.FormatInt(int64(t), 10)
	case int16:
		return strconv.FormatInt(int64(t), 10)
	case int32:
		return strconv.FormatInt(int64(t), 10)
	case int64:
		return strconv.FormatInt(t, 10)
	case float32:
		return strconv.FormatFloat(float64(t), 'f', -1, 32)
	case float64:
		return strconv.FormatFloat(t, 'f', -1, 64)
	case time.Time:
		if dbType == "DATE" {
			return t.Format(time.DateOnly)
		}
		return t.UTC().Format(time.RFC3339Nano)
	default:
		b, err := json.Marshal(t)
		if err != nil {
			return fmt.Sprintf("<%T>", t)
		}
		return string(b)
	}
}

// asInt reads a COUNT or BIGINT cell.
func asInt(v any) (int64, error) {
	switch t := v.(type) {
	case int64:
		return t, nil
	case int32:
		return int64(t), nil
	case int:
		return int64(t), nil
	case string:
		return strconv.ParseInt(t, 10, 64)
	case []byte:
		return strconv.ParseInt(string(t), 10, 64)
	}
	return 0, errors.New("not an integer")
}
