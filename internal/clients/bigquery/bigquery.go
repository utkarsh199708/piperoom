// Package bigquery implements clients.Warehouse and clients.LoadErrorReader
// against Google BigQuery, selected with WAREHOUSE_KIND=bigquery. See
// README.md in this folder for the DSN shape and the IAM roles.
//
// Read-only access comes from the credential, not from this package: give
// the service account roles/bigquery.dataViewer on the sink datasets,
// roles/bigquery.jobUser on the billing project (to run query jobs) and,
// for LoadErrors, roles/bigquery.resourceViewer on the project (the
// bigquery.jobs.listAll permission INFORMATION_SCHEMA.JOBS needs). None of
// those can write table data. The package itself only ever sends the fixed
// SELECT statements below: identifiers are checked against
// ^[a-zA-Z_][a-zA-Z0-9_]*$ and backtick-quoted, values are named query
// parameters, and no SQL text from a tool reaches a query.
package bigquery

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"time"

	bq "cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
	"google.golang.org/api/option"

	"piperoom/internal/clients"
)

const defaultTimeout = 10 * time.Second

// DB is a BigQuery client implementing clients.Warehouse and
// clients.LoadErrorReader.
type DB struct {
	cfg   Config
	r     runner
	close func() error
}

var (
	_ clients.Warehouse       = (*DB)(nil)
	_ clients.LoadErrorReader = (*DB)(nil)
)

// New parses dsn (see ParseDSN), opens a client with Application Default
// Credentials and verifies it with a SELECT 1 under a 10 s deadline. opts are
// passed to bigquery.NewClient (tests point it at a fake endpoint).
func New(ctx context.Context, dsn string, opts ...option.ClientOption) (*DB, error) {
	cfg, err := ParseDSN(dsn)
	if err != nil {
		return nil, err
	}
	c, err := bq.NewClient(ctx, cfg.BillingProject, append([]option.ClientOption{option.WithUserAgent("piperoom")}, opts...)...)
	if err != nil {
		return nil, fmt.Errorf("bigquery: new client: %w", err)
	}
	d := &DB{cfg: cfg, r: clientRunner{c: c, location: cfg.Location, maxBytesBilled: cfg.MaxBytesBilled}, close: c.Close}
	pctx, cancel := context.WithTimeout(ctx, defaultTimeout)
	defer cancel()
	if _, err := d.run(pctx, query{SQL: "SELECT 1", MaxRows: 1}); err != nil {
		_ = c.Close()
		return nil, wrapQueryErr(pctx, "ping", cfg.Project, err)
	}
	return d, nil
}

// newWithRunner builds a DB over r, for tests.
func newWithRunner(cfg Config, r runner) *DB {
	return &DB{cfg: cfg, r: r}
}

// Close releases the client.
func (d *DB) Close() error {
	if d == nil || d.close == nil {
		return nil
	}
	return d.close()
}

// Freshness returns max(tsColumn) of table ("dataset.table") in UTC. A zero
// time means the table has no rows (or only NULLs). tsColumn must be a
// TIMESTAMP or DATETIME column (DATETIME is read as UTC wall time); any other
// type is ErrNotTimestamp, decided from the result schema before the value is
// looked at, so no error carries a value of the column.
func (d *DB) Freshness(ctx context.Context, table, tsColumn string) (time.Time, error) {
	ctx, cancel := context.WithTimeout(ctx, defaultTimeout)
	defer cancel()

	q, err := freshnessQuery(d.cfg.Project, table, tsColumn)
	if err != nil {
		return time.Time{}, err
	}
	res, err := d.run(ctx, q)
	if err != nil {
		return time.Time{}, wrapQueryErr(ctx, "freshness of", table, err)
	}
	if len(res.Rows) == 0 || len(res.Rows[0]) == 0 {
		return time.Time{}, nil
	}
	if len(res.Fields) != 1 || (res.Fields[0].Type != bq.TimestampFieldType && res.Fields[0].Type != bq.DateTimeFieldType) {
		return time.Time{}, fmt.Errorf("bigquery: freshness of %s: column %q is not a TIMESTAMP or DATETIME column: %w", table, tsColumn, ErrNotTimestamp)
	}
	switch v := res.Rows[0][0].(type) {
	case nil:
		return time.Time{}, nil
	case time.Time:
		return v.UTC(), nil
	case civil.DateTime:
		return v.In(time.UTC), nil
	default:
		return time.Time{}, fmt.Errorf("bigquery: freshness of %s: column %q decoded as %T: %w", table, tsColumn, v, ErrNotTimestamp)
	}
}

func freshnessQuery(project, table, tsColumn string) (query, error) {
	t, err := parseTable(project, table)
	if err != nil {
		return query{}, err
	}
	if err := validateIdent(tsColumn); err != nil {
		return query{}, err
	}
	return query{SQL: "SELECT MAX(" + quote(tsColumn) + ") AS newest FROM " + t.sql(), MaxRows: 1}, nil
}

// Row reads one row of table ("dataset.table") whose pkColumn equals pk.
// The key is compared as STRING (CAST(col AS STRING) = @key), so an INT64,
// NUMERIC or STRING key all match the string a caller has. Column names are
// lower-cased (BigQuery names are case-insensitive), matching the Postgres
// warehouse client; values become JSON-friendly (numbers as strings, times
// as RFC 3339).
func (d *DB) Row(ctx context.Context, table, pkColumn, pk string) (map[string]any, bool, error) {
	ctx, cancel := context.WithTimeout(ctx, defaultTimeout)
	defer cancel()

	q, err := rowQuery(d.cfg.Project, table, pkColumn, pk)
	if err != nil {
		return nil, false, err
	}
	res, err := d.run(ctx, q)
	if err != nil {
		return nil, false, wrapQueryErr(ctx, "query row from", table, err)
	}
	if len(res.Rows) == 0 {
		return nil, false, nil
	}
	row := res.Rows[0]
	if len(row) != len(res.Fields) {
		return nil, false, fmt.Errorf("bigquery: row from %s: %d values for %d columns", table, len(row), len(res.Fields))
	}
	m := make(map[string]any, len(row))
	for i, f := range res.Fields {
		m[strings.ToLower(f.Name)] = toJSON(f.Type, row[i])
	}
	return m, true, nil
}

func rowQuery(project, table, pkColumn, pk string) (query, error) {
	t, err := parseTable(project, table)
	if err != nil {
		return query{}, err
	}
	if err := validateIdent(pkColumn); err != nil {
		return query{}, err
	}
	return query{
		SQL:     "SELECT * FROM " + t.sql() + " WHERE CAST(" + quote(pkColumn) + " AS STRING) = @key LIMIT 1",
		Params:  []bq.QueryParameter{{Name: "key", Value: pk}},
		MaxRows: 1,
	}, nil
}

// PrimaryKeyColumn returns the declared primary key column of table
// ("dataset.table") from the dataset's INFORMATION_SCHEMA. BigQuery does not
// enforce primary keys, but a declared one is what the pipeline owners
// maintain. No declared key is clients.ErrNotFound; a composite key is
// clients.ErrNotSupported (no single column names one row). warehouse_row
// uses it to refuse lookups by any other column.
func (d *DB) PrimaryKeyColumn(ctx context.Context, table string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, defaultTimeout)
	defer cancel()

	q, err := pkQuery(d.cfg.Project, table)
	if err != nil {
		return "", err
	}
	res, err := d.run(ctx, q)
	if err != nil {
		return "", wrapQueryErr(ctx, "primary key of", table, err)
	}
	var cols []string
	for _, r := range res.Rows {
		if len(r) > 0 {
			cols = append(cols, asString(r[0]))
		}
	}
	switch {
	case len(cols) == 0:
		return "", fmt.Errorf("bigquery: primary key of %s: table has no declared primary key (it may exist): %w", table, clients.ErrNotFound)
	case len(cols) > 1:
		return "", fmt.Errorf("bigquery: primary key of %s is composite (%d columns): %w", table, len(cols), clients.ErrNotSupported)
	}
	return cols[0], nil
}

func pkQuery(project, table string) (query, error) {
	t, err := parseTable(project, table)
	if err != nil {
		return query{}, err
	}
	ds := t.datasetSQL()
	return query{
		SQL: "SELECT k.column_name FROM " + ds + ".INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS c" +
			" JOIN " + ds + ".INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS k" +
			" ON k.constraint_name = c.constraint_name AND k.table_name = c.table_name" +
			" WHERE c.table_name = @table AND c.constraint_type = 'PRIMARY KEY'" +
			" ORDER BY k.ordinal_position",
		Params:  []bq.QueryParameter{{Name: "table", Value: t.table}},
		MaxRows: 16,
	}, nil
}

// toJSON converts a BigQuery cell into a JSON-friendly value. Numbers become
// strings (INT64 beyond 2^53 and NUMERIC must not pass through float64).
func toJSON(ft bq.FieldType, v bq.Value) any {
	switch t := v.(type) {
	case nil:
		return nil
	case bool, string:
		return t
	case []byte:
		return fmt.Sprintf("<%d bytes>", len(t))
	case int64:
		return strconv.FormatInt(t, 10)
	case float64:
		return strconv.FormatFloat(t, 'f', -1, 64)
	case time.Time:
		return t.UTC().Format(time.RFC3339Nano)
	case civil.DateTime:
		return t.String()
	case civil.Date:
		return t.String()
	case civil.Time:
		return t.String()
	case *big.Rat:
		if t == nil {
			return nil
		}
		if ft == bq.BigNumericFieldType {
			return bq.BigNumericString(t)
		}
		return bq.NumericString(t)
	case []bq.Value:
		out := make([]any, len(t))
		for i, e := range t {
			out[i] = toJSON(ft, e)
		}
		return jsonString(out)
	default:
		return jsonString(t)
	}
}

func jsonString(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprintf("%v", v)
	}
	return string(b)
}

func asString(v bq.Value) string {
	switch t := v.(type) {
	case nil:
		return ""
	case string:
		return t
	default:
		return fmt.Sprint(t)
	}
}

func asInt(v bq.Value) int64 {
	switch t := v.(type) {
	case int64:
		return t
	case float64:
		return int64(t)
	case string:
		n, _ := strconv.ParseInt(t, 10, 64)
		return n
	}
	return 0
}
