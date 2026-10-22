package pgconv

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"

	"piperoom/internal/clients"
	"piperoom/internal/tools"
)

// QueryFunc runs one read on a client's read-only pool and records it as a
// provenance call. receipt replaces args in that record when it is not nil,
// so a call can show "<n keys>" instead of the key values themselves.
type QueryFunc func(ctx context.Context, receipt []any, sql string, args ...any) (pgx.Rows, error)

// Reconcile is the clients.Reconciler (X1) of both Postgres-backed clients:
// the source database (postgres) and the dev warehouse (warehouse). They
// differ only in the scope list a table must be on and in the error prefix.
type Reconcile struct {
	// Name prefixes every error: "postgres" or "warehouse".
	Name string
	// Sink selects the scope list: the pipeline's sink_tables when true,
	// its source_tables otherwise.
	Sink bool
	// Query is the client's recorded query on its read-only pool.
	Query QueryFunc
}

// maxIdentBytes is Postgres's NAMEDATALEN - 1. A longer name is cut by the
// server without a word, so it could address another column: refused.
const maxIdentBytes = 63

var reconcileIdentRe = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

// timeTypes are the column types RowTimes returns: date and time of day
// together, so a value is a point in time.
var timeTypes = []uint32{pgtype.TimestamptzOID, pgtype.TimestampOID, pgtype.DateOID}

// CountBetween counts the rows of table whose tsColumn is in [from, to). An
// empty window (to == from) is 0 with no I/O.
func (r Reconcile) CountBetween(ctx context.Context, table, tsColumn string, from, to time.Time) (int64, error) {
	rel, err := r.table(table)
	if err != nil {
		return 0, err
	}
	if err := r.ident("timestamp column", tsColumn); err != nil {
		return 0, err
	}
	if to.Before(from) {
		return 0, fmt.Errorf("%s: count %s: window ends (%s) before it starts (%s): %w", r.Name, table,
			to.UTC().Format(time.RFC3339), from.UTC().Format(time.RFC3339), tools.ErrBadInput)
	}
	if err := r.scope(ctx, table); err != nil {
		return 0, err
	}
	if to.Equal(from) {
		return 0, nil
	}
	ctx, cancel := WithDeadline(ctx, clients.ReconcileTimeout)
	defer cancel()

	// The bounds are untyped parameters: they take the column's type, so a
	// timestamp column compares against the UTC wall clock.
	rows, err := r.Query(ctx, nil, countBetweenSQL(rel, tsColumn), from.UTC(), to.UTC())
	if err != nil {
		return 0, r.fail(ctx, "count "+table, err)
	}
	defer rows.Close()
	var n int64
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return 0, r.fail(ctx, "count "+table, err)
		}
		return 0, fmt.Errorf("%s: count %s: no row returned", r.Name, table)
	}
	if err := rows.Scan(&n); err != nil {
		return 0, r.fail(ctx, "count "+table, err)
	}
	if err := rows.Err(); err != nil {
		return 0, r.fail(ctx, "count "+table, err)
	}
	return n, nil
}

// SampleKeys returns the primary-key values, as text, of the newest rows of
// table with tsColumn >= since, newest first, at most
// clients.ReconcileSampleSize(n). Only pkColumn is selected.
func (r Reconcile) SampleKeys(ctx context.Context, table, pkColumn, tsColumn string, since time.Time, n int) ([]string, error) {
	rel, err := r.table(table)
	if err != nil {
		return nil, err
	}
	if err := r.ident("key column", pkColumn); err != nil {
		return nil, err
	}
	if err := r.ident("timestamp column", tsColumn); err != nil {
		return nil, err
	}
	size := clients.ReconcileSampleSize(n)
	if size == 0 {
		return nil, fmt.Errorf("%s: sample keys of %s: n %d: want at least 1: %w", r.Name, table, n, tools.ErrBadInput)
	}
	if err := r.scope(ctx, table); err != nil {
		return nil, err
	}
	ctx, cancel := WithDeadline(ctx, clients.ReconcileTimeout)
	defer cancel()

	rows, err := r.Query(ctx, nil, sampleKeysSQL(rel, pkColumn, tsColumn), since.UTC(), size)
	if err != nil {
		return nil, r.fail(ctx, "sample keys of "+table, err)
	}
	defer rows.Close()
	keys := make([]string, 0, size)
	for rows.Next() {
		var k string
		if err := rows.Scan(&k); err != nil {
			return nil, r.fail(ctx, "sample keys of "+table, err)
		}
		keys = append(keys, k)
	}
	if err := rows.Err(); err != nil {
		return nil, r.fail(ctx, "sample keys of "+table, err)
	}
	return keys, nil
}

// RowTimes reads the timestamp columns tsColumns of the rows whose pkColumn
// is one of keys. It first reads the columns' types from the catalog: a
// column that is not a date/time type is refused before any row is read,
// and the key type is what the text keys are cast to, so the key column's
// index is used. An infinite timestamp is absent like a NULL one: it is no
// point in time to compare.
func (r Reconcile) RowTimes(ctx context.Context, table, pkColumn string, keys []string, tsColumns []string) (map[string]map[string]time.Time, error) {
	rel, err := r.table(table)
	if err != nil {
		return nil, err
	}
	if err := r.ident("key column", pkColumn); err != nil {
		return nil, err
	}
	if len(tsColumns) == 0 {
		return nil, fmt.Errorf("%s: row times of %s: no timestamp column named: %w", r.Name, table, tools.ErrBadInput)
	}
	var cols []string
	for _, c := range tsColumns {
		if err := r.ident("timestamp column", c); err != nil {
			return nil, err
		}
		if !slices.Contains(cols, c) {
			cols = append(cols, c)
		}
	}
	if len(keys) > clients.ReconcileMaxKeys {
		return nil, fmt.Errorf("%s: row times of %s: %d keys, at most %d: %w", r.Name, table, len(keys), clients.ReconcileMaxKeys, tools.ErrBadInput)
	}
	if err := r.scope(ctx, table); err != nil {
		return nil, err
	}
	out := map[string]map[string]time.Time{}
	if len(keys) == 0 {
		return out, nil
	}
	distinct := make([]string, 0, len(keys))
	seen := make(map[string]bool, len(keys))
	for _, k := range keys {
		if !seen[k] {
			seen[k] = true
			distinct = append(distinct, k)
		}
	}
	ctx, cancel := WithDeadline(ctx, clients.ReconcileTimeout)
	defer cancel()

	types, err := r.columnTypes(ctx, rel, table, append([]string{pkColumn}, cols...))
	if err != nil {
		return nil, err
	}
	keyType := types[pkColumn]
	for _, c := range cols {
		if !slices.Contains(timeTypes, types[c].oid) {
			return nil, fmt.Errorf("%s: row times of %s: column %q is %s, not a date or timestamp column: %w", r.Name, table, c, types[c].name, tools.ErrBadInput)
		}
	}
	if !reconcileIdentRe.MatchString(keyType.schema) || !reconcileIdentRe.MatchString(keyType.name) {
		return nil, fmt.Errorf("%s: row times of %s: key column %q has type %q, which this client cannot cast to: %w", r.Name, table, pkColumn, cut(keyType.schema+"."+keyType.name), clients.ErrNotSupported)
	}

	// LIMIT one more row than keys: a unique key never reaches it, so more
	// rows mean pkColumn is no key and the read stops there.
	limit := len(distinct) + 1
	receipt := []any{fmt.Sprintf("<%d keys>", len(distinct)), limit}
	rows, err := r.Query(ctx, receipt, rowTimesSQL(rel, pkColumn, cols, pgx.Identifier{keyType.schema, keyType.name}), distinct, limit)
	if err != nil {
		return nil, r.fail(ctx, "row times of "+table, err)
	}
	defer rows.Close()

	// The catalog was read in another statement: check the row description
	// too, so a column whose type changed in between is refused before a
	// value of it is decoded.
	fds := rows.FieldDescriptions()
	if len(fds) != len(cols)+1 || fds[0].DataTypeOID != pgtype.TextOID {
		return nil, fmt.Errorf("%s: row times of %s: unexpected result columns: %w", r.Name, table, tools.ErrBadInput)
	}
	for i, c := range cols {
		if oid := fds[i+1].DataTypeOID; oid != types[c].oid || !slices.Contains(timeTypes, oid) {
			return nil, fmt.Errorf("%s: row times of %s: column %q changed type during the read: %w", r.Name, table, c, tools.ErrBadInput)
		}
	}
	read := 0
	for rows.Next() {
		if read++; read > len(distinct) {
			return nil, fmt.Errorf("%s: row times of %s: %q is not a unique key (more rows than keys): %w", r.Name, table, pkColumn, tools.ErrBadInput)
		}
		var key string
		dest := make([]any, 0, len(cols)+1)
		dest = append(dest, &key)
		for _, fd := range fds[1:] {
			dest = append(dest, timeDest(fd.DataTypeOID))
		}
		if err := rows.Scan(dest...); err != nil {
			return nil, r.fail(ctx, "row times of "+table, err)
		}
		times := map[string]time.Time{}
		for i, c := range cols {
			if t, ok := timeOf(dest[i+1]); ok {
				times[c] = t
			}
		}
		out[key] = times
	}
	if err := rows.Err(); err != nil {
		return nil, r.fail(ctx, "row times of "+table, err)
	}
	return out, nil
}

// columnType is one column's type as the catalog names it.
type columnType struct {
	oid          uint32
	schema, name string
}

// columnTypes reads the types of the named columns of rel. A missing table
// or column is clients.ErrNotFound.
func (r Reconcile) columnTypes(ctx context.Context, rel pgx.Identifier, table string, cols []string) (map[string]columnType, error) {
	rows, err := r.Query(ctx, nil, columnTypesSQL, rel[0], rel[1], cols)
	if err != nil {
		return nil, r.fail(ctx, "column types of "+table, err)
	}
	defer rows.Close()
	out := map[string]columnType{}
	for rows.Next() {
		var name string
		var oid int64
		var ct columnType
		if err := rows.Scan(&name, &oid, &ct.schema, &ct.name); err != nil {
			return nil, r.fail(ctx, "column types of "+table, err)
		}
		ct.oid = uint32(oid)
		out[name] = ct
	}
	if err := rows.Err(); err != nil {
		return nil, r.fail(ctx, "column types of "+table, err)
	}
	for _, c := range cols {
		if _, ok := out[c]; !ok {
			return nil, fmt.Errorf("%s: %s has no column %q: %w", r.Name, table, c, clients.ErrNotFound)
		}
	}
	return out, nil
}

// columnTypesSQL reads column types from pg_catalog, which a SELECT-only
// role can see (information_schema hides what the role does not own).
const columnTypesSQL = `SELECT a.attname, a.atttypid::int8, tn.nspname, t.typname
		FROM pg_catalog.pg_attribute a
		JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
		JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
		JOIN pg_catalog.pg_type t ON t.oid = a.atttypid
		JOIN pg_catalog.pg_namespace tn ON tn.oid = t.typnamespace
		WHERE n.nspname = $1 AND c.relname = $2 AND a.attname = ANY($3::text[])
		  AND a.attnum > 0 AND NOT a.attisdropped`

// countBetweenSQL builds the window count. Every identifier was validated
// and is quoted again here.
func countBetweenSQL(rel pgx.Identifier, ts string) string {
	c := pgx.Identifier{ts}.Sanitize()
	return fmt.Sprintf(`SELECT count(*) FROM %s WHERE %s >= $1 AND %s < $2`, rel.Sanitize(), c, c)
}

// sampleKeysSQL builds the newest-keys read: the key as text and nothing
// else. The key breaks ties so a sample is repeatable.
func sampleKeysSQL(rel pgx.Identifier, pk, ts string) string {
	k, c := pgx.Identifier{pk}.Sanitize(), pgx.Identifier{ts}.Sanitize()
	return fmt.Sprintf(`SELECT %[1]s::text FROM %[2]s WHERE %[3]s >= $1 AND %[1]s IS NOT NULL ORDER BY %[3]s DESC, %[1]s DESC LIMIT $2`, k, rel.Sanitize(), c)
}

// rowTimesSQL builds the timestamp read: the key as text and the named
// columns, never another. The text keys are cast to the key's own type, so
// the comparison can use the key's index.
func rowTimesSQL(rel pgx.Identifier, pk string, cols []string, keyType pgx.Identifier) string {
	k := pgx.Identifier{pk}.Sanitize()
	sel := make([]string, 0, len(cols)+1)
	sel = append(sel, k+"::text")
	for _, c := range cols {
		sel = append(sel, pgx.Identifier{c}.Sanitize())
	}
	return fmt.Sprintf(`SELECT %s FROM %s WHERE %s = ANY($1::text[]::%s[]) LIMIT $2`, strings.Join(sel, ", "), rel.Sanitize(), k, keyType.Sanitize())
}

func timeDest(oid uint32) any {
	switch oid {
	case pgtype.TimestampOID:
		return &pgtype.Timestamp{}
	case pgtype.DateOID:
		return &pgtype.Date{}
	}
	return &pgtype.Timestamptz{}
}

// timeOf returns a scanned value in UTC; NULL and +/-infinity are no time.
func timeOf(v any) (time.Time, bool) {
	switch t := v.(type) {
	case *pgtype.Timestamptz:
		return t.Time.UTC(), t.Valid && t.InfinityModifier == pgtype.Finite
	case *pgtype.Timestamp:
		return t.Time.UTC(), t.Valid && t.InfinityModifier == pgtype.Finite
	case *pgtype.Date:
		return t.Time.UTC(), t.Valid && t.InfinityModifier == pgtype.Finite
	}
	return time.Time{}, false
}

// table validates "schema.table" and returns it as an identifier.
func (r Reconcile) table(table string) (pgx.Identifier, error) {
	schema, tbl, ok := strings.Cut(table, ".")
	if !ok || !validIdent(schema) || !validIdent(tbl) {
		return nil, fmt.Errorf("%s: table must be \"schema.table\" of letters, digits and _, got %q: %w", r.Name, cut(table), tools.ErrBadInput)
	}
	return pgx.Identifier{schema, tbl}, nil
}

func (r Reconcile) ident(what, name string) error {
	if !validIdent(name) {
		return fmt.Errorf("%s: invalid %s %q: want letters, digits and _: %w", r.Name, what, cut(name), tools.ErrBadInput)
	}
	return nil
}

func validIdent(s string) bool {
	return len(s) <= maxIdentBytes && reconcileIdentRe.MatchString(s)
}

// scope fails closed like the source writer: the pipeline in ctx must list
// table on this side; the explicit tools.WithUnscoped opt-out passes; a
// context with no pipeline, or with only its id, is tools.ErrNoScope (a
// client has no owners config to resolve an id and must not guess).
func (r Reconcile) scope(ctx context.Context, table string) error {
	sc, err := tools.ScopeFor(ctx, nil)
	if err != nil {
		if !errors.Is(err, tools.ErrNoScope) {
			err = fmt.Errorf("%w: %w", tools.ErrNoScope, err)
		}
		return fmt.Errorf("%s: reconcile refused: it needs the pipeline in ctx (tools.WithPipeline) or the explicit tools.WithUnscoped opt-out: %w", r.Name, err)
	}
	if r.Sink {
		err = sc.CheckSinkTable(table)
	} else {
		err = sc.CheckSourceTable(table)
	}
	if err != nil {
		return fmt.Errorf("%s: reconcile: %w", r.Name, err)
	}
	return nil
}

// fail wraps a read error so that it never carries a value: a data
// exception (class 22: a key that is not of the key's type, a bound out of
// range) and a decode error become fixed phrases, since their messages can
// quote the value. A missing object gains clients.ErrNotFound. A call cut by
// its deadline always wraps context.DeadlineExceeded, whatever pgx made of
// the server's cancel.
func (r Reconcile) fail(ctx context.Context, what string, err error) error {
	var pgErr *pgconn.PgError
	var scanErr pgx.ScanArgError
	switch {
	case errors.As(err, &pgErr) && strings.HasPrefix(pgErr.Code, "22"):
		err = fmt.Errorf("a key or bound is not a value of the column's type (SQLSTATE %s): %w", pgErr.Code, tools.ErrBadInput)
	case errors.As(err, &pgErr) && (pgErr.Code == "3F000" || pgErr.Code == "42P01" || pgErr.Code == "42703"):
		err = fmt.Errorf("%w: %w", clients.ErrNotFound, err)
	case errors.As(err, &scanErr):
		err = fmt.Errorf("a result column could not be decoded (%T)", scanErr.Err)
	}
	if errors.Is(ctx.Err(), context.DeadlineExceeded) && !errors.Is(err, context.DeadlineExceeded) {
		err = fmt.Errorf("%w: %w", context.DeadlineExceeded, err)
	}
	return fmt.Errorf("%s: %s: %w", r.Name, what, err)
}

// cut shortens a caller-supplied name before an error echoes it.
func cut(s string) string {
	const max = 80
	if utf8.RuneCountInString(s) <= max {
		return s
	}
	return string([]rune(s)[:max]) + "…"
}
