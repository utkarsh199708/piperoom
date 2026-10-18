package postgres

import (
	"context"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"piperoom/internal/tools"
)

// querier is the part of *pgxpool.Pool that the query helpers use; tests
// replace it with a fake.
type querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// maxParamBytes cuts a long string argument in a receipt.
const maxParamBytes = 64

// targetOf names the database a pool talks to for a receipt: database, host
// and port, never the user or password.
func targetOf(pool *pgxpool.Pool) string {
	cc := pool.Config().ConnConfig
	return fmt.Sprintf("postgres %s@%s:%d", cc.Database, cc.Host, cc.Port)
}

// query runs d.q.Query and records the statement as a provenance call. The
// call is recorded when Query returns, on success and on error.
func (d *DB) query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	start := time.Now()
	rows, err := d.q.Query(ctx, sql, args...)
	d.record(ctx, start, sql, args)
	return rows, err
}

// queryRow is query for a single row. pgx reports a QueryRow error only at
// Scan, so the call is recorded there, with the time to the end of Scan.
func (d *DB) queryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	start := time.Now()
	return &recordedRow{Row: d.q.QueryRow(ctx, sql, args...), d: d, ctx: ctx, start: start, sql: sql, args: args}
}

type recordedRow struct {
	pgx.Row
	d     *DB
	ctx   context.Context
	start time.Time
	sql   string
	args  []any
}

func (r *recordedRow) Scan(dest ...any) error {
	err := r.Row.Scan(dest...)
	r.d.record(r.ctx, r.start, r.sql, r.args)
	return err
}

func (d *DB) record(ctx context.Context, start time.Time, sql string, args []any) {
	recordSQL(ctx, d.target, start, sql, args)
}

// recordSQL records one statement against target as a provenance call; the
// reader (DB) and the writer (Writer) share it.
func recordSQL(ctx context.Context, target string, start time.Time, sql string, args []any) {
	tools.Record(ctx, tools.Call{
		Kind:           tools.CallSQL,
		Target:         target,
		Text:           sqlText(sql),
		ParamsRedacted: redactArgs(args),
		At:             start,
		TookMS:         time.Since(start).Milliseconds(),
	})
}

// sqlText collapses whitespace runs to one space. Placeholders stay as
// written: values are never substituted into the text.
func sqlText(sql string) string {
	return strings.Join(strings.Fields(sql), " ")
}

// redactArgs renders query arguments as "$n=value". Names and identifiers are
// shown as they are; a long string is cut; bytes are never shown. pgx query
// options (the exec mode) are not arguments and are skipped. The primary key
// of Row stays, like in the tool summary.
func redactArgs(args []any) []string {
	var out []string
	n := 0
	for _, a := range args {
		switch a.(type) {
		case pgx.QueryExecMode, pgx.QueryResultFormats, pgx.QueryResultFormatsByOID:
			continue
		}
		n++
		out = append(out, fmt.Sprintf("$%d=%s", n, renderArg(a)))
	}
	return out
}

func renderArg(a any) string {
	switch v := a.(type) {
	case nil:
		return "NULL"
	case string:
		return cutParam(v)
	case []string:
		parts := make([]string, len(v))
		for i, s := range v {
			parts[i] = cutParam(s)
		}
		return "[" + strings.Join(parts, ",") + "]"
	case time.Time:
		return v.UTC().Format(time.RFC3339)
	case time.Duration:
		return v.String()
	case []byte:
		return fmt.Sprintf("<%d bytes>", len(v))
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64, bool:
		return fmt.Sprint(v)
	default:
		return cutParam(fmt.Sprintf("%v", v))
	}
}

// cutParam shortens s to maxParamBytes on a rune boundary.
func cutParam(s string) string {
	if len(s) <= maxParamBytes {
		return s
	}
	cut := maxParamBytes
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut] + "…"
}
