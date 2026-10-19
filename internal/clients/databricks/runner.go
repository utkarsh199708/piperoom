package databricks

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"piperoom/internal/tools"
)

// query is one read statement: fixed SQL text with quoted, validated
// identifiers and every value as a named parameter (:name).
type query struct {
	SQL     string
	Params  []sql.NamedArg
	MaxRows int // rows read from the result; the rest are not fetched
}

// field is one result column: its name and the driver's type name
// ("TIMESTAMP", "STRING", "DECIMAL", ...).
type field struct {
	Name string
	Type string
}

// result is a query's columns and at most query.MaxRows rows.
type result struct {
	Fields []field
	Rows   [][]any
}

// runner runs a read query. The production implementation (sqlRunner)
// wraps *sql.DB over the databricks-sql-go driver; unit tests use a fake.
type runner interface {
	run(ctx context.Context, q query) (result, error)
}

// sqlRunner runs queries through database/sql.
type sqlRunner struct {
	db *sql.DB
}

func (r sqlRunner) run(ctx context.Context, q query) (result, error) {
	args := make([]any, len(q.Params))
	for i, p := range q.Params {
		args[i] = p
	}
	rows, err := r.db.QueryContext(ctx, q.SQL, args...)
	if err != nil {
		return result{}, err
	}
	defer rows.Close()
	return scanRows(rows, q.MaxRows)
}

// scanRows reads the column types and at most maxRows rows. Closing rows
// early (the caller's defer) stops the fetch of the rest.
func scanRows(rows *sql.Rows, maxRows int) (result, error) {
	cts, err := rows.ColumnTypes()
	if err != nil {
		return result{}, fmt.Errorf("columns: %w", err)
	}
	var res result
	for _, ct := range cts {
		res.Fields = append(res.Fields, field{Name: ct.Name(), Type: strings.ToUpper(ct.DatabaseTypeName())})
	}
	for len(res.Rows) < maxRows && rows.Next() {
		vals := make([]any, len(cts))
		ptrs := make([]any, len(cts))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return result{}, fmt.Errorf("decode: %w", err)
		}
		res.Rows = append(res.Rows, vals)
	}
	if err := rows.Err(); err != nil {
		return result{}, err
	}
	return res, nil
}

// run executes q and records it as a provenance call, on success and on
// error. Text keeps the :name placeholders; values appear only as rendered
// parameters. The HTTP status the transport saw for this call rides along in
// ctx so wrapQueryErr can classify a 401/403/404 without reading text.
func (d *DB) run(ctx context.Context, q query) (result, *callStatus, error) {
	st := &callStatus{}
	ctx = withCallStatus(ctx, st)
	start := time.Now()
	res, err := d.r.run(ctx, q)
	tools.Record(ctx, tools.Call{
		Kind:           tools.CallSQL,
		Target:         d.cfg.target(),
		Text:           strings.Join(strings.Fields(q.SQL), " "),
		ParamsRedacted: renderParams(q.Params),
		At:             start,
		TookMS:         time.Since(start).Milliseconds(),
	})
	return res, st, err
}

// maxParamBytes cuts a long string parameter in a receipt.
const maxParamBytes = 64

func renderParams(ps []sql.NamedArg) []string {
	var out []string
	for _, p := range ps {
		out = append(out, ":"+p.Name+"="+renderValue(p.Value))
	}
	return out
}

func renderValue(v any) string {
	switch t := v.(type) {
	case nil:
		return "NULL"
	case string:
		return cut(t, maxParamBytes)
	case time.Time:
		return t.UTC().Format(time.RFC3339)
	case []byte:
		return fmt.Sprintf("<%d bytes>", len(t))
	default:
		return cut(fmt.Sprint(t), maxParamBytes)
	}
}

// cut shortens s to n bytes on a rune boundary.
func cut(s string, n int) string {
	if len(s) <= n {
		return s
	}
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n] + "…"
}
