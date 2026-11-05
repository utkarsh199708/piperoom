package bigquery

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	bq "cloud.google.com/go/bigquery"
	"google.golang.org/api/iterator"

	"piperoom/internal/tools"
)

// query is one read statement: fixed SQL text with quoted, validated
// identifiers and every value as a named parameter.
type query struct {
	SQL     string
	Params  []bq.QueryParameter
	MaxRows int // rows read from the result; the rest are not fetched
}

// field is one result column.
type field struct {
	Name string
	Type bq.FieldType
}

// result is a query's columns and at most query.MaxRows rows.
type result struct {
	Fields []field
	Rows   [][]bq.Value
}

// runner runs a read query. The production implementation (clientRunner)
// wraps *bigquery.Client; unit tests use a fake, so they need no network.
type runner interface {
	run(ctx context.Context, q query) (result, error)
}

// clientRunner runs queries through the BigQuery client library.
type clientRunner struct {
	c              *bq.Client
	location       string
	maxBytesBilled int64
}

func (r clientRunner) run(ctx context.Context, q query) (result, error) {
	bqq := r.c.Query(q.SQL)
	bqq.Parameters = q.Params
	bqq.Location = r.location
	bqq.MaxBytesBilled = r.maxBytesBilled
	// Labels make Piperoom's jobs easy to find (and audit) in the project's
	// own INFORMATION_SCHEMA.JOBS.
	bqq.Labels = map[string]string{"app": "piperoom"}
	it, err := bqq.Read(ctx)
	if err != nil {
		return result{}, err
	}
	var res result
	for len(res.Rows) < q.MaxRows {
		var vals []bq.Value
		err := it.Next(&vals)
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return result{}, err
		}
		res.Rows = append(res.Rows, vals)
	}
	// The schema is only known after the first Next on the jobs.query path;
	// with no rows it can still be empty, which callers treat as "no rows".
	for _, f := range it.Schema {
		res.Fields = append(res.Fields, field{Name: f.Name, Type: f.Type})
	}
	return res, nil
}

// run executes q under the client's deadline and records it as a provenance
// call, on success and on error. Text keeps the @name placeholders; values
// appear only as rendered parameters.
func (d *DB) run(ctx context.Context, q query) (result, error) {
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
	return res, err
}

// maxParamBytes cuts a long string parameter in a receipt.
const maxParamBytes = 64

func renderParams(ps []bq.QueryParameter) []string {
	var out []string
	for _, p := range ps {
		out = append(out, "@"+p.Name+"="+renderValue(p.Value))
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
