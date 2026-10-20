package clickhouse

import (
	"context"
	"errors"
	"fmt"
)

type queryResponse struct {
	rows sqlRows
	err  error
}

type queryCall struct {
	query string
	args  []any
}

type fakeQuerier struct {
	responses []queryResponse
	calls     []queryCall
}

func (q *fakeQuerier) QueryContext(ctx context.Context, query string, args ...any) (sqlRows, error) {
	call := queryCall{query: query, args: append([]any(nil), args...)}
	q.calls = append(q.calls, call)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(q.responses) == 0 {
		return nil, errors.New("unexpected query")
	}
	response := q.responses[0]
	q.responses = q.responses[1:]
	return response.rows, response.err
}

type fakeRows struct {
	columns  []string
	data     [][]any
	index    int
	current  []any
	err      error
	scanErr  error
	closeErr error
}

func (r *fakeRows) Next() bool {
	if r.index >= len(r.data) {
		return false
	}
	r.current = r.data[r.index]
	r.index++
	return true
}

func (r *fakeRows) Scan(dest ...any) error {
	if r.scanErr != nil {
		return r.scanErr
	}
	if len(dest) != len(r.current) {
		return fmt.Errorf("scan got %d destinations for %d values", len(dest), len(r.current))
	}
	for i, target := range dest {
		ptr, ok := target.(*any)
		if !ok {
			return fmt.Errorf("destination %d is %T, want *any", i, target)
		}
		*ptr = r.current[i]
	}
	return nil
}

func (r *fakeRows) Columns() ([]string, error) {
	return append([]string(nil), r.columns...), nil
}

func (r *fakeRows) Err() error   { return r.err }
func (r *fakeRows) Close() error { return r.closeErr }

func newTestClient(q querier) *Client {
	return &Client{
		q:               q,
		dsn:             "clickhouse://piperoom:supersecret@localhost:9000/analytics",
		target:          "clickhouse localhost:9000/analytics",
		defaultDatabase: "analytics",
	}
}
