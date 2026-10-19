package clickhouse

import (
	"context"
	"database/sql"
	"time"

	"piperoom/internal/tools"
)

// sqlRows narrows *sql.Rows so unit tests can provide deterministic results.
type sqlRows interface {
	Next() bool
	Scan(dest ...any) error
	Columns() ([]string, error)
	Err() error
	Close() error
}

type querier interface {
	QueryContext(ctx context.Context, query string, args ...any) (sqlRows, error)
}

type dbQuerier struct {
	db *sql.DB
}

func (q dbQuerier) QueryContext(ctx context.Context, query string, args ...any) (sqlRows, error) {
	// clickhouse-go safely binds the '?' values client-side before sending SQL.
	rows, err := q.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return rows, nil
}

func (c *Client) query(ctx context.Context, query string, args ...any) (sqlRows, error) {
	start := time.Now()
	rows, err := c.q.QueryContext(ctx, query, args...)
	tools.Record(ctx, tools.Call{
		Kind:   tools.CallSQL,
		Target: c.target,
		Text:   sqlText(query),
		At:     start,
		TookMS: time.Since(start).Milliseconds(),
	})
	if err != nil {
		return nil, Redact(err, c.dsn)
	}
	return rows, nil
}
