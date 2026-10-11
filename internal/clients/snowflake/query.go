package snowflake

import (
	"context"
	"database/sql"
)

// sqlRows is the subset of *sql.Rows the query helpers use. It lets tests
// substitute a fake result set without a real driver connection.
type sqlRows interface {
	Next() bool
	Scan(dest ...any) error
	Columns() ([]string, error)
	Err() error
	Close() error
}

// querier runs a read query. The production implementation (dbQuerier) wraps
// *sql.DB; tests use a fake.
type querier interface {
	QueryContext(ctx context.Context, query string, args ...any) (sqlRows, error)
}

// dbQuerier adapts *sql.DB to querier. The adaptation only narrows the
// return type of QueryContext from *sql.Rows to the sqlRows interface it
// already satisfies; no behavior changes.
type dbQuerier struct {
	db *sql.DB
}

func (q dbQuerier) QueryContext(ctx context.Context, query string, args ...any) (sqlRows, error) {
	rows, err := q.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return rows, nil
}
