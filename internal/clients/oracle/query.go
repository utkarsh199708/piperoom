package oracle

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

// queryTimeout is the per-call deadline for every read. It is applied on top of
// the caller's context, so a shorter caller deadline still wins.
const queryTimeout = 10 * time.Second

// sqlRows narrows *sql.Rows so unit tests can provide deterministic results.
type sqlRows interface {
	Next() bool
	Scan(dest ...any) error
	Err() error
	Close() error
}

// querier runs a read query. The concrete implementation is a single *sql.DB;
// a fake replaces it in tests.
type querier interface {
	QueryContext(ctx context.Context, query string, args ...any) (sqlRows, error)
}

type dbQuerier struct {
	db *sql.DB
}

func (q dbQuerier) QueryContext(ctx context.Context, query string, args ...any) (sqlRows, error) {
	return q.db.QueryContext(ctx, query, args...)
}

// query runs one read with the per-call deadline and wraps the driver error.
// It is only ever called with SELECT statements built in this package.
func (c *Client) query(ctx context.Context, q string) (sqlRows, error) {
	ctx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()
	rows, err := c.q.QueryContext(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("oracle: query: %w", err)
	}
	return rows, nil
}

// scanOne runs q and scans its single row into dest. A query that returns no
// row is an error, never a silent zero value.
func (c *Client) scanOne(ctx context.Context, q string, dest ...any) error {
	rows, err := c.query(ctx, q)
	if err != nil {
		return err
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return fmt.Errorf("oracle: read: %w", err)
		}
		return fmt.Errorf("oracle: query returned no rows")
	}
	if err := rows.Scan(dest...); err != nil {
		return fmt.Errorf("oracle: scan: %w", err)
	}
	return rows.Err()
}

// yesNo reports whether a V$ view "YES"/"NO" value is "YES".
func yesNo(v sql.NullString) bool {
	return v.Valid && strings.EqualFold(v.String, "YES")
}

// nullStr returns v when valid, else the empty string.
func nullStr(v sql.NullString) string {
	if !v.Valid {
		return ""
	}
	return v.String
}
