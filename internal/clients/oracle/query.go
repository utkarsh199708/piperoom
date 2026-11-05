package oracle

import (
	"context"
	"database/sql"
	"errors"
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

// errStop is the control-flow sentinel a scan callback returns to stop the
// iteration after the current row without reporting an error.
var errStop = errors.New("oracle: stop iteration")

// each runs one read with the per-call deadline and calls scan for each row.
// The deadline stays in force until the rows are closed after the loop, so
// database/sql does not close the rows under a cancelled context mid-iteration.
// A scan callback returns errStop to stop after the current row.
func (c *Client) each(ctx context.Context, q string, args []any, scan func(sqlRows) error) error {
	ctx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()
	rows, err := c.q.QueryContext(ctx, q, args...)
	if err != nil {
		return fmt.Errorf("oracle: query: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		if err := scan(rows); err != nil {
			if errors.Is(err, errStop) {
				return nil
			}
			return err
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("oracle: read: %w", err)
	}
	return nil
}

// scanOne runs q and scans its single row into dest. A query that returns no
// row is an error, never a silent zero value.
func (c *Client) scanOne(ctx context.Context, q string, dest ...any) error {
	found := false
	err := c.each(ctx, q, nil, func(rows sqlRows) error {
		found = true
		if err := rows.Scan(dest...); err != nil {
			return fmt.Errorf("oracle: scan: %w", err)
		}
		return errStop
	})
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("oracle: query returned no rows")
	}
	return nil
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
