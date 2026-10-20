package clickhouse

import (
	"context"
	"fmt"
	"strings"
	"time"

	"piperoom/internal/clients"
)

// Freshness returns max(tsColumn) in UTC. ClickHouse DateTime and DateTime64
// are decoded by the driver as time.Time, preserving the column timezone and
// DateTime64 fractional precision before normalization to UTC. Empty tables
// return the zero time.
func (c *Client) Freshness(ctx context.Context, table, tsColumn string) (time.Time, error) {
	ctx, cancel := context.WithTimeout(ctx, defaultTimeout)
	defer cancel()

	query, err := freshnessSQL(table, tsColumn)
	if err != nil {
		return time.Time{}, err
	}
	rows, err := c.query(ctx, query)
	if err != nil {
		return time.Time{}, c.queryError("freshness of", table, err)
	}
	defer rows.Close()

	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return time.Time{}, c.queryError("freshness of", table, err)
		}
		return time.Time{}, nil
	}
	var raw any
	if err := rows.Scan(&raw); err != nil {
		return time.Time{}, c.queryError("decode freshness of", table, err)
	}
	value, ok, err := timestamp(raw)
	if err != nil {
		return time.Time{}, c.queryError("decode freshness of", table, err)
	}
	if !ok {
		return time.Time{}, nil
	}
	return value, nil
}

// Row returns one row matching pkColumn. FINAL ensures a ReplacingMergeTree
// lookup sees the current deduplicated row before background merges finish. If
// the engine rejects FINAL, it retries without FINAL for compatible engines.
func (c *Client) Row(ctx context.Context, table, pkColumn, pk string) (map[string]any, bool, error) {
	ctx, cancel := context.WithTimeout(ctx, defaultTimeout)
	defer cancel()

	query, err := rowSQL(table, pkColumn)
	if err != nil {
		return nil, false, err
	}
	rows, err := c.query(ctx, query, pk)
	if illegalFinal(err) {
		query, err = rowFallbackSQL(table, pkColumn)
		if err == nil {
			rows, err = c.query(ctx, query, pk)
		}
	}
	if err != nil {
		return nil, false, c.queryError("query row from", table, err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, false, c.queryError("read columns from", table, err)
	}
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return nil, false, c.queryError("query row from", table, err)
		}
		return nil, false, nil
	}

	values := make([]any, len(columns))
	dest := make([]any, len(columns))
	for i := range values {
		dest[i] = &values[i]
	}
	if err := rows.Scan(dest...); err != nil {
		return nil, false, c.queryError("decode row from", table, err)
	}

	folded := make(map[string]int, len(columns))
	for _, column := range columns {
		folded[strings.ToLower(column)]++
	}
	row := make(map[string]any, len(columns))
	for i, column := range columns {
		if folded[strings.ToLower(column)] == 1 {
			column = strings.ToLower(column)
		}
		row[column] = toJSON(values[i])
	}
	return row, true, nil
}

var _ clients.WarehouseReader = (*Client)(nil)

func invalidWindow(since, until time.Time, width time.Duration) error {
	return fmt.Errorf("clickhouse: invalid bucket window %s..%s width %s", since.Format(time.RFC3339), until.Format(time.RFC3339), width)
}
