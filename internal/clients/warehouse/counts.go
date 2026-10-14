package warehouse

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"piperoom/internal/clients"
	"piperoom/internal/clients/pgconv"
)

// maxCountBuckets bounds the groups one CountByBucket call may return.
const maxCountBuckets = 200

// CountByBucket counts the rows of table ("schema.table") whose tsColumn is in
// [since, until), grouped in buckets of width that start at since. Counts only:
// no value of any column is selected. Empty buckets are not returned. It needs
// PostgreSQL 14 (date_bin).
func (d *DB) CountByBucket(ctx context.Context, table, tsColumn string, since, until time.Time, width time.Duration) ([]clients.MinuteCount, error) {
	// The context deadline is the statement timeout: pgx cancels the query on
	// the server when it expires.
	ctx, cancel := pgconv.WithDeadline(ctx, 10*time.Second)
	defer cancel()

	if width < time.Second || !until.After(since) || until.Sub(since)/width > maxCountBuckets {
		return nil, fmt.Errorf("warehouse: count by bucket: bad window %s..%s width %s", since.Format(time.RFC3339), until.Format(time.RFC3339), width)
	}
	schema, tbl, col, err := validateTableColumn(table, tsColumn)
	if err != nil {
		return nil, err
	}
	q := countQuery(schema, tbl, col)
	// $2 is untyped on purpose: it takes the type of the column, so both
	// timestamp and timestamptz columns resolve to a date_bin overload. UTC
	// times keep the wall clock of a timestamp column right.
	rows, err := d.query(ctx, q, fmt.Sprintf("%d microseconds", width.Microseconds()), since.UTC(), until.UTC())
	if err != nil {
		return nil, fmt.Errorf("warehouse: count %s by %s: %w", table, tsColumn, err)
	}
	defer rows.Close()

	var out []clients.MinuteCount
	for rows.Next() {
		var b time.Time
		var n int64
		if err := rows.Scan(&b, &n); err != nil {
			return nil, fmt.Errorf("warehouse: count %s: scan: %w", table, err)
		}
		out = append(out, clients.MinuteCount{Bucket: b.UTC(), Count: n})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("warehouse: count %s: %w", table, err)
	}
	return out, nil
}

// countQuery builds the grouped count; schema, table and column are already
// validated and are quoted again here.
func countQuery(schema, tbl, col string) string {
	c := pgx.Identifier{col}.Sanitize()
	return fmt.Sprintf(`SELECT date_bin($1::interval, %[1]s, $2) AS b, count(*) FROM %[2]s WHERE %[1]s >= $2 AND %[1]s < $3 GROUP BY 1 ORDER BY 1`,
		c, pgx.Identifier{schema, tbl}.Sanitize())
}

var _ clients.CountReader = (*DB)(nil)
