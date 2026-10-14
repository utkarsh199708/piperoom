package snowflake

import (
	"context"
	"fmt"
	"time"

	"piperoom/internal/clients"
)

// maxCountBuckets bounds the groups one CountByBucket call may return.
const maxCountBuckets = 200

// CountByBucket counts the rows of table whose tsColumn is in [since, until),
// in buckets of width (whole seconds) that start at since. Counts only. The
// bounds are bound as epoch seconds and read as UTC wall time
// (TO_TIMESTAMP_NTZ), the convention Freshness uses for TIMESTAMP_NTZ columns.
// Empty buckets are not returned.
func (d *DB) CountByBucket(ctx context.Context, table, tsColumn string, since, until time.Time, width time.Duration) ([]clients.MinuteCount, error) {
	ctx, cancel := context.WithTimeout(ctx, defaultTimeout)
	defer cancel()

	if width < time.Second || !until.After(since) || until.Sub(since)/width > maxCountBuckets {
		return nil, fmt.Errorf("snowflake: count by bucket: bad window %s..%s width %s", since.Format(time.RFC3339), until.Format(time.RFC3339), width)
	}
	qualified, err := quoteTable(table)
	if err != nil {
		return nil, err
	}
	col, err := quoteIdent(tsColumn)
	if err != nil {
		return nil, err
	}
	// TIME_SLICE cannot take an origin, so the bucket is computed from since.
	q := fmt.Sprintf(`SELECT DATEADD(second, FLOOR(DATEDIFF(second, TO_TIMESTAMP_NTZ(?), %[1]s) / ?) * ?, TO_TIMESTAMP_NTZ(?)) AS B, COUNT(*) `+
		`FROM %[2]s WHERE %[1]s >= TO_TIMESTAMP_NTZ(?) AND %[1]s < TO_TIMESTAMP_NTZ(?) GROUP BY 1 ORDER BY 1`, col, qualified)
	w := int64(width / time.Second)
	rows, err := d.query(ctx, q, since.Unix(), w, w, since.Unix(), since.Unix(), until.Unix())
	if err != nil {
		return nil, wrapQueryErr("count rows of", table, err)
	}
	defer rows.Close()

	var out []clients.MinuteCount
	for rows.Next() {
		var b, n any
		if err := rows.Scan(&b, &n); err != nil {
			return nil, fmt.Errorf("snowflake: count rows of %s: decode: %w", table, err)
		}
		t, ok := b.(time.Time)
		if !ok {
			return nil, fmt.Errorf("snowflake: count rows of %s: bucket is %T, want a timestamp", table, b)
		}
		out = append(out, clients.MinuteCount{Bucket: t.UTC(), Count: asInt(n)})
	}
	if err := rows.Err(); err != nil {
		return nil, wrapQueryErr("count rows of", table, err)
	}
	return out, nil
}

var _ clients.CountReader = (*DB)(nil)
