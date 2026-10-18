package databricks

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"piperoom/internal/clients"
)

// maxCountBuckets bounds the groups one CountByBucket call may return, as in
// the Postgres and Snowflake warehouse clients.
const maxCountBuckets = 200

// CountByBucket counts the rows of table ("schema.table") whose tsColumn is
// in [since, until), grouped in buckets of width (at least a second) that
// start at since. Counts only: no value of any column is selected; the
// bucket is computed in epoch microseconds, so a TIMESTAMP_NTZ column is read
// as UTC wall time (the session zone is UTC). Empty buckets are not returned.
func (d *DB) CountByBucket(ctx context.Context, table, tsColumn string, since, until time.Time, width time.Duration) ([]clients.MinuteCount, error) {
	ctx, cancel := context.WithTimeout(ctx, defaultTimeout)
	defer cancel()

	if width < time.Second || !until.After(since) || until.Sub(since)/width > maxCountBuckets {
		return nil, fmt.Errorf("databricks: count by bucket: bad window %s..%s width %s", since.Format(time.RFC3339), until.Format(time.RFC3339), width)
	}
	q, err := countQuery(d.cfg.Catalog, table, tsColumn, since, until, width)
	if err != nil {
		return nil, err
	}
	res, st, err := d.run(ctx, q)
	if err != nil {
		return nil, d.wrapQueryErr(ctx, st, "count rows of", table, err)
	}
	out := make([]clients.MinuteCount, 0, len(res.Rows))
	for _, r := range res.Rows {
		if len(r) != 2 {
			return nil, fmt.Errorf("databricks: count rows of %s: %d columns, want 2", table, len(r))
		}
		b, err := asInt(r[0])
		if err != nil {
			return nil, fmt.Errorf("databricks: count rows of %s: bucket is %T: %w", table, r[0], err)
		}
		n, err := asInt(r[1])
		if err != nil {
			return nil, fmt.Errorf("databricks: count rows of %s: count is %T: %w", table, r[1], err)
		}
		out = append(out, clients.MinuteCount{Bucket: time.UnixMicro(b).UTC(), Count: n})
	}
	return out, nil
}

func countQuery(catalog, table, tsColumn string, since, until time.Time, width time.Duration) (query, error) {
	t, err := parseTable(catalog, table)
	if err != nil {
		return query{}, err
	}
	if err := validateIdent(tsColumn); err != nil {
		return query{}, err
	}
	c := quote(tsColumn)
	return query{
		SQL: "SELECT :since + ((unix_micros(" + c + ") - :since) div :width) * :width AS bucket, COUNT(*) AS n" +
			" FROM " + t.sql() +
			" WHERE " + c + " >= timestamp_micros(:since) AND " + c + " < timestamp_micros(:until)" +
			" GROUP BY 1 ORDER BY 1",
		Params: []sql.NamedArg{
			sql.Named("since", since.UnixMicro()),
			sql.Named("until", until.UnixMicro()),
			sql.Named("width", width.Microseconds()),
		},
		MaxRows: maxCountBuckets,
	}, nil
}
