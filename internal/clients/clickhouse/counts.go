package clickhouse

import (
	"context"
	"fmt"
	"time"

	"piperoom/internal/clients"
)

const maxCountBuckets = 200

// CountByBucket returns counts in intervals anchored to since, matching the
// source and Snowflake readers used by reconcile. It never reads row values.
func (c *Client) CountByBucket(ctx context.Context, table, tsColumn string, since, until time.Time, width time.Duration) ([]clients.MinuteCount, error) {
	ctx, cancel := context.WithTimeout(ctx, defaultTimeout)
	defer cancel()

	if width < time.Second || width%time.Second != 0 || !until.After(since) {
		return nil, invalidWindow(since, until, width)
	}
	duration := until.Sub(since)
	bucketCount := duration / width
	if duration%width != 0 {
		bucketCount++
	}
	if bucketCount > maxCountBuckets {
		return nil, invalidWindow(since, until, width)
	}
	query, err := countSQL(table, tsColumn)
	if err != nil {
		return nil, err
	}
	offset := bucketOriginOffset(since, width)
	rows, err := c.query(ctx, query, offset, int64(width/time.Second), offset, since.UTC().UnixNano(), until.UTC().UnixNano())
	if err != nil {
		return nil, c.queryError("count rows of", table, err)
	}
	defer rows.Close()

	var out []clients.MinuteCount
	for rows.Next() {
		var bucketRaw, countRaw any
		if err := rows.Scan(&bucketRaw, &countRaw); err != nil {
			return nil, c.queryError("decode counts of", table, err)
		}
		bucket, ok, err := timestamp(bucketRaw)
		if err != nil {
			return nil, c.queryError("decode counts of", table, err)
		}
		if !ok {
			return nil, fmt.Errorf("clickhouse: count rows of %s: null bucket", table)
		}
		out = append(out, clients.MinuteCount{Bucket: bucket, Count: asInt64(countRaw)})
	}
	if err := rows.Err(); err != nil {
		return nil, c.queryError("count rows of", table, err)
	}
	return out, nil
}

func bucketOriginOffset(since time.Time, width time.Duration) int64 {
	widthSeconds := int64(width / time.Second)
	offsetSeconds := since.Unix() % widthSeconds
	if offsetSeconds < 0 {
		offsetSeconds += widthSeconds
	}
	return offsetSeconds*int64(time.Second) + int64(since.Nanosecond())
}
