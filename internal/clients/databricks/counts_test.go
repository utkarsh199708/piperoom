package databricks

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestCountByBucket(t *testing.T) {
	since := time.Date(2026, 10, 14, 3, 0, 0, 0, time.FixedZone("X", 3600))
	b0 := since.UnixMicro()
	b2 := since.Add(10 * time.Minute).UnixMicro()
	d, f := newFake(ok([]field{{"bucket", "BIGINT"}, {"n", "BIGINT"}},
		[]any{b0, int64(12)}, []any{b2, int64(3)}))
	got, err := d.CountByBucket(context.Background(), "sales.orders", "updated_at", since, since.Add(time.Hour), 5*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || !got[0].Bucket.Equal(since) || got[0].Count != 12 || !got[1].Bucket.Equal(since.Add(10*time.Minute)) || got[1].Count != 3 {
		t.Fatalf("got %+v", got)
	}
	if got[0].Bucket.Location() != time.UTC {
		t.Errorf("bucket not UTC: %v", got[0].Bucket)
	}
	q := f.calls()[0]
	for _, want := range []string{"COUNT(*)", "FROM `main`.`sales`.`orders`", "unix_micros(`updated_at`)", "`updated_at` >= timestamp_micros(:since)", "`updated_at` < timestamp_micros(:until)", "div :width", "GROUP BY 1 ORDER BY 1"} {
		if !strings.Contains(q.SQL, want) {
			t.Errorf("sql lacks %s: %s", want, q.SQL)
		}
	}
	if strings.Contains(q.SQL, "SELECT *") {
		t.Errorf("selects values: %s", q.SQL)
	}
	if len(q.Params) != 3 || q.Params[0].Value != since.UnixMicro() || q.Params[1].Value != since.Add(time.Hour).UnixMicro() || q.Params[2].Value != int64(300_000_000) {
		t.Errorf("params = %#v", q.Params)
	}
	if q.MaxRows != maxCountBuckets {
		t.Errorf("MaxRows = %d", q.MaxRows)
	}
}

func TestCountByBucketRejects(t *testing.T) {
	d, f := newFake()
	since := time.Now().Add(-time.Hour)
	for _, c := range []struct {
		until time.Time
		width time.Duration
	}{
		{since, time.Minute},                           // empty window
		{since.Add(-time.Minute), time.Minute},         // reversed
		{since.Add(time.Hour), 0},                      // no width
		{since.Add(time.Hour), 500 * time.Millisecond}, // under a second
		{since.Add(100 * time.Hour), time.Minute},      // too many buckets
	} {
		if _, err := d.CountByBucket(context.Background(), "sales.orders", "ts", since, c.until, c.width); err == nil {
			t.Errorf("%+v: want error", c)
		}
	}
	if n := len(f.calls()); n != 0 {
		t.Errorf("ran %d statements for bad input", n)
	}
}

func TestCountByBucketBadCells(t *testing.T) {
	since := time.Now().Add(-time.Hour)
	for _, row := range [][]any{
		{"not a number", int64(1)},
		{int64(1), 1.5},
		{int64(1)},
	} {
		d, _ := newFake(ok([]field{{"bucket", "BIGINT"}, {"n", "BIGINT"}}, row))
		if _, err := d.CountByBucket(context.Background(), "sales.orders", "ts", since, since.Add(time.Hour), time.Minute); err == nil {
			t.Errorf("%v: want error", row)
		}
	}
}
