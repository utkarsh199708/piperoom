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
	if q.MaxRows != maxCountBuckets+1 {
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
		// 200.5 buckets: the partial last one makes 201.
		{since.Add(200*time.Minute + 30*time.Second), time.Minute},
	} {
		if _, err := d.CountByBucket(context.Background(), "sales.orders", "ts", since, c.until, c.width); err == nil {
			t.Errorf("%+v: want error", c)
		}
	}
	if n := len(f.calls()); n != 0 {
		t.Errorf("ran %d statements for bad input", n)
	}
}

func TestCountByBucketWindowEdges(t *testing.T) {
	since := time.Date(2026, 10, 14, 0, 0, 0, 0, time.UTC)
	// Exactly 200 buckets is allowed.
	d, _ := newFake(ok([]field{{"bucket", "BIGINT"}, {"n", "BIGINT"}}))
	if _, err := d.CountByBucket(context.Background(), "sales.orders", "ts", since, since.Add(200*time.Minute), time.Minute); err != nil {
		t.Fatalf("200 buckets: %v", err)
	}
	for _, c := range []struct {
		window, width time.Duration
		want          time.Duration
	}{
		{time.Hour, time.Minute, 60},
		{time.Hour + time.Second, time.Minute, 61},
		{time.Second, time.Minute, 1},
	} {
		if got := bucketCount(c.window, c.width); got != c.want {
			t.Errorf("bucketCount(%s, %s) = %d, want %d", c.window, c.width, got, c.want)
		}
	}
}

// A result longer than the window allows is refused, never cut to 200.
func TestCountByBucketRefusesExtraBucket(t *testing.T) {
	since := time.Date(2026, 10, 14, 0, 0, 0, 0, time.UTC)
	rows := make([][]any, maxCountBuckets+1)
	for i := range rows {
		rows[i] = []any{since.Add(time.Duration(i) * time.Minute).UnixMicro(), int64(1)}
	}
	d, _ := newFake(ok([]field{{"bucket", "BIGINT"}, {"n", "BIGINT"}}, rows...))
	if got, err := d.CountByBucket(context.Background(), "sales.orders", "ts", since, since.Add(200*time.Minute), time.Minute); err == nil {
		t.Fatalf("201 buckets accepted (%d returned)", len(got))
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
