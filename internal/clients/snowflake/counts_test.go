package snowflake

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestCountByBucketIdentifiers(t *testing.T) {
	since := time.Date(2026, 10, 14, 3, 0, 0, 0, time.UTC)
	b := since.Add(5 * time.Minute)
	fq := &fakeQuerier{rows: &fakeRows{cols: []string{"B", "COUNT"}, data: [][]any{{b, int64(42)}}}}
	db := &DB{q: fq}
	got, err := db.CountByBucket(context.Background(), "analytics.orders", "updated_at", since, since.Add(time.Hour), 5*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Count != 42 || !got[0].Bucket.Equal(b) {
		t.Errorf("got %+v", got)
	}
	for _, want := range []string{`"UPDATED_AT"`, `"ANALYTICS"."ORDERS"`, "GROUP BY 1 ORDER BY 1", "COUNT(*)"} {
		if !strings.Contains(fq.lastQuery, want) {
			t.Errorf("query lacks %s: %s", want, fq.lastQuery)
		}
	}
	if strings.Contains(fq.lastQuery, "SELECT *") || len(fq.lastArgs) != 6 || fq.lastArgs[1] != int64(300) {
		t.Errorf("query/args: %s %#v", fq.lastQuery, fq.lastArgs)
	}
}

func TestCountByBucketRejects(t *testing.T) {
	db := &DB{q: &fakeQuerier{}}
	since := time.Now().Add(-time.Hour)
	cases := []struct {
		name, table, col string
		until            time.Time
		width            time.Duration
	}{
		{"injection table", "a.b; drop", "ts", since.Add(time.Minute), time.Minute},
		{"injection column", "a.b", `ts"; drop`, since.Add(time.Minute), time.Minute},
		{"empty window", "a.b", "ts", since, time.Minute},
		{"tiny width", "a.b", "ts", since.Add(time.Hour), time.Millisecond},
		{"too many buckets", "a.b", "ts", since.Add(48 * time.Hour), time.Minute},
	}
	for _, c := range cases {
		if _, err := db.CountByBucket(context.Background(), c.table, c.col, since, c.until, c.width); err == nil {
			t.Errorf("%s: want error", c.name)
		}
	}
}
