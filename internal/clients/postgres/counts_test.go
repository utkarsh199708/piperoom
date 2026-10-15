package postgres

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestCountByBucketIdentifiers(t *testing.T) {
	q := &fakeQuerier{}
	d := testDB(q)
	since := time.Date(2026, 10, 14, 3, 0, 0, 0, time.FixedZone("X", 3600))
	if _, err := d.CountByBucket(context.Background(), "public.orders", "updated_at", since, since.Add(time.Hour), 5*time.Minute); err != nil {
		t.Fatal(err)
	}
	if len(q.sqls) != 1 {
		t.Fatalf("%d statements", len(q.sqls))
	}
	sql := q.sqls[0]
	for _, want := range []string{`date_bin($1::interval, "updated_at", $2)`, `FROM "public"."orders"`, `"updated_at" >= $2`, `"updated_at" < $3`, "count(*)", "GROUP BY 1 ORDER BY 1"} {
		if !strings.Contains(sql, want) {
			t.Errorf("sql lacks %s: %s", want, sql)
		}
	}
	if strings.Contains(sql, "SELECT *") {
		t.Errorf("selects values: %s", sql)
	}
	a := q.args[0]
	if a[0] != "300000000 microseconds" || !a[1].(time.Time).Equal(since) || a[1].(time.Time).Location() != time.UTC {
		t.Errorf("args = %#v", a)
	}
}

func TestCountByBucketRejects(t *testing.T) {
	q := &fakeQuerier{}
	d := testDB(q)
	since := time.Now().Add(-time.Hour)
	for _, c := range []struct {
		table, col string
		until      time.Time
		width      time.Duration
	}{
		{"public.orders; drop", "ts", since.Add(time.Minute), time.Minute},
		{"orders", "ts", since.Add(time.Minute), time.Minute},
		{"public.orders", "ts; drop", since.Add(time.Minute), time.Minute},
		{"public.orders", "ts", since, time.Minute},
		{"public.orders", "ts", since.Add(time.Hour), 0},
		{"public.orders", "ts", since.Add(100 * time.Hour), time.Minute},
	} {
		if _, err := d.CountByBucket(context.Background(), c.table, c.col, since, c.until, c.width); err == nil {
			t.Errorf("%+v: want error", c)
		}
	}
	if len(q.sqls) != 0 {
		t.Errorf("ran %d statements for bad input", len(q.sqls))
	}
}
