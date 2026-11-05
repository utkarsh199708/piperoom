package bigquery

import (
	"context"
	"strings"
	"testing"
	"time"

	bq "cloud.google.com/go/bigquery"
)

var loadFields = []field{
	{Name: "at", Type: bq.TimestampFieldType},
	{Name: "reason", Type: bq.StringFieldType},
	{Name: "message", Type: bq.StringFieldType},
	{Name: "error_count", Type: bq.IntegerFieldType},
	{Name: "inserted_rows", Type: bq.IntegerFieldType},
	{Name: "failed", Type: bq.BooleanFieldType},
}

func TestLoadErrorsQuery(t *testing.T) {
	since := time.Date(2026, 10, 17, 0, 0, 0, 0, time.FixedZone("x", 3600))
	q, err := loadErrorsQuery(testCfg, "analytics.orders", since, 500)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(q.SQL, "FROM `acme-data`.`region-us`.INFORMATION_SCHEMA.JOBS") {
		t.Fatalf("view not region-qualified: %s", q.SQL)
	}
	for _, frag := range []string{"creation_time >= @since", "destination_table.dataset_id = @dataset",
		"destination_table.table_id = @table", "destination_table.project_id = @project", "LIMIT @limit"} {
		if !strings.Contains(q.SQL, frag) {
			t.Errorf("SQL lacks %q", frag)
		}
	}
	if strings.Contains(q.SQL, "'orders'") || strings.Contains(q.SQL, "'analytics'") {
		t.Fatalf("value interpolated: %s", q.SQL)
	}
	got := map[string]any{}
	for _, p := range q.Params {
		got[p.Name] = p.Value
	}
	want := map[string]any{"since": since.UTC(), "project": "acme-data", "dataset": "analytics", "table": "orders", "limit": int64(maxLoadLimit)}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("param %s = %#v, want %#v", k, got[k], v)
		}
	}
	if q.MaxRows != maxLoadLimit {
		t.Errorf("MaxRows = %d", q.MaxRows)
	}

	q, _ = loadErrorsQuery(testCfg, "analytics.orders", since, 0)
	if q.MaxRows != defaultLoadLimit {
		t.Errorf("default limit = %d", q.MaxRows)
	}
}

func TestLoadErrorsMapsJobsRows(t *testing.T) {
	at := time.Date(2026, 10, 18, 8, 0, 0, 0, time.UTC)
	secret := "password=hunter2-very-secret"
	db, f := newFake(ok(loadFields,
		[]bq.Value{at, "invalid", "Error while reading data, error message: CSV processing encountered too many errors\n row 7: " + secret, int64(12), nil, true},
		[]bq.Value{at.Add(-time.Hour), "invalidQuery", "Unrecognized name: risk_score at [3:5]", nil, int64(0), true},
		[]bq.Value{at.Add(-2 * time.Hour), "invalid", strings.Repeat("y", 1000), int64(3), int64(997), false},
		[]bq.Value{at.Add(-3 * time.Hour), nil, nil, int64(0), nil, true},
	))
	got, err := db.LoadErrors(context.Background(), "analytics.orders", at.Add(-24*time.Hour), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 4 {
		t.Fatalf("got %d rows", len(got))
	}
	g := got[0]
	if g.Table != "analytics.orders" || !g.At.Equal(at) || g.Status != "LOAD_FAILED" || g.Errors != 12 || g.Rows != 0 || g.File != "" {
		t.Fatalf("row 0 = %+v", g)
	}
	if strings.Contains(g.FirstError, "hunter2") || strings.Contains(g.FirstError, "\n") || !strings.HasPrefix(g.FirstError, "invalid: Error while reading data") {
		t.Fatalf("row 0 FirstError not cleaned: %q", g.FirstError)
	}
	if got[1].FirstError != "invalidQuery: Unrecognized name: risk_score at [3:5]" || got[1].Errors != 1 {
		t.Fatalf("row 1 = %+v", got[1])
	}
	if got[2].Status != "PARTIALLY_LOADED" || got[2].Rows != 997 || got[2].Errors != 3 || len([]rune(got[2].FirstError)) > maxErrRunes {
		t.Fatalf("row 2 = %+v", got[2])
	}
	if got[3].FirstError != "" || got[3].Status != "LOAD_FAILED" {
		t.Fatalf("row 3 = %+v", got[3])
	}
	if q := f.calls(); len(q) != 1 || q[0].MaxRows != 10 {
		t.Fatalf("queries = %+v", q)
	}
}

func TestLoadErrorsSinceFloor(t *testing.T) {
	db, f := newFake(ok(loadFields))
	if _, err := db.LoadErrors(context.Background(), "analytics.orders", time.Time{}, 5); err != nil {
		t.Fatal(err)
	}
	var since time.Time
	for _, p := range f.calls()[0].Params {
		if p.Name == "since" {
			since = p.Value.(time.Time)
		}
	}
	if d := time.Since(since); d > jobsRetention+time.Minute || d < jobsRetention-time.Minute {
		t.Fatalf("since = %v (%v ago), want about %v ago", since, d, jobsRetention)
	}
}

func TestLoadErrorsEmpty(t *testing.T) {
	db, _ := newFake(ok(loadFields))
	got, err := db.LoadErrors(context.Background(), "analytics.orders", time.Now().Add(-time.Hour), 5)
	if err != nil || len(got) != 0 {
		t.Fatalf("got %v err %v", got, err)
	}
}

func TestLoadErrorsBadRowShape(t *testing.T) {
	db, _ := newFake(ok(loadFields[:2], []bq.Value{time.Now(), "x"}))
	if _, err := db.LoadErrors(context.Background(), "analytics.orders", time.Now().Add(-time.Hour), 5); err == nil {
		t.Fatal("short row accepted")
	}
}
