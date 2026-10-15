package snowflake

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func loadRows(data ...[]any) *fakeRows {
	return &fakeRows{cols: []string{"TABLE_NAME", "LAST_LOAD_TIME", "STATUS", "ROW_COUNT", "ERROR_COUNT", "FIRST_ERROR_MESSAGE", "FILE_NAME"}, data: data}
}

func TestLoadErrorsSQLAndRedaction(t *testing.T) {
	at := time.Date(2026, 10, 14, 3, 41, 0, 0, time.FixedZone("PST", -8*3600))
	secret := "password=hunter2-very-secret"
	long := strings.Repeat("é", 400)
	fq := &fakeQuerier{rows: loadRows(
		[]any{"ORDERS", at, "LOAD_FAILED", int64(0), int64(12000), "SQL compilation error:\n  invalid identifier 'RISK_SCORE' " + secret, "s3://bucket/prefix/2026/10/14/part-0001.csv.gz?X-Amz-Signature=abc"},
		[]any{"ORDERS", at, "PARTIALLY_LOADED", "7", "3", long, "dir\\file.csv"},
	)}
	db := &DB{q: fq}
	since := time.Now().Add(-time.Hour)

	got, err := db.LoadErrors(context.Background(), "analytics.orders", since, 5)
	if err != nil {
		t.Fatalf("LoadErrors: %v", err)
	}
	if !strings.Contains(fq.lastQuery, "COPY_HISTORY(TABLE_NAME => ?, START_TIME => TO_TIMESTAMP_LTZ(?))") ||
		!strings.Contains(fq.lastQuery, "STATUS <> 'LOADED'") || !strings.Contains(fq.lastQuery, "LIMIT ?") {
		t.Errorf("query = %q", fq.lastQuery)
	}
	if strings.Contains(fq.lastQuery, "ANALYTICS") {
		t.Errorf("table name is interpolated: %q", fq.lastQuery)
	}
	if len(fq.lastArgs) != 3 || fq.lastArgs[0] != "ANALYTICS.ORDERS" || fq.lastArgs[1] != since.Unix() || fq.lastArgs[2] != int64(5) {
		t.Errorf("args = %#v", fq.lastArgs)
	}
	if len(got) != 2 {
		t.Fatalf("got %d rows", len(got))
	}
	r := got[0]
	if r.Status != "LOAD_FAILED" || r.Errors != 12000 || r.Rows != 0 || !r.At.Equal(at) || r.At.Location() != time.UTC {
		t.Errorf("row 0 = %+v", r)
	}
	if strings.Contains(r.FirstError, "hunter2") || strings.Contains(r.FirstError, "\n") || !strings.Contains(r.FirstError, "invalid identifier 'RISK_SCORE'") {
		t.Errorf("FirstError = %q", r.FirstError)
	}
	if r.File != "part-0001.csv.gz" {
		t.Errorf("File = %q", r.File)
	}
	if got[1].Rows != 7 || got[1].Errors != 3 || got[1].File != "file.csv" {
		t.Errorf("row 1 = %+v", got[1])
	}
	if n := len([]rune(got[1].FirstError)); n > 300 {
		t.Errorf("FirstError has %d runes", n)
	}
}

func TestLoadErrorsWindowAndLimit(t *testing.T) {
	fq := &fakeQuerier{rows: loadRows()}
	db := &DB{q: fq}
	if _, err := db.LoadErrors(context.Background(), "db.analytics.orders", time.Now().Add(-90*24*time.Hour), 10_000); err != nil {
		t.Fatal(err)
	}
	floor := time.Now().Add(-14 * 24 * time.Hour).Unix()
	if s := fq.lastArgs[1].(int64); s < floor-5 || s > floor+5 {
		t.Errorf("START_TIME = %d, want about %d", s, floor)
	}
	if fq.lastArgs[0] != "DB.ANALYTICS.ORDERS" || fq.lastArgs[2] != int64(100) {
		t.Errorf("args = %#v", fq.lastArgs)
	}
	if _, err := db.LoadErrors(context.Background(), "analytics.orders", time.Now(), 0); err != nil || fq.lastArgs[2] != int64(20) {
		t.Errorf("default limit: err=%v args=%#v", err, fq.lastArgs)
	}
}

func TestLoadErrorsBadInputAndErrors(t *testing.T) {
	fq := &fakeQuerier{rows: loadRows()}
	db := &DB{q: fq}
	for _, tbl := range []string{"orders", `a."b"`, "a.b; drop", "a.b.c.d", ""} {
		if _, err := db.LoadErrors(context.Background(), tbl, time.Now(), 5); err == nil {
			t.Errorf("table %q: want error", tbl)
		}
	}
	if fq.lastQuery != "" {
		t.Errorf("a query ran for bad input: %q", fq.lastQuery)
	}
	down := &DB{q: &fakeQuerier{err: errors.New("connection refused")}}
	if _, err := down.LoadErrors(context.Background(), "analytics.orders", time.Now(), 5); err == nil || !strings.Contains(err.Error(), "connection refused") {
		t.Errorf("err = %v", err)
	}
	scan := &DB{q: &fakeQuerier{rows: func() *fakeRows {
		r := loadRows([]any{"X", nil, "LOAD_FAILED", nil, nil, nil, nil})
		r.scanErr = errors.New("boom")
		return r
	}()}}
	if _, err := scan.LoadErrors(context.Background(), "analytics.orders", time.Now(), 5); err == nil {
		t.Error("want scan error")
	}
}

func TestLoadErrorsNullColumns(t *testing.T) {
	db := &DB{q: &fakeQuerier{rows: loadRows([]any{"ORDERS", nil, "LOAD_FAILED", nil, nil, nil, nil})}}
	got, err := db.LoadErrors(context.Background(), "analytics.orders", time.Now(), 5)
	if err != nil || len(got) != 1 || !got[0].At.IsZero() || got[0].FirstError != "" || got[0].File != "" {
		t.Fatalf("got %+v err %v", got, err)
	}
}
