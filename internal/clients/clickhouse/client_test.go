package clickhouse

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	driver "github.com/ClickHouse/clickhouse-go/v2"

	"piperoom/internal/clients"
)

func TestOptionsForDSNEnforcesReadonlyAndTimeouts(t *testing.T) {
	opts, err := optionsForDSN("clickhouse://reader:secret@localhost:9000/analytics?secure=true")
	if err != nil {
		t.Fatal(err)
	}
	if opts.Settings["readonly"] != 1 {
		t.Fatalf("readonly setting = %v, want 1", opts.Settings["readonly"])
	}
	if opts.DialTimeout != defaultTimeout || opts.ReadTimeout != defaultTimeout {
		t.Fatalf("timeouts = %s/%s, want %s", opts.DialTimeout, opts.ReadTimeout, defaultTimeout)
	}
	if opts.Auth.Database != "analytics" {
		t.Fatalf("database = %q, want analytics", opts.Auth.Database)
	}
}

func TestFreshnessDateTimesAndEmptyTable(t *testing.T) {
	location := time.FixedZone("UTC+05:30", 5*60*60+30*60)
	want := time.Date(2026, 10, 19, 9, 4, 5, 123456789, location)
	q := &fakeQuerier{responses: []queryResponse{
		{rows: &fakeRows{columns: []string{"max"}, data: [][]any{{want}}}},
		{rows: &fakeRows{columns: []string{"max"}, data: [][]any{{nil}}}},
	}}
	client := newTestClient(q)

	got, err := client.Freshness(context.Background(), "analytics.orders", "loaded_at")
	if err != nil {
		t.Fatal(err)
	}
	if !got.Equal(want.UTC()) || got.Location() != time.UTC || got.Nanosecond() != want.Nanosecond() {
		t.Fatalf("Freshness = %s, want %s in UTC", got.Format(time.RFC3339Nano), want.UTC().Format(time.RFC3339Nano))
	}
	if !strings.Contains(q.calls[0].query, "max(") {
		t.Fatalf("query = %q, want max", q.calls[0].query)
	}

	empty, err := client.Freshness(context.Background(), "analytics.orders", "loaded_at")
	if err != nil {
		t.Fatal(err)
	}
	if !empty.IsZero() {
		t.Fatalf("empty-table freshness = %s, want zero time", empty)
	}
}

func TestWarehouseQueryErrorsAndBadInput(t *testing.T) {
	t.Run("unknown table", func(t *testing.T) {
		q := &fakeQuerier{responses: []queryResponse{{err: &driver.Exception{Code: errUnknownTable, Name: "UNKNOWN_TABLE"}}}}
		_, err := newTestClient(q).Freshness(context.Background(), "analytics.missing", "loaded_at")
		if !errors.Is(err, clients.ErrNotFound) {
			t.Fatalf("Freshness error = %v, want ErrNotFound", err)
		}
	})

	t.Run("target down", func(t *testing.T) {
		q := &fakeQuerier{responses: []queryResponse{{err: errors.New("connection failed for clickhouse://piperoom:supersecret@localhost:9000/analytics")}}}
		_, err := newTestClient(q).Freshness(context.Background(), "analytics.orders", "loaded_at")
		if err == nil {
			t.Fatal("Freshness succeeded for a disconnected target")
		}
		if strings.Contains(err.Error(), "supersecret") {
			t.Fatalf("error leaked DSN password: %v", err)
		}
	})

	t.Run("deadline", func(t *testing.T) {
		q := &fakeQuerier{responses: []queryResponse{{err: context.DeadlineExceeded}}}
		_, err := newTestClient(q).Freshness(context.Background(), "analytics.orders", "loaded_at")
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Freshness error = %v, want DeadlineExceeded", err)
		}
	})

	t.Run("bad input does not query", func(t *testing.T) {
		q := &fakeQuerier{}
		_, err := newTestClient(q).Freshness(context.Background(), "analytics.orders", "x; DROP TABLE t")
		if err == nil {
			t.Fatal("Freshness accepted an unsafe timestamp column")
		}
		if len(q.calls) != 0 {
			t.Fatalf("unsafe input ran %d queries", len(q.calls))
		}
	})
}

func TestRowUsesFinalAndConvertsValues(t *testing.T) {
	at := time.Date(2026, 10, 19, 14, 0, 0, 123456789, time.FixedZone("east", 5*60*60+30*60))
	q := &fakeQuerier{responses: []queryResponse{{
		rows: &fakeRows{
			columns: []string{"ID", "amount", "created_at"},
			data:    [][]any{{uint64(9007199254740993), float64(12.5), at}},
		},
	}}}
	client := newTestClient(q)

	row, found, err := client.Row(context.Background(), "analytics.orders", "id", "order-1")
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("Row returned found=false")
	}
	if got := row["id"]; got != "9007199254740993" {
		t.Fatalf("id = %#v, want exact integer string", got)
	}
	if got := row["amount"]; got != "12.5" {
		t.Fatalf("amount = %#v, want decimal string", got)
	}
	if got := toJSON(testDecimal("12.340")); got != "12.340" {
		t.Fatalf("decimal value = %#v, want exact decimal string", got)
	}
	if got := row["created_at"]; got != at.UTC().Format(time.RFC3339Nano) {
		t.Fatalf("created_at = %#v, want UTC RFC3339Nano", got)
	}
	if !strings.Contains(q.calls[0].query, " FINAL WHERE ") || q.calls[0].args[0] != "order-1" {
		t.Fatalf("query/args = %q / %#v; want FINAL and a bound key", q.calls[0].query, q.calls[0].args)
	}
}

func TestCountByBucket(t *testing.T) {
	since := time.Date(2026, 10, 19, 10, 0, 0, 0, time.FixedZone("east", 5*60*60+30*60)).Add(30*time.Second + 250*time.Millisecond)
	bucket := since.UTC()
	q := &fakeQuerier{responses: []queryResponse{{
		rows: &fakeRows{columns: []string{"bucket", "count"}, data: [][]any{
			{bucket, uint64(4)},
			{bucket.Add(time.Minute), uint64(2)},
		}},
	}}}
	client := newTestClient(q)

	got, err := client.CountByBucket(context.Background(), "analytics.orders", "loaded_at", since, since.Add(2*time.Minute), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].Bucket != bucket || got[0].Count != 4 || got[1].Count != 2 {
		t.Fatalf("CountByBucket = %#v", got)
	}
	if q.calls[0].args[0] != int64(30_250_000_000) || q.calls[0].args[1] != int64(60) || q.calls[0].args[2] != int64(30_250_000_000) {
		t.Fatalf("bucket origin/width arguments = %#v, want offset 30.25s and width 60s", q.calls[0].args[:3])
	}

	if _, err := client.CountByBucket(context.Background(), "analytics.orders", "loaded_at", since, since.Add(time.Minute), 1500*time.Millisecond); err == nil {
		t.Fatal("CountByBucket accepted a fractional-second width")
	}
	if _, err := client.CountByBucket(context.Background(), "analytics.orders", "loaded_at", since, since.Add(200*time.Minute+time.Second), time.Minute); err == nil {
		t.Fatal("CountByBucket accepted a window that spans more than 200 buckets")
	}
	if len(q.calls) != 1 {
		t.Fatalf("invalid window ran query; calls = %d", len(q.calls))
	}
}

type testDecimal string

func (d testDecimal) String() string { return string(d) }
