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

func TestLoadErrorsReturnsSafeQueryAndKafkaFailures(t *testing.T) {
	since := time.Date(2026, 10, 19, 0, 0, 0, 0, time.UTC)
	queryAt := since.Add(time.Hour)
	kafkaAt := since.Add(2 * time.Hour)
	q := &fakeQuerier{responses: []queryResponse{
		{rows: &fakeRows{
			columns: []string{"event_time", "exception_code", "written_rows"},
			data:    [][]any{{queryAt, int32(252), uint64(8)}},
		}},
		{rows: &fakeRows{
			columns: []string{"at"},
			data:    [][]any{{kafkaAt}},
		}},
	}}
	got, err := newTestClient(q).LoadErrors(context.Background(), "analytics.orders", since, 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("LoadErrors returned %d results, want 2", len(got))
	}
	if got[0].At != kafkaAt || got[0].Status != "LOAD_FAILED" || !strings.Contains(got[0].FirstError, "Kafka engine") {
		t.Fatalf("Kafka load error = %#v", got[0])
	}
	if got[1].At != queryAt || got[1].Status != "PARTIALLY_LOADED" || got[1].Rows != 8 || got[1].Errors != 1 {
		t.Fatalf("query-log load error = %#v", got[1])
	}
	for _, item := range got {
		if len([]rune(item.FirstError)) > 300 {
			t.Fatalf("FirstError has %d runes", len([]rune(item.FirstError)))
		}
		if strings.Contains(item.FirstError, "record-value") {
			t.Fatalf("FirstError leaked record text: %q", item.FirstError)
		}
	}
	if !strings.Contains(q.calls[0].query, "ExceptionBeforeStart") || !strings.Contains(q.calls[0].query, "event_date >= toDate(?)") || !strings.Contains(q.calls[0].query, "has(tables, ?)") {
		t.Fatalf("query-log SQL = %q", q.calls[0].query)
	}
	if q.calls[0].args[2] != "analytics.orders" {
		t.Fatalf("query-log table filter = %#v, want qualified table", q.calls[0].args[2])
	}
	if strings.Contains(strings.ToLower(q.calls[0].query), "exception,") {
		t.Fatalf("query-log SQL reads raw exception text: %q", q.calls[0].query)
	}
}

func TestLoadErrorsKeepsQueryLogEvidenceWhenKafkaTableIsDenied(t *testing.T) {
	since := time.Date(2026, 10, 19, 0, 0, 0, 0, time.UTC)
	q := &fakeQuerier{responses: []queryResponse{
		{rows: &fakeRows{columns: []string{"event_time", "exception_code", "written_rows"}, data: [][]any{{since.Add(time.Minute), int32(252), uint64(0)}}}},
		{err: &driver.Exception{Code: errAccessDenied, Name: "ACCESS_DENIED"}},
	}}
	got, err := newTestClient(q).LoadErrors(context.Background(), "analytics.orders", since, 5)
	if err != nil {
		t.Fatalf("LoadErrors with inaccessible kafka_consumers: %v", err)
	}
	if len(got) != 1 || got[0].Status != "LOAD_FAILED" {
		t.Fatalf("LoadErrors = %#v, want query-log evidence only", got)
	}
}

func TestLoadErrorsOptionalKafkaTableAndUnsupportedQueryLog(t *testing.T) {
	since := time.Now().UTC()
	q := &fakeQuerier{responses: []queryResponse{
		{rows: &fakeRows{columns: []string{"event_time", "exception_code", "written_rows"}}},
		{err: &driver.Exception{Code: errUnknownTable, Name: "UNKNOWN_TABLE"}},
	}}
	got, err := newTestClient(q).LoadErrors(context.Background(), "orders", since, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("LoadErrors = %#v, want no errors", got)
	}
	if q.calls[0].args[3] != defaultLoadLimit {
		t.Fatalf("default limit = %#v, want %d", q.calls[0].args[3], defaultLoadLimit)
	}

	q = &fakeQuerier{responses: []queryResponse{{err: &driver.Exception{Code: errUnknownTable, Name: "UNKNOWN_TABLE"}}}}
	_, err = newTestClient(q).LoadErrors(context.Background(), "analytics.orders", since, 5)
	if !errors.Is(err, clients.ErrNotSupported) {
		t.Fatalf("LoadErrors error = %v, want ErrNotSupported", err)
	}
}

func TestLoadErrorsBadInputAndTargetDown(t *testing.T) {
	t.Run("bad table", func(t *testing.T) {
		q := &fakeQuerier{}
		_, err := newTestClient(q).LoadErrors(context.Background(), "analytics.orders; DROP TABLE x", time.Now(), 10)
		if err == nil {
			t.Fatal("LoadErrors accepted an unsafe table")
		}
		if len(q.calls) != 0 {
			t.Fatalf("unsafe input ran %d queries", len(q.calls))
		}
	})

	t.Run("target down", func(t *testing.T) {
		q := &fakeQuerier{responses: []queryResponse{{err: errors.New("connection refused")}}}
		_, err := newTestClient(q).LoadErrors(context.Background(), "analytics.orders", time.Now(), 10)
		if err == nil || strings.Contains(err.Error(), "supersecret") {
			t.Fatalf("LoadErrors error = %v, want safe target-down error", err)
		}
	})
}
