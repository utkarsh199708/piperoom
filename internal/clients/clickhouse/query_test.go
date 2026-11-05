package clickhouse

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"piperoom/internal/tools"
)

func TestQueryRecordsRedactedParams(t *testing.T) {
	rec := tools.NewRecorder()
	ctx := tools.WithRecorder(context.Background(), rec)
	q := &fakeQuerier{responses: []queryResponse{
		{rows: &fakeRows{columns: []string{"id"}}},
		{err: errors.New("connection refused")},
	}}
	client := newTestClient(q)

	if _, _, err := client.Row(ctx, "analytics.orders", "id", "order-1"); err != nil {
		t.Fatal(err)
	}
	since := time.Date(2026, 10, 19, 12, 0, 0, 0, time.FixedZone("IST", 5*60*60+30*60))
	if _, err := client.LoadErrors(ctx, "analytics.orders", since, 3); err == nil {
		t.Fatal("LoadErrors succeeded on a down target")
	}

	calls := rec.Calls()
	if len(calls) != 2 {
		t.Fatalf("recorded %d calls, want 2 (on success and on error)", len(calls))
	}
	if got, want := calls[0].ParamsRedacted, []string{"?1=order-1"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Row params = %q, want %q", got, want)
	}
	params := calls[1].ParamsRedacted
	if len(params) != 7 {
		t.Fatalf("LoadErrors params = %q, want 7", params)
	}
	for i, want := range map[int]string{0: "?1=2026-10-19T06:30:00Z", 2: "?3=analytics.orders", 4: "?5=analytics", 6: "?7=3"} {
		if params[i] != want {
			t.Fatalf("LoadErrors param %d = %q, want %q", i, params[i], want)
		}
	}
	for _, call := range calls {
		if call.Kind != tools.CallSQL || call.Target != client.target || strings.Contains(strings.Join(call.ParamsRedacted, " "), "supersecret") {
			t.Fatalf("call = %#v", call)
		}
	}
}

func TestRedactArgs(t *testing.T) {
	long := strings.Repeat("é", 40) // 80 bytes: the cut must land on a rune boundary
	got := redactArgs([]any{nil, long, []byte("secret"), int64(42), time.Duration(1500) * time.Millisecond, uint64(7)})
	want := []string{"?1=NULL", "?2=" + strings.Repeat("é", 32) + "…", "?3=<6 bytes>", "?4=42", "?5=1.5s", "?6=7"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("redactArgs = %q, want %q", got, want)
	}
	if redactArgs(nil) != nil {
		t.Fatal("redactArgs(nil) should be nil")
	}
}
