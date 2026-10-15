package snowflake

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"piperoom/internal/tools"
)

func recordCtx() (context.Context, *tools.Recorder) {
	rec := tools.NewRecorder()
	return tools.WithRecorder(context.Background(), rec), rec
}

func TestTargetFromDSNHasNoCredentials(t *testing.T) {
	got := targetFromDSN("alice:hunter2-secret@myorg-acct/ANALYTICS/PUBLIC?warehouse=WH&role=R&token=tok-123")
	if got != "snowflake myorg-acct/ANALYTICS" {
		t.Fatalf("target = %q", got)
	}
	for _, bad := range []string{"alice", "hunter2", "tok-123"} {
		if strings.Contains(got, bad) {
			t.Fatalf("%q in target %q", bad, got)
		}
	}
	if got := targetFromDSN("not a dsn"); got != "snowflake" {
		t.Fatalf("bad dsn target = %q", got)
	}
}

func TestFreshnessRecordsStatement(t *testing.T) {
	ts := time.Date(2026, 10, 14, 3, 0, 0, 0, time.UTC)
	db := &DB{q: &fakeQuerier{rows: &fakeRows{cols: []string{"max"}, data: [][]any{{&ts}}}}, target: "snowflake acct/DB"}
	ctx, rec := recordCtx()
	if _, err := db.Freshness(ctx, "analytics.orders", "updated_at"); err != nil {
		t.Fatal(err)
	}
	calls := rec.Calls()
	if len(calls) != 1 {
		t.Fatalf("calls = %+v", calls)
	}
	c := calls[0]
	if c.Kind != tools.CallSQL || c.Target != "snowflake acct/DB" || c.Text != `SELECT MAX("UPDATED_AT") FROM "ANALYTICS"."ORDERS"` {
		t.Fatalf("call = %+v", c)
	}
	if len(c.ParamsRedacted) != 0 || c.TookMS < 0 || c.At.IsZero() {
		t.Fatalf("call = %+v", c)
	}
}

func TestRowRecordsPlaceholderAndPK(t *testing.T) {
	db := &DB{q: &fakeQuerier{rows: &fakeRows{cols: []string{"ID"}}}, target: "snowflake acct/DB"}
	ctx, rec := recordCtx()
	if _, _, err := db.Row(ctx, "analytics.orders", "id", "o-1001"); err != nil {
		t.Fatal(err)
	}
	calls := rec.Calls()
	if len(calls) != 1 {
		t.Fatalf("calls = %+v", calls)
	}
	if calls[0].Text != `SELECT * FROM "ANALYTICS"."ORDERS" WHERE "ID" = ? LIMIT 1` || strings.Contains(calls[0].Text, "o-1001") {
		t.Fatalf("text = %q", calls[0].Text)
	}
	if len(calls[0].ParamsRedacted) != 1 || calls[0].ParamsRedacted[0] != "?1=o-1001" {
		t.Fatalf("params = %v", calls[0].ParamsRedacted)
	}
}

func TestQueryErrorStillRecords(t *testing.T) {
	db := &DB{q: &fakeQuerier{err: errors.New("connection refused")}}
	ctx, rec := recordCtx()
	if _, err := db.Freshness(ctx, "analytics.orders", "updated_at"); err == nil {
		t.Fatal("want error")
	}
	if _, _, err := db.Row(ctx, "analytics.orders", "id", "x"); err == nil {
		t.Fatal("want error")
	}
	if n := len(rec.Calls()); n != 2 {
		t.Fatalf("got %d calls, want 2", n)
	}
}

func TestInvalidIdentifierRecordsNothing(t *testing.T) {
	db := &DB{q: &fakeQuerier{}}
	ctx, rec := recordCtx()
	if _, err := db.Freshness(ctx, "analytics.orders; DROP TABLE x", "id"); err == nil {
		t.Fatal("want error")
	}
	if n := len(rec.Calls()); n != 0 {
		t.Fatalf("got %d calls, want 0 (nothing ran)", n)
	}
}

func TestRedactArgsTable(t *testing.T) {
	when := time.Date(2026, 10, 14, 5, 0, 0, 0, time.FixedZone("x", 2*3600))
	cases := []struct {
		name string
		args []any
		want string
	}{
		{"string", []any{"o-1"}, "?1=o-1"},
		{"slice", []any{[]string{"a", "b"}}, "?1=[a,b]"},
		{"time", []any{when}, "?1=2026-10-14T03:00:00Z"},
		{"duration", []any{90 * time.Second}, "?1=1m30s"},
		{"number", []any{42}, "?1=42"},
		{"long", []any{strings.Repeat("a", 100)}, "?1=" + strings.Repeat("a", 64) + "…"},
		{"bytes", []any{[]byte("secret")}, "?1=<6 bytes>"},
		{"two", []any{"a", 2}, "?1=a|?2=2"},
	}
	for _, tc := range cases {
		if got := strings.Join(redactArgs(tc.args), "|"); got != tc.want {
			t.Errorf("%s: got %q, want %q", tc.name, got, tc.want)
		}
	}
	if got := redactArgs(nil); len(got) != 0 {
		t.Errorf("nil args: %v", got)
	}
}

func TestSQLTextCollapsesWhitespaceKeepsPlaceholders(t *testing.T) {
	if got := sqlText("SELECT *\n\tFROM t\n WHERE id =  ?"); got != "SELECT * FROM t WHERE id = ?" {
		t.Fatalf("sqlText = %q", got)
	}
}

func TestProvenanceHasNoDSNSecrets(t *testing.T) {
	db := &DB{q: &fakeQuerier{rows: &fakeRows{cols: []string{"max"}}}, target: targetFromDSN("u:pw-secret-1@acct/DB/S")}
	ctx, rec := recordCtx()
	_, _ = db.Freshness(ctx, "analytics.orders", "ts")
	if b, _ := json.Marshal(rec.Calls()); strings.Contains(string(b), "pw-secret-1") {
		t.Fatalf("secret in %s", b)
	}
}

func TestNoRecorderIsNoop(t *testing.T) {
	db := &DB{q: &fakeQuerier{rows: &fakeRows{cols: []string{"max"}}}}
	if _, err := db.Freshness(context.Background(), "analytics.orders", "ts"); err != nil {
		t.Fatal(err)
	}
}
