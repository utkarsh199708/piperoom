package dbtools

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/clients/postgres"
	"piperoom/internal/tools"
)

// txnSource serves fixed long transactions, the way a client that returns the
// full statement text would.
type txnSource struct {
	*fakeSource
	txns []clients.Transaction
}

func (s *txnSource) LongTransactions(context.Context, time.Duration) ([]clients.Transaction, error) {
	return s.txns, nil
}

func TestLongTransactionsReturnNoQueryText(t *testing.T) {
	d := testDeps()
	d.Src = &txnSource{fakeSource: d.Src.(*fakeSource), txns: []clients.Transaction{
		{PID: 41, Username: "billing_app", ApplicationName: "billing-api", State: "idle in transaction", Age: 12 * time.Minute,
			WaitEvent: "Client:ClientRead", Query: "update payments set card_number='4111111111111111' where id=7"},
		{PID: 42, Username: "etl", ApplicationName: "etl", State: "active", Age: 9 * time.Minute,
			Query: "/* batch */ SELECT ssn FROM hr_salaries WHERE name='Jane Doe'"},
		{PID: 43, Username: "u", State: "active", Age: 6 * time.Minute, Query: "UPDATE"},
	}}
	r, err := scopedTool(t, d, "pg_long_transactions").Run(roomCtx(), json.RawMessage(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(r.Data)
	for _, leak := range []string{"4111", "card_number", "payments", "Jane", "ssn", "hr_salaries", "batch", "where"} {
		if strings.Contains(r.Summary, leak) || strings.Contains(string(data), leak) {
			t.Errorf("output leaks %q:\nsummary: %s\ndata: %s", leak, r.Summary, data)
		}
	}
	for _, want := range []string{"pid=41", "user=billing_app", "app=billing-api", "state=idle in transaction", "wait=Client:ClientRead", "verb=UPDATE", "pid=42", "pid=43"} {
		if !strings.Contains(r.Summary, want) {
			t.Errorf("summary missing %q: %s", want, r.Summary)
		}
	}
	rows := r.Data.([]longTxn)
	if rows[0].Verb != "UPDATE" || rows[1].Verb != "" || rows[2].Verb != "UPDATE" {
		t.Errorf("verbs = %q %q %q", rows[0].Verb, rows[1].Verb, rows[2].Verb)
	}
	if rows[0].AgeSeconds != 720 {
		t.Errorf("age seconds = %d", rows[0].AgeSeconds)
	}
}

func TestStatementVerb(t *testing.T) {
	for in, want := range map[string]string{
		"select 1":               "SELECT",
		"  Update t set a=1":     "UPDATE",
		"COMMIT;":                "COMMIT",
		"":                       "",
		"/* c */ select 1":       "",
		"1; drop table x":        "",
		"abcdefghijklmnopqrstuv": "ABCDEFGHIJKLMNOP",
	} {
		if got := statementVerb(in); got != want {
			t.Errorf("statementVerb(%q) = %q, want %q", in, got, want)
		}
	}
}

// With a FOR ALL TABLES publication the client lists every table as a member;
// the tool shows only the pipeline's own source tables.
func TestPublicationMembersAreFilteredToSourceTables(t *testing.T) {
	d := testDeps()
	d.Src = &fakeSource{pubTables: []postgres.PublicationTable{
		{Schema: "public", Table: "orders", InPublication: true, Selectable: true},
		{Schema: "public", Table: "payments", InPublication: true, Selectable: true},
		{Schema: "hr", Table: "salaries", InPublication: true, Selectable: false},
	}}
	r, err := scopedTool(t, d, "pg_publication_tables").Run(roomCtx(), json.RawMessage(`{"pipeline_id":"orders"}`))
	if err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(r.Data)
	for _, leak := range []string{"payments", "salaries", "hr"} {
		if strings.Contains(r.Summary, leak) || strings.Contains(string(data), leak) {
			t.Errorf("output leaks %q:\nsummary: %s\ndata: %s", leak, r.Summary, data)
		}
	}
	m := r.Data.(map[string]any)["publication_tables"].([]postgres.PublicationTable)
	if len(m) != 1 || m[0].Table != "orders" {
		t.Fatalf("publication_tables = %+v, want only public.orders", m)
	}
}

func TestScopeEmptyPipelineIDClearsInheritedScope(t *testing.T) {
	// A context that carries the room's pipeline, then an alert with none.
	ctx := tools.WithPipelineID(roomCtx(), "")
	d, c := scopedDeps(false)
	_, err := scopedTool(t, d, "pg_row").Run(ctx, json.RawMessage(`{"pipeline_id":"orders","table":"public.orders","pk_column":"id","key":"7"}`))
	if err == nil || c.n() != 0 {
		t.Fatalf("err = %v, calls = %v; want a refusal", err, c.calls)
	}
}
