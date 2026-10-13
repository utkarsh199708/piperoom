package dbtools

import (
	"context"
	"encoding/json"
	"slices"
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
	// locks maps a pid to the tables its transaction locks. With locks set,
	// LongTransactionsOnTables keeps a transaction only when it locks one of
	// the tables asked for and counts the rest, like the SQL; nil keeps all.
	locks map[int][]string
	// otherDB marks the pids whose session is in another database.
	otherDB map[int]bool
}

func (s *txnSource) LongTransactions(context.Context, time.Duration) ([]clients.Transaction, error) {
	return s.txns, nil
}

// LongTransactionsOnTables models the SQL: the sessions that lock one of the
// tables (and are in this database) are listed, every other one is counted,
// and the oldest other writers come back as postgres.OtherWriter.
func (s *txnSource) LongTransactionsOnTables(_ context.Context, _ time.Duration, tables []string) (postgres.TableTransactions, error) {
	var out postgres.TableTransactions
	var writers []clients.Transaction
	for _, t := range s.txns {
		if s.locks == nil || (!s.otherDB[t.PID] && slices.ContainsFunc(s.locks[t.PID], func(x string) bool { return slices.Contains(tables, x) })) {
			out.Transactions = append(out.Transactions, t)
			continue
		}
		out.Other++
		if t.HasXID {
			out.OtherWriters++
			writers = append(writers, t)
		}
	}
	slices.SortStableFunc(writers, func(a, b clients.Transaction) int { return int(b.Age - a.Age) })
	for _, w := range writers[:min(len(writers), postgres.MaxOtherWriters)] {
		waitType, _, _ := strings.Cut(w.WaitEvent, ":")
		out.OldestOtherWriters = append(out.OldestOtherWriters, postgres.OtherWriter{
			PID: w.PID, SameDatabase: !s.otherDB[w.PID], State: w.State,
			AgeSeconds: int64(w.Age / time.Second), XIDAge: int64(1000 + w.PID), WaitEventType: waitType,
		})
	}
	return out, nil
}

// txnRows returns the pipeline's own rows of a pg_long_transactions result:
// all of Data when unscoped, Data.Transactions when scoped.
func txnRows(t *testing.T, data any) []longTxn {
	t.Helper()
	switch d := data.(type) {
	case []longTxn:
		return d
	case scopedTxns:
		return d.Transactions
	}
	t.Fatalf("pg_long_transactions data is %T", data)
	return nil
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
	rows := txnRows(t, r.Data)
	if rows[0].Verb != "UPDATE" || rows[1].Verb != "" || rows[2].Verb != "UPDATE" {
		t.Errorf("verbs = %q %q %q", rows[0].Verb, rows[1].Verb, rows[2].Verb)
	}
	if rows[0].AgeSeconds != 720 {
		t.Errorf("age seconds = %d", rows[0].AgeSeconds)
	}
}

// has_xid marks writers: it reaches both the Summary (writer=yes|no) and Data.
func TestLongTransactionsMarkWriters(t *testing.T) {
	d := testDeps()
	d.Src = &txnSource{fakeSource: d.Src.(*fakeSource), txns: []clients.Transaction{
		{PID: 51, Username: "etl", State: "active", Age: 9 * time.Minute, Query: "UPDATE", HasXID: true},
		{PID: 52, Username: "bi", State: "idle in transaction", Age: 8 * time.Minute, Query: "SELECT", HasXID: false},
	}}
	r, err := scopedTool(t, d, "pg_long_transactions").Run(roomCtx(), json.RawMessage(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	rows := txnRows(t, r.Data)
	if len(rows) != 2 || !rows[0].HasXID || rows[1].HasXID {
		t.Fatalf("HasXID = %+v, want [true false]", rows)
	}
	data, _ := json.Marshal(r.Data)
	if !strings.Contains(string(data), `"has_xid":true`) || !strings.Contains(string(data), `"has_xid":false`) {
		t.Errorf("data lacks has_xid: %s", data)
	}
	lines := strings.Split(strings.TrimSpace(r.Summary), "\n")
	if len(lines) != 2 || !strings.Contains(lines[0], "pid=51") || !strings.HasSuffix(lines[0], "writer=yes") ||
		!strings.Contains(lines[1], "pid=52") || !strings.HasSuffix(lines[1], "writer=no") {
		t.Errorf("summary writer flags wrong:\n%s", r.Summary)
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
