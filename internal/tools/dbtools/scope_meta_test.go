package dbtools

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/clients/postgres"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
)

// Task T2: database-wide metadata (replication slots, long transactions,
// publication members) is scoped to the room's pipeline. Every fake serves
// two pipelines' objects; the other pipeline's names carry foreignMark, so a
// leak into Summary or the marshalled Data is a plain substring check.

const foreignMark = "FOREIGNMARK"

const pgClass = "io.debezium.connector.postgresql.PostgresConnector"

// assertNoForeign fails when the summary or the marshalled data name an object
// outside the room's pipeline.
func assertNoForeign(t *testing.T, r tools.Result) {
	t.Helper()
	data, err := json.Marshal(r.Data)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(r.Summary, foreignMark) || strings.Contains(string(data), foreignMark) {
		t.Fatalf("output names another pipeline's object:\nsummary: %s\ndata: %s", r.Summary, data)
	}
}

// metaConnect has both pipelines' Debezium source connectors, each with its
// own slot.
func metaConnect() *fakeConnect {
	return &fakeConnect{configs: map[string]map[string]string{
		"pg-orders-connector":   {"connector.class": pgClass, "database.user": "piperoom", "slot.name": "orders_slot"},
		"pg-payments-connector": {"connector.class": pgClass, "database.user": "billing", "slot.name": "pay_" + foreignMark + "_slot"},
	}}
}

func twoPipelineSlots() []clients.ReplicationSlot {
	return []clients.ReplicationSlot{
		{Name: "hr_" + foreignMark + "_slot", Plugin: "pgoutput", Active: true, WALStatus: "reserved"},
		{Name: "orders_slot", Plugin: "pgoutput", Active: true, RetainedWALBytes: 2048, WALStatus: "reserved"},
		{Name: "pay_" + foreignMark + "_slot", Plugin: "pgoutput", Active: false, RetainedWALBytes: 1 << 30, WALStatus: "extended"},
	}
}

// slotSQLSource filters slots by name "in SQL" (namedSlotLister) and fails
// the test if the unfiltered list is read.
type slotSQLSource struct {
	*fakeSource
	asked   [][]string
	listAll int
}

func (s *slotSQLSource) ReplicationSlots(ctx context.Context) ([]clients.ReplicationSlot, error) {
	s.listAll++
	return s.fakeSource.ReplicationSlots(ctx)
}

func (s *slotSQLSource) ReplicationSlotsNamed(_ context.Context, names []string) ([]clients.ReplicationSlot, int, error) {
	s.asked = append(s.asked, names)
	var out []clients.ReplicationSlot
	for _, sl := range s.slots {
		if slices.Contains(names, sl.Name) {
			out = append(out, sl)
		}
	}
	return out, len(s.slots) - len(out), nil
}

func metaDeps(src clients.SourceDB) Deps {
	d := testDeps()
	d.Src = src
	d.Connect = metaConnect()
	return d
}

func TestReplicationSlotsScopedShowOnlyThePipelinesSlot(t *testing.T) {
	check := func(t *testing.T, r tools.Result) {
		t.Helper()
		assertNoForeign(t, r)
		if !strings.Contains(r.Summary, "slot orders_slot: active=true retained=2.0 KiB wal_status=reserved") {
			t.Fatalf("summary misses the pipeline's slot: %q", r.Summary)
		}
		if !strings.Contains(r.Summary, "2 other replication slots not shown (not part of this room's pipeline)") {
			t.Fatalf("summary misses the count-only line: %q", r.Summary)
		}
		got := r.Data.([]clients.ReplicationSlot)
		if len(got) != 1 || got[0].Name != "orders_slot" {
			t.Fatalf("data = %+v, want only orders_slot", got)
		}
	}
	t.Run("SQL filter", func(t *testing.T) {
		src := &slotSQLSource{fakeSource: &fakeSource{slots: twoPipelineSlots()}}
		r, err := scopedTool(t, metaDeps(src), "pg_replication_slots").Run(roomCtx(), json.RawMessage(`{}`))
		if err != nil {
			t.Fatal(err)
		}
		check(t, r)
		if len(src.asked) != 1 || !slices.Equal(src.asked[0], []string{"orders_slot"}) || src.listAll != 0 {
			t.Fatalf("asked = %v, unfiltered reads = %d; want one filtered read for [orders_slot]", src.asked, src.listAll)
		}
	})
	t.Run("in-memory filter", func(t *testing.T) {
		r, err := scopedTool(t, metaDeps(&fakeSource{slots: twoPipelineSlots()}), "pg_replication_slots").Run(roomCtx(), json.RawMessage(`{}`))
		if err != nil {
			t.Fatal(err)
		}
		check(t, r)
	})
	t.Run("opt-out keeps every slot", func(t *testing.T) {
		r, err := scopedTool(t, metaDeps(&fakeSource{slots: twoPipelineSlots()}), "pg_replication_slots").Run(tools.WithUnscoped(context.Background()), json.RawMessage(`{}`))
		if err != nil {
			t.Fatal(err)
		}
		if strings.Count(r.Summary, foreignMark) != 2 || strings.Contains(r.Summary, "not shown") || len(r.Data.([]clients.ReplicationSlot)) != 3 {
			t.Fatalf("unscoped output changed: %q", r.Summary)
		}
	})
}

func TestReplicationSlotsScopedMissingSlotIsReported(t *testing.T) {
	src := &fakeSource{slots: []clients.ReplicationSlot{{Name: "pay_" + foreignMark + "_slot"}}}
	r, err := scopedTool(t, metaDeps(src), "pg_replication_slots").Run(roomCtx(), json.RawMessage(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	assertNoForeign(t, r)
	want := "slot orders_slot: not found in pg_replication_slots\n1 other replication slot not shown (not part of this room's pipeline)\n"
	if r.Summary != want || len(r.Data.([]clients.ReplicationSlot)) != 0 {
		t.Fatalf("summary = %q, data = %v", r.Summary, r.Data)
	}
}

// The slot name comes from the connector config. When it cannot be read the
// tool fails closed: an error, no data, and the source is never read.
func TestReplicationSlotsScopedFailClosed(t *testing.T) {
	down := errors.New("connect: connection refused")
	cases := map[string]func(d *Deps){
		"config unreadable": func(d *Deps) { d.Connect = &fakeConnect{configErr: down} },
		"connector missing": func(d *Deps) { d.Connect = &fakeConnect{configs: map[string]map[string]string{}} },
		"no Connect client": func(d *Deps) { d.Connect = nil },
		"invalid slot.name": func(d *Deps) {
			d.Connect = &fakeConnect{configs: map[string]map[string]string{"pg-orders-connector": {"connector.class": pgClass, "slot.name": "x' OR 1=1"}}}
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			src := &slotSQLSource{fakeSource: &fakeSource{slots: twoPipelineSlots()}}
			d := metaDeps(src)
			mutate(&d)
			r, err := scopedTool(t, d, "pg_replication_slots").Run(roomCtx(), json.RawMessage(`{}`))
			if err == nil {
				t.Fatalf("want an error, got %q", r.Summary)
			}
			if r.Summary != "" || r.Data != nil {
				t.Fatalf("a refused call returned data: %+v", r)
			}
			if len(src.asked) != 0 || src.listAll != 0 {
				t.Fatalf("source read although the slot is unknown: asked %v, all %d", src.asked, src.listAll)
			}
			if name == "config unreadable" && !errors.Is(err, down) {
				t.Fatalf("err = %v, want it to wrap the Connect error", err)
			}
		})
	}
}

// watch.slot in owners.yaml names the slot, so the tool works while Connect is down.
func TestReplicationSlotsDeclaredSlotWithoutConnect(t *testing.T) {
	path := filepath.Join(t.TempDir(), "owners.yaml")
	yaml := `
pipelines:
  - id: orders
    topic_prefix: pg
    source_tables: [public.orders]
    connectors: [pg-orders-connector]
    watch:
      slot: orders_slot
layers:
  postgres: priya
`
	if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}
	o, err := owners.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	d := metaDeps(&fakeSource{slots: twoPipelineSlots()})
	d.Owners = o
	d.Connect = &fakeConnect{configErr: errors.New("connect down")}
	r, err := scopedTool(t, d, "pg_replication_slots").Run(roomCtx(), json.RawMessage(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	assertNoForeign(t, r)
	if !strings.HasPrefix(r.Summary, "slot orders_slot: active=true") {
		t.Fatalf("summary = %q", r.Summary)
	}
}

// Four long transactions: one locks the room's table; one is a writer on
// another pipeline's table; one is a reader with no lock on the room's
// tables; one is a writer in another database. Only the first is listed in
// full. The two writers, which hold back every logical slot, are listed
// anonymised, oldest first. The reader is only counted. No user, application
// or verb of the others shows anywhere.
func TestLongTransactionsScopedShowOnlyThePipelinesSessions(t *testing.T) {
	src := &txnSource{fakeSource: &fakeSource{}, txns: []clients.Transaction{
		{PID: 11, Username: "orders_app", ApplicationName: "orders-api", State: "idle in transaction", Age: 20 * time.Minute, Query: "UPDATE", HasXID: true},
		{PID: 12, Username: foreignMark + "_billing", ApplicationName: foreignMark + "-api", State: "idle in transaction", Age: 30 * time.Minute,
			WaitEvent: "Client:ClientRead", Query: "DELETE", HasXID: true},
		{PID: 13, Username: foreignMark + "_etl", ApplicationName: foreignMark + "-etl", State: "active", Age: 9 * time.Minute, Query: "SELECT"},
		{PID: 14, Username: foreignMark + "_hr", ApplicationName: foreignMark + "-hr", State: "active", Age: 40 * time.Minute,
			WaitEvent: "Lock:transactionid", Query: "INSERT", HasXID: true},
	}, locks: map[int][]string{
		11: {"public.orders"},
		12: {"public.payments"},
		14: {"public.orders"}, // the same table name, but in another database
	}, otherDB: map[int]bool{14: true}}
	tl := scopedTool(t, metaDeps(src), "pg_long_transactions")

	r, err := tl.Run(roomCtx(), json.RawMessage(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	assertNoForeign(t, r)
	// The bounded block about the other sessions comes first, the
	// pipeline's own rows after it.
	want := "3 other long transactions not shown (not part of this room's pipeline); 2 of them are writers (has_xid)\n" +
		"oldest writers outside this pipeline's tables (they hold back every logical slot's restart_lsn; no user, application, database or query shown):\n" +
		"  pid=14 same_database=no state=active age=40m xid_age=1014 wait_type=Lock\n" +
		"  pid=12 same_database=yes state=idle in transaction age=30m xid_age=1012 wait_type=Client\n" +
		"pid=11 user=orders_app app=orders-api state=idle in transaction age=20m wait=- verb=UPDATE writer=yes\n"
	if r.Summary != want {
		t.Fatalf("summary =\n%s\nwant\n%s", r.Summary, want)
	}
	for _, leak := range []string{"pid=13", "DELETE", "INSERT", "ClientRead", "transactionid"} {
		if strings.Contains(r.Summary, leak) || strings.Contains(string(mustJSON(t, r.Data)), leak) {
			t.Fatalf("output shows %q of another session:\n%s\n%s", leak, r.Summary, mustJSON(t, r.Data))
		}
	}
	data := r.Data.(scopedTxns)
	if len(data.Transactions) != 1 || data.Transactions[0].PID != 11 || data.OtherNotShown != 3 || data.OtherWriters != 2 {
		t.Fatalf("data = %+v", data)
	}
	if len(data.OldestOtherWriters) != 2 || data.OldestOtherWriters[0].PID != 14 || data.OldestOtherWriters[0].SameDatabase ||
		data.OldestOtherWriters[1].PID != 12 || !data.OldestOtherWriters[1].SameDatabase {
		t.Fatalf("oldest other writers = %+v", data.OldestOtherWriters)
	}

	// The opt-out keeps the old, unfiltered list.
	r, err = tl.Run(tools.WithUnscoped(context.Background()), json.RawMessage(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(r.Summary, "pid=12") || !strings.Contains(r.Summary, "pid=13") || strings.Contains(r.Summary, "not shown") {
		t.Fatalf("unscoped summary = %q", r.Summary)
	}
	if _, ok := r.Data.([]longTxn); !ok {
		t.Fatalf("unscoped data is %T, want []longTxn as before", r.Data)
	}
}

func TestLongTransactionsScopedNoneOnThePipeline(t *testing.T) {
	src := &txnSource{fakeSource: &fakeSource{}, txns: []clients.Transaction{
		{PID: 12, Username: foreignMark, State: "active", Age: 30 * time.Minute, HasXID: false},
	}, locks: map[int][]string{12: {"public.payments"}}}
	r, err := scopedTool(t, metaDeps(src), "pg_long_transactions").Run(roomCtx(), json.RawMessage(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	assertNoForeign(t, r)
	// A reader cannot hold a slot back: it stays a count, with no listing.
	want := "1 other long transaction not shown (not part of this room's pipeline); 0 of them are writers (has_xid)\n(no long transactions on this pipeline's tables)\n"
	if r.Summary != want {
		t.Fatalf("summary = %q, want %q", r.Summary, want)
	}
}

func TestLongTransactionsScopedOtherWritersCapped(t *testing.T) {
	src := &txnSource{fakeSource: &fakeSource{}, locks: map[int][]string{}}
	for pid := 20; pid < 27; pid++ {
		src.txns = append(src.txns, clients.Transaction{PID: pid, Username: foreignMark, State: "idle in transaction", Age: time.Duration(pid) * time.Minute, HasXID: true})
	}
	r, err := scopedTool(t, metaDeps(src), "pg_long_transactions").Run(roomCtx(), json.RawMessage(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	assertNoForeign(t, r)
	if n := strings.Count(r.Summary, "  pid="); n != postgres.MaxOtherWriters {
		t.Fatalf("%d writers listed, want %d:\n%s", n, postgres.MaxOtherWriters, r.Summary)
	}
	if !strings.HasPrefix(r.Summary, "7 other long transactions not shown") || !strings.Contains(r.Summary, "  (2 more writers not listed)\n") {
		t.Fatalf("summary = %q", r.Summary)
	}
	if got := r.Data.(scopedTxns).OldestOtherWriters; len(got) != postgres.MaxOtherWriters || got[0].PID != 26 {
		t.Fatalf("oldest other writers = %+v, want 5, oldest (pid 26) first", got)
	}
}

// The anonymised writer has exactly the allow-listed fields, and its two text
// fields take only Postgres's fixed vocabulary: anything else becomes "?".
func TestOtherWriterAllowList(t *testing.T) {
	var keys []string
	var m map[string]any
	if err := json.Unmarshal(mustJSON(t, otherWriter{}), &m); err != nil {
		t.Fatal(err)
	}
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	want := []string{"age", "age_seconds", "pid", "same_database", "state", "wait_event_type", "xid_age"}
	if !slices.Equal(keys, want) {
		t.Fatalf("otherWriter fields = %v, want exactly %v", keys, want)
	}

	cases := []struct {
		state, wait         string
		wantState, wantWait string
	}{
		{"idle in transaction", "Client", "idle in transaction", "Client"},
		{"active", "", "active", ""},
		{"idle in transaction (aborted)", "Lock", "idle in transaction (aborted)", "Lock"},
		{foreignMark + " billing-api", "Client:ClientRead", "?", "?"},
		{"UPDATE payments SET card='4111'", foreignMark + " app", "?", "?"},
	}
	for _, tc := range cases {
		got := anonWriter(postgres.OtherWriter{PID: 7, State: tc.state, WaitEventType: tc.wait, AgeSeconds: 90, XIDAge: 3})
		if got.State != tc.wantState || got.WaitEventType != tc.wantWait || got.PID != 7 || got.Age != "1m" || got.XIDAge != 3 {
			t.Errorf("anonWriter(%q, %q) = %+v, want state %q wait %q", tc.state, tc.wait, got, tc.wantState, tc.wantWait)
		}
	}

	// End to end: a client that returns free text in those fields leaks none of it.
	src := &writerSource{fakeSource: &fakeSource{}, ws: []postgres.OtherWriter{{PID: 9, State: foreignMark + "-state", WaitEventType: foreignMark}}}
	r, err := scopedTool(t, metaDeps(src), "pg_long_transactions").Run(roomCtx(), json.RawMessage(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	assertNoForeign(t, r)
	if !strings.Contains(r.Summary, "pid=9 same_database=no state=? age=0s xid_age=0 wait_type=?") {
		t.Fatalf("summary = %q", r.Summary)
	}
}

// writerSource returns other writers as the client gives them, uncapped.
type writerSource struct {
	*fakeSource
	ws []postgres.OtherWriter
}

func (s *writerSource) LongTransactionsOnTables(context.Context, time.Duration, []string) (postgres.TableTransactions, error) {
	return postgres.TableTransactions{Other: len(s.ws), OtherWriters: len(s.ws), OldestOtherWriters: s.ws}, nil
}

// The tool caps the listing itself, whatever the client returns.
func TestLongTransactionsScopedToolCapsOtherWriters(t *testing.T) {
	src := &writerSource{fakeSource: &fakeSource{}}
	for pid := 30; pid < 38; pid++ {
		src.ws = append(src.ws, postgres.OtherWriter{PID: pid, State: "idle in transaction", WaitEventType: "Client"})
	}
	r, err := scopedTool(t, metaDeps(src), "pg_long_transactions").Run(roomCtx(), json.RawMessage(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	if n := strings.Count(r.Summary, "  pid="); n != postgres.MaxOtherWriters || strings.Contains(r.Summary, "pid=35") {
		t.Fatalf("%d writers listed, want the first %d:\n%s", n, postgres.MaxOtherWriters, r.Summary)
	}
	if got := r.Data.(scopedTxns).OldestOtherWriters; len(got) != postgres.MaxOtherWriters {
		t.Fatalf("data lists %d writers, want %d", len(got), postgres.MaxOtherWriters)
	}
	if !strings.Contains(r.Summary, "  (3 more writers not listed)\n") {
		t.Fatalf("summary = %q", r.Summary)
	}
}

// Hundreds of long sessions on the pipeline's tables, with application names
// of the maximum length, must not push the other writers out of Summary: the
// slot-lag cause is in the first lines, and Summary shows only the 30 oldest
// pipeline sessions (Data has all of them).
func TestLongTransactionsScopedOtherWritersSurviveManyPipelineRows(t *testing.T) {
	src := &txnSource{fakeSource: &fakeSource{}, locks: map[int][]string{}, otherDB: map[int]bool{9999: true}}
	app := strings.Repeat("a", 63) // NAMEDATALEN - 1, the longest application_name
	for pid := 1000; pid < 1200; pid++ {
		src.txns = append(src.txns, clients.Transaction{PID: pid, Username: strings.Repeat("u", 63), ApplicationName: app,
			State: "idle in transaction", Age: time.Duration(pid) * time.Second, WaitEvent: "Client:ClientRead", Query: "UPDATE", HasXID: true})
		src.locks[pid] = []string{"public.orders"}
	}
	src.txns = append(src.txns, clients.Transaction{PID: 9999, Username: foreignMark, ApplicationName: foreignMark,
		State: "idle in transaction", Age: time.Minute, WaitEvent: "Client:ClientRead", HasXID: true})

	r, err := scopedTool(t, metaDeps(src), "pg_long_transactions").Run(roomCtx(), json.RawMessage(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	assertNoForeign(t, r)
	if len(r.Summary) > tools.MaxSummaryBytes || r.Truncated {
		t.Fatalf("summary is %d bytes (truncated %v), over %d", len(r.Summary), r.Truncated, tools.MaxSummaryBytes)
	}
	// The bounded block is first, whatever the pipeline rows hold.
	if !strings.HasPrefix(r.Summary, "1 other long transaction not shown") {
		t.Fatalf("summary does not start with the count line:\n%s", r.Summary[:200])
	}
	for _, want := range []string{
		"1 other long transaction not shown (not part of this room's pipeline); 1 of them is a writer (has_xid)\n",
		"  pid=9999 same_database=no state=idle in transaction age=1m xid_age=10999 wait_type=Client\n",
		"(170 more sessions on this pipeline's tables, in Data)\n",
	} {
		if !strings.Contains(r.Summary, want) {
			t.Fatalf("summary misses %q:\n%s", want, r.Summary)
		}
	}
	// The 30 oldest pipeline sessions are shown: pids 1199 down to 1170.
	if n := strings.Count(r.Summary, " user=uuu"); n != 30 || !strings.Contains(r.Summary, "pid=1199 ") || !strings.Contains(r.Summary, "pid=1170 ") || strings.Contains(r.Summary, "pid=1169 ") {
		t.Fatalf("%d pipeline rows in summary, want the 30 oldest:\n%s", n, r.Summary)
	}
	if got := r.Data.(scopedTxns).Transactions; len(got) != 200 {
		t.Fatalf("data has %d pipeline rows, want all 200", len(got))
	}
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// tablesSource records the tables the lock filter is asked for.
type tablesSource struct {
	*fakeSource
	asked [][]string
}

func (s *tablesSource) LongTransactionsOnTables(_ context.Context, _ time.Duration, tables []string) (postgres.TableTransactions, error) {
	s.asked = append(s.asked, tables)
	return postgres.TableTransactions{}, nil
}

func TestLongTransactionsScopedAskForThePipelinesTables(t *testing.T) {
	src := &tablesSource{fakeSource: &fakeSource{}}
	if _, err := scopedTool(t, metaDeps(src), "pg_long_transactions").Run(roomCtx(), json.RawMessage(`{}`)); err != nil {
		t.Fatal(err)
	}
	if len(src.asked) != 1 || !slices.Equal(src.asked[0], []string{"public.orders"}) {
		t.Fatalf("asked = %v, want [[public.orders]]", src.asked)
	}
}

// A client that cannot filter by table is refused in a room: it would have to
// list every session.
func TestLongTransactionsScopedFailClosedWithoutTableFilter(t *testing.T) {
	tl := scopedTool(t, metaDeps(plainSource{}), "pg_long_transactions")
	r, err := tl.Run(roomCtx(), json.RawMessage(`{}`))
	if err == nil || r.Summary != "" || r.Data != nil {
		t.Fatalf("r = %+v, err = %v; want a refusal", r, err)
	}
	if _, err := tl.Run(tools.WithUnscoped(context.Background()), json.RawMessage(`{}`)); err != nil {
		t.Fatalf("unscoped call with a plain client: %v", err)
	}
}

// pubSQLSource reads only the asked tables "in SQL" (PublicationSourceTables)
// and fails the test if the full member list is read.
type pubSQLSource struct {
	*fakeSource
	asked   [][]string
	listAll int
}

func (s *pubSQLSource) PublicationTables(ctx context.Context, p, r string, t []string) ([]postgres.PublicationTable, error) {
	s.listAll++
	return s.fakeSource.PublicationTables(ctx, p, r, t)
}

func (s *pubSQLSource) PublicationSourceTables(_ context.Context, _, _ string, tables []string) ([]postgres.PublicationTable, int, error) {
	s.asked = append(s.asked, tables)
	var out []postgres.PublicationTable
	other := 0
	for _, pt := range s.pubTables {
		if slices.Contains(tables, pt.Schema+"."+pt.Table) {
			out = append(out, pt)
		} else if pt.InPublication {
			other++
		}
	}
	return out, other, nil
}

// A FOR ALL TABLES publication: both pipelines' tables are members.
func twoPipelinePublication() []postgres.PublicationTable {
	return []postgres.PublicationTable{
		{Schema: "public", Table: "orders", InPublication: true, Selectable: true},
		{Schema: "public", Table: foreignMark + "_payments", InPublication: true, Selectable: true},
		{Schema: "hr", Table: foreignMark + "_salaries", InPublication: true, Selectable: false},
	}
}

func TestPublicationTablesScopedCountOtherMembers(t *testing.T) {
	const countLine = "2 other tables in publication dbz_publication not shown (not part of this room's pipeline)"
	check := func(t *testing.T, r tools.Result) {
		t.Helper()
		assertNoForeign(t, r)
		if !strings.Contains(r.Summary, "public.orders: in_publication=yes, selectable=yes") || !strings.Contains(r.Summary, countLine) {
			t.Fatalf("summary = %q", r.Summary)
		}
	}
	t.Run("SQL filter", func(t *testing.T) {
		src := &pubSQLSource{fakeSource: &fakeSource{pubTables: twoPipelinePublication()}}
		r, err := scopedTool(t, metaDeps(src), "pg_publication_tables").Run(roomCtx(), json.RawMessage(`{"pipeline_id":"orders"}`))
		if err != nil {
			t.Fatal(err)
		}
		check(t, r)
		if len(src.asked) != 1 || !slices.Equal(src.asked[0], []string{"public.orders"}) || src.listAll != 0 {
			t.Fatalf("asked = %v, full reads = %d", src.asked, src.listAll)
		}
	})
	t.Run("in-memory filter", func(t *testing.T) {
		src := &fakeSource{pubTables: twoPipelinePublication()}
		r, err := scopedTool(t, metaDeps(src), "pg_publication_tables").Run(roomCtx(), json.RawMessage(`{"pipeline_id":"orders"}`))
		if err != nil {
			t.Fatal(err)
		}
		check(t, r)
	})
	t.Run("opt-out has no count line", func(t *testing.T) {
		src := &fakeSource{pubTables: twoPipelinePublication()}
		r, err := scopedTool(t, metaDeps(src), "pg_publication_tables").Run(tools.WithUnscoped(context.Background()), json.RawMessage(`{"pipeline_id":"orders"}`))
		if err != nil {
			t.Fatal(err)
		}
		assertNoForeign(t, r)
		if strings.Contains(r.Summary, "not shown") {
			t.Fatalf("unscoped summary changed: %q", r.Summary)
		}
	})
}

// Q5 T6 (behaviour changed from fail-closed): with Connect unreachable the
// tool falls back to the pipeline's own data (the default publication name,
// the pipeline's own source tables), says role checks need the connector
// config and shows nothing of other pipelines.
func TestPublicationTablesScopedConnectDownFallsBack(t *testing.T) {
	src := &pubSQLSource{fakeSource: &fakeSource{pubTables: twoPipelinePublication()}}
	d := metaDeps(src)
	d.Connect = &fakeConnect{configErr: errors.New("connect: connection refused")}
	r, err := scopedTool(t, d, "pg_publication_tables").Run(roomCtx(), json.RawMessage(`{"pipeline_id":"orders"}`))
	if err != nil {
		t.Fatal(err)
	}
	assertNoForeign(t, r)
	for _, want := range []string{"Connect unreachable: role checks need the connector config", "publication dbz_publication", "public.orders: in_publication=yes"} {
		if !strings.Contains(r.Summary, want) {
			t.Errorf("summary lacks %q:\n%s", want, r.Summary)
		}
	}
	for _, bad := range []string{"selectable=", "PROBLEM", "NOTE"} {
		if strings.Contains(r.Summary, bad) {
			t.Errorf("summary states %q without the role:\n%s", bad, r.Summary)
		}
	}
	if len(src.asked) != 1 || src.listAll != 0 {
		t.Fatalf("source reads: asked %v, full %d; want one filtered read", src.asked, src.listAll)
	}
}
