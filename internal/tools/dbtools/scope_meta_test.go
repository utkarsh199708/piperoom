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
	want := "slot orders_slot: not found in pg_replication_slots\n1 other replication slots not shown (not part of this room's pipeline)\n"
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

// Three long transactions: one locks the room's table, one another
// pipeline's table (a writer), one no table at all. Only the first is shown;
// the others are counted, with the writer count, and named nowhere.
func TestLongTransactionsScopedShowOnlyThePipelinesSessions(t *testing.T) {
	src := &txnSource{fakeSource: &fakeSource{}, txns: []clients.Transaction{
		{PID: 11, Username: "orders_app", ApplicationName: "orders-api", State: "idle in transaction", Age: 20 * time.Minute, Query: "UPDATE", HasXID: true},
		{PID: 12, Username: foreignMark + "_billing", ApplicationName: foreignMark + "-api", State: "idle in transaction", Age: 30 * time.Minute, Query: "UPDATE", HasXID: true},
		{PID: 13, Username: foreignMark + "_etl", ApplicationName: foreignMark + "-etl", State: "active", Age: 9 * time.Minute, Query: "SELECT"},
	}, locks: map[int][]string{
		11: {"public.orders"},
		12: {"public.payments"},
	}}
	tl := scopedTool(t, metaDeps(src), "pg_long_transactions")

	r, err := tl.Run(roomCtx(), json.RawMessage(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	assertNoForeign(t, r)
	if !strings.Contains(r.Summary, "pid=11 user=orders_app") || strings.Contains(r.Summary, "pid=12") || strings.Contains(r.Summary, "pid=13") {
		t.Fatalf("summary = %q", r.Summary)
	}
	if !strings.Contains(r.Summary, "2 other long transactions not shown (not part of this room's pipeline); 1 of them are writers") {
		t.Fatalf("summary misses the count-only line: %q", r.Summary)
	}
	if rows := r.Data.([]longTxn); len(rows) != 1 || rows[0].PID != 11 {
		t.Fatalf("data = %+v, want only pid 11", rows)
	}

	// The opt-out keeps the old, unfiltered list.
	r, err = tl.Run(tools.WithUnscoped(context.Background()), json.RawMessage(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(r.Summary, "pid=12") || !strings.Contains(r.Summary, "pid=13") || strings.Contains(r.Summary, "not shown") {
		t.Fatalf("unscoped summary = %q", r.Summary)
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
	want := "(no long transactions on this pipeline's tables)\n1 other long transactions not shown (not part of this room's pipeline); 0 of them are writers (has_xid) and can hold back the WAL a slot keeps\n"
	if r.Summary != want {
		t.Fatalf("summary = %q, want %q", r.Summary, want)
	}
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

// The publication name comes from the connector config: unreadable means an
// error and no source read at all.
func TestPublicationTablesScopedFailClosed(t *testing.T) {
	src := &pubSQLSource{fakeSource: &fakeSource{pubTables: twoPipelinePublication()}}
	d := metaDeps(src)
	down := errors.New("connect: connection refused")
	d.Connect = &fakeConnect{configErr: down}
	r, err := scopedTool(t, d, "pg_publication_tables").Run(roomCtx(), json.RawMessage(`{"pipeline_id":"orders"}`))
	if !errors.Is(err, down) || r.Summary != "" || r.Data != nil {
		t.Fatalf("r = %+v, err = %v; want the Connect error and no data", r, err)
	}
	if len(src.asked) != 0 || src.listAll != 0 {
		t.Fatalf("source read without the publication name: asked %v, full %d", src.asked, src.listAll)
	}
}
