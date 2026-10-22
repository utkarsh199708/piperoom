package dbtools

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
)

// toastSource is a SourceDB with the ToastColumns catalog read.
type toastSource struct {
	fakeSource
	facts  map[string]clients.ToastTable
	err    error
	calls  int
	tables []string
	scoped bool // the call's ctx carried the full pipeline
}

func (s *toastSource) ToastColumns(ctx context.Context, tables []string) ([]clients.ToastTable, bool, error) {
	s.calls++
	s.tables = tables
	_, s.scoped = tools.PipelineFrom(ctx)
	if s.err != nil {
		return nil, false, s.err
	}
	out := make([]clients.ToastTable, 0, len(tables))
	for _, t := range tables {
		f, ok := s.facts[t]
		if !ok {
			f = clients.ToastTable{Table: t}
		}
		out = append(out, f)
	}
	return out, false, nil
}

// countingWarehouse is a warehouse with CountPlaceholder.
type countingWarehouse struct {
	fakeWarehouse
	n     int64
	err   error
	calls int
	got   []string // table|column|placeholder|ts per call
}

func (w *countingWarehouse) CountPlaceholder(_ context.Context, table, column, placeholder, ts string, since, until time.Time) (int64, error) {
	w.calls++
	w.got = append(w.got, strings.Join([]string{table, column, placeholder, ts}, "|"))
	if until.Sub(since) != 24*time.Hour {
		return 0, fmt.Errorf("window %s", until.Sub(since))
	}
	return w.n, w.err
}

func ordersToast(ident string, avg int, toast int64) map[string]clients.ToastTable {
	return map[string]clients.ToastTable{"public.orders": {
		Table: "public.orders", Found: true, ReplicaIdentity: ident, ToastBytes: toast,
		Columns: []clients.ToastColumn{
			{Name: "notes", Type: "text", Storage: "x", AvgWidth: avg},
			{Name: "status", Type: "text", Storage: "x", AvgWidth: 7},
		},
	}}
}

func toastTool(t *testing.T, d Deps) tools.Tool {
	t.Helper()
	reg := tools.NewReadOnlyRegistry()
	if err := Register(reg, d); err != nil {
		t.Fatal(err)
	}
	tt, ok := reg.Get("pg_toast_risk")
	if !ok {
		t.Fatal("pg_toast_risk not registered")
	}
	return tt
}

func toastRoom(t *testing.T, id string) context.Context {
	t.Helper()
	p, ok := testOwners().Pipeline(id)
	if !ok {
		t.Fatalf("no pipeline %s", id)
	}
	return tools.WithPipeline(context.Background(), p)
}

// TC1: a 4 KB text column on a DEFAULT table is one risk row; FULL or a
// 500-byte column is none.
func TestToastRiskCatalogRule(t *testing.T) {
	for _, c := range []struct {
		name     string
		ident    string
		avg      int
		toast    int64
		want     int
		evidence string
	}{
		{"4 KB column, DEFAULT", "DEFAULT", 4096, 0, 1, toastWide},
		{"4 KB column, FULL", "FULL", 4096, 0, 0, ""},
		{"500-byte column", "DEFAULT", 500, 0, 0, ""},
		{"out-of-line values, DEFAULT", "DEFAULT", 18, 5 << 20, 1, toastOutOfLine},
		{"out-of-line values, INDEX", "INDEX", -1, 8192, 1, toastOutOfLine},
		{"out-of-line values, FULL", "FULL", 18, 5 << 20, 0, ""},
	} {
		t.Run(c.name, func(t *testing.T) {
			d := testDeps()
			d.Src = &toastSource{facts: ordersToast(c.ident, c.avg, c.toast)}
			res, err := toastTool(t, d).Run(toastRoom(t, "orders"), json.RawMessage(`{"pipeline_id":"orders"}`))
			if err != nil {
				t.Fatal(err)
			}
			data := res.Data.(ToastRiskData)
			if len(data.Risks) != c.want {
				t.Fatalf("risks = %+v, want %d\n%s", data.Risks, c.want, res.Summary)
			}
			if c.want == 0 {
				if !strings.Contains(res.Summary, "no TOAST truncation risk found in 1 source table") {
					t.Errorf("summary: %s", res.Summary)
				}
				return
			}
			r := data.Risks[0]
			if r.Table != "public.orders" || r.Column != "notes" || r.Evidence != c.evidence || r.ReplicaIdentity != c.ident {
				t.Errorf("risk = %+v", r)
			}
			// The catalog cannot tell which column's values are out of line,
			// so that evidence is worded as a possibility.
			want := "public.orders.notes (text): TOASTed (average width"
			if c.evidence == toastOutOfLine {
				want = "public.orders.notes (text): may be TOASTed (the table stores out-of-line values"
			}
			if !strings.Contains(res.Summary, want) || !strings.Contains(res.Summary, "replica identity "+c.ident) {
				t.Errorf("summary lacks %q: %s", want, res.Summary)
			}
		})
	}
}

// TC2: the placeholder count is in the risk line; ErrNotSupported, or a
// warehouse that cannot count, is the fixed note.
func TestToastRiskPlaceholderCount(t *testing.T) {
	d := testDeps()
	d.Src = &toastSource{facts: ordersToast("DEFAULT", 18, 1<<20)}
	wh := &countingWarehouse{n: 14}
	d.Wh = wh
	d.Connect.(*fakeConnect).configs["pg-orders-connector"]["unavailable.value.placeholder"] = "__gone"
	res, err := toastTool(t, d).Run(toastRoom(t, "orders"), json.RawMessage(`{"pipeline_id":"orders"}`))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(res.Summary, "public.orders.notes (text): may be TOASTed (the table stores out-of-line values, TOAST relation 1.0 MiB), replica identity DEFAULT, placeholder seen 14 times in sink analytics.orders last 24 h") {
		t.Errorf("summary: %s", res.Summary)
	}
	if !strings.Contains(res.Summary, `placeholder "__gone", connector pg-orders-connector config unavailable.value.placeholder`) {
		t.Errorf("summary lacks the configured placeholder: %s", res.Summary)
	}
	if len(wh.got) != 1 || wh.got[0] != "analytics.orders|notes|__gone|updated_at" {
		t.Errorf("count calls = %v", wh.got)
	}
	data := res.Data.(ToastRiskData)
	if data.Risks[0].PlaceholderCount == nil || *data.Risks[0].PlaceholderCount != 14 || data.Since == nil {
		t.Errorf("data = %+v", data)
	}

	for name, wh := range map[string]clients.Warehouse{
		"ErrNotSupported": &countingWarehouse{err: fmt.Errorf("snowflake: %w", clients.ErrNotSupported)},
		"no counter":      &fakeWarehouse{},
	} {
		t.Run(name, func(t *testing.T) {
			d := testDeps()
			d.Src = &toastSource{facts: ordersToast("DEFAULT", 4096, 0)}
			d.Wh = wh
			res, err := toastTool(t, d).Run(toastRoom(t, "orders"), json.RawMessage(`{"pipeline_id":"orders"}`))
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(res.Summary, "note: placeholder count not available on this warehouse") {
				t.Errorf("summary: %s", res.Summary)
			}
			if strings.Contains(res.Summary, "placeholder seen") {
				t.Errorf("a count was claimed: %s", res.Summary)
			}
			if r := res.Data.(ToastRiskData).Risks[0]; r.PlaceholderCount != nil {
				t.Errorf("count = %d", *r.PlaceholderCount)
			}
		})
	}
}

// E5: a count that fails is a note on its row; the catalog fact stands.
func TestToastRiskPlaceholderCountErrors(t *testing.T) {
	for _, c := range []struct {
		err  error
		want string
	}{
		{fmt.Errorf("warehouse: %w", clients.ErrNotFound), "column or table not in the sink"},
		{fmt.Errorf("warehouse: %w", context.DeadlineExceeded), "placeholder count timed out (2s)"},
		{errors.New("warehouse: connection refused"), "placeholder count failed: warehouse: connection refused"},
	} {
		d := testDeps()
		d.Src = &toastSource{facts: ordersToast("DEFAULT", 4096, 0)}
		d.Wh = &countingWarehouse{err: c.err}
		res, err := toastTool(t, d).Run(toastRoom(t, "orders"), json.RawMessage(`{"pipeline_id":"orders"}`))
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(res.Summary, "replica identity DEFAULT, "+c.want) {
			t.Errorf("%v: summary: %s", c.err, res.Summary)
		}
	}

	// No Connect client: the default placeholder, said so.
	d := testDeps()
	d.Src = &toastSource{facts: ordersToast("DEFAULT", 4096, 0)}
	d.Connect = nil
	wh := &countingWarehouse{n: 0}
	d.Wh = wh
	res, err := toastTool(t, d).Run(toastRoom(t, "orders"), json.RawMessage(`{"pipeline_id":"orders"}`))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(res.Summary, "assuming the Debezium default placeholder") || wh.got[0] != "analytics.orders|notes|__debezium_unavailable_value|updated_at" {
		t.Errorf("summary: %s calls %v", res.Summary, wh.got)
	}
}

// TC3 and E2/E3: an out-of-scope table, another pipeline, bad input and a
// call without a room are refused before any client call.
func TestToastRiskRefusedBeforeIO(t *testing.T) {
	src := &toastSource{facts: ordersToast("DEFAULT", 4096, 0)}
	wh := &countingWarehouse{}
	d := testDeps()
	d.Src, d.Wh = src, wh
	tt := toastTool(t, d)
	for _, c := range []struct {
		name string
		ctx  context.Context
		in   string
		want error
	}{
		{"other pipeline's table", toastRoom(t, "orders"), `{"pipeline_id":"orders","table":"public.payments"}`, tools.ErrOutOfScope},
		{"other pipeline", toastRoom(t, "orders"), `{"pipeline_id":"payments"}`, tools.ErrOutOfScope},
		{"unscoped, table not in pipeline", tools.WithUnscoped(context.Background()), `{"pipeline_id":"orders","table":"public.payments"}`, tools.ErrBadInput},
		{"unknown pipeline", tools.WithUnscoped(context.Background()), `{"pipeline_id":"nope"}`, tools.ErrBadInput},
		{"bad table", toastRoom(t, "orders"), `{"pipeline_id":"orders","table":"public.orders; drop"}`, tools.ErrBadInput},
		{"no pipeline_id", toastRoom(t, "orders"), `{}`, tools.ErrBadInput},
		{"no room", context.Background(), `{"pipeline_id":"orders"}`, tools.ErrNoScope},
	} {
		t.Run(c.name, func(t *testing.T) {
			before := src.calls
			_, err := tt.Run(c.ctx, json.RawMessage(c.in))
			if !errors.Is(err, c.want) {
				t.Fatalf("err = %v, want %v", err, c.want)
			}
			if src.calls != before || wh.calls != 0 {
				t.Fatalf("a client was called: source %d, warehouse %d", src.calls-before, wh.calls)
			}
		})
	}
}

// The client gets the resolved pipeline (it checks scope itself and has no
// owners config), and one table when the input names one.
func TestToastRiskPassesPipelineToClient(t *testing.T) {
	src := &toastSource{facts: ordersToast("DEFAULT", 4096, 0)}
	d := testDeps()
	d.Src = src
	ctx := tools.WithPipelineID(context.Background(), "orders")
	if _, err := toastTool(t, d).Run(ctx, json.RawMessage(`{"pipeline_id":"orders","table":"public.orders"}`)); err != nil {
		t.Fatal(err)
	}
	if !src.scoped || len(src.tables) != 1 || src.tables[0] != "public.orders" {
		t.Errorf("scoped=%v tables=%v", src.scoped, src.tables)
	}
}

// E5: an unsupported source client and a catalog error are errors.
func TestToastRiskSourceErrors(t *testing.T) {
	d := testDeps() // fakeSource has no ToastColumns
	if _, err := toastTool(t, d).Run(toastRoom(t, "orders"), json.RawMessage(`{"pipeline_id":"orders"}`)); err == nil || !strings.Contains(err.Error(), "does not support ToastColumns") {
		t.Errorf("err = %v", err)
	}
	cause := errors.New("connection refused")
	d.Src = &toastSource{err: cause}
	_, err := toastTool(t, d).Run(toastRoom(t, "orders"), json.RawMessage(`{"pipeline_id":"orders"}`))
	if !errors.Is(err, cause) || !strings.HasPrefix(err.Error(), "dbtools: pg_toast_risk: ") {
		t.Errorf("err = %v", err)
	}
}

// TC4: 200 risky tables stay within MaxSummaryBytes, marked truncated; at
// most 10 sink counts run.
func TestToastRiskSummaryCap(t *testing.T) {
	var tables []string
	facts := map[string]clients.ToastTable{}
	for i := range 200 {
		name := fmt.Sprintf("public.events_%03d", i)
		tables = append(tables, name)
		facts[name] = clients.ToastTable{Table: name, Found: true, ReplicaIdentity: "DEFAULT", ToastBytes: 1 << 20,
			Columns: []clients.ToastColumn{{Name: "payload_document", Type: "jsonb", Storage: "x", AvgWidth: 18}}}
	}
	p := owners.Pipeline{ID: "events", SourceTables: tables, SinkTables: []string{"analytics.events"}, SinkTSColumn: "updated_at"}
	d := testDeps()
	d.Owners = &owners.Owners{Pipelines: []owners.Pipeline{p}}
	d.Src = &toastSource{facts: facts}
	wh := &countingWarehouse{n: 3}
	d.Wh = wh
	res, err := toastTool(t, d).Run(tools.WithPipeline(context.Background(), p), json.RawMessage(`{"pipeline_id":"events"}`))
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Summary) > tools.MaxSummaryBytes || !res.Truncated {
		t.Fatalf("summary %d bytes, truncated %v", len(res.Summary), res.Truncated)
	}
	data := res.Data.(ToastRiskData)
	if len(data.Risks) != 200 || len(data.Tables) != 200 || wh.calls != maxPlaceholderCounts {
		t.Fatalf("risks %d tables %d counts %d", len(data.Risks), len(data.Tables), wh.calls)
	}
	if !strings.Contains(strings.Join(data.Notes, "\n"), "placeholder counted for the first 10 columns only") {
		t.Errorf("notes = %v", data.Notes)
	}
}

// Missing tables and FULL tables are named apart from the risks.
func TestToastRiskSummaryGroups(t *testing.T) {
	p := owners.Pipeline{ID: "mix", SourceTables: []string{"public.a", "public.b", "public.c", "public.d"}, SinkTSColumn: "updated_at"}
	d := testDeps()
	d.Wh = &countingWarehouse{}
	d.Owners = &owners.Owners{Pipelines: []owners.Pipeline{p}}
	d.Src = &toastSource{facts: map[string]clients.ToastTable{
		"public.a": {Table: "public.a", Found: true, ReplicaIdentity: "DEFAULT", Columns: []clients.ToastColumn{{Name: "body", Type: "text", Storage: "e", AvgWidth: 5000}}},
		"public.b": {Table: "public.b", Found: true, ReplicaIdentity: "FULL", ToastBytes: 1 << 20, Columns: []clients.ToastColumn{{Name: "body", Type: "text", Storage: "x", AvgWidth: 18}}},
		"public.c": {Table: "public.c", Found: true, ReplicaIdentity: "DEFAULT"},
	}}
	res, err := toastTool(t, d).Run(tools.WithPipeline(context.Background(), p), json.RawMessage(`{"pipeline_id":"mix"}`))
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"1 column at risk in 4 source tables",
		"public.a.body (text): TOASTed (average width 5000 B, above 2 KB), replica identity DEFAULT, pipeline has no sink_tables",
		"replica identity FULL (updates carry every column): public.b",
		"no out-of-line or wide toastable column: public.c",
		"not found in the source catalog: public.d",
	} {
		if !strings.Contains(res.Summary, want) {
			t.Errorf("summary lacks %q:\n%s", want, res.Summary)
		}
	}
}

// TC6: the tool is a read tool and registers in a read-only registry.
func TestToastRiskIsReadOnly(t *testing.T) {
	tt := toastTool(t, testDeps())
	if tt.Kind() != tools.KindRead {
		t.Fatalf("kind = %s", tt.Kind())
	}
}
