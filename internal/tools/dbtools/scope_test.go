package dbtools

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/clients/postgres"
	"piperoom/internal/events"
	"piperoom/internal/tools"
	"piperoom/internal/topology"
)

// Tests for finding F11: the db read tools only see the room's own pipeline.

// counted records every client call so a test can assert that a refused tool
// call reached no client.
type counted struct {
	mu    sync.Mutex
	calls []string
}

func (c *counted) add(s string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls = append(c.calls, s)
}

func (c *counted) n() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.calls)
}

type cSource struct {
	*fakeSource
	c *counted
}

func (s *cSource) ReplicationSlots(ctx context.Context) ([]clients.ReplicationSlot, error) {
	s.c.add("src.slots")
	return s.fakeSource.ReplicationSlots(ctx)
}
func (s *cSource) RecentDDL(ctx context.Context, since time.Time, limit int) ([]clients.DDLChange, error) {
	s.c.add("src.ddl")
	now := time.Now()
	return []clients.DDLChange{
		{At: now, CommandTag: "ALTER TABLE", ObjectIdentity: "public.payments", Query: "ALTER TABLE payments"},
		{At: now.Add(-time.Minute), CommandTag: "ALTER TABLE", ObjectIdentity: "public.orders", Query: "ALTER TABLE orders"},
		{At: now.Add(-2 * time.Minute), CommandTag: "ALTER TABLE", ObjectIdentity: "public.hr_salaries", Query: "ALTER TABLE hr_salaries"},
	}, nil
}
func (s *cSource) LongTransactions(ctx context.Context, d time.Duration) ([]clients.Transaction, error) {
	s.c.add("src.txns")
	return s.fakeSource.LongTransactions(ctx, d)
}
func (s *cSource) LongTransactionsOnTables(ctx context.Context, d time.Duration, tables []string) (postgres.TableTransactions, error) {
	s.c.add("src.txns.tables:" + strings.Join(tables, ","))
	return s.fakeSource.LongTransactionsOnTables(ctx, d, tables)
}
func (s *cSource) Row(ctx context.Context, table, pk, key string) (map[string]any, bool, error) {
	s.c.add("src.row:" + table)
	return s.fakeSource.Row(ctx, table, pk, key)
}
func (s *cSource) PublicationTables(ctx context.Context, p, r string, t []string) ([]postgres.PublicationTable, error) {
	s.c.add("src.pub")
	return s.fakeSource.PublicationTables(ctx, p, r, t)
}
func (s *cSource) Settings(ctx context.Context) ([]postgres.DBSetting, error) {
	s.c.add("src.settings")
	return s.fakeSource.Settings(ctx)
}
func (s *cSource) TableReplication(ctx context.Context, tables []string) ([]postgres.TableReplication, error) {
	s.c.add("src.repl")
	return s.fakeSource.TableReplication(ctx, tables)
}
func (s *cSource) RoleInfo(ctx context.Context, role string, tables []string) (postgres.RoleInfo, error) {
	s.c.add("src.role")
	return s.fakeSource.RoleInfo(ctx, role, tables)
}
func (s *cSource) Blockers(ctx context.Context, olderThan time.Duration) ([]postgres.DBSession, error) {
	s.c.add("src.blockers")
	return s.fakeSource.Blockers(ctx, olderThan)
}

// cSourceDDL also has the SQL table filter (tableDDLFilterer).
type cSourceDDL struct{ *cSource }

func (s *cSourceDDL) RecentDDLTable(_ context.Context, _ time.Time, table string, _ int) ([]clients.DDLChange, error) {
	s.c.add("src.ddltable:" + table)
	return []clients.DDLChange{{At: time.Now(), CommandTag: "ALTER TABLE", ObjectIdentity: table, Query: "ALTER TABLE " + table}}, nil
}

type cWarehouse struct {
	*fakeWarehouse
	c *counted
}

func (w *cWarehouse) Row(ctx context.Context, table, pk, key string) (map[string]any, bool, error) {
	w.c.add("wh.row:" + table)
	return w.fakeWarehouse.Row(ctx, table, pk, key)
}
func (w *cWarehouse) Freshness(ctx context.Context, table, col string) (time.Time, error) {
	w.c.add("wh.fresh:" + table)
	return w.fakeWarehouse.Freshness(ctx, table, col)
}

type cBuilder struct {
	*fakeBuilder
	c *counted
}

func (b *cBuilder) Build(ctx context.Context, id string) (topology.Graph, error) {
	b.c.add("builder.build:" + id)
	return b.fakeBuilder.Build(ctx, id)
}
func (b *cBuilder) Trace(ctx context.Context, id, table, key string) ([]events.TraceStage, error) {
	b.c.add("builder.trace:" + id)
	return b.fakeBuilder.Trace(ctx, id, table, key)
}

type cConnect struct {
	*fakeConnect
	c *counted
}

func (f *cConnect) Config(ctx context.Context, name string) (map[string]string, error) {
	f.c.add("connect.config:" + name)
	return f.fakeConnect.Config(ctx, name)
}

// scopedDeps wraps the standard test fakes with call counters. When ddlFilter
// is true the source also implements the SQL table filter.
func scopedDeps(ddlFilter bool) (Deps, *counted) {
	d := testDeps()
	c := &counted{}
	src := &cSource{fakeSource: d.Src.(*fakeSource), c: c}
	if ddlFilter {
		d.Src = &cSourceDDL{cSource: src}
	} else {
		d.Src = src
	}
	d.Wh = &cWarehouse{fakeWarehouse: d.Wh.(*fakeWarehouse), c: c}
	d.Builder = &cBuilder{fakeBuilder: d.Builder.(*fakeBuilder), c: c}
	d.Connect = &cConnect{fakeConnect: d.Connect.(*fakeConnect), c: c}
	return d, c
}

func scopedTool(t *testing.T, d Deps, name string) tools.Tool {
	t.Helper()
	reg := tools.NewRegistry()
	if err := Register(reg, d); err != nil {
		t.Fatal(err)
	}
	tl, ok := reg.Get(name)
	if !ok {
		t.Fatalf("%s not registered", name)
	}
	return tl
}

func roomCtx() context.Context { return tools.WithPipelineID(context.Background(), "orders") }

// One in-scope and one out-of-scope input per tool.
var scopeCases = []struct {
	tool string
	in   string
	out  string
}{
	{"pipeline_map", `{"pipeline_id":"orders"}`, `{"pipeline_id":"payments"}`},
	{"trace_record", `{"pipeline_id":"orders","table":"public.orders","key":"7"}`, `{"pipeline_id":"payments","table":"public.payments","key":"7"}`},
	{"pg_publication_tables", `{"pipeline_id":"orders"}`, `{"pipeline_id":"payments"}`},
	{"source_table_replication", `{"pipeline_id":"orders"}`, `{"pipeline_id":"payments"}`},
	{"source_role_grants", `{"pipeline_id":"orders"}`, `{"pipeline_id":"payments"}`},
	{"pg_row", `{"pipeline_id":"orders","table":"public.orders","pk_column":"id","key":"7"}`, `{"pipeline_id":"payments","table":"public.payments","pk_column":"id","key":"7"}`},
	{"warehouse_row", `{"pipeline_id":"orders","table":"analytics.orders","pk_column":"id","key":"7"}`, `{"pipeline_id":"payments","table":"analytics.payments","pk_column":"id","key":"7"}`},
	{"warehouse_freshness", `{"table":"analytics.orders","ts_column":"updated_at"}`, `{"table":"analytics.payments","ts_column":"updated_at"}`},
	{"pg_recent_ddl", `{"since":"2026-10-11T00:00:00Z","table":"public.orders"}`, `{"since":"2026-10-11T00:00:00Z","table":"public.payments"}`},
}

func TestScopeInScopeWorks(t *testing.T) {
	for _, tc := range scopeCases {
		t.Run(tc.tool, func(t *testing.T) {
			d, c := scopedDeps(false)
			if _, err := scopedTool(t, d, tc.tool).Run(roomCtx(), json.RawMessage(tc.in)); err != nil {
				t.Fatalf("in-scope call failed: %v", err)
			}
			if c.n() == 0 {
				t.Fatal("no client was called")
			}
		})
	}
}

func TestScopeOutOfScopeRefusedWithoutClientCall(t *testing.T) {
	for _, tc := range scopeCases {
		t.Run(tc.tool, func(t *testing.T) {
			d, c := scopedDeps(false)
			res, err := scopedTool(t, d, tc.tool).Run(roomCtx(), json.RawMessage(tc.out))
			if !errors.Is(err, tools.ErrOutOfScope) {
				t.Fatalf("err = %v, want ErrOutOfScope", err)
			}
			if !strings.Contains(err.Error(), `"orders"`) {
				t.Errorf("error does not name the room's pipeline: %v", err)
			}
			if c.n() != 0 {
				t.Fatalf("clients were called: %v", c.calls)
			}
			if res.Summary != "" || res.Data != nil {
				t.Fatalf("refusal returned data: %+v", res)
			}
		})
	}
}

// The room's own pipeline_id with a table of another pipeline is refused too.
func TestScopeOtherPipelineTableRefused(t *testing.T) {
	cases := []struct{ tool, in string }{
		{"pg_row", `{"pipeline_id":"orders","table":"public.payments","pk_column":"id","key":"7"}`},
		{"warehouse_row", `{"pipeline_id":"orders","table":"analytics.payments","pk_column":"id","key":"7"}`},
		{"trace_record", `{"pipeline_id":"orders","table":"public.payments","key":"7"}`},
	}
	for _, tc := range cases {
		t.Run(tc.tool, func(t *testing.T) {
			d, c := scopedDeps(false)
			_, err := scopedTool(t, d, tc.tool).Run(roomCtx(), json.RawMessage(tc.in))
			if err == nil {
				t.Fatal("want refusal")
			}
			if !errors.Is(err, tools.ErrOutOfScope) && !errors.Is(err, tools.ErrBadInput) {
				t.Fatalf("err = %v", err)
			}
			if c.n() != 0 {
				t.Fatalf("clients were called: %v", c.calls)
			}
		})
	}
}

// The model passes another pipeline's id (a prompt-injected steer); even a
// pipeline that exists in owners.yaml is refused, and the data call uses the
// room's pipeline id, never the input's.
func TestScopePipelineIDMismatchRefused(t *testing.T) {
	d, c := scopedDeps(false)
	for _, tool := range []string{"pipeline_map", "trace_record", "pg_publication_tables", "pg_row", "warehouse_row"} {
		in := `{"pipeline_id":"payments","table":"public.orders","pk_column":"id","key":"7"}`
		if tool == "warehouse_row" {
			in = `{"pipeline_id":"payments","table":"analytics.orders","pk_column":"id","key":"7"}`
		}
		_, err := scopedTool(t, d, tool).Run(roomCtx(), json.RawMessage(in))
		if !errors.Is(err, tools.ErrOutOfScope) || !strings.Contains(err.Error(), "pipeline_id") {
			t.Errorf("%s: err = %v, want a pipeline_id ErrOutOfScope", tool, err)
		}
	}
	if c.n() != 0 {
		t.Fatalf("clients were called: %v", c.calls)
	}
}

func TestScopeBuilderGetsTheRoomsPipelineID(t *testing.T) {
	d, c := scopedDeps(false)
	if _, err := scopedTool(t, d, "pipeline_map").Run(roomCtx(), json.RawMessage(`{"pipeline_id":"orders"}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := scopedTool(t, d, "trace_record").Run(roomCtx(), json.RawMessage(`{"pipeline_id":"orders","table":"public.orders","key":"7"}`)); err != nil {
		t.Fatal(err)
	}
	want := []string{"builder.build:orders", "builder.trace:orders"}
	if len(c.calls) != 2 || c.calls[0] != want[0] || c.calls[1] != want[1] {
		t.Fatalf("calls = %v, want %v", c.calls, want)
	}
}

func TestScopeNoRoomRefusedByDefault(t *testing.T) {
	all := append([]struct{ tool, in, out string }{
		{"pg_replication_slots", `{}`, ``},
		{"pg_long_transactions", `{}`, ``},
		{"pg_recent_ddl", `{"since":"2026-10-11T00:00:00Z"}`, ``},
		{"source_settings", `{}`, ``},
		{"source_blocking_sessions", `{}`, ``},
	}, scopeCases...)
	for _, tc := range all {
		t.Run(tc.tool, func(t *testing.T) {
			d, c := scopedDeps(false)
			_, err := scopedTool(t, d, tc.tool).Run(context.Background(), json.RawMessage(tc.in))
			if !errors.Is(err, tools.ErrNoScope) {
				t.Fatalf("err = %v, want ErrNoScope", err)
			}
			if c.n() != 0 {
				t.Fatalf("clients were called: %v", c.calls)
			}
		})
	}
}

func TestScopeOptOutKeepsOldBehaviour(t *testing.T) {
	ctx := tools.WithUnscoped(context.Background())
	for _, tc := range scopeCases {
		t.Run(tc.tool, func(t *testing.T) {
			d, _ := scopedDeps(false)
			tl := scopedTool(t, d, tc.tool)
			// The in-scope input works, and so does the one for another
			// pipeline: nothing is scoped when the caller opted out.
			for _, in := range []string{tc.in, tc.out} {
				_, err := tl.Run(ctx, json.RawMessage(in))
				if err != nil && !errors.Is(err, tools.ErrBadInput) {
					t.Fatalf("opted-out call %s failed: %v", in, err)
				}
				if errors.Is(err, tools.ErrOutOfScope) || errors.Is(err, tools.ErrNoScope) {
					t.Fatalf("scope error with opt-out: %v", err)
				}
			}
		})
	}
	for _, name := range []string{"pg_replication_slots", "pg_long_transactions"} {
		d, _ := scopedDeps(false)
		if _, err := scopedTool(t, d, name).Run(ctx, json.RawMessage(`{}`)); err != nil {
			t.Fatalf("%s with opt-out: %v", name, err)
		}
		if _, err := scopedTool(t, d, name).Run(roomCtx(), json.RawMessage(`{}`)); err != nil {
			t.Fatalf("%s with a room: %v", name, err)
		}
	}
}

func TestScopeRoomPipelineWinsOverOptOut(t *testing.T) {
	ctx := tools.WithUnscoped(roomCtx())
	d, c := scopedDeps(false)
	_, err := scopedTool(t, d, "pg_row").Run(ctx, json.RawMessage(`{"pipeline_id":"payments","table":"public.payments","pk_column":"id","key":"7"}`))
	if !errors.Is(err, tools.ErrOutOfScope) || c.n() != 0 {
		t.Fatalf("err = %v, calls = %v", err, c.calls)
	}
}

func TestScopeUnknownRoomPipelineRefused(t *testing.T) {
	d, c := scopedDeps(false)
	ctx := tools.WithPipelineID(context.Background(), "no-such-pipeline")
	_, err := scopedTool(t, d, "pg_row").Run(ctx, json.RawMessage(`{"pipeline_id":"no-such-pipeline","table":"public.orders","pk_column":"id","key":"7"}`))
	if !errors.Is(err, tools.ErrOutOfScope) || c.n() != 0 {
		t.Fatalf("err = %v, calls = %v", err, c.calls)
	}
}

func TestScopeRecentDDLIsFilteredToTheRoomsTables(t *testing.T) {
	t.Run("in-memory filter", func(t *testing.T) {
		d, _ := scopedDeps(false)
		r, err := scopedTool(t, d, "pg_recent_ddl").Run(roomCtx(), json.RawMessage(`{"since":"2026-10-11T00:00:00Z"}`))
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(r.Summary, "public.orders") || strings.Contains(r.Summary, "payments") || strings.Contains(r.Summary, "hr_salaries") {
			t.Fatalf("summary = %q", r.Summary)
		}
		if got := r.Data.([]clients.DDLChange); len(got) != 1 {
			t.Fatalf("data has %d changes, want 1", len(got))
		}
	})
	t.Run("SQL table filter", func(t *testing.T) {
		d, c := scopedDeps(true)
		r, err := scopedTool(t, d, "pg_recent_ddl").Run(roomCtx(), json.RawMessage(`{"since":"2026-10-11T00:00:00Z"}`))
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(r.Summary, "payments") || len(c.calls) != 1 || c.calls[0] != "src.ddltable:public.orders" {
			t.Fatalf("summary = %q, calls = %v", r.Summary, c.calls)
		}
	})
	t.Run("opt-out is unfiltered", func(t *testing.T) {
		d, _ := scopedDeps(false)
		r, err := scopedTool(t, d, "pg_recent_ddl").Run(tools.WithUnscoped(context.Background()), json.RawMessage(`{"since":"2026-10-11T00:00:00Z"}`))
		if err != nil || !strings.Contains(r.Summary, "hr_salaries") {
			t.Fatalf("summary = %q, err = %v", r.Summary, err)
		}
	})
}

func TestScopeFullPipelineInContext(t *testing.T) {
	d, _ := scopedDeps(false)
	p, _ := testOwners().Pipeline("orders")
	ctx := tools.WithPipeline(context.Background(), p)
	got, ok := tools.PipelineFrom(ctx)
	if !ok || got.ID != "orders" {
		t.Fatalf("PipelineFrom = %v, %v", got, ok)
	}
	if _, err := scopedTool(t, d, "pg_row").Run(ctx, json.RawMessage(`{"pipeline_id":"orders","table":"public.orders","pk_column":"id","key":"7"}`)); err != nil {
		t.Fatal(err)
	}
}
