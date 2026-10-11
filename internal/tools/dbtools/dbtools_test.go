package dbtools

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/events"
	"piperoom/internal/tools"
	"piperoom/internal/topology"
)

type fakeSource struct {
	slots []clients.ReplicationSlot
	rows  map[string]map[string]any
}

func (f *fakeSource) ReplicationSlots(context.Context) ([]clients.ReplicationSlot, error) {
	return f.slots, nil
}
func (f *fakeSource) RecentDDL(context.Context, time.Time, int) ([]clients.DDLChange, error) {
	return []clients.DDLChange{{At: time.Now(), CommandTag: "ALTER TABLE", ObjectIdentity: "public.orders", Query: "ALTER TABLE orders", Username: "priya"}}, nil
}
func (f *fakeSource) LongTransactions(context.Context, time.Duration) ([]clients.Transaction, error) {
	return []clients.Transaction{{PID: 1, Username: "u", State: "active", Age: time.Minute, Query: "SELECT 1"}}, nil
}
func (f *fakeSource) Row(_ context.Context, table, _, key string) (map[string]any, bool, error) {
	row, ok := f.rows[table+"|"+key]
	return row, ok, nil
}

type fakeWarehouse struct {
	rows  map[string]map[string]any
	fresh time.Time
}

func (f *fakeWarehouse) Row(_ context.Context, table, _, key string) (map[string]any, bool, error) {
	row, ok := f.rows[table+"|"+key]
	return row, ok, nil
}
func (f *fakeWarehouse) Freshness(context.Context, string, string) (time.Time, error) {
	return f.fresh, nil
}

type fakeBuilder struct {
	graph    topology.Graph
	buildErr error
	stages   []events.TraceStage
	traceErr error
}

func (f *fakeBuilder) Build(context.Context, string) (topology.Graph, error) {
	return f.graph, f.buildErr
}
func (f *fakeBuilder) Render(g topology.Graph) string {
	return "pipeline " + g.PipelineID + "\n"
}
func (f *fakeBuilder) Trace(context.Context, string, string, string) ([]events.TraceStage, error) {
	return f.stages, f.traceErr
}
func (f *fakeBuilder) LayerOwner(string, string) (string, bool) { return "", false }

func testDeps() Deps {
	return Deps{
		Src: &fakeSource{
			slots: []clients.ReplicationSlot{{Name: "pg-orders-connector", Active: true, RetainedWALBytes: 1 << 30, WALStatus: "reserved"}},
			rows: map[string]map[string]any{
				"public.orders|7": {"id": "7", "customer_id": "100", "amount": "50", "status": "placed", "updated_at": "2026-10-11T10:00:00Z"},
			},
		},
		Wh: &fakeWarehouse{
			rows:  map[string]map[string]any{"analytics.orders|7": {"id": "7", "updated_at": "2026-10-11T10:00:00Z", "loaded_at": "2026-10-11T10:00:01Z"}},
			fresh: time.Now().Add(-2 * time.Minute),
		},
		Builder: &fakeBuilder{graph: topology.Graph{PipelineID: "orders"}},
	}
}

func TestRegister(t *testing.T) {
	reg := tools.NewRegistry()
	if err := Register(reg, testDeps()); err != nil {
		t.Fatalf("Register: %v", err)
	}
	all := reg.All()
	if len(all) != 8 {
		t.Fatalf("registered %d tools, want 8", len(all))
	}
	want := []string{"pg_replication_slots", "pg_recent_ddl", "pg_long_transactions", "pg_row", "warehouse_freshness", "warehouse_row", "pipeline_map", "trace_record"}
	for i, name := range want {
		if all[i].Name() != name {
			t.Fatalf("tool %d = %s, want %s", i, all[i].Name(), name)
		}
		if all[i].Kind() != tools.KindRead {
			t.Fatalf("tool %s kind = %s, want read", name, all[i].Kind())
		}
	}
}

func runTool(t *testing.T, tt tools.Tool, input string) (tools.Result, error) {
	t.Helper()
	return tt.Run(context.Background(), json.RawMessage(input))
}

func TestValidationErrors(t *testing.T) {
	reg := tools.NewRegistry()
	if err := Register(reg, testDeps()); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name  string
		input string
	}{
		{"pg_row bad table", `{"table":"orders; drop","pk_column":"id","key":"7"}`},
		{"pg_row missing key", `{"table":"public.orders","pk_column":"id"}`},
		{"pg_recent_ddl bad since", `{"since":"not-a-time"}`},
		{"pg_long_transactions negative", `{"older_than_minutes":-1}`},
		{"warehouse_freshness bad ts_column", `{"table":"analytics.orders","ts_column":"1=1"}`},
		{"trace_record bad table", `{"pipeline_id":"orders","table":"x","key":"7"}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tt, ok := reg.Get(tc.name[:strings.Index(tc.name, " ")])
			if !ok {
				t.Fatalf("tool not found for %s", tc.name)
			}
			_, err := runTool(t, tt, tc.input)
			if err == nil {
				t.Fatalf("expected error for input %s", tc.input)
			}
			if !errors.Is(err, tools.ErrBadInput) {
				t.Fatalf("error %v does not wrap ErrBadInput", err)
			}
		})
	}
}

func TestPgRowSummaryRestrictsValues(t *testing.T) {
	reg := tools.NewRegistry()
	if err := Register(reg, testDeps()); err != nil {
		t.Fatal(err)
	}
	tt2, _ := reg.Get("pg_row")
	res, err := runTool(t, tt2, `{"table":"public.orders","pk_column":"id","key":"7"}`)
	if err != nil {
		t.Fatalf("pg_row: %v", err)
	}
	// Key value and timestamps appear; other values do not.
	if !strings.Contains(res.Summary, "id=7") {
		t.Fatalf("summary missing key value: %q", res.Summary)
	}
	if !strings.Contains(res.Summary, "customer_id") {
		t.Fatalf("summary missing column name: %q", res.Summary)
	}
	if strings.Contains(res.Summary, "customer_id=100") {
		t.Fatalf("summary leaked a non-key value: %q", res.Summary)
	}
}

func TestPgReplicationSlotsSummary(t *testing.T) {
	reg := tools.NewRegistry()
	if err := Register(reg, testDeps()); err != nil {
		t.Fatal(err)
	}
	tt, _ := reg.Get("pg_replication_slots")
	res, err := runTool(t, tt, `{}`)
	if err != nil {
		t.Fatalf("pg_replication_slots: %v", err)
	}
	if !strings.Contains(res.Summary, "pg-orders-connector") || !strings.Contains(res.Summary, "reserved") {
		t.Fatalf("summary: %q", res.Summary)
	}
}

func TestTraceSummaryNamesStoppedStage(t *testing.T) {
	stages := []events.TraceStage{
		{Layer: "postgres", Name: "public.orders", Status: "seen"},
		{Layer: "kafka-connect", Name: "pg-orders-connector", Status: "stopped_here"},
		{Layer: "kafka", Name: "pg.public.orders", Status: "not_seen"},
	}
	got := renderTraceSummary(stages)
	if !strings.Contains(got, "stopped at: kafka-connect pg-orders-connector") {
		t.Fatalf("summary missing stopped stage: %q", got)
	}
}
