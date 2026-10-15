package main

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"piperoom/internal/actions"
	"piperoom/internal/agent"
	"piperoom/internal/clients"
	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
	"piperoom/internal/topology"
	"piperoom/internal/wiring"
)

// fakeClients implements every client interface the wiring touches, replaying
// a healthy schema-change pipeline. Counters let tests prove the builder was
// wired to these clients rather than stubs.
type fakeClients struct {
	now            time.Time
	slotsCalls     atomic.Int64
	statusCalls    atomic.Int64
	findByKeyCalls atomic.Int64
}

func newFakeClients() *fakeClients { return &fakeClients{now: time.Now()} }

// SourceDB
func (f *fakeClients) ReplicationSlots(context.Context) ([]clients.ReplicationSlot, error) {
	f.slotsCalls.Add(1)
	return []clients.ReplicationSlot{{Name: "debezium", Plugin: "pgoutput", Active: true, RetainedWALBytes: 0, WALStatus: "unreserved"}}, nil
}
func (f *fakeClients) RecentDDL(context.Context, time.Time, int) ([]clients.DDLChange, error) {
	return []clients.DDLChange{{At: f.now, CommandTag: "ALTER TABLE", ObjectIdentity: "public.orders", Query: "ALTER TABLE public.orders ALTER COLUMN amount TYPE numeric(12,2)", Username: "priya"}}, nil
}
func (f *fakeClients) LongTransactions(context.Context, time.Duration) ([]clients.Transaction, error) {
	return nil, nil
}
func (f *fakeClients) Row(_ context.Context, _, _, pk string) (map[string]any, bool, error) {
	return map[string]any{"id": pk, "updated_at": f.now.Add(-2 * time.Minute).Format(time.RFC3339Nano)}, true, nil
}

// Warehouse
func (f *fakeClients) Freshness(context.Context, string, string) (time.Time, error) {
	return f.now.Add(-time.Minute), nil
}

// ConnectClient
func (f *fakeClients) ListConnectors(context.Context) ([]string, error) {
	return []string{"pg-orders-connector"}, nil
}
func (f *fakeClients) Status(context.Context, string) (clients.ConnectorStatus, error) {
	f.statusCalls.Add(1)
	return clients.ConnectorStatus{Name: "pg-orders-connector", State: "RUNNING", Tasks: []clients.TaskStatus{{ID: 0, State: "RUNNING"}}}, nil
}
func (f *fakeClients) Config(context.Context, string) (map[string]string, error) {
	return map[string]string{"topic.prefix": "pg", "table.include.list": "public.orders", "slot.name": "debezium"}, nil
}
func (f *fakeClients) RestartTask(context.Context, string, int) error { return nil }
func (f *fakeClients) RestartConnector(context.Context, string) error { return nil }

// KafkaAdmin
func (f *fakeClients) Topics(context.Context) ([]string, error) {
	return []string{"pg.public.orders"}, nil
}
func (f *fakeClients) TopicOffsets(context.Context, string) ([]clients.PartitionOffsets, error) {
	return []clients.PartitionOffsets{{Partition: 0, End: 10, LastTimestamp: f.now}}, nil
}
func (f *fakeClients) ConsumerGroups(context.Context) ([]string, error) {
	return []string{"order-sync"}, nil
}
func (f *fakeClients) GroupLag(context.Context, string) (clients.GroupLag, error) {
	return clients.GroupLag{Group: "order-sync", State: "Stable", TotalLag: 0, Partitions: []clients.PartitionLag{{Topic: "pg.public.orders", Partition: 0, Committed: 10, End: 10, Lag: 0}}}, nil
}
func (f *fakeClients) FindByKey(_ context.Context, topic, key string, _ time.Time, _ int) ([]clients.RecordRef, error) {
	f.findByKeyCalls.Add(1)
	return []clients.RecordRef{{Topic: topic, Partition: 0, Offset: 5, Timestamp: f.now, Key: key}}, nil
}

// SchemaRegistryClient
func (f *fakeClients) Subjects(context.Context) ([]string, error) {
	return []string{"pg.public.orders-value"}, nil
}
func (f *fakeClients) Versions(context.Context, string) ([]int, error) { return []int{1}, nil }
func (f *fakeClients) Schema(context.Context, string, int) (clients.SchemaVersion, error) {
	return clients.SchemaVersion{Subject: "pg.public.orders-value", Version: 1, ID: 1, Schema: `{"type":"record"}`}, nil
}
func (f *fakeClients) Compatibility(context.Context, string) (string, bool, error) {
	return "BACKWARD", true, nil
}
func (f *fakeClients) SetCompatibility(context.Context, string, string) error { return nil }
func (f *fakeClients) Register(context.Context, string, string) (int, error)  { return 1, nil }

var (
	_ clients.SourceDB             = (*fakeClients)(nil)
	_ clients.Warehouse            = (*fakeClients)(nil)
	_ clients.ConnectClient        = (*fakeClients)(nil)
	_ clients.KafkaAdmin           = (*fakeClients)(nil)
	_ clients.SchemaRegistryClient = (*fakeClients)(nil)
)

func testOwners() *owners.Owners {
	return &owners.Owners{
		Pipelines: []owners.Pipeline{{
			ID:             "orders",
			Title:          "Orders CDC",
			SourceTables:   []string{"public.orders"},
			Connectors:     []string{"pg-orders-connector"},
			ConsumerGroups: []string{"order-sync"},
			SinkTables:     []string{"analytics.orders"},
			SinkTSColumn:   "updated_at",
		}},
		Layers: owners.UserLayers(map[string]string{
			"postgres":        "priya",
			"kafka-connect":   "arjun",
			"schema-registry": "priya",
			"kafka":           "arjun",
			"consumer":        "kabir",
			"warehouse":       "meera",
		}),
	}
}

func wireFakes(t *testing.T) (*tools.Registry, *actions.Registry, *fakeClients) {
	t.Helper()
	f := newFakeClients()
	reg := tools.NewRegistry()
	areg := actions.NewRegistry()
	w := &wiring.Wired{
		Source:    f,
		Warehouse: f,
		Connect:   f,
		Kafka:     f,
		Registry:  f,
		Owners:    testOwners(),
	}
	if err := wiring.RegisterTools(reg, areg, w); err != nil {
		t.Fatalf("wiring.RegisterTools: %v", err)
	}
	return reg, areg, f
}

func TestRegisterToolsWiresAllTools(t *testing.T) {
	reg, areg, _ := wireFakes(t)

	want := []string{
		"pg_replication_slots", "pg_recent_ddl", "pg_long_transactions", "pg_row",
		"pg_publication_tables",
		"warehouse_freshness", "warehouse_row", "warehouse_load_errors", "reconcile", "pipeline_map", "trace_record",
		"connect_list_connectors", "connect_status", "connect_config",
		"schema_registry_subjects", "schema_registry_versions", "schema_registry_diff",
		"kafka_topic_offsets", "kafka_list_groups", "kafka_consumer_lag", "kafka_find_by_key",
		"kafka_topic_config",
		"dlq_peek",
		"debezium_metrics",
		"runbook_lookup",
		"what_changed",
	}
	got := make([]string, 0, len(reg.All()))
	for _, tl := range reg.All() {
		got = append(got, tl.Name())
		if tl.Kind() != tools.KindRead {
			t.Errorf("tool %s kind = %s, want %s", tl.Name(), tl.Kind(), tools.KindRead)
		}
	}
	if !slices.Equal(want, got) {
		t.Errorf("registered tools = %v, want %v", got, want)
	}

	wantActions := []string{
		"connect_restart_task", "connect_restart_connector", "connect_resume_connector", "connect_update_connector_config",
		"schema_set_compatibility", "schema_register",
	}
	gotActions := make([]string, 0, len(areg.All()))
	for _, a := range areg.All() {
		gotActions = append(gotActions, a.Name())
	}
	if !slices.Equal(wantActions, gotActions) {
		t.Errorf("registered actions = %v, want %v", gotActions, wantActions)
	}
}

func TestPipelineMapUsesRealClients(t *testing.T) {
	reg, _, f := wireFakes(t)

	tool, ok := reg.Get("pipeline_map")
	if !ok {
		t.Fatal("pipeline_map not registered")
	}
	res, err := tool.Run(tools.WithPipelineID(context.Background(), "orders"), json.RawMessage(`{"pipeline_id":"orders"}`))
	if err != nil {
		t.Fatalf("pipeline_map run: %v", err)
	}
	g, ok := res.Data.(topology.Graph)
	if !ok {
		t.Fatalf("pipeline_map data type %T, want topology.Graph", res.Data)
	}
	if g.PipelineID != "orders" {
		t.Errorf("graph pipeline id = %q, want %q", g.PipelineID, "orders")
	}
	// Five pipeline stages plus the registry subject the fake lists.
	if len(g.Nodes) != 6 || g.Nodes[5].ID != "sr:pg.public.orders-value" {
		t.Errorf("graph nodes = %+v, want 5 stages and sr:pg.public.orders-value (builder wired to the registry)", g.Nodes)
	}
	if f.statusCalls.Load() == 0 {
		t.Error("connect Status never called: builder not wired to the connect client")
	}
	if f.slotsCalls.Load() == 0 {
		t.Error("ReplicationSlots never called: builder not wired to the source client")
	}
}

func TestTraceRecordUsesRealClients(t *testing.T) {
	reg, _, f := wireFakes(t)

	tool, ok := reg.Get("trace_record")
	if !ok {
		t.Fatal("trace_record not registered")
	}
	res, err := tool.Run(tools.WithPipelineID(context.Background(), "orders"), json.RawMessage(`{"pipeline_id":"orders","table":"public.orders","key":"1"}`))
	if err != nil {
		t.Fatalf("trace_record run: %v", err)
	}
	stages, ok := res.Data.([]events.TraceStage)
	if !ok {
		t.Fatalf("trace_record data type %T, want []events.TraceStage", res.Data)
	}
	if len(stages) != 5 {
		t.Fatalf("trace stages = %d, want 5", len(stages))
	}
	for _, s := range stages {
		if s.Status != "seen" {
			t.Errorf("stage %s %s = %s, want seen", s.Layer, s.Name, s.Status)
		}
	}
	if f.findByKeyCalls.Load() == 0 {
		t.Error("FindByKey never called: trace not wired to the kafka client")
	}
}

func TestPgRowScopedToPipeline(t *testing.T) {
	reg, _, _ := wireFakes(t)
	tool, ok := reg.Get("pg_row")
	if !ok {
		t.Fatal("pg_row not registered")
	}

	res, err := tool.Run(tools.WithPipelineID(context.Background(), "orders"), json.RawMessage(`{"pipeline_id":"orders","table":"public.orders","pk_column":"id","key":"1"}`))
	if err != nil {
		t.Fatalf("pg_row in-scope: %v", err)
	}
	if _, ok := res.Data.(map[string]any); !ok {
		t.Fatalf("pg_row data type %T, want map[string]any", res.Data)
	}

	if _, err := tool.Run(tools.WithPipelineID(context.Background(), "orders"), json.RawMessage(`{"pipeline_id":"orders","table":"public.payments","pk_column":"id","key":"1"}`)); !errors.Is(err, tools.ErrBadInput) {
		t.Fatalf("pg_row other-table: err = %v, want ErrBadInput", err)
	}
}

// TestSystemPromptNamesOnlyRealTools covers C13 P11: every tool or action name
// the agent's system prompt mentions must be something the model can really
// reach with this wiring — a read tool registered here, one of the agent's
// room tools, or a write action it may name inside propose_fix. A prompt edit
// that names a nonexistent callable fails this test.
func TestSystemPromptNamesOnlyRealTools(t *testing.T) {
	reg, areg, _ := wireFakes(t)

	available := map[string]bool{}
	for _, tl := range reg.All() {
		available[tl.Name()] = true
	}
	// The agent builds these on top of the registry it is given.
	for _, name := range agent.RoomToolNames() {
		available[name] = true
	}
	// The write actions the model may name in propose_fix steps (the
	// proposal tool refuses anything else).
	for _, a := range areg.All() {
		available[a.Name()] = true
	}

	names := agent.PromptToolNames()
	if len(names) == 0 {
		t.Fatal("no tool names found in the system prompt; the extraction may be broken")
	}
	for _, name := range names {
		if !available[name] {
			t.Errorf("system prompt tells the model to call %q, which is not a real tool", name)
		}
	}
}
