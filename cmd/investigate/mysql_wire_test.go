package main

import (
	"context"
	"slices"
	"strings"
	"testing"

	"piperoom/internal/actions"
	"piperoom/internal/agent"
	"piperoom/internal/clients"
	"piperoom/internal/clients/kafka"
	"piperoom/internal/clients/mysql"
	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
	"piperoom/internal/tools/similarincidents"
	"piperoom/internal/wiring"
	"piperoom/internal/wiring/netwire"
)

// mysqlFakes is fakeClients as a MySQL source (mysql.BinlogReader) and a
// Kafka client with the schema history reader, as the real *mysql.DB and
// *kafka.Client are.
type mysqlFakes struct{ *fakeClients }

func (mysqlFakes) BinlogStatus(context.Context) (mysql.BinlogStatus, error) {
	return mysql.BinlogStatus{File: "binlog.000003", Pos: 157, GTIDMode: "ON", ServerID: 1}, nil
}

func (mysqlFakes) SchemaHistoryTail(context.Context, string, int) ([]kafka.SchemaHistoryRecord, error) {
	return nil, nil
}

var (
	_ mysql.BinlogReader        = mysqlFakes{}
	_ kafka.SchemaHistoryReader = mysqlFakes{}
)

func mysqlOwners() *owners.Owners {
	return &owners.Owners{
		Pipelines: []owners.Pipeline{{
			ID:             "orders-mysql",
			SourceKind:     owners.SourceKindMySQL,
			TopicPrefix:    "shop",
			SourceTables:   []string{"shop.orders"},
			Connectors:     []string{"orders-mysql"},
			ConsumerGroups: []string{"order-sync"},
			SinkTables:     []string{"analytics.orders"},
			SinkTSColumn:   "updated_at",
		}},
		Layers: owners.UserLayers(map[string]string{
			"mysql":           "dana",
			"kafka-connect":   "arjun",
			"schema-registry": "priya",
			"kafka":           "arjun",
			"consumer":        "kabir",
			"warehouse":       "meera",
		}),
	}
}

// wireFakesFor registers the tools and actions of a set of o over f, the way
// the server and cmd/investigate do.
func wireFakesFor(t *testing.T, o *owners.Owners, src clients.SourceDB, kc clients.KafkaAdmin) (*tools.Registry, *actions.Registry) {
	t.Helper()
	f := newFakeClients()
	w := &wiring.Wired{Source: src, Warehouse: f, Connect: f, Kafka: kc, Registry: f, Owners: o, ProbeTools: netwire.Register}
	reg, areg := tools.NewReadOnlyRegistry(), actions.NewRegistry()
	if err := wiring.RegisterTools(reg, areg, w); err != nil {
		t.Fatalf("wiring.RegisterTools: %v", err)
	}
	return reg, areg
}

func toolNames(reg *tools.Registry) []string {
	var out []string
	for _, tl := range reg.All() {
		out = append(out, tl.Name())
	}
	return out
}

func actionNamesOf(areg *actions.Registry) []string {
	var out []string
	for _, a := range areg.All() {
		out = append(out, a.Name())
	}
	return out
}

// A MySQL set offers no pg_* tool or action; it offers mysql_binlog_status,
// the neutral source_row and source_long_transactions, and
// kafka_schema_history (Q8g). The list is pinned: it is the MySQL room's
// tool list as the model sees it.
func TestRegisterToolsMySQLSet(t *testing.T) {
	m := mysqlFakes{newFakeClients()}
	reg, areg := wireFakesFor(t, mysqlOwners(), m, m)

	want := []string{
		"source_settings", "source_table_replication", "source_role_grants", "source_blocking_sessions",
		"source_long_transactions", "source_row",
		"warehouse_freshness", "warehouse_row", "warehouse_load_errors", "reconcile", "pipeline_map", "trace_record",
		"mysql_binlog_status",
		"connect_list_connectors", "connect_status", "connect_config",
		"schema_registry_subjects", "schema_registry_versions", "schema_registry_diff",
		"kafka_topic_offsets", "kafka_list_groups", "kafka_consumer_lag", "kafka_find_by_key",
		"kafka_topic_config",
		"dlq_peek",
		"connect_validate_config",
		"kafka_schema_history",
		"debezium_metrics", "debezium_snapshot_progress",
		"connect_worker_metrics", "connect_worker_logs",
		"net_probe",
		"runbook_lookup",
		"what_changed",
	}
	if got := toolNames(reg); !slices.Equal(got, want) {
		t.Errorf("MySQL tools = %v, want %v", got, want)
	}
	wantActions := []string{
		"connect_restart_task", "connect_restart_connector", "connect_resume_connector", "connect_pause_connector", "connect_update_connector_config",
		"schema_set_compatibility", "schema_register",
		"connect_tune_throughput",
		"kafka_reset_consumer_offsets", "dlq_replay", "kafka_replay_range", "connect_snapshot_table",
		"source_create_heartbeat_table", "source_grant_select", "source_end_session",
		"connect_stop_connector", "connect_reset_source_offsets", "connect_create_connector", "connect_delete_connector",
	}
	if got := actionNamesOf(areg); !slices.Equal(got, wantActions) {
		t.Errorf("MySQL actions = %v, want %v", got, wantActions)
	}
	for _, n := range append(toolNames(reg), actionNamesOf(areg)...) {
		if strings.HasPrefix(n, "pg_") {
			t.Errorf("MySQL set offers %s", n)
		}
	}

	// The room the agent builds from it: same tools, mysql source.
	if k, err := agent.RoomSourceKind(reg); err != nil || k != events.SourceKindMySQL {
		t.Fatalf("RoomSourceKind(MySQL set) = %q, %v", k, err)
	}
	// Each source admin action is the mysql owner's in this room.
	for _, a := range areg.All() {
		if strings.HasPrefix(a.Name(), "source_") {
			if l := actions.LayerFor(a, events.SourceKindMySQL); l != events.LayerMySQL {
				t.Errorf("%s: LayerFor(mysql) = %q, want mysql", a.Name(), l)
			}
		}
	}
}

// A Postgres set over a Kafka client that has the schema history reader
// still offers exactly the list it had before MySQL (TestRegisterToolsWiresAllTools):
// no kafka_schema_history, no mysql_* tool, no source_row.
func TestRegisterToolsPostgresSetUnchangedByMySQL(t *testing.T) {
	f := newFakeClients()
	pgReg, pgActs, _ := wireFakes(t)
	reg, areg := wireFakesFor(t, testOwners(), f, mysqlFakes{f})
	if got, want := toolNames(reg), toolNames(pgReg); !slices.Equal(got, want) {
		t.Errorf("Postgres tools with a history-capable Kafka client = %v, want %v", got, want)
	}
	if got, want := actionNamesOf(areg), actionNamesOf(pgActs); !slices.Equal(got, want) {
		t.Errorf("Postgres actions = %v, want %v", got, want)
	}
	for _, n := range toolNames(reg) {
		if strings.HasPrefix(n, "mysql_") || n == "kafka_schema_history" || n == "source_row" || n == "source_long_transactions" {
			t.Errorf("Postgres set offers %s", n)
		}
	}
}

// Every callable the MySQL prompt names is reachable in a MySQL room: a read
// tool of the MySQL set, a room tool, or one of its write actions.
func TestMySQLSystemPromptNamesOnlyRealTools(t *testing.T) {
	m := mysqlFakes{newFakeClients()}
	reg, areg := wireFakesFor(t, mysqlOwners(), m, m)
	available := map[string]bool{similarincidents.ToolName: true}
	for _, n := range toolNames(reg) {
		available[n] = true
	}
	for _, n := range agent.RoomToolNames() {
		available[n] = true
	}
	for _, n := range actionNamesOf(areg) {
		available[n] = true
	}
	names, err := agent.PromptToolNamesForSource(events.PipelineKindCDC, events.SourceKindMySQL)
	if err != nil || len(names) == 0 {
		t.Fatalf("PromptToolNamesForSource = %v, %v", names, err)
	}
	for _, n := range names {
		if !available[n] {
			t.Errorf("MySQL system prompt tells the model to call %q, which a MySQL room does not offer", n)
		}
	}
}

// -list-tools lists the pipeline's source kind's tools.
func TestListOwnersBySourceKind(t *testing.T) {
	own := &owners.Owners{Pipelines: append(mysqlOwners().Pipelines, testOwners().Pipelines...)}
	pipe, _ := own.Pipeline("orders-mysql")
	view := listOwners(own, pipe, true)
	if len(view.Pipelines) != 1 || view.Pipelines[0].ID != "orders-mysql" {
		t.Fatalf("listOwners(mysql) = %+v", view.Pipelines)
	}
	if sourceKindOf(pipe, true) != owners.SourceKindMySQL || sourceKindOf(owners.Pipeline{}, false) != "" {
		t.Fatal("sourceKindOf")
	}
	// Without a pipeline the whole file, which mixes kinds: RegisterTools refuses.
	if _, err := listToolsRegistry(listOwners(own, owners.Pipeline{}, false), false); err == nil || !strings.Contains(err.Error(), "more than one source kind") {
		t.Fatalf("listing a mixed file without -pipeline: err = %v", err)
	}
}
