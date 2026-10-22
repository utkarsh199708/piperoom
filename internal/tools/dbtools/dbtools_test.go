package dbtools

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"piperoom/internal/clients"
	"piperoom/internal/clients/postgres"
	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
	"piperoom/internal/topology"
)

type fakeSource struct {
	slots     []clients.ReplicationSlot
	rows      map[string]map[string]any
	rowCalls  int
	pubTables []postgres.PublicationTable
	pubErr    error

	// fresh is the newest source timestamp warehouse_freshness compares
	// against; freshErr models an unreadable source.
	fresh       time.Time
	freshErr    error
	freshCalls  int
	freshTable  string
	freshColumn string
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

// LongTransactionsOnTables treats every long transaction as one on the
// pipeline's tables; txnSource models the lock filter itself.
func (f *fakeSource) LongTransactionsOnTables(ctx context.Context, d time.Duration, _ []string) (postgres.TableTransactions, error) {
	txns, err := f.LongTransactions(ctx, d)
	return postgres.TableTransactions{Transactions: txns}, err
}
func (f *fakeSource) Row(_ context.Context, table, _, key string) (map[string]any, bool, error) {
	f.rowCalls++
	row, ok := f.rows[table+"|"+key]
	return row, ok, nil
}
func (f *fakeSource) PublicationTables(_ context.Context, _, _ string, _ []string) ([]postgres.PublicationTable, error) {
	return f.pubTables, f.pubErr
}
func (f *fakeSource) Freshness(_ context.Context, table, tsColumn string) (time.Time, error) {
	f.freshCalls++
	f.freshTable, f.freshColumn = table, tsColumn
	return f.fresh, f.freshErr
}

// fakeConnect implements clients.ConnectClient. Config looks up the connector
// in configs and reports clients.ErrNotFound (wrapped) for unknown names, so
// the publication tool's connector-selection rule is exercised like the real
// Connect REST client.
type fakeConnect struct {
	configs   map[string]map[string]string
	configErr error
}

func (f *fakeConnect) ListConnectors(context.Context) ([]string, error) { return nil, nil }
func (f *fakeConnect) Status(context.Context, string) (clients.ConnectorStatus, error) {
	return clients.ConnectorStatus{}, nil
}
func (f *fakeConnect) Config(_ context.Context, name string) (map[string]string, error) {
	if f.configErr != nil {
		return nil, f.configErr
	}
	cfg, ok := f.configs[name]
	if !ok {
		return nil, fmt.Errorf("connect: connector %q: %w", name, clients.ErrNotFound)
	}
	return cfg, nil
}
func (f *fakeConnect) RestartTask(context.Context, string, int) error { return nil }
func (f *fakeConnect) RestartConnector(context.Context, string) error { return nil }

type fakeWarehouse struct {
	rows     map[string]map[string]any
	fresh    time.Time
	rowCalls int
}

func (f *fakeWarehouse) Row(_ context.Context, table, _, key string) (map[string]any, bool, error) {
	f.rowCalls++
	row, ok := f.rows[table+"|"+key]
	return row, ok, nil
}

// PrimaryKeyColumn reports "id", the key of every fake table.
func (f *fakeWarehouse) PrimaryKeyColumn(context.Context, string) (string, error) { return "id", nil }
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

func testOwners() *owners.Owners {
	return &owners.Owners{
		Pipelines: []owners.Pipeline{
			{ID: "orders", SourceTables: []string{"public.orders"}, SinkTables: []string{"analytics.orders"}, SinkTSColumn: "updated_at", Connectors: []string{"pg-orders-connector"}},
			{ID: "payments", SourceTables: []string{"public.payments"}, SinkTables: []string{"analytics.payments"}, SinkTSColumn: "updated_at", Connectors: []string{"pg-payments-connector"}},
		},
	}
}

func testDeps() Deps {
	return Deps{
		Src: &fakeSource{
			slots: []clients.ReplicationSlot{{Name: "pg-orders-connector", Active: true, RetainedWALBytes: 1 << 30, WALStatus: "reserved"}},
			rows: map[string]map[string]any{
				"public.orders|7": {"id": "7", "customer_id": "100", "amount": "50", "status": "placed", "updated_at": "2026-10-11T10:00:00Z"},
			},
			// The source is as old as the fake sink's newest row: the default
			// pipeline is in sync, not idle.
			fresh: time.Now().Add(-2 * time.Minute),
		},
		Wh: &fakeWarehouse{
			rows:  map[string]map[string]any{"analytics.orders|7": {"id": "7", "updated_at": "2026-10-11T10:00:00Z", "loaded_at": "2026-10-11T10:00:01Z"}},
			fresh: time.Now().Add(-2 * time.Minute),
		},
		Builder: &fakeBuilder{graph: topology.Graph{PipelineID: "orders"}},
		Connect: &fakeConnect{configs: map[string]map[string]string{
			"pg-orders-connector": {
				"connector.class":      "io.debezium.connector.postgresql.PostgresConnector",
				"database.user":        "piperoom",
				"database.server.name": "pg",
			},
			"pg-payments-connector": {
				"connector.class":      "io.debezium.connector.postgresql.PostgresConnector",
				"database.user":        "piperoom",
				"database.server.name": "pg",
			},
		}},
		Owners: testOwners(),
	}
}

func TestRegister(t *testing.T) {
	reg := tools.NewRegistry()
	if err := Register(reg, testDeps()); err != nil {
		t.Fatalf("Register: %v", err)
	}
	all := reg.All()
	if len(all) != 12 {
		t.Fatalf("registered %d tools, want 12", len(all))
	}
	want := []string{"pg_replication_slots", "pg_recent_ddl", "pg_long_transactions", "pg_row", "pg_publication_tables", "pg_connector_scope_gap", "warehouse_freshness", "warehouse_row", "warehouse_load_errors", "reconcile", "pipeline_map", "trace_record"}
	for i, name := range want {
		if all[i].Name() != name {
			t.Fatalf("tool %d = %s, want %s", i, all[i].Name(), name)
		}
		if all[i].Kind() != tools.KindRead {
			t.Fatalf("tool %s kind = %s, want read", name, all[i].Kind())
		}
	}
}

func TestRegisterRequiresOwners(t *testing.T) {
	d := testDeps()
	d.Owners = nil
	if err := Register(tools.NewRegistry(), d); err == nil {
		t.Fatal("Register with nil Owners must fail at wiring time")
	}
}

func runTool(t *testing.T, tt tools.Tool, input string) (tools.Result, error) {
	t.Helper()
	return tt.Run(tools.WithUnscoped(context.Background()), json.RawMessage(input))
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
		{"pg_row bad table", `{"pipeline_id":"orders","table":"orders; drop","pk_column":"id","key":"7"}`},
		{"pg_row missing key", `{"pipeline_id":"orders","table":"public.orders","pk_column":"id"}`},
		{"pg_row missing pipeline", `{"table":"public.orders","pk_column":"id","key":"7"}`},
		{"pg_row unknown pipeline", `{"pipeline_id":"nope","table":"public.orders","pk_column":"id","key":"7"}`},
		{"pg_row other pipeline table", `{"pipeline_id":"orders","table":"public.payments","pk_column":"id","key":"7"}`},
		{"warehouse_row missing pipeline", `{"table":"analytics.orders","pk_column":"id","key":"7"}`},
		{"warehouse_row unknown pipeline", `{"pipeline_id":"nope","table":"analytics.orders","pk_column":"id","key":"7"}`},
		{"warehouse_row other pipeline table", `{"pipeline_id":"orders","table":"analytics.payments","pk_column":"id","key":"7"}`},
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
	res, err := runTool(t, tt2, `{"pipeline_id":"orders","table":"public.orders","pk_column":"id","key":"7"}`)
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

func TestRowToolsRefuseOtherPipelineWithoutQuery(t *testing.T) {
	src := &fakeSource{rows: map[string]map[string]any{"public.payments|7": {"id": "7", "note": "secret"}}}
	wh := &fakeWarehouse{rows: map[string]map[string]any{"analytics.payments|7": {"id": "7"}}}
	d := testDeps()
	d.Src = src
	d.Wh = wh
	reg := tools.NewRegistry()
	if err := Register(reg, d); err != nil {
		t.Fatal(err)
	}

	pg, _ := reg.Get("pg_row")
	if _, err := runTool(t, pg, `{"pipeline_id":"orders","table":"public.payments","pk_column":"id","key":"7"}`); !errors.Is(err, tools.ErrBadInput) {
		t.Fatalf("pg_row other-pipeline table: err = %v, want ErrBadInput", err)
	}
	if src.rowCalls != 0 {
		t.Fatalf("pg_row queried the source %d time(s) for an out-of-scope table", src.rowCalls)
	}

	whTool, _ := reg.Get("warehouse_row")
	if _, err := runTool(t, whTool, `{"pipeline_id":"orders","table":"analytics.payments","pk_column":"id","key":"7"}`); !errors.Is(err, tools.ErrBadInput) {
		t.Fatalf("warehouse_row other-pipeline table: err = %v, want ErrBadInput", err)
	}
	if wh.rowCalls != 0 {
		t.Fatalf("warehouse_row queried the warehouse %d time(s) for an out-of-scope table", wh.rowCalls)
	}
}

func TestRowToolsRedactData(t *testing.T) {
	reg := tools.NewRegistry()
	if err := Register(reg, testDeps()); err != nil {
		t.Fatal(err)
	}

	pg, _ := reg.Get("pg_row")
	res, err := runTool(t, pg, `{"pipeline_id":"orders","table":"public.orders","pk_column":"id","key":"7"}`)
	if err != nil {
		t.Fatalf("pg_row: %v", err)
	}
	data, ok := res.Data.(map[string]any)
	if !ok {
		t.Fatalf("pg_row data type %T, want map[string]any", res.Data)
	}
	if data["id"] != "7" {
		t.Fatalf("data[id] = %v, want \"7\"", data["id"])
	}
	if data["updated_at"] == nil {
		t.Fatal("data[updated_at] should keep its timestamp value")
	}
	if v := data["customer_id"]; v != nil {
		t.Fatalf("data[customer_id] = %v, want nil (name only)", v)
	}
	if v := data["amount"]; v != nil {
		t.Fatalf("data[amount] = %v, want nil (name only)", v)
	}
	// Every Data key is a column the Summary also names; no extra columns.
	for k := range data {
		if !strings.Contains(res.Summary, k) {
			t.Fatalf("pg_row data key %q not present in summary %q", k, res.Summary)
		}
	}

	wh, _ := reg.Get("warehouse_row")
	wres, err := runTool(t, wh, `{"pipeline_id":"orders","table":"analytics.orders","pk_column":"id","key":"7"}`)
	if err != nil {
		t.Fatalf("warehouse_row: %v", err)
	}
	wdata, ok := wres.Data.(map[string]any)
	if !ok {
		t.Fatalf("warehouse_row data type %T, want map[string]any", wres.Data)
	}
	for k := range wdata {
		if !strings.Contains(wres.Summary, k) {
			t.Fatalf("warehouse_row data key %q not present in summary %q", k, wres.Summary)
		}
	}
	if _, leaked := wdata["id"]; leaked {
		t.Fatalf("warehouse_row data leaked non-timestamp column id: %v", wdata)
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

func TestPipelineMapTargetError(t *testing.T) {
	d := testDeps()
	d.Builder = &fakeBuilder{buildErr: errors.New("connect: EOF")}
	reg := tools.NewRegistry()
	if err := Register(reg, d); err != nil {
		t.Fatal(err)
	}
	tt, _ := reg.Get("pipeline_map")
	_, err := tt.Run(tools.WithUnscoped(context.Background()), json.RawMessage(`{"pipeline_id":"orders"}`))
	if err == nil {
		t.Fatal("expected target error")
	}
	if errors.Is(err, tools.ErrBadInput) {
		t.Fatalf("target-system error must not wrap ErrBadInput: %v", err)
	}
}

func TestTraceRecordTargetError(t *testing.T) {
	d := testDeps()
	d.Builder = &fakeBuilder{traceErr: errors.New("connect: EOF")}
	reg := tools.NewRegistry()
	if err := Register(reg, d); err != nil {
		t.Fatal(err)
	}
	tt, _ := reg.Get("trace_record")
	_, err := tt.Run(tools.WithUnscoped(context.Background()), json.RawMessage(`{"pipeline_id":"orders","table":"public.orders","key":"7"}`))
	if err == nil {
		t.Fatal("expected target error")
	}
	if errors.Is(err, tools.ErrBadInput) {
		t.Fatalf("target-system error must not wrap ErrBadInput: %v", err)
	}
}

func TestPipelineMapUnknownPipelineBadInput(t *testing.T) {
	d := testDeps()
	d.Builder = &fakeBuilder{buildErr: topology.ErrBadInput}
	reg := tools.NewRegistry()
	if err := Register(reg, d); err != nil {
		t.Fatal(err)
	}
	tt, _ := reg.Get("pipeline_map")
	_, err := tt.Run(tools.WithUnscoped(context.Background()), json.RawMessage(`{"pipeline_id":"nope"}`))
	if err == nil {
		t.Fatal("expected error")
	}
	if !errors.Is(err, tools.ErrBadInput) {
		t.Fatalf("unknown pipeline must wrap ErrBadInput: %v", err)
	}
}

type ctxErrBuilder struct{ fakeBuilder }

func (b *ctxErrBuilder) Build(ctx context.Context, _ string) (topology.Graph, error) {
	return topology.Graph{}, ctx.Err()
}

func TestPipelineMapCancelledCtx(t *testing.T) {
	d := testDeps()
	ctx, cancel := context.WithCancel(tools.WithUnscoped(context.Background()))
	cancel()
	d.Builder = &ctxErrBuilder{}
	reg := tools.NewRegistry()
	if err := Register(reg, d); err != nil {
		t.Fatal(err)
	}
	tt, _ := reg.Get("pipeline_map")
	_, err := tt.Run(ctx, json.RawMessage(`{"pipeline_id":"orders"}`))
	if err == nil {
		t.Fatal("expected context-cancelled error")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error %v does not wrap context.Canceled", err)
	}
}

func TestToolCtxCapsDeadline(t *testing.T) {
	parent, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	ctx, cf := toolCtx(parent)
	defer cf()
	dl, ok := ctx.Deadline()
	if !ok {
		t.Fatal("expected a deadline")
	}
	if d := time.Until(dl); d > 10*time.Second {
		t.Fatalf("deadline = %v, want <= 10s", d)
	}
}

func TestCutShortRuneSafe(t *testing.T) {
	in := "héllo wörld"
	got := cutShort(in, 5)
	if got != "héllo…" {
		t.Fatalf("cutShort = %q, want %q", got, "héllo…")
	}
	// The UTF-8 sequence must not be split mid-rune.
	if !utf8.ValidString(got) {
		t.Fatalf("cutShort produced invalid UTF-8: %q", got)
	}
}

func publicationTool(t *testing.T, src clients.SourceDB, connects ...clients.ConnectClient) tools.Tool {
	t.Helper()
	d := testDeps()
	d.Src = src
	if len(connects) > 0 {
		d.Connect = connects[0]
	}
	reg := tools.NewRegistry()
	if err := Register(reg, d); err != nil {
		t.Fatal(err)
	}
	tt, ok := reg.Get("pg_publication_tables")
	if !ok {
		t.Fatal("pg_publication_tables not registered")
	}
	return tt
}

func TestPgPublicationTablesHappyPath(t *testing.T) {
	src := &fakeSource{pubTables: []postgres.PublicationTable{
		{Schema: "public", Table: "orders", InPublication: true, Selectable: true},
	}}
	tt := publicationTool(t, src)
	res, err := runTool(t, tt, `{"pipeline_id":"orders"}`)
	if err != nil {
		t.Fatalf("pg_publication_tables: %v", err)
	}
	if !strings.Contains(res.Summary, "public.orders: in_publication=yes, selectable=yes") {
		t.Fatalf("summary missing expected line: %q", res.Summary)
	}
	data, ok := res.Data.(map[string]any)
	if !ok {
		t.Fatalf("data type %T, want map[string]any", res.Data)
	}
	if data["publication"] != defaultPublication {
		t.Fatalf("publication = %v, want %q", data["publication"], defaultPublication)
	}
	if data["publication_source"] != "default" {
		t.Fatalf("publication_source = %v, want %q (config has no publication.name)", data["publication_source"], "default")
	}
	if data["connector_user"] != "piperoom" {
		t.Fatalf("connector_user = %v, want %q", data["connector_user"], "piperoom")
	}
	if data["connector_user_source"] != "connector pg-orders-connector config database.user" {
		t.Fatalf("connector_user_source = %v", data["connector_user_source"])
	}
	members, ok := data["publication_tables"].([]postgres.PublicationTable)
	if !ok || len(members) != 1 || !members[0].InPublication {
		t.Fatalf("publication_tables = %v", data["publication_tables"])
	}
}

func TestPgPublicationTablesUnknownPipeline(t *testing.T) {
	tt := publicationTool(t, &fakeSource{})
	_, err := runTool(t, tt, `{"pipeline_id":"nope"}`)
	if err == nil {
		t.Fatal("expected error for unknown pipeline")
	}
	if !errors.Is(err, tools.ErrBadInput) {
		t.Fatalf("error %v does not wrap ErrBadInput", err)
	}
}

func TestPgPublicationTablesMissingPublication(t *testing.T) {
	src := &fakeSource{pubErr: clients.ErrNotFound}
	tt := publicationTool(t, src)
	_, err := runTool(t, tt, `{"pipeline_id":"orders"}`)
	if err == nil {
		t.Fatal("expected error for missing publication")
	}
	if !errors.Is(err, clients.ErrNotFound) {
		t.Fatalf("error %v does not wrap clients.ErrNotFound", err)
	}
	if errors.Is(err, tools.ErrBadInput) {
		t.Fatalf("missing publication must not be reported as bad input: %v", err)
	}
}

func TestPgPublicationTablesPrivilegeDenied(t *testing.T) {
	src := &fakeSource{pubTables: []postgres.PublicationTable{
		{Schema: "public", Table: "orders", InPublication: true, Selectable: false},
	}}
	tt := publicationTool(t, src)
	res, err := runTool(t, tt, `{"pipeline_id":"orders"}`)
	if err != nil {
		t.Fatalf("privilege-denied must be a result, not an error: %v", err)
	}
	if !strings.Contains(res.Summary, "selectable=no") {
		t.Fatalf("summary should report selectable=no: %q", res.Summary)
	}
}

func TestPgPublicationTablesSourceMissingFromPublication(t *testing.T) {
	// The source table is not in the publication but the role can still SELECT it.
	src := &fakeSource{pubTables: []postgres.PublicationTable{
		{Schema: "public", Table: "orders", InPublication: false, Selectable: true},
	}}
	tt := publicationTool(t, src)
	res, err := runTool(t, tt, `{"pipeline_id":"orders"}`)
	if err != nil {
		t.Fatalf("pg_publication_tables: %v", err)
	}
	if !strings.Contains(res.Summary, "in_publication=no, selectable=yes") {
		t.Fatalf("summary should report in_publication=no: %q", res.Summary)
	}
}

func TestPgPublicationTablesDBError(t *testing.T) {
	src := &fakeSource{pubErr: errors.New("postgres: connection refused")}
	tt := publicationTool(t, src)
	_, err := runTool(t, tt, `{"pipeline_id":"orders"}`)
	if err == nil {
		t.Fatal("expected error")
	}
	if errors.Is(err, tools.ErrBadInput) {
		t.Fatalf("DB error must not wrap ErrBadInput: %v", err)
	}
}

// plainSource implements clients.SourceDB but not publicationTablesClient, to
// exercise the type-assertion fallback.
type plainSource struct{}

func (plainSource) ReplicationSlots(context.Context) ([]clients.ReplicationSlot, error) {
	return nil, nil
}
func (plainSource) RecentDDL(context.Context, time.Time, int) ([]clients.DDLChange, error) {
	return nil, nil
}
func (plainSource) LongTransactions(context.Context, time.Duration) ([]clients.Transaction, error) {
	return nil, nil
}
func (plainSource) Row(context.Context, string, string, string) (map[string]any, bool, error) {
	return nil, false, nil
}

func TestPgPublicationTablesUnsupportedClient(t *testing.T) {
	tt := publicationTool(t, plainSource{})
	_, err := runTool(t, tt, `{"pipeline_id":"orders"}`)
	if err == nil {
		t.Fatal("expected error for a source client without PublicationTables")
	}
	if !strings.Contains(err.Error(), "does not support PublicationTables") {
		t.Fatalf("error %q does not name the missing capability", err)
	}
}

func TestPgPublicationTablesReadsRoleAndPublicationFromConfig(t *testing.T) {
	src := &fakeSource{pubTables: []postgres.PublicationTable{
		{Schema: "public", Table: "orders", InPublication: true, Selectable: true},
	}}
	connect := &fakeConnect{configs: map[string]map[string]string{
		"pg-orders-connector": {
			"connector.class":      "io.debezium.connector.postgresql.PostgresConnector",
			"database.user":        "alice",
			"database.server.name": "pg",
			"publication.name":     "orders_pub",
		},
	}}
	tt := publicationTool(t, src, connect)
	res, err := runTool(t, tt, `{"pipeline_id":"orders"}`)
	if err != nil {
		t.Fatalf("pg_publication_tables: %v", err)
	}
	data := res.Data.(map[string]any)
	if data["connector_user"] != "alice" {
		t.Fatalf("connector_user = %v, want %q", data["connector_user"], "alice")
	}
	if data["publication"] != "orders_pub" {
		t.Fatalf("publication = %v, want %q", data["publication"], "orders_pub")
	}
	if data["publication_source"] == "default" {
		t.Fatalf("publication_source = %v, must not be default when publication.name is set", data["publication_source"])
	}
}

func TestPgPublicationTablesPublicationDefaulted(t *testing.T) {
	src := &fakeSource{pubTables: []postgres.PublicationTable{
		{Schema: "public", Table: "orders", InPublication: true, Selectable: true},
	}}
	connect := &fakeConnect{configs: map[string]map[string]string{
		"pg-orders-connector": {
			"connector.class":      "io.debezium.connector.postgresql.PostgresConnector",
			"database.user":        "alice",
			"database.server.name": "pg",
		},
	}}
	tt := publicationTool(t, src, connect)
	res, err := runTool(t, tt, `{"pipeline_id":"orders"}`)
	if err != nil {
		t.Fatalf("pg_publication_tables: %v", err)
	}
	data := res.Data.(map[string]any)
	if data["publication"] != defaultPublication {
		t.Fatalf("publication = %v, want %q", data["publication"], defaultPublication)
	}
	if data["publication_source"] != "default" {
		t.Fatalf("publication_source = %v, want %q", data["publication_source"], "default")
	}
}

func TestPgPublicationTablesNoDatabaseUser(t *testing.T) {
	connect := &fakeConnect{configs: map[string]map[string]string{
		"pg-orders-connector": {
			"connector.class":      "io.debezium.connector.postgresql.PostgresConnector",
			"database.server.name": "pg",
		},
	}}
	tt := publicationTool(t, &fakeSource{}, connect)
	_, err := runTool(t, tt, `{"pipeline_id":"orders"}`)
	if err == nil {
		t.Fatal("expected error when database.user is absent")
	}
	if !strings.Contains(err.Error(), "database.user") || !strings.Contains(err.Error(), "pg-orders-connector") {
		t.Fatalf("error %q must name the missing database.user and the connector", err)
	}
	if errors.Is(err, tools.ErrBadInput) {
		t.Fatalf("wiring/config error must not wrap ErrBadInput: %v", err)
	}
}

func TestPgPublicationTablesNilConnect(t *testing.T) {
	d := testDeps()
	d.Src = &fakeSource{}
	d.Connect = nil
	reg := tools.NewRegistry()
	if err := Register(reg, d); err != nil {
		t.Fatal(err)
	}
	tt, _ := reg.Get("pg_publication_tables")
	_, err := runTool(t, tt, `{"pipeline_id":"orders"}`)
	if err == nil {
		t.Fatal("expected error when Connect is nil")
	}
	if !strings.Contains(err.Error(), "Connect client is nil") || !strings.Contains(err.Error(), "orders") {
		t.Fatalf("error %q must name the nil Connect client and the pipeline", err)
	}
}

func TestPgPublicationTablesConnectorNotFound(t *testing.T) {
	// The pipeline lists a connector the Connect client does not know.
	connect := &fakeConnect{configs: map[string]map[string]string{}}
	tt := publicationTool(t, &fakeSource{}, connect)
	_, err := runTool(t, tt, `{"pipeline_id":"orders"}`)
	if err == nil {
		t.Fatal("expected error when no Debezium source connector is found")
	}
	if !strings.Contains(err.Error(), "pg-orders-connector") {
		t.Fatalf("error %q must name the missing connector", err)
	}
}

func TestPgPublicationTablesNoSourceConnector(t *testing.T) {
	// The only listed connector is a sink, so no Debezium source is found.
	connect := &fakeConnect{configs: map[string]map[string]string{
		"pg-orders-connector": {"connector.class": "org.apache.kafka.connect.sink.SomeSink"},
	}}
	tt := publicationTool(t, &fakeSource{}, connect)
	_, err := runTool(t, tt, `{"pipeline_id":"orders"}`)
	if err == nil {
		t.Fatal("expected error when no Debezium source connector is found")
	}
	if !strings.Contains(err.Error(), "no Debezium Postgres source connector") {
		t.Fatalf("error %q must explain the missing source connector", err)
	}
}
