package wiring

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"

	"piperoom/internal/actions"
	"piperoom/internal/agent"
	"piperoom/internal/clients"
	"piperoom/internal/config"
	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/store/memstore"
	"piperoom/internal/tools"
	"piperoom/internal/tools/batchtools"
	"piperoom/internal/tools/changes"
	"piperoom/internal/tools/dbtools"
	"piperoom/internal/tools/dbttools"
	"piperoom/internal/tools/debeziumtools"
	"piperoom/internal/tools/kafkatools"
	"piperoom/internal/tools/runbooks"
	"piperoom/internal/tools/similarincidents"
)

func cdcPipe(id string) owners.Pipeline {
	return owners.Pipeline{ID: id, TopicPrefix: "pg", SourceTables: []string{"public.t"}, Connectors: []string{id + "-c"}}
}

func batchPipe(id string) owners.Pipeline {
	return owners.Pipeline{ID: id, Kind: events.PipelineKindBatch, DAGs: []string{"revenue_daily"}, Models: []string{"fct_revenue"}}
}

func ownersOf(ps ...owners.Pipeline) *owners.Owners { return &owners.Owners{Pipelines: ps} }

func regNames(reg *tools.Registry) []string {
	var out []string
	for _, t := range reg.All() {
		out = append(out, t.Name())
	}
	return out
}

func registered(t *testing.T, w *Wired) (*tools.Registry, *actions.Registry) {
	t.Helper()
	reg, areg := tools.NewReadOnlyRegistry(), actions.NewRegistry()
	if err := RegisterTools(reg, areg, w); err != nil {
		t.Fatalf("RegisterTools: %v", err)
	}
	return reg, areg
}

var cdcOnlyFamilies = []string{"kafka_", "pg_", "connect_", "schema_registry_", "debezium_", "dlq_"}

func anyPrefix(name string, ps []string) bool {
	for _, p := range ps {
		if strings.HasPrefix(name, p) {
			return true
		}
	}
	return false
}

func TestKindsOf(t *testing.T) {
	for _, tt := range []struct {
		o    *owners.Owners
		want []string
	}{
		{nil, []string{"cdc"}},
		{ownersOf(), []string{"cdc"}},
		{ownersOf(cdcPipe("a")), []string{"cdc"}},
		{ownersOf(batchPipe("b")), []string{"batch"}},
		{ownersOf(cdcPipe("a"), batchPipe("b")), []string{"batch", "cdc"}},
	} {
		if got := (&Wired{Owners: tt.o}).Kinds(); !slices.Equal(got, tt.want) {
			t.Errorf("Kinds = %v, want %v", got, tt.want)
		}
	}
}

// A CDC set registers exactly what it registered before kinds existed, in the
// same order (the model's tool list, and its prompt cache, stay unchanged).
func TestRegisterToolsCDCUnchanged(t *testing.T) {
	o := ownersOf(cdcPipe("orders"))
	w := &Wired{Owners: o}
	reg, areg := registered(t, w)

	old := tools.NewReadOnlyRegistry()
	for _, err := range []error{
		dbtools.Register(old, dbtools.Deps{Builder: w.Builder, Owners: o}),
		kafkatools.Register(old, kafkatools.Deps{Owners: o}),
		debeziumtools.Register(old, debeziumtools.Deps{Owners: o}),
		runbooks.Register(old),
		changes.Register(old, changes.Deps{Owners: o}),
	} {
		if err != nil {
			t.Fatal(err)
		}
	}
	if got, want := regNames(reg), regNames(old); !slices.Equal(got, want) {
		t.Fatalf("CDC tools changed:\n got %v\nwant %v", got, want)
	}
	if len(areg.All()) == 0 {
		t.Fatal("a CDC set lost its actions")
	}
}

// A batch-only set registers no CDC tool and no action; it keeps the shared
// warehouse, graph and client-free tools.
func TestRegisterToolsBatchOnly(t *testing.T) {
	w := &Wired{Owners: ownersOf(batchPipe("rev")), Rooms: similarincidents.FromStore(memstore.New())}
	reg, areg := registered(t, w)
	got := regNames(reg)
	for _, n := range got {
		if anyPrefix(n, cdcOnlyFamilies) || n == "trace_record" || n == "reconcile" || n == "tls_cert_expiry" {
			t.Fatalf("batch-only set registers CDC tool %s: %v", n, got)
		}
	}
	for _, n := range []string{"pipeline_map", "warehouse_freshness", "warehouse_row", "warehouse_load_errors",
		"runbook_lookup", "similar_incidents", "what_changed",
		batchtools.NameDAGRuns, batchtools.NameTaskInstances, batchtools.NameTaskLogTail, batchtools.NameScheduleGap,
		dbttools.RunResultsTool, dbttools.LineageTool, dbttools.SQLTool} {
		if !slices.Contains(got, n) {
			t.Fatalf("batch-only set lacks %s: %v", n, got)
		}
	}
	if a := areg.All(); len(a) != 0 {
		t.Fatalf("batch-only set registers %d actions; every action fixes a CDC system", len(a))
	}
}

// fakeTool is a read tool known only by its name.
type fakeTool string

func (f fakeTool) Name() string              { return string(f) }
func (fakeTool) Description() string         { return "fake" }
func (fakeTool) InputSchema() map[string]any { return map[string]any{"type": "object"} }
func (fakeTool) Kind() tools.Kind            { return tools.KindRead }
func (fakeTool) Run(context.Context, json.RawMessage) (tools.Result, error) {
	return tools.Result{}, nil
}

func fakePackage(name string, kinds []string, toolNames ...string) toolPackage {
	return toolPackage{name: name, kinds: kinds, register: func(reg *tools.Registry, _ *Wired) error {
		for _, n := range toolNames {
			if err := reg.Register(fakeTool(n)); err != nil {
				return err
			}
		}
		return nil
	}}
}

// fakePackages stand for the real packages, the batch ones (P4c-2
// batchtools, P4d-2 dbttools) included, tagged by kind.
func fakePackages() []toolPackage {
	return []toolPackage{
		fakePackage("db tools", bothKinds, "pg_replication_slots", "warehouse_freshness", "reconcile", "pipeline_map", "trace_record"),
		fakePackage("kafka tools", cdcKind, "connect_status", "kafka_topic_offsets", "schema_registry_diff"),
		fakePackage("batch tools", batchKind, "dag_runs", "task_instances", "task_log_tail", "dag_schedule_gap"),
		fakePackage("dbt tools", batchKind, "dbt_run_results", "dbt_model_lineage", "dbt_model_sql"),
		fakePackage("runbooks", nil, "runbook_lookup"),
	}
}

func TestRegisterToolsByKindsPresent(t *testing.T) {
	for _, tt := range []struct {
		name string
		o    *owners.Owners
		want []string
	}{
		{"cdc", ownersOf(cdcPipe("a")), []string{
			"pg_replication_slots", "warehouse_freshness", "reconcile", "pipeline_map", "trace_record",
			"connect_status", "kafka_topic_offsets", "schema_registry_diff", "runbook_lookup"}},
		{"batch", ownersOf(batchPipe("b")), []string{
			"warehouse_freshness", "pipeline_map",
			"dag_runs", "task_instances", "task_log_tail", "dag_schedule_gap",
			"dbt_run_results", "dbt_model_lineage", "dbt_model_sql", "runbook_lookup"}},
		{"both", ownersOf(cdcPipe("a"), batchPipe("b")), []string{
			"pg_replication_slots", "warehouse_freshness", "reconcile", "pipeline_map", "trace_record",
			"connect_status", "kafka_topic_offsets", "schema_registry_diff",
			"dag_runs", "task_instances", "task_log_tail", "dag_schedule_gap",
			"dbt_run_results", "dbt_model_lineage", "dbt_model_sql", "runbook_lookup"}},
	} {
		reg := tools.NewReadOnlyRegistry()
		if err := registerToolsWith(reg, &Wired{Owners: tt.o}, fakePackages()); err != nil {
			t.Fatalf("%s: %v", tt.name, err)
		}
		if got := regNames(reg); !slices.Equal(got, tt.want) {
			t.Fatalf("%s:\n got %v\nwant %v", tt.name, got, tt.want)
		}
	}
}

// A set with both kinds serves each room only its own kind's tools: the
// registry is the union, and the agent filters it by Alert.PipelineKind.
func TestRoomToolsFilteredByKindBothWays(t *testing.T) {
	reg := tools.NewReadOnlyRegistry()
	if err := registerToolsWith(reg, &Wired{Owners: ownersOf(cdcPipe("a"), batchPipe("b"))}, fakePackages()); err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		kind   string
		banned []string
	}{
		{"", []string{"dag_", "dbt_", "task_"}},
		{events.PipelineKindCDC, []string{"dag_", "dbt_", "task_"}},
		{events.PipelineKindBatch, cdcOnlyFamilies},
	} {
		room, err := agent.ToolsForKind(reg, tt.kind)
		if err != nil {
			t.Fatal(err)
		}
		got := regNames(room)
		for _, n := range got {
			if anyPrefix(n, tt.banned) {
				t.Fatalf("kind %q room offers %s: %v", tt.kind, n, got)
			}
		}
		if !slices.Contains(got, "warehouse_freshness") || !slices.Contains(got, "runbook_lookup") {
			t.Fatalf("kind %q room lacks the shared tools: %v", tt.kind, got)
		}
	}
}

func TestRegisterToolsPackageErrorNamesPackage(t *testing.T) {
	boom := toolPackage{name: "boom tools", kinds: batchKind, register: func(*tools.Registry, *Wired) error {
		return errors.New("bad deps")
	}}
	err := registerToolsWith(tools.NewReadOnlyRegistry(), &Wired{Owners: ownersOf(batchPipe("b"))}, []toolPackage{boom})
	if err == nil || !strings.Contains(err.Error(), "boom tools") {
		t.Fatalf("err = %v", err)
	}
	// Not registered at all for a set without its kind.
	if err := registerToolsWith(tools.NewReadOnlyRegistry(), &Wired{Owners: ownersOf(cdcPipe("a"))}, []toolPackage{boom}); err != nil {
		t.Fatalf("a batch package ran for a CDC set: %v", err)
	}
}

// Every tool a full set registers is named in the agent's kind table (an
// exact name or a family prefix), so no tool reaches a room by the default.
func TestEveryRegisteredToolHasAKind(t *testing.T) {
	w := &Wired{Owners: ownersOf(cdcPipe("a"), batchPipe("b")), Rooms: similarincidents.FromStore(memstore.New())}
	reg, _ := registered(t, w)
	for _, n := range regNames(reg) {
		if _, known := agent.ToolPipelineKinds(n); !known {
			t.Errorf("tool %s is not in agent's kind table (internal/agent/toolkind.go)", n)
		}
	}
}

// openRecorder records which constructors openClientsWith called.
type openRecorder struct {
	calls   []string
	writers []*fakeWriter
}

func (r *openRecorder) openers(t *testing.T) clientOpeners {
	return clientOpeners{
		a2: func(cfg config.Config, awsCfg *aws.Config) (*a2Clients, error) {
			r.calls = append(r.calls, "a2")
			return buildA2Clients(devA2Config(), nil) // no I/O until used
		},
		source: func(context.Context, string) (clients.SourceDB, func(), error) {
			r.calls = append(r.calls, "source")
			return downSource{}, func() {}, nil
		},
		warehouse: warehouseOpeners{
			postgres: func(context.Context, string) (clients.Warehouse, func(), error) {
				r.calls = append(r.calls, "warehouse")
				return nil, func() {}, nil
			},
			snowflake: func(context.Context, string) (clients.Warehouse, func(), error) {
				t.Fatal("snowflake opened")
				return nil, nil, nil
			},
		},
		writers: fakeWriterOpeners("set", &r.writers),
		batch: batchOpeners{
			airflow: func(a config.Airflow) (clients.OrchestratorReader, error) {
				r.calls = append(r.calls, "airflow")
				return nil, nil
			},
			dbt: func(config.DBT) (clients.TransformReader, error) {
				r.calls = append(r.calls, "dbt")
				return nil, nil
			},
		},
	}
}

func TestOpenClientsByKind(t *testing.T) {
	for _, tt := range []struct {
		name  string
		o     *owners.Owners
		write bool
		want  []string
		cdc   bool
	}{
		{"batch only", ownersOf(batchPipe("b")), false, []string{"airflow", "dbt", "warehouse"}, false},
		{"batch only, write mode on", ownersOf(batchPipe("b")), true, []string{"airflow", "dbt", "warehouse"}, false},
		{"cdc only", ownersOf(cdcPipe("a")), false, []string{"a2", "source", "warehouse"}, true},
		{"both", ownersOf(cdcPipe("a"), batchPipe("b")), false, []string{"airflow", "dbt", "a2", "source", "warehouse"}, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			rec := &openRecorder{}
			cfg := devA2Config()
			cfg.AirflowURL = "https://airflow.example.com"
			w, err := openClientsWith(context.Background(), cfg, nil, tt.o, tt.write, rec.openers(t))
			if err != nil {
				t.Fatal(err)
			}
			defer w.Close()
			if !slices.Equal(rec.calls, tt.want) {
				t.Fatalf("opened %v, want %v", rec.calls, tt.want)
			}
			if !tt.cdc {
				if w.Source != nil || w.Connect != nil || w.Kafka != nil || w.Registry != nil {
					t.Fatalf("batch-only set holds a CDC client: %+v", w)
				}
				if len(rec.writers) != 0 {
					t.Fatalf("batch-only set built %d writers", len(rec.writers))
				}
				if w.Builder == nil {
					t.Fatal("no topology builder")
				}
				// Its tools register over the absent clients.
				registered(t, w)
			}
		})
	}
}

func TestOpenBatchReaders(t *testing.T) {
	var gotAirflow config.Airflow
	var gotDBT config.DBT
	op := batchOpeners{
		airflow: func(a config.Airflow) (clients.OrchestratorReader, error) { gotAirflow = a; return nil, nil },
		dbt:     func(d config.DBT) (clients.TransformReader, error) { gotDBT = d; return nil, nil },
	}
	cfg := config.Config{AirflowURL: "https://airflow.example.com", AirflowToken: "secret-token", DBTArtifactsDir: "/srv/dbt/target"}
	if _, _, err := openBatch(cfg, op); err != nil {
		t.Fatal(err)
	}
	if gotAirflow.URL != cfg.AirflowURL || gotAirflow.Token != "secret-token" || gotDBT.ArtifactsDir != "/srv/dbt/target" {
		t.Fatalf("settings not passed: %+v %+v", gotAirflow, gotDBT)
	}
	// A bad setting fails before any reader is built, without the secret.
	bad := config.Config{AirflowURL: "http://airflow.example.com", AirflowToken: "secret-token"}
	_, _, err := openBatch(bad, op)
	if err == nil || strings.Contains(err.Error(), "secret-token") {
		t.Fatalf("err = %v", err)
	}
	// The real openers: off is a nil interface, set is a reader.
	o, tr, err := openBatch(config.Config{}, defaultBatchOpeners)
	if err != nil || o != nil || tr != nil {
		t.Fatalf("off: %v %v %v", o, tr, err)
	}
	o, tr, err = openBatch(cfg, defaultBatchOpeners)
	if err != nil || o == nil || tr == nil {
		t.Fatalf("on: %v %v %v", o, tr, err)
	}
}

// A batch pipeline's own connections: fill the Airflow and dbt settings of its
// set (pipelineConfig over connFields), and its set opens no CDC client.
func TestPipelineConfigBatchKeys(t *testing.T) {
	p := batchPipe("rev")
	p.Connections = owners.Connections{
		"warehouse_db_url":  {Env: "REV_WH"},
		"airflow_url":       {Env: "REV_AIRFLOW_URL"},
		"airflow_token":     {Env: "REV_AIRFLOW_TOKEN"},
		"dbt_artifacts_dir": {Env: "REV_DBT_DIR"},
	}
	pc, err := pipelineConfig(p, env(map[string]string{
		"REV_WH": "wh-rev", "REV_AIRFLOW_URL": "https://airflow.rev", "REV_AIRFLOW_TOKEN": "secret-tok", "REV_DBT_DIR": "/srv/dbt",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if pc.WarehouseDBURL != "wh-rev" || pc.AirflowURL != "https://airflow.rev" || pc.AirflowToken != "secret-tok" || pc.DBTArtifactsDir != "/srv/dbt" {
		t.Fatalf("pipelineConfig = %+v", pc)
	}
	if pc.SourceDBURL != "" || pc.KafkaBrokers != "" || pc.ConnectURL != "" {
		t.Fatalf("a batch pipeline's set got CDC settings: %+v", pc)
	}
}

// batchFamilies are the batch tool families a CDC set must never register.
var batchFamilies = []string{"dag_", "task_", "dbt_"}

// A CDC-only set registers no batch or dbt tool, and its tool list is the one
// it had before the batch packages were wired (TestRegisterToolsCDCUnchanged
// pins the names and order).
func TestRegisterToolsCDCHasNoBatchTools(t *testing.T) {
	for _, o := range []*owners.Owners{ownersOf(), ownersOf(cdcPipe("orders"))} {
		reg, _ := registered(t, &Wired{Owners: o, Orchestrator: &fakeOrch{}, Transform: &fakeTransform{}})
		for _, n := range regNames(reg) {
			if anyPrefix(n, batchFamilies) {
				t.Fatalf("CDC set registers batch tool %s: %v", n, regNames(reg))
			}
		}
	}
}

// fakeOrch is an Airflow reader that also reads a DAG's schedule.
type fakeOrch struct{ runs, dags int }

func (f *fakeOrch) DAGRuns(context.Context, string, int) ([]clients.DAGRun, error) {
	f.runs++
	return nil, nil
}

func (*fakeOrch) TaskInstances(context.Context, string, string) ([]clients.TaskInstance, error) {
	return nil, nil
}

func (*fakeOrch) TaskLogTail(context.Context, string, string, string, int, int) (string, error) {
	return "", nil
}

func (f *fakeOrch) DAG(_ context.Context, id string) (clients.DAG, error) {
	f.dags++
	return clients.DAG{ID: id, Schedule: "@daily"}, nil
}

// fakeOrchNoSchedule is an Airflow reader that cannot read a DAG's schedule.
type fakeOrchNoSchedule struct{}

func (fakeOrchNoSchedule) DAGRuns(context.Context, string, int) ([]clients.DAGRun, error) {
	return nil, nil
}

func (fakeOrchNoSchedule) TaskInstances(context.Context, string, string) ([]clients.TaskInstance, error) {
	return nil, nil
}

func (fakeOrchNoSchedule) TaskLogTail(context.Context, string, string, string, int, int) (string, error) {
	return "", nil
}

// fakeTransform is a dbt reader.
type fakeTransform struct{ results int }

func (f *fakeTransform) RunResults(context.Context) (clients.RunResults, error) {
	f.results++
	return clients.RunResults{}, nil
}

func (*fakeTransform) ModelLineage(_ context.Context, m string) (clients.Lineage, error) {
	return clients.Lineage{Model: m, Parents: []string{}, Children: []string{}}, nil
}

func runTool(t *testing.T, reg *tools.Registry, name, pipeline, in string) (tools.Result, error) {
	t.Helper()
	tool, ok := reg.Get(name)
	if !ok {
		t.Fatalf("%s not registered: %v", name, regNames(reg))
	}
	return tool.Run(tools.WithPipelineID(context.Background(), pipeline), json.RawMessage(in))
}

// A batch set's tools read through the readers Open built: the orchestrator
// for the runs, the same client as the schedule reader for dag_schedule_gap,
// the dbt reader for the run results; and dag_schedule_gap uses w.Now.
func TestBatchToolsUseTheSetsReaders(t *testing.T) {
	orch, tr := &fakeOrch{}, &fakeTransform{}
	clock := 0
	now := func() time.Time { clock++; return time.Date(2026, 10, 18, 9, 0, 0, 0, time.UTC) }
	w := &Wired{Owners: ownersOf(batchPipe("rev")), Orchestrator: orch, Transform: tr, Now: now}
	reg, _ := registered(t, w)

	if _, err := runTool(t, reg, batchtools.NameDAGRuns, "rev", `{"dag_id":"revenue_daily"}`); err != nil {
		t.Fatalf("dag_runs: %v", err)
	}
	if orch.runs == 0 {
		t.Fatal("dag_runs did not read through w.Orchestrator")
	}
	if _, err := runTool(t, reg, batchtools.NameScheduleGap, "rev", `{"dag_id":"revenue_daily"}`); err != nil {
		t.Fatalf("dag_schedule_gap: %v", err)
	}
	if orch.dags == 0 {
		t.Fatal("dag_schedule_gap did not read the schedule through w.Orchestrator")
	}
	if clock == 0 {
		t.Fatal("dag_schedule_gap did not use w.Now")
	}
	if _, err := runTool(t, reg, dbttools.RunResultsTool, "rev", `{}`); err != nil {
		t.Fatalf("dbt_run_results: %v", err)
	}
	if tr.results == 0 {
		t.Fatal("dbt_run_results did not read through w.Transform")
	}
}

// An orchestrator that cannot read a DAG's schedule leaves dag_schedule_gap
// unsupported: wiring passes no Schedule of its own, and batchtools' fallback
// finds no clients.DAGScheduleReader on it.
func TestScheduleGapWithoutScheduleReader(t *testing.T) {
	reg, _ := registered(t, &Wired{Owners: ownersOf(batchPipe("rev")), Orchestrator: fakeOrchNoSchedule{}})
	_, err := runTool(t, reg, batchtools.NameScheduleGap, "rev", `{"dag_id":"revenue_daily"}`)
	if !errors.Is(err, clients.ErrNotSupported) {
		t.Fatalf("dag_schedule_gap without a schedule reader: err = %v, want clients.ErrNotSupported", err)
	}
}

// Without AIRFLOW_URL or dbt settings the batch tools stay registered (the
// tool list does not depend on setup) and answer clients.ErrNotSupported.
func TestBatchToolsWithoutReaders(t *testing.T) {
	reg, _ := registered(t, &Wired{Owners: ownersOf(batchPipe("rev"))})
	for _, c := range []struct{ name, in string }{
		{batchtools.NameDAGRuns, `{"dag_id":"revenue_daily"}`},
		{batchtools.NameTaskInstances, `{"dag_id":"revenue_daily","run_id":"r1"}`},
		{batchtools.NameTaskLogTail, `{"dag_id":"revenue_daily","run_id":"r1","task_id":"load"}`},
		{batchtools.NameScheduleGap, `{"dag_id":"revenue_daily"}`},
		{dbttools.RunResultsTool, `{}`},
		{dbttools.LineageTool, `{"model":"fct_revenue"}`},
		{dbttools.SQLTool, `{"model":"fct_revenue"}`},
	} {
		_, err := runTool(t, reg, c.name, "rev", c.in)
		if !errors.Is(err, clients.ErrNotSupported) {
			t.Errorf("%s without a reader: err = %v, want clients.ErrNotSupported", c.name, err)
		}
	}
}
