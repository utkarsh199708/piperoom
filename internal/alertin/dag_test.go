package alertin

import (
	"os"
	"path/filepath"
	"testing"

	"piperoom/internal/events"
	"piperoom/internal/owners"
)

// batchOwnersYAML has one cdc pipeline and two batch pipelines. shared_nightly
// is listed by both batch pipelines, which owners.Load allows (one DAG can
// load the models of two pipelines), so the rule for it matters in practice.
// The padded entry checks that the lookup uses the ids as Load trims them.
const batchOwnersYAML = `pipelines:
  - id: orders
    topic_prefix: dbserver1
    source_tables: [public.orders]
    connectors: [pg-orders-connector]
    consumer_groups: [order-sync]
  - id: daily-revenue
    kind: batch
    dags: [revenue_daily, " shared_nightly "]
    models: [fct_revenue]
    sink_tables: [analytics.fct_revenue]
    sink_ts_column: loaded_at
  - id: daily-customers
    kind: batch
    dags: [customers_daily, shared_nightly]
    models: [dim_customer]
layers:
  warehouse: meera
`

// batchOwners loads batchOwnersYAML through owners.Load, so the dags lists are
// what a real file gives.
func batchOwners(t *testing.T) *owners.Owners {
	t.Helper()
	path := filepath.Join(t.TempDir(), "owners.yaml")
	if err := os.WriteFile(path, []byte(batchOwnersYAML), 0o600); err != nil {
		t.Fatalf("write owners: %v", err)
	}
	o, err := owners.Load(path)
	if err != nil {
		t.Fatalf("load owners: %v", err)
	}
	return o
}

func TestMatchDAG(t *testing.T) {
	o := batchOwners(t)
	tests := []struct {
		name string
		ids  []string
		want string
	}{
		{"listed by one pipeline", []string{"revenue_daily"}, "daily-revenue"},
		{"other pipeline", []string{"customers_daily"}, "daily-customers"},
		{"trimmed", []string{"  revenue_daily\t"}, "daily-revenue"},
		{"case-sensitive", []string{"Revenue_Daily"}, ""},
		{"unknown", []string{"no_such_dag"}, ""},
		{"listed by two pipelines", []string{"shared_nightly"}, ""},
		{"shared plus own: the one pipeline listing both", []string{"shared_nightly", "revenue_daily"}, "daily-revenue"},
		{"DAGs of two pipelines", []string{"revenue_daily", "customers_daily"}, ""},
		{"unknown ids are skipped", []string{"no_such_dag", "customers_daily"}, "daily-customers"},
		{"repeats are one id", []string{"revenue_daily", "revenue_daily"}, "daily-revenue"},
		{"a connector name is not a DAG", []string{"pg-orders-connector"}, ""},
		{"a pipeline id is not a DAG", []string{"daily-revenue"}, ""},
		{"a model name is not a DAG", []string{"fct_revenue"}, ""},
		{"blank", []string{" ", ""}, ""},
		{"none", nil, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := MatchDAG(o, tt.ids...); got != tt.want {
				t.Fatalf("MatchDAG(%q) = %q, want %q", tt.ids, got, tt.want)
			}
		})
	}
	if got := MatchDAG(nil, "revenue_daily"); got != "" {
		t.Fatalf("nil owners matched %q", got)
	}
}

// TestMatchDAGKind: only a batch pipeline is matched by a dag_id. owners.Load
// refuses dags on a cdc pipeline, so the Owners is built in code, the one way a
// CDC pipeline can carry a dags list.
func TestMatchDAGKind(t *testing.T) {
	o := &owners.Owners{Pipelines: []owners.Pipeline{
		{ID: "orders", DAGs: []string{"orders_nightly", "shared_dag"}}, // kind unset: cdc
		{ID: "payments", Kind: events.PipelineKindCDC, DAGs: []string{"payments_nightly", "shared_dag"}},
		{ID: "odd", Kind: "streaming", DAGs: []string{"odd_dag"}}, // unknown kind: not batch
		{ID: "daily-revenue", Kind: events.PipelineKindBatch, DAGs: []string{"revenue_daily", "shared_dag", "both_batch"}},
		{ID: "daily-customers", Kind: events.PipelineKindBatch, DAGs: []string{"customers_daily", "both_batch"}},
	}}
	tests := []struct {
		name string
		ids  []string
		want string
	}{
		{"batch pipeline matched", []string{"revenue_daily"}, "daily-revenue"},
		{"other batch pipeline matched", []string{"customers_daily"}, "daily-customers"},
		{"cdc pipeline (default kind) with the dag is not matched", []string{"orders_nightly"}, ""},
		{"cdc pipeline (explicit kind) with the dag is not matched", []string{"payments_nightly"}, ""},
		{"unknown kind is not matched", []string{"odd_dag"}, ""},
		{"dag listed by cdc and batch: the batch one", []string{"shared_dag"}, "daily-revenue"},
		{"no match", []string{"no_such_dag"}, ""},
		{"several batch matches are not guessed", []string{"both_batch"}, ""},
		{"cdc dag plus batch dag in one alert: the batch one", []string{"orders_nightly", "revenue_daily"}, "daily-revenue"},
		{"several matches narrowed by a second id", []string{"both_batch", "customers_daily"}, "daily-customers"},
		{"single id, as room passes it", []string{" revenue_daily "}, "daily-revenue"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := MatchDAG(o, tt.ids...); got != tt.want {
				t.Fatalf("MatchDAG(%q) = %q, want %q", tt.ids, got, tt.want)
			}
		})
	}
	if got := MatchDAG(o); got != "" {
		t.Fatalf("no ids matched %q", got)
	}
}

// TestParseDAGPipeline runs whole payloads through Parse. The default pipeline
// is set, so a row that resolves to ViaDefault shows that the dag_id was
// skipped and the chain went on as it does without one.
func TestParseDAGPipeline(t *testing.T) {
	cfg := testConfig()
	cfg.Owners = batchOwners(t)
	cfg.DefaultPipeline = "fallback"

	am := func(groupLabels, commonLabels, alerts string) string {
		return `{"groupKey":"g","status":"firing","groupLabels":` + groupLabels + `,"commonLabels":` + commonLabels +
			`,"alerts":` + alerts + `}`
	}
	tests := []struct {
		name string
		src  Source
		body string
		id   string
		via  string
	}{
		// Alertmanager.
		{"am: common label", Alertmanager,
			am(`{}`, `{"alertname":"AirflowDagFailed","dag_id":"revenue_daily"}`, `[{"status":"firing"}]`),
			"daily-revenue", ViaDAG},
		{"am: alert label", Alertmanager,
			am(`{}`, `{}`, `[{"status":"firing","labels":{"dag_id":"customers_daily","task_id":"load"}}]`),
			"daily-customers", ViaDAG},
		{"am: group label", Alertmanager,
			am(`{"dag_id":"revenue_daily"}`, `{}`, `[{"status":"firing"}]`),
			"daily-revenue", ViaDAG},
		{"am: unknown dag falls back to default", Alertmanager,
			am(`{}`, `{"dag_id":"no_such_dag"}`, `[{"status":"firing"}]`),
			"fallback", ViaDefault},
		{"am: unknown dag, the owners scan still runs", Alertmanager,
			am(`{}`, `{"dag_id":"no_such_dag","table":"analytics.fct_revenue"}`, `[{"status":"firing"}]`),
			"daily-revenue", ViaOwners},
		{"am: dag listed by two pipelines is not guessed", Alertmanager,
			am(`{}`, `{"dag_id":"shared_nightly"}`, `[{"status":"firing"}]`),
			"fallback", ViaDefault},
		{"am: shared dag, the owners scan decides", Alertmanager,
			am(`{}`, `{"dag_id":"shared_nightly","table":"analytics.fct_revenue"}`, `[{"status":"firing"}]`),
			"daily-revenue", ViaOwners},
		{"am: group with DAGs of two pipelines", Alertmanager,
			am(`{}`, `{}`, `[{"status":"firing","labels":{"dag_id":"revenue_daily"}},{"status":"firing","labels":{"dag_id":"customers_daily"}}]`),
			"fallback", ViaDefault},
		{"am: group with own and shared DAG", Alertmanager,
			am(`{}`, `{}`, `[{"status":"firing","labels":{"dag_id":"shared_nightly"}},{"status":"firing","labels":{"dag_id":"revenue_daily"}}]`),
			"daily-revenue", ViaDAG},
		{"am: resolved alert's dag is not read", Alertmanager,
			am(`{}`, `{}`, `[{"status":"resolved","labels":{"dag_id":"customers_daily"}},{"status":"firing","labels":{"dag_id":"revenue_daily"}}]`),
			"daily-revenue", ViaDAG},
		{"am: pipeline label wins", Alertmanager,
			am(`{}`, `{"pipeline":"orders","dag_id":"revenue_daily"}`, `[{"status":"firing"}]`),
			"orders", ViaLabel},
		{"am: pipeline_id label wins", Alertmanager,
			am(`{}`, `{}`, `[{"status":"firing","labels":{"pipeline_id":"daily-customers","dag_id":"revenue_daily"}}]`),
			"daily-customers", ViaLabel},
		{"am: bad pipeline label is dropped, dag decides", Alertmanager,
			am(`{}`, `{"pipeline":"../etc; drop","dag_id":"revenue_daily"}`, `[{"status":"firing"}]`),
			"daily-revenue", ViaDAG},
		{"am: dag beats the owners scan", Alertmanager,
			am(`{}`, `{"dag_id":"revenue_daily","connector":"pg-orders-connector"}`, `[{"status":"firing"}]`),
			"daily-revenue", ViaDAG},
		{"am: case differs", Alertmanager,
			am(`{}`, `{"dag_id":"REVENUE_DAILY"}`, `[{"status":"firing"}]`),
			"fallback", ViaDefault},
		{"am: a label named dag is not dag_id", Alertmanager,
			am(`{}`, `{"dag":"revenue_daily"}`, `[{"status":"firing"}]`),
			"fallback", ViaDefault},
		// Grafana shares the Alertmanager parser.
		{"grafana: alert label", Grafana,
			`{"groupKey":"g","status":"firing","orgId":1,"alerts":[{"status":"firing","labels":{"dag_id":"revenue_daily"}}]}`,
			"daily-revenue", ViaDAG},

		// Generic.
		{"generic: field", Generic, `{"title":"DAG failed","dag_id":"revenue_daily"}`, "daily-revenue", ViaDAG},
		{"generic: padded field", Generic, `{"dag_id":"  customers_daily "}`, "daily-customers", ViaDAG},
		{"generic: unknown dag falls back to default", Generic, `{"title":"DAG failed","dag_id":"no_such_dag"}`, "fallback", ViaDefault},
		{"generic: unknown dag, the owners scan still runs", Generic,
			`{"dag_id":"no_such_dag","table":"analytics.fct_revenue"}`, "daily-revenue", ViaOwners},
		{"generic: a connector in dag_id reaches only the owners scan", Generic,
			`{"dag_id":"pg-orders-connector"}`, "orders", ViaOwners},
		{"generic: dag listed by two pipelines is not guessed", Generic, `{"dag_id":"shared_nightly"}`, "fallback", ViaDefault},
		{"generic: pipeline_id wins", Generic, `{"pipeline_id":"orders","dag_id":"revenue_daily"}`, "orders", ViaLabel},
		{"generic: bad pipeline_id is dropped, dag decides", Generic,
			`{"pipeline_id":"no spaces allowed","dag_id":"revenue_daily"}`, "daily-revenue", ViaDAG},
		{"generic: non-string dag_id is ignored", Generic, `{"dag_id":42}`, "fallback", ViaDefault},
		{"generic: nested dag_id is not read", Generic, `{"dag":{"dag_id":"revenue_daily"}}`, "fallback", ViaDefault},
		{"generic: dag in the title only is not a dag_id", Generic, `{"title":"revenue_daily failed"}`, "fallback", ViaDefault},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a, err := cfg.Parse(tt.src, []byte(tt.body))
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			if a.PipelineID != tt.id || a.PipelineVia != tt.via {
				t.Fatalf("pipeline %q via %q, want %q via %q", a.PipelineID, a.PipelineVia, tt.id, tt.via)
			}
		})
	}
}

// TestParseDAGWithoutOwners: with no owners.yaml (tests, --demo) a dag_id
// cannot be looked up, and resolution is what it was before dag_id was read.
func TestParseDAGWithoutOwners(t *testing.T) {
	cfg := testConfig()
	cfg.DefaultPipeline = "fallback"
	for _, src := range []Source{Alertmanager, Generic} {
		body := `{"dag_id":"revenue_daily"}`
		if src == Alertmanager {
			body = `{"groupKey":"g","status":"firing","commonLabels":{"dag_id":"revenue_daily"},"alerts":[{"status":"firing"}]}`
		}
		a, err := cfg.Parse(src, []byte(body))
		if err != nil {
			t.Fatalf("%s: parse: %v", src, err)
		}
		if a.PipelineID != "fallback" || a.PipelineVia != ViaDefault {
			t.Fatalf("%s: pipeline %q via %q, want fallback via default", src, a.PipelineID, a.PipelineVia)
		}
	}
}
