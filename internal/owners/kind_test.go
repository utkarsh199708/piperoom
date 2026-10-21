package owners

import (
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	"piperoom/internal/events"
)

// batchYAML returns one batch pipeline block (indented for `pipelines:`) with
// the given extra lines appended, each already indented by four spaces.
func batchYAML(id, extra string) string {
	return "  - id: " + id + "\n    kind: batch\n" + extra
}

// loadBatch loads one batch pipeline with the given lines and top-level layers.
func loadBatch(t *testing.T, extra string) (*Owners, error) {
	t.Helper()
	return Load(writeTemp(t, "pipelines:\n"+batchYAML("daily-revenue", extra)+"layers:\n  warehouse: meera\n"))
}

// The example of tasks/P4-pipeline-kinds-batch.md, character for character.
const p4Example = `pipelines:
  - id: daily-revenue
    kind: batch
    dags: [revenue_daily]
    dbt_project: /srv/dbt/analytics      # or dbt Cloud (config)
    models: [fct_revenue, dim_customer]
    sink_tables: [analytics.fct_revenue]
    sink_ts_column: loaded_at
layers:
  orchestrator: priya
  transform: priya
  warehouse: meera
`

func TestKindValues(t *testing.T) {
	cases := []struct {
		name     string
		pipeline string // one block for `pipelines:`
		wantKind string // KindOrDefault; "" when wantErr is set
		wantErr  string // substring; "" = must load
	}{
		{name: "absent is cdc", pipeline: pipelineYAML("orders", ""), wantKind: "cdc"},
		{name: "explicit cdc", pipeline: pipelineYAML("orders", "    kind: cdc\n"), wantKind: "cdc"},
		{name: "empty string is cdc", pipeline: pipelineYAML("orders", "    kind: \"\"\n"), wantKind: "cdc"},
		{name: "null is cdc", pipeline: pipelineYAML("orders", "    kind:\n"), wantKind: "cdc"},
		{name: "batch", pipeline: batchYAML("orders", "    dags: [d1]\n"), wantKind: "batch"},
		{
			name:     "bogus",
			pipeline: pipelineYAML("orders", "    kind: stream\n"),
			wantErr:  `pipeline "orders": kind "stream": want cdc or batch`,
		},
		{
			name:     "upper case is not batch",
			pipeline: pipelineYAML("orders", "    kind: Batch\n"),
			wantErr:  `pipeline "orders": kind "Batch"`,
		},
		{
			name:     "padded value is not batch",
			pipeline: "  - id: orders\n    kind: \" batch\"\n    dags: [d1]\n",
			wantErr:  `pipeline "orders": kind " batch"`,
		},
		{
			name:     "bogus kind on a batch-looking pipeline",
			pipeline: "  - id: orders\n    kind: spark\n    dags: [d1]\n",
			wantErr:  `pipeline "orders": kind "spark"`,
		},
		{
			name:     "list instead of a string",
			pipeline: pipelineYAML("orders", "    kind: [batch]\n"),
			wantErr:  "parse",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			o, err := Load(writeTemp(t, "pipelines:\n"+c.pipeline+sixLayers+"  orchestrator: priya\n  transform: priya\n"))
			if c.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), c.wantErr) {
					t.Fatalf("err = %v, want it to contain %q", err, c.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			p := o.Pipelines[0]
			if got := p.KindOrDefault(); got != c.wantKind {
				t.Errorf("KindOrDefault = %q, want %q", got, c.wantKind)
			}
		})
	}
}

func TestKindOrDefaultBuiltInCode(t *testing.T) {
	var nilPipeline *Pipeline
	for _, c := range []struct {
		name string
		p    *Pipeline
		want string
	}{
		{"nil pipeline", nilPipeline, events.PipelineKindCDC},
		{"zero value", &Pipeline{}, events.PipelineKindCDC},
		{"cdc", &Pipeline{Kind: "cdc"}, events.PipelineKindCDC},
		{"batch", &Pipeline{Kind: "batch"}, events.PipelineKindBatch},
		{"unknown stays as written", &Pipeline{Kind: "stream"}, "stream"},
	} {
		if got := c.p.KindOrDefault(); got != c.want {
			t.Errorf("%s: KindOrDefault = %q, want %q", c.name, got, c.want)
		}
	}
}

func TestBatchRefusesCDCFields(t *testing.T) {
	for _, c := range []struct{ field, line string }{
		{"topic_prefix", "    topic_prefix: pg\n"},
		{"source_tables", "    source_tables: [public.orders]\n"},
		{"connectors", "    connectors: [c1]\n"},
		{"consumer_groups", "    consumer_groups: [g1]\n"},
	} {
		t.Run(c.field, func(t *testing.T) {
			_, err := loadBatch(t, "    dags: [d1]\n"+c.line)
			want := `pipeline "daily-revenue": ` + c.field + " is a cdc field, but the pipeline kind is batch"
			if err == nil || !strings.Contains(err.Error(), want) {
				t.Fatalf("err = %v, want it to contain %q", err, want)
			}
		})
	}
	t.Run("first field in file order wins", func(t *testing.T) {
		_, err := loadBatch(t, "    dags: [d1]\n    connectors: [c1]\n    topic_prefix: pg\n")
		if err == nil || !strings.Contains(err.Error(), "topic_prefix is a cdc field") {
			t.Fatalf("err = %v, want topic_prefix named", err)
		}
	})
	t.Run("an empty list is not a field", func(t *testing.T) {
		if _, err := loadBatch(t, "    dags: [d1]\n    connectors: []\n    source_tables: []\n    consumer_groups: []\n"); err != nil {
			t.Fatalf("empty cdc lists on a batch pipeline: %v", err)
		}
	})
}

func TestCDCRefusesBatchFields(t *testing.T) {
	for _, kind := range []struct{ name, line string }{
		{"implied cdc", ""},
		{"explicit cdc", "    kind: cdc\n"},
	} {
		for _, c := range []struct{ field, line string }{
			{"dags", "    dags: [d1]\n"},
			{"dbt_project", "    dbt_project: /srv/dbt\n"},
			{"models", "    models: [m1]\n"},
		} {
			t.Run(kind.name+"/"+c.field, func(t *testing.T) {
				_, err := Load(writeTemp(t, "pipelines:\n"+pipelineYAML("orders", kind.line+c.line)+sixLayers))
				want := `pipeline "orders": ` + c.field + " is a batch field, but the pipeline kind is cdc"
				if err == nil || !strings.Contains(err.Error(), want) {
					t.Fatalf("err = %v, want it to contain %q", err, want)
				}
				hint := strings.Contains(err.Error(), "write kind: batch")
				if hint != (kind.line == "") {
					t.Errorf("hint to write kind: batch = %v for %s: %v", hint, kind.name, err)
				}
			})
		}
	}
	t.Run("empty values are not fields", func(t *testing.T) {
		extra := "    dags: []\n    models: []\n    dbt_project: \"\"\n"
		if _, err := Load(writeTemp(t, "pipelines:\n"+pipelineYAML("orders", extra)+sixLayers)); err != nil {
			t.Fatalf("empty batch fields on a cdc pipeline: %v", err)
		}
	})
	t.Run("a mistyped kind does not leave the fields unchecked", func(t *testing.T) {
		_, err := Load(writeTemp(t, "pipelines:\n  - id: x\n    kind: btach\n    dags: [d1]\n"+sixLayers))
		if err == nil || !strings.Contains(err.Error(), `kind "btach"`) {
			t.Fatalf("err = %v, want the bad kind named", err)
		}
	})
}

func TestBatchNeedsADAGOrAModel(t *testing.T) {
	cases := []struct {
		name    string
		extra   string
		wantErr string
	}{
		{name: "dags only", extra: "    dags: [d1]\n"},
		{name: "models only", extra: "    models: [m1]\n"},
		{name: "both", extra: "    dags: [d1]\n    models: [m1]\n"},
		{name: "neither", extra: "", wantErr: "needs at least one of dags or models"},
		{name: "empty lists", extra: "    dags: []\n    models: []\n", wantErr: "needs at least one of dags or models"},
		{name: "null lists", extra: "    dags:\n    models:\n", wantErr: "needs at least one of dags or models"},
		{name: "only sink tables", extra: "    sink_tables: [a.b]\n    sink_ts_column: ts\n", wantErr: "needs at least one of dags or models"},
		{name: "only a dbt project", extra: "    dbt_project: /srv/dbt\n", wantErr: "needs at least one of dags or models"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := loadBatch(t, c.extra)
			if c.wantErr == "" {
				if err != nil {
					t.Fatalf("Load: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), c.wantErr) || !strings.Contains(err.Error(), `pipeline "daily-revenue"`) {
				t.Fatalf("err = %v, want pipeline named and %q", err, c.wantErr)
			}
		})
	}
}

func TestBatchNameRules(t *testing.T) {
	cases := []struct {
		name    string
		extra   string
		wantErr string // substring; "" = must load
	}{
		// DAG ids: letters, digits, '_', '.', '-'.
		{name: "dag with dot dash underscore", extra: "    dags: [a.b-c_1, Revenue.Daily]\n"},
		{name: "dag one dot", extra: "    dags: [\".\"]\n", wantErr: `pipeline "daily-revenue": dags: "." is not a DAG id`},
		{name: "dag two dots", extra: "    dags: [\"..\"]\n", wantErr: `dags: ".." is not a DAG id`},
		{name: "dag three dots", extra: "    dags: [d1, \"...\"]\n", wantErr: `dags: "..." is not a DAG id`},
		{name: "dag dots after trim", extra: "    dags: [\" .. \"]\n", wantErr: `dags: ".." is not a DAG id`},
		{name: "dag dotted id", extra: "    dags: [revenue.daily, a..b, .hidden, trailing.]\n"},
		{name: "dag dashes and dots only", extra: "    dags: [\"-.\"]\n"},
		{name: "dag slash", extra: "    dags: [\"a/b\"]\n", wantErr: `dags: "a/b" is not a DAG id`},
		{name: "dag dot dot slash", extra: "    dags: [\"../x\"]\n", wantErr: `dags: "../x" is not a DAG id`},
		{name: "dag space inside", extra: "    dags: [\"a b\"]\n", wantErr: `dags: "a b" is not a DAG id`},
		{name: "dag semicolon", extra: "    dags: [\"a;b\"]\n", wantErr: "is not a DAG id"},
		{name: "dag colon", extra: "    dags: [\"dag:x\"]\n", wantErr: "is not a DAG id"},
		{name: "dag wildcard", extra: "    dags: [\"*\"]\n", wantErr: "is not a DAG id"},
		{name: "dag non-ascii", extra: "    dags: [\"r\u00e9venue\"]\n", wantErr: "is not a DAG id"},
		{name: "dag newline inside", extra: "    dags: [\"a\\nb\"]\n", wantErr: "is not a DAG id"},
		{name: "dag empty", extra: "    dags: [\"\"]\n", wantErr: "dags[0]: empty entry"},
		{name: "dag blank", extra: "    dags: [d1, \"   \"]\n", wantErr: "dags[1]: empty entry"},
		// Models: letters, digits, '_'.
		{name: "model underscore and digits", extra: "    models: [fct_revenue, Dim2]\n"},
		{name: "model dot", extra: "    models: [\"analytics.fct\"]\n", wantErr: `models: "analytics.fct" is not a model name`},
		{name: "model dash", extra: "    models: [\"fct-revenue\"]\n", wantErr: "is not a model name"},
		{name: "model space inside", extra: "    models: [\"fct revenue\"]\n", wantErr: "is not a model name"},
		{name: "model slash", extra: "    models: [\"a/b\"]\n", wantErr: "is not a model name"},
		{name: "model empty", extra: "    models: [\"\"]\n", wantErr: "models[0]: empty entry"},
		{name: "model blank", extra: "    models: [\"\\t\"]\n", wantErr: "models[0]: empty entry"},
		// Duplicates, also after trimming.
		{name: "dag twice", extra: "    dags: [d1, d2, d1]\n", wantErr: `dags lists "d1" twice`},
		{name: "dag twice after trim", extra: "    dags: [d1, \" d1 \"]\n", wantErr: `dags lists "d1" twice`},
		{name: "model twice", extra: "    models: [m1, m1]\n", wantErr: `models lists "m1" twice`},
		{name: "same name in dags and models is fine", extra: "    dags: [x]\n    models: [x]\n"},
		{name: "sink table twice", extra: "    dags: [d1]\n    sink_tables: [a.b, a.b]\n    sink_ts_column: ts\n", wantErr: `sink_tables lists "a.b" twice`},
		// The rules shared with cdc still apply.
		{name: "sink tables need a ts column", extra: "    dags: [d1]\n    sink_tables: [a.b]\n", wantErr: "sink_ts_column is required"},
		{name: "bad retention", extra: "    dags: [d1]\n    retention_days: 0\n", wantErr: "retention_days"},
		{name: "bad auto_approve", extra: "    dags: [d1]\n    auto_approve: [Bad-Name]\n", wantErr: "is not an action name"},
		// Null entries are refused, not dropped.
		{name: "null dag", extra: "    dags: [d1, null]\n", wantErr: "dags: has an empty (null) entry"},
		{name: "null dag in block list", extra: "    dags:\n      - d1\n      -\n", wantErr: "dags: has an empty (null) entry"},
		{name: "null model", extra: "    models: [~]\n", wantErr: "models: has an empty (null) entry"},
		// Unknown keys are still refused (KnownFields): a typo must not drop the scope.
		{name: "dag typo", extra: "    dag: [d1]\n", wantErr: "field dag not found"},
		{name: "model typo", extra: "    dags: [d1]\n    model: [m1]\n", wantErr: "field model not found"},
		{name: "string instead of a list", extra: "    dags: d1\n", wantErr: "parse"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := loadBatch(t, c.extra)
			if c.wantErr == "" {
				if err != nil {
					t.Fatalf("Load: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), c.wantErr) {
				t.Fatalf("err = %v, want it to contain %q", err, c.wantErr)
			}
		})
	}
}

func TestBatchModelsAcceptUniqueID(t *testing.T) {
	cases := []struct {
		name    string
		models  string
		wantErr string // substring; "" = must load
	}{
		{name: "unique_id", models: `[model.pkg_a.orders]`},
		{name: "unique_id with version", models: `[model.pkg_a.orders.v2]`},
		{name: "unique_id next to a bare name", models: `[fct_revenue, model.pkg_a.orders]`},
		{name: "empty package", models: `[model..orders]`, wantErr: "model unique_id"},
		{name: "empty name", models: `[model.pkg_a.]`, wantErr: "model unique_id"},
		{name: "seed is not a model", models: `[seed.pkg_a.orders]`, wantErr: "model unique_id"},
		{name: "space in package", models: `["model.pkg a.orders"]`, wantErr: "model unique_id"},
		{name: "semicolon in package", models: `["model.pkg;a.orders"]`, wantErr: "model unique_id"},
		{name: "quote in package", models: `["model.pkg\"a.orders"]`, wantErr: "model unique_id"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := loadBatch(t, "    dags: [d1]\n    models: "+c.models+"\n")
			if c.wantErr == "" {
				if err != nil {
					t.Fatalf("Load: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), c.wantErr) || !strings.Contains(err.Error(), `pipeline "daily-revenue"`) {
				t.Fatalf("err = %v, want the pipeline named and %q", err, c.wantErr)
			}
		})
	}
}

func TestBatchFieldsAreTrimmed(t *testing.T) {
	o, err := loadBatch(t, "    dags: [\" d1\", \"d2 \"]\n    models: [\"\\tm1 \"]\n    dbt_project: \"  /srv/dbt/analytics \"\n")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	p, _ := o.Pipeline("daily-revenue")
	if want := []string{"d1", "d2"}; !reflect.DeepEqual(p.DAGs, want) {
		t.Errorf("DAGs = %q, want %q", p.DAGs, want)
	}
	if want := []string{"m1"}; !reflect.DeepEqual(p.Models, want) {
		t.Errorf("Models = %q, want %q", p.Models, want)
	}
	if p.DBTProject != "/srv/dbt/analytics" {
		t.Errorf("DBTProject = %q", p.DBTProject)
	}
}

func TestBatchDBTProject(t *testing.T) {
	for _, c := range []struct {
		name, line, want, wantErr string
	}{
		{name: "path", line: "/srv/dbt/analytics", want: "/srv/dbt/analytics"},
		{name: "url", line: "https://cloud.getdbt.com/projects/12", want: "https://cloud.getdbt.com/projects/12"},
		{name: "absent", line: "", want: ""},
		{name: "blank is absent", line: "\"   \"", want: ""},
		{name: "newline inside", line: "\"/srv/dbt\\nanalytics\"", wantErr: "dbt_project"},
		{name: "escape character", line: "\"/srv/\\u001b[31m\"", wantErr: "control character"},
		{name: "delete character", line: "\"/srv/\\u007fx\"", wantErr: "control character"},
		{name: "C1 next line (NEL)", line: "\"/srv/\\u0085analytics\"", wantErr: "control character"},
		{name: "C1 last control", line: "\"/srv/\\u009fx\"", wantErr: "control character"},
		{name: "line separator U+2028", line: "\"/srv/\\u2028analytics\"", wantErr: "control character"},
		{name: "paragraph separator U+2029", line: "\"/srv/\\u2029analytics\"", wantErr: "control character"},
		{name: "non-ascii letters are fine", line: "\"/srv/d\\u00e9p\\u00f4t\"", want: "/srv/dépôt"},
		{name: "inner space is fine", line: "\"/srv/my project\"", want: "/srv/my project"},
	} {
		t.Run(c.name, func(t *testing.T) {
			extra := "    dags: [d1]\n"
			if c.line != "" {
				extra += "    dbt_project: " + c.line + "\n"
			}
			o, err := loadBatch(t, extra)
			if c.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), c.wantErr) {
					t.Fatalf("err = %v, want %q", err, c.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			if got := o.Pipelines[0].DBTProject; got != c.want {
				t.Errorf("DBTProject = %q, want %q", got, c.want)
			}
		})
	}
}

func TestLayersPerKind(t *testing.T) {
	o, err := Load(writeTemp(t, "pipelines:\n"+pipelineYAML("orders", "")+pipelineYAML("legacy", "    kind: cdc\n")+
		batchYAML("daily-revenue", "    dags: [d1]\n")+sixLayers+"  orchestrator: priya\n  transform: priya\n"))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	cdc := []string{"postgres", "kafka-connect", "schema-registry", "kafka", "consumer", "warehouse", "environment"} // U0: the setup layer last
	batch := []string{"orchestrator", "transform", "warehouse"}
	for id, want := range map[string][]string{"orders": cdc, "legacy": cdc, "daily-revenue": batch} {
		p, ok := o.Pipeline(id)
		if !ok {
			t.Fatalf("pipeline %s missing", id)
		}
		got := p.Layers()
		if !slices.Equal(got, want) {
			t.Errorf("%s: Layers = %v, want %v", id, got, want)
		}
		// A caller may change the result; the next call must not see it.
		got[0], got[len(got)-1] = "changed", "changed"
		if again := p.Layers(); !slices.Equal(again, want) {
			t.Errorf("%s: second Layers = %v after the first was changed, want %v", id, again, want)
		}
	}
}

func TestLayersFailClosed(t *testing.T) {
	var nilPipeline *Pipeline
	if got := nilPipeline.Layers(); got != nil {
		t.Errorf("nil pipeline: Layers = %v, want nil", got)
	}
	if got := (&Pipeline{Kind: "stream"}).Layers(); got != nil {
		t.Errorf("unknown kind: Layers = %v, want nil", got)
	}
	// An unknown kind has no layers, so approve-all is refused rather than
	// vacuously allowed.
	o := &Owners{Layers: map[string]Principals{"warehouse": {{KindUser, "meera"}}}, Pipelines: []Pipeline{{ID: "x", Kind: "stream"}}}
	p, _ := o.Pipeline("x")
	if o.OwnsAll("x", p.Layers(), "meera", nil) {
		t.Error("OwnsAll over the layers of an unknown kind must be false")
	}
}

// Every layer a kind lists must be a layer the rest of the package accepts in
// `layers:` and `owners:`.
func TestLayersOfKindsAreKnownLayers(t *testing.T) {
	for _, kind := range []string{events.PipelineKindCDC, events.PipelineKindBatch} {
		p := &Pipeline{Kind: kind}
		for _, l := range p.Layers() {
			if !slices.Contains(events.Layers, l) {
				t.Errorf("kind %s: layer %q is not in events.Layers", kind, l)
			}
		}
	}
}

func TestP4ExampleLoads(t *testing.T) {
	o, err := Load(writeTemp(t, p4Example))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	p, ok := o.Pipeline("daily-revenue")
	if !ok {
		t.Fatal("pipeline daily-revenue missing")
	}
	if p.KindOrDefault() != "batch" || p.Kind != "batch" {
		t.Errorf("kind = %q / %q", p.Kind, p.KindOrDefault())
	}
	if !reflect.DeepEqual(p.DAGs, []string{"revenue_daily"}) {
		t.Errorf("DAGs = %q", p.DAGs)
	}
	if p.DBTProject != "/srv/dbt/analytics" {
		t.Errorf("DBTProject = %q", p.DBTProject)
	}
	if !reflect.DeepEqual(p.Models, []string{"fct_revenue", "dim_customer"}) {
		t.Errorf("Models = %q", p.Models)
	}
	if !reflect.DeepEqual(p.SinkTables, []string{"analytics.fct_revenue"}) || p.SinkTSColumn != "loaded_at" {
		t.Errorf("sink = %q / %q", p.SinkTables, p.SinkTSColumn)
	}
	if p.TopicPrefix != "" || p.Connectors != nil || p.ConsumerGroups != nil || p.SourceTables != nil {
		t.Errorf("a batch pipeline carries cdc fields: %+v", p)
	}
	for _, l := range p.Layers() {
		if !o.IsOwned("daily-revenue", l) {
			t.Errorf("layer %s of daily-revenue is unowned", l)
		}
	}
}

// One file may hold a cdc and a batch pipeline (tasks/P4-pipeline-kinds-batch.md,
// "Done when"); the cdc one keeps its watcher settings.
func TestMixedKindsInOneFile(t *testing.T) {
	doc := "pipelines:\n" +
		pipelineYAML("orders", "    watch:\n      signals:\n        connector_down: {for: 30s}\n") +
		batchYAML("daily-revenue", "    dags: [revenue_daily]\n    models: [fct_revenue]\n") +
		sixLayers + "  orchestrator: priya\n  transform: priya\n"
	o, err := Load(writeTemp(t, doc))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	orders, _ := o.Pipeline("orders")
	if w := orders.Watch(); !w.Enabled || !w.ConnectorDown.Enabled || w.ConnectorDown.For.String() != "30s" {
		t.Errorf("cdc watch settings changed: %+v", w.ConnectorDown)
	}
	if orders.KindOrDefault() != "cdc" {
		t.Errorf("orders kind = %q", orders.KindOrDefault())
	}
	batch, _ := o.Pipeline("daily-revenue")
	if batch.KindOrDefault() != "batch" {
		t.Errorf("daily-revenue kind = %q", batch.KindOrDefault())
	}
}

func TestBatchWatchBlockRefused(t *testing.T) {
	for name, block := range map[string]string{
		"enabled false": "    watch:\n      enabled: false\n",
		"enabled true":  "    watch:\n      enabled: true\n",
		"signal":        "    watch:\n      signals:\n        connector_down: {for: 30s}\n",
		"empty mapping": "    watch: {}\n",
		"dlq topic":     "    watch:\n      dlq_topics:\n        - {topic: x.dlq, written_by: c1}\n",
		"slot":          "    watch:\n      slot: s1\n",
	} {
		t.Run(name, func(t *testing.T) {
			_, err := loadBatch(t, "    dags: [d1]\n"+block)
			want := `pipeline "daily-revenue": watch: not supported for kind batch in this wave`
			if err == nil || !strings.Contains(err.Error(), want) {
				t.Fatalf("err = %v, want it to contain %q", err, want)
			}
		})
	}
}

// Without a watch block a batch pipeline has no watcher signal on, so the
// watcher (which reads Watch()) skips it; this holds for a pipeline built in
// code too.
func TestBatchWatchIsOff(t *testing.T) {
	o, err := loadBatch(t, "    dags: [d1]\n")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	loaded, _ := o.Pipeline("daily-revenue")
	for name, p := range map[string]Pipeline{"loaded": loaded, "built in code": {ID: "x", Kind: "batch"}} {
		w := p.Watch()
		if w.Enabled {
			t.Errorf("%s: Watch().Enabled = true", name)
		}
		for sig, on := range map[string]bool{
			"connector_down": w.ConnectorDown.Enabled, "slot_inactive": w.SlotInactive.Enabled,
			"slot_wal": w.SlotWAL.Enabled, "dlq_growth": w.DLQGrowth.Enabled, "config_risk": w.ConfigRisk.Enabled,
			"consumer_lag": w.ConsumerLag.Enabled, "group_unstable": w.GroupUnstable.Enabled,
			"offset_below_log_start": w.OffsetBelowLogStart.Enabled,
		} {
			if on {
				t.Errorf("%s: signal %s is on", name, sig)
			}
		}
		if len(w.DLQTopics) != 0 {
			t.Errorf("%s: DLQTopics = %v", name, w.DLQTopics)
		}
	}
	// A cdc pipeline built in code still gets the defaults.
	if w := (Pipeline{ID: "c"}).Watch(); !reflect.DeepEqual(w, DefaultWatch()) {
		t.Errorf("cdc built in code: Watch() is not DefaultWatch()")
	}
}

func TestBatchShadowAllowed(t *testing.T) {
	o, err := loadBatch(t, "    dags: [d1]\n    shadow:\n      enabled: true\n")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	p, _ := o.Pipeline("daily-revenue")
	if !p.Shadow() || !o.AnyShadow() {
		t.Error("shadow: enabled on a batch pipeline is not read")
	}
	// A typo inside the block is still refused.
	if _, err := loadBatch(t, "    dags: [d1]\n    shadow:\n      enable: true\n"); err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Errorf("shadow typo: err = %v", err)
	}
}

// The checks that need the kind must not weaken the null-block checks of
// tenant: and connections:.
func TestBatchKeepsNullBlockChecks(t *testing.T) {
	for name, extra := range map[string]string{
		"null tenant":      "    tenant:\n",
		"null connections": "    connections:\n",
	} {
		_, err := loadBatch(t, "    dags: [d1]\n"+extra)
		if err == nil || !strings.Contains(err.Error(), "(null)") {
			t.Errorf("%s: err = %v, want a null-block error", name, err)
		}
	}
}

// A batch pipeline with owners on orchestrator, transform and warehouse works
// end to end through IsOwned, OwnsAll and CanApprove: from the top-level
// layers: and from the pipeline's own owners: block.
func TestBatchOwnershipEndToEnd(t *testing.T) {
	const doc = `pipelines:
  - id: daily-revenue
    kind: batch
    dags: [revenue_daily]
    models: [fct_revenue]
    sink_tables: [analytics.fct_revenue]
    sink_ts_column: loaded_at
    owners:
      transform:
        approvers: [group:analytics-eng]
        backup: [user:sam]
  - id: weekly-kpi
    kind: batch
    models: [kpi_weekly]
layers:
  orchestrator: priya
  warehouse: meera
`
	o, err := Load(writeTemp(t, doc))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	p, _ := o.Pipeline("daily-revenue")
	layers := p.Layers()
	if want := []string{"orchestrator", "transform", "warehouse"}; !slices.Equal(layers, want) {
		t.Fatalf("Layers = %v, want %v", layers, want)
	}
	for _, l := range layers {
		if !o.IsOwned("daily-revenue", l) {
			t.Errorf("IsOwned(%s) = false", l)
		}
	}
	// A cdc layer is not a layer of this pipeline and nobody owns it.
	if o.IsOwned("daily-revenue", "kafka") {
		t.Error("IsOwned(kafka) = true on a batch pipeline nobody owns kafka for")
	}

	for _, c := range []struct {
		user   string
		groups []string
		layer  string
		grant  string
	}{
		{"priya", nil, "orchestrator", "user:priya"},
		{"meera", nil, "warehouse", "user:meera"},
		{"nia", []string{"analytics-eng"}, "transform", "group:analytics-eng"},
		{"sam", nil, "transform", "backup:user:sam"},
	} {
		grant, ok := o.CanApprove("daily-revenue", c.layer, c.user, c.groups)
		if !ok || grant != c.grant {
			t.Errorf("CanApprove(%s, %s) = %q, %v; want %q", c.user, c.layer, grant, ok, c.grant)
		}
	}
	// The pipeline's own transform entry replaces the top-level one: priya
	// owns orchestrator only.
	if _, ok := o.CanApprove("daily-revenue", "transform", "priya", nil); ok {
		t.Error("priya approves transform of daily-revenue")
	}
	// weekly-kpi has no owners: block, and no top-level transform owner.
	if o.IsOwned("weekly-kpi", "transform") {
		t.Error("transform of weekly-kpi is owned by nobody and says it is owned")
	}

	// approve-all over exactly the layers of the kind.
	if o.OwnsAll("daily-revenue", layers, "priya", nil) {
		t.Error("priya owns all three layers of daily-revenue")
	}
	if !o.OwnsAll("daily-revenue", []string{"orchestrator"}, "priya", nil) {
		t.Error("priya does not own orchestrator")
	}
	if !o.OwnsAll("daily-revenue", []string{"transform", "warehouse"}, "meera", []string{"analytics-eng"}) {
		t.Error("meera with the analytics-eng group does not own transform and warehouse")
	}
	// LayersFor sees the batch layers.
	if got := o.LayersFor("priya", nil); !slices.Equal(got, []string{"orchestrator"}) {
		t.Errorf("LayersFor(priya) = %v, want [orchestrator]", got)
	}
	if got := o.LayersFor("nia", []string{"analytics-eng"}); !slices.Equal(got, []string{"transform"}) {
		t.Errorf("LayersFor(nia) = %v, want [transform]", got)
	}
}

func TestSoleOwnerOfAllBatchLayers(t *testing.T) {
	o, err := Load(writeTemp(t, "pipelines:\n"+batchYAML("daily-revenue", "    dags: [d1]\n")+
		"layers:\n  orchestrator: priya\n  transform: priya\n  warehouse: priya\n"))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	p, _ := o.Pipeline("daily-revenue")
	if !o.OwnsAll("daily-revenue", p.Layers(), "priya", nil) {
		t.Error("priya owns every layer of the batch pipeline but OwnsAll is false")
	}
	if o.OwnsAll("daily-revenue", p.Layers(), "meera", nil) {
		t.Error("meera owns nothing and OwnsAll is true")
	}
}

// The shipped dev file has no kind and must load unchanged as a cdc file.
func TestDevOwnersIsCDC(t *testing.T) {
	o, err := Load(filepath.Join("..", "..", "dev", "owners.yaml"))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(o.Pipelines) == 0 {
		t.Fatal("dev/owners.yaml has no pipelines")
	}
	for i := range o.Pipelines {
		p := &o.Pipelines[i]
		if p.KindOrDefault() != "cdc" {
			continue // A1 may add a batch pipeline later (P4g)
		}
		if p.Kind != "" && p.Kind != "cdc" {
			t.Errorf("pipeline %s: Kind = %q", p.ID, p.Kind)
		}
		if p.DAGs != nil || p.Models != nil || p.DBTProject != "" {
			t.Errorf("pipeline %s: a cdc pipeline carries batch fields", p.ID)
		}
		if got := p.Layers(); !slices.Equal(got, events.LayersOf("cdc")) {
			t.Errorf("pipeline %s: Layers = %v", p.ID, got)
		}
		if !p.Watch().Enabled {
			t.Errorf("pipeline %s: the watcher was switched off", p.ID)
		}
	}
}

// A pipeline `owners:` block may name only layers of the pipeline's kind:
// validateRights checks against every known layer, so a wrong-kind entry would
// load and never grant anything.
func TestOwnersBlockMustNameLayersOfTheKind(t *testing.T) {
	const cdcOwners = "    owners:\n      kafka-connect: {approvers: [alice]}\n      warehouse: {approvers: [meera]}\n"
	cases := []struct {
		name     string
		pipeline string
		wantErr  string // substring; "" = must load
	}{
		{
			name:     "cdc with its own layers",
			pipeline: pipelineYAML("orders", cdcOwners),
		},
		{
			name:     "cdc with every layer of the kind",
			pipeline: pipelineYAML("orders", "    owners:\n      postgres: {approvers: [a]}\n      kafka-connect: {approvers: [a]}\n      schema-registry: {approvers: [a]}\n      kafka: {approvers: [a]}\n      consumer: {approvers: [a]}\n      warehouse: {approvers: [a]}\n"),
		},
		{
			name:     "batch with its own layers",
			pipeline: batchYAML("daily-revenue", "    dags: [d1]\n    owners:\n      orchestrator: {approvers: [priya]}\n      transform: {approvers: [priya], backup: [sam]}\n      warehouse: {approvers: [meera]}\n"),
		},
		{
			name:     "batch with no owners block",
			pipeline: batchYAML("daily-revenue", "    dags: [d1]\n"),
		},
		{
			name:     "batch naming a cdc layer",
			pipeline: batchYAML("daily-revenue", "    dags: [d1]\n    owners:\n      kafka-connect: {approvers: [alice]}\n"),
			wantErr:  `pipeline "daily-revenue": owners: layer "kafka-connect" is not a batch layer (want one of orchestrator, transform, warehouse)`,
		},
		{
			name:     "batch naming postgres",
			pipeline: batchYAML("daily-revenue", "    models: [m1]\n    owners:\n      postgres: {approvers: [alice]}\n"),
			wantErr:  `layer "postgres" is not a batch layer`,
		},
		{
			name:     "cdc naming a batch layer (implied kind)",
			pipeline: pipelineYAML("orders", "    owners:\n      orchestrator: {approvers: [alice]}\n"),
			wantErr:  `pipeline "orders": owners: layer "orchestrator" is not a cdc layer (want one of postgres, kafka-connect, schema-registry, kafka, consumer, warehouse, environment)`,
		},
		{
			name:     "cdc naming a batch layer (explicit kind)",
			pipeline: pipelineYAML("orders", "    kind: cdc\n    owners:\n      transform: {approvers: [alice]}\n"),
			wantErr:  `layer "transform" is not a cdc layer`,
		},
		{
			name:     "valid and invalid layer together",
			pipeline: batchYAML("daily-revenue", "    dags: [d1]\n    owners:\n      warehouse: {approvers: [meera]}\n      kafka: {approvers: [alice]}\n"),
			wantErr:  `layer "kafka" is not a batch layer`,
		},
		{
			// Sorted keys: the same file always gives the same error.
			name:     "first wrong layer in sorted order",
			pipeline: batchYAML("daily-revenue", "    dags: [d1]\n    owners:\n      postgres: {approvers: [a]}\n      consumer: {approvers: [a]}\n      kafka: {approvers: [a]}\n"),
			wantErr:  `layer "consumer" is not a batch layer`,
		},
		{
			name:     "unknown layer is still the generic error",
			pipeline: batchYAML("daily-revenue", "    dags: [d1]\n    owners:\n      bogus: {approvers: [a]}\n"),
			wantErr:  `owners: unknown layer "bogus"`,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// Top-level layers: of both kinds are fine in one file: they apply
			// to whichever pipelines have that layer.
			_, err := Load(writeTemp(t, "pipelines:\n"+c.pipeline+sixLayers+"  orchestrator: priya\n  transform: priya\n"))
			if c.wantErr == "" {
				if err != nil {
					t.Fatalf("Load: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), c.wantErr) {
				t.Fatalf("err = %v, want it to contain %q", err, c.wantErr)
			}
		})
	}
	// The error is the same on every load, whatever the map order.
	doc := "pipelines:\n" + batchYAML("daily-revenue", "    dags: [d1]\n    owners:\n      postgres: {approvers: [a]}\n      consumer: {approvers: [a]}\n      kafka: {approvers: [a]}\n      schema-registry: {approvers: [a]}\n") + "layers:\n  warehouse: meera\n"
	path := writeTemp(t, doc)
	var first string
	for i := 0; i < 20; i++ {
		_, err := Load(path)
		if err == nil {
			t.Fatal("Load accepted wrong-kind owners")
		}
		if i == 0 {
			first = err.Error()
		} else if err.Error() != first {
			t.Fatalf("error changed between loads:\n%s\n%s", first, err.Error())
		}
	}
}

// checkNullBlocks reads literal keys only, so a `<<` merge key in a pipeline
// (which could bring in a null dags: entry) is refused.
func TestMergeKeyInPipelineRefused(t *testing.T) {
	const wantErr = "merge key << is not supported in a pipeline"
	cases := []struct {
		name string
		yaml string
	}{
		{
			name: "merged dags with a null entry",
			yaml: "pipelines:\n  - id: daily-revenue\n    kind: batch\n    <<: {dags: [d, ~]}\nlayers:\n  warehouse: meera\n",
		},
		{
			name: "merged models with a null entry",
			yaml: "pipelines:\n  - id: daily-revenue\n    kind: batch\n    dags: [d]\n    <<: {models: [m, ~]}\nlayers:\n  warehouse: meera\n",
		},
		{
			name: "merged auto_approve with a null entry",
			yaml: "pipelines:\n  - id: daily-revenue\n    kind: batch\n    dags: [d]\n    <<: {auto_approve: [~]}\nlayers:\n  warehouse: meera\n",
		},
		{
			name: "merged from an anchor",
			yaml: "pipelines:\n  - &base\n    id: orders\n    topic_prefix: pg\n    source_tables: [public.orders]\n    connectors: [c1]\n  - <<: *base\n    id: orders2\nlayers:\n  warehouse: meera\n",
		},
		{
			name: "merge list",
			yaml: "pipelines:\n  - id: daily-revenue\n    kind: batch\n    dags: [d]\n    <<: [{sink_ts_column: ts}]\nlayers:\n  warehouse: meera\n",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := Load(writeTemp(t, c.yaml))
			if err == nil || !strings.Contains(err.Error(), wantErr) {
				t.Fatalf("err = %v, want it to contain %q", err, wantErr)
			}
		})
	}
	t.Run("a quoted << key is an ordinary unknown key, not a merge", func(t *testing.T) {
		_, err := Load(writeTemp(t, "pipelines:\n  - id: daily-revenue\n    kind: batch\n    dags: [d]\n    \"<<\": {x: 1}\nlayers:\n  warehouse: meera\n"))
		if err == nil || strings.Contains(err.Error(), "merge key") || !strings.Contains(err.Error(), "not found") {
			t.Fatalf("err = %v, want the unknown-field error", err)
		}
	})
	t.Run("no merge key still loads", func(t *testing.T) {
		if _, err := Load(writeTemp(t, p4Example)); err != nil {
			t.Fatalf("Load: %v", err)
		}
	})
}
