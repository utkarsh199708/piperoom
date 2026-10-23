package evals

import (
	"context"
	"encoding/json"
	"slices"
	"strings"
	"testing"

	"piperoom/internal/actions"
	"piperoom/internal/events"
	"piperoom/internal/tools"
)

// Q6d: the step 1 and 3 to 6 readers in replay and the Q5 lifecycle actions
// on replay clients. The scenarios of Q1e, Q3f, Q4c, Q5f and Q6c record the
// readers since the Q batch 4 review (each summary and data is the real
// tool's output over fake clients holding the scenario's values); the tests
// below grade each scenario on them.

// q6Readers names, per scenario, the step readers its file records and the
// labelled fix it grades (stale-source-offsets only).
var q6Readers = map[string][]string{
	"tolerance-all-skips":              {"connect_worker_metrics"},
	"rebalance-loop":                   {"connect_worker_metrics", "connect_worker_info"},
	"internal-topic-not-compacted":     {"connect_worker_info"},
	"stale-source-offsets":             {"connect_source_offsets"},
	"register-rejected-renamed-key":    {"connect_validate_config"},
	"register-rejected-missing-plugin": {"connect_validate_config"},
	"renamed-property-after-upgrade":   {"connect_plugins"},
	"removed-decoder-plugin":           {"connect_plugins"},
	"advertised-listener-wrong":        {"net_probe"},
	"db-unreachable-from-connect":      {"net_probe"},
}

// q6Fixes are the labelled fixes of the Q6d-graded scenarios, for
// TestEveryScenarioCanPass and the tests below.
var q6Fixes = map[string][]events.ProposalStep{
	"stale-source-offsets": {
		ps("connect_stop_connector", `{"connector":"pg-orders-connector"}`),
		ps("connect_reset_source_offsets", `{"connector":"pg-orders-connector","stop_first":true}`),
	},
}

// requiredFacts returns sc's required facts on tool.
func requiredFacts(sc Scenario, tool string) []Fact {
	var out []Fact
	for _, f := range sc.Expect.ExpectedFacts {
		if f.Required && f.Tool == tool {
			out = append(out, f)
		}
	}
	return out
}

// Each file records its readers, and a required fact on a reader makes it an
// evidence tool.
func TestQ6ScenariosRecordTheirReaders(t *testing.T) {
	for name, readers := range q6Readers {
		sc := loadQ2(t, name)
		for _, tool := range readers {
			if !slices.ContainsFunc(sc.Recordings, func(r Recording) bool { return r.Tool == tool }) {
				t.Errorf("%s: no %s recording", name, tool)
			}
			if len(requiredFacts(sc, tool)) > 0 && !slices.Contains(sc.Expect.EvidenceTools, tool) {
				t.Errorf("%s: required fact on %s, but it is not an evidence tool", name, tool)
			}
		}
		if fix, ok := q6Fixes[name]; ok {
			expectsFix(t, sc, fix)
		}
	}
}

// The step readers are offered to the model with their production
// definitions: kafkatools' optional reads in its own order, then the worker
// metrics, worker logs and network probe after the Debezium tools.
func TestLiveToolsIncludeStepReaders(t *testing.T) {
	defs, err := LiveTools()
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, d := range defs {
		names = append(names, d.Name())
		if d.Kind() != tools.KindRead {
			t.Errorf("%s is %s, want read", d.Name(), d.Kind())
		}
	}
	idx := func(n string) int {
		i := slices.Index(names, n)
		if i < 0 {
			t.Fatalf("%s not offered: %v", n, names)
		}
		return i
	}
	inOrder := func(seq ...string) {
		for i := 1; i < len(seq); i++ {
			if idx(seq[i-1]) >= idx(seq[i]) {
				t.Errorf("%s not before %s: %v", seq[i-1], seq[i], names)
			}
		}
	}
	inOrder("dlq_peek", "connect_validate_config", "kafka_cluster_health", "connect_source_offsets", "connect_plugins", "connect_worker_info", "debezium_metrics")
	inOrder("debezium_metrics", "connect_worker_metrics", "connect_worker_logs", "net_probe", "runbook_lookup")
}

// The four lifecycle actions come after the Q2 source actions, production's
// order (wiring: P3, source actions, lifecycle).
func TestLiveActionsIncludeLifecycleActions(t *testing.T) {
	reg, err := LiveActions(Resources{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, a := range reg.All() {
		names = append(names, a.Name())
	}
	want := []string{actions.StopConnectorName, actions.ResetSourceOffsetsName, actions.CreateConnectorName, actions.DeleteConnectorName}
	i := slices.Index(names, want[0])
	if i < 0 || len(names) < i+len(want) || !slices.Equal(names[i:i+len(want)], want) {
		t.Fatalf("actions = %v, want %v in that order", names, want)
	}
	if j := slices.Index(names, "source_end_session"); j < 0 || j > i {
		t.Errorf("lifecycle actions before the source actions: %v", names)
	}
}

// Done when: with the step readers' recordings, facts and fix, each scenario
// validates and a run that reads them (and proposes the labelled fix) passes
// with full score and no miss.
func TestQ6ScenariosScoreWithStepReaders(t *testing.T) {
	defs, err := LiveTools()
	if err != nil {
		t.Fatal(err)
	}
	for name, readers := range q6Readers {
		t.Run(name, func(t *testing.T) {
			sc := loadQ2(t, name)
			if err := ValidateAll([]Scenario{sc}, defs); err != nil {
				t.Fatalf("scenario does not validate: %v", err)
			}
			fix := q6Fixes[name]
			r, tr := evalOnce(t, sc, canPassScript(t, sc, fix))
			if !r.Pass || r.Score != 100 || len(r.Misses) != 0 {
				t.Fatalf("pass=%v score=%v misses=%v failures=%v attempts=%+v", r.Pass, r.Score, r.Misses, r.Failures, tr.Attempts)
			}
			if r.RejectedAttempts != 0 {
				t.Errorf("rejected attempts: %+v", tr.Attempts)
			}
			if len(fix) > 0 && (!r.FixExpected || r.RightAction == nil || !r.RightAction.OK) {
				t.Errorf("right action = %+v", r.RightAction)
			}
			for _, f := range sc.Expect.ExpectedFacts {
				if slices.Contains(readers, f.Tool) && !slices.Contains(r.FactsFound, f.ID) {
					t.Errorf("fact %s not found: %v", f.ID, r.FactsFound)
				}
			}
		})
	}
}

// A run that skips the new required read no longer passes: the fact is what
// grades the step reader.
func TestQ6RequiredFactsNeedTheReader(t *testing.T) {
	for name, readers := range q6Readers {
		for _, tool := range readers {
			if len(requiredFacts(loadQ2(t, name), tool)) == 0 {
				continue
			}
			t.Run(name+"/"+tool, func(t *testing.T) {
				sc := loadQ2(t, name)
				var script []step
				for _, s := range canPassScript(t, sc, q6Fixes[name]) {
					if s.tool != tool {
						script = append(script, s)
					}
				}
				if r, _ := evalOnce(t, sc, script); r.Pass {
					t.Fatalf("passed without %s", tool)
				}
			})
		}
	}
}

// stale-source-offsets' labelled fix on the scenario's own recordings: the
// reset refuses alone (the connector is RUNNING) and passes after the stop
// step in the same proposal; its Preview shows the stored LSN as the rollback.
func TestQ6StaleOffsetsFixValidatesOnReplay(t *testing.T) {
	fix := q6Fixes["stale-source-offsets"]
	sc := loadQ2(t, "stale-source-offsets")
	reg, err := LiveActions(sc.Resources, sc.Recordings)
	if err != nil {
		t.Fatal(err)
	}
	ctx := tools.WithPipeline(context.Background(), sc.Pipeline)
	stop, _ := reg.Get(actions.StopConnectorName)
	if err := stop.Validate(ctx, fix[0].Params); err != nil {
		t.Fatalf("stop Validate: %v", err)
	}
	reset, _ := reg.Get(actions.ResetSourceOffsetsName)
	if err := reset.Validate(ctx, json.RawMessage(`{"connector":"pg-orders-connector"}`)); err == nil || !strings.Contains(err.Error(), "stop_first") {
		t.Fatalf("reset without the stop step = %v, want the stop-step refusal", err)
	}
	if err := reset.Validate(ctx, fix[1].Params); err != nil {
		t.Fatalf("reset Validate: %v", err)
	}
	pv, err := reset.(actions.Preview).Preview(ctx, fix[1].Params)
	if err != nil {
		t.Fatalf("reset Preview: %v", err)
	}
	if !strings.Contains(pv.Current, "263235684640") || !pv.Reversible || !strings.Contains(pv.Rollback, "263235684640") {
		t.Errorf("preview = %+v, want the stored LSN and a rollback to it", pv)
	}

	// Without a connect_source_offsets recording the reset cannot be checked.
	bare := slices.DeleteFunc(slices.Clone(sc.Recordings), func(r Recording) bool { return r.Tool == "connect_source_offsets" })
	reg, err = LiveActions(sc.Resources, bare)
	if err != nil {
		t.Fatal(err)
	}
	reset, _ = reg.Get(actions.ResetSourceOffsetsName)
	if err := reset.Validate(ctx, fix[1].Params); err == nil || !strings.Contains(err.Error(), "connect_source_offsets") {
		t.Fatalf("reset Validate without the recording = %v", err)
	}
}

// Execute on the replay clients writes nothing: the stop and the reset are
// no-ops recorded in the run's log, and the stop is seen by the reset.
func TestQ6LifecycleWritesAreRecordedNoOps(t *testing.T) {
	fix := q6Fixes["stale-source-offsets"]
	sc := loadQ2(t, "stale-source-offsets")
	reg, wl, err := liveActions(sc.Resources, sc.Recordings)
	if err != nil {
		t.Fatal(err)
	}
	ctx := tools.WithPipeline(context.Background(), sc.Pipeline)
	for _, st := range fix {
		a, _ := reg.Get(st.Action)
		if _, err := a.Execute(ctx, st.Params); err != nil {
			t.Fatalf("%s Execute: %v", st.Action, err)
		}
	}
	got := wl.snapshot()
	if len(got) != 2 || got[0].Method != "StopConnector" || got[1].Method != "ResetSourceOffsets" {
		t.Fatalf("writes = %+v", got)
	}
	for _, n := range writeNotes(got) {
		if n.Target != "pg-orders-connector" {
			t.Errorf("note = %+v", n)
		}
	}
}

// connect_create_connector's Preview reads the connect_validate_config
// recording of the class: the renamed-key config is refused as Connect
// refused it, before anything is created.
func TestQ6CreatePreviewUsesRecordedValidation(t *testing.T) {
	sc := loadQ2(t, "register-rejected-renamed-key")
	sc.Resources.Connectors = nil // deleted: the create step's precondition
	reg, err := LiveActions(sc.Resources, sc.Recordings)
	if err != nil {
		t.Fatal(err)
	}
	create, _ := reg.Get(actions.CreateConnectorName)
	params := json.RawMessage(`{"connector":"pg-orders-connector","config":{"connector.class":"io.debezium.connector.postgresql.PostgresConnector","database.server.name":"pg"}}`)
	ctx := tools.WithPipeline(context.Background(), sc.Pipeline)
	_, err = create.(actions.Preview).Preview(ctx, params)
	if err == nil || !strings.Contains(err.Error(), "topic.prefix") {
		t.Fatalf("create Preview = %v, want Connect's recorded refusal", err)
	}
}

// statusType reads the connector type from connect_status's summary, which
// is the only place the real tool reports it.
func TestStatusType(t *testing.T) {
	for _, c := range []struct{ summary, want string }{
		{"pg-orders-connector (source): RUNNING on connect:8083.", "source"},
		{"pg-orders-connector (sink): PAUSED.", "sink"},
		{"pg-orders-connector (): FAILED:", ""},
		{"other (source): RUNNING.", ""},
	} {
		if got := statusType("pg-orders-connector", c.summary); got != c.want {
			t.Errorf("statusType(%q) = %q, want %q", c.summary, got, c.want)
		}
	}
}
