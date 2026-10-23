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
// on replay clients. The scenarios of Q3f, Q4c and Q5f were written before
// their tools could be replayed; q6Additions are the recordings, facts and
// fix each file gets once this lands (C7 adds them to the YAML), and the
// tests below grade each scenario with them.

// The recordings below are the real tools' output: each summary and data was
// produced by running the tool over fake clients holding the scenario's
// values.
var (
	recWorkerMetricsSkips = Recording{
		Tool:  "connect_worker_metrics",
		Input: map[string]any{"connector": "pg-orders-connector"},
		Summary: "pg-orders-connector: FLAGS: 2187 records skipped: errors.tolerance=all drops them unless a DLQ is set. " +
			"tasks total 1 (running 1, failed 0, paused 0, unassigned 0, restarting 0). " +
			"task 0: 2187 record errors, 2187 record failures, 2187 skipped, 0 retries, 0 DLQ produced, 0 DLQ produce failures, last error 0.8 s ago. " +
			"worker (all connectors): 0 task startup failures of 1 attempts, 0 connector startup failures of 1 attempts, 1 connectors, 1 tasks. " +
			"rebalance: rebalancing=false, 2 completed, epoch 3, last 144.0 h ago, avg 0.2 s, max 0.3 s",
	}
	recWorkerMetricsRebalance = Recording{
		Tool:  "connect_worker_metrics",
		Input: map[string]any{"connector": "pg-orders-connector"},
		Summary: "pg-orders-connector: FLAGS: 1 unassigned tasks; the worker is rebalancing now. " +
			"tasks total 1 (running 0, failed 0, paused 0, unassigned 1, restarting 0). " +
			"task 0: 0 record errors, 0 record failures, 0 skipped, 0 retries, 0 DLQ produced, 0 DLQ produce failures. " +
			"worker (all connectors): 0 task startup failures of 23 attempts, 0 connector startup failures of 23 attempts, 1 connectors, 1 tasks. " +
			"rebalance: rebalancing=true, 46 completed, epoch 61, last 4.0 s ago, avg 3.1 s, max 9.8 s",
	}
	recWorkerInfoNotCompacted = Recording{
		Tool: "connect_worker_info",
		Summary: `Connect worker groups: 1
- group connect-cluster (Stable): 3 workers; runs the pipeline's connectors
  - worker 10.0.4.11 http://connect-1:8083/ leader; runs pg-orders-connector
  - worker 10.0.4.12 http://connect-2:8083/
  - worker 10.0.4.13 http://connect-3:8083/
Internal topics (name match):
- config connect-configs: 1 partitions, RF 3, cleanup compact
- offset connect-offsets: 25 partitions, RF 3, cleanup delete
- status connect-status: 5 partitions, RF 3, cleanup compact
Findings:
- connect-offsets has cleanup.policy=delete; Connect's internal topics must be compacted, or configs, offsets and status are deleted after retention
Plugins: Kafka 3.6.1; 2 plugins (1 source, 1 converter) (connect_plugins lists them)
Log levels: root INFO`,
	}
	recWorkerInfoRebalance = Recording{
		Tool: "connect_worker_info",
		Summary: `Connect worker groups: 1
- group connect-cluster (PreparingRebalance): 5 workers; runs the pipeline's connectors
  - worker 10.0.4.11 http://connect-1:8083/ leader
  - worker 10.0.4.12 http://connect-2:8083/
  - worker 10.0.4.13 http://connect-3:8083/; runs pg-orders-connector
  - worker 10.0.9.21 http://connect-stg-1:8083/
  - worker 10.0.9.22 http://connect-stg-2:8083/
Internal topics (name match):
- config connect-configs: 1 partitions, RF 3, cleanup compact
- offset connect-offsets: 25 partitions, RF 3, cleanup compact
- status connect-status: 5 partitions, RF 3, cleanup compact
- config connect-stg-configs: 1 partitions, RF 3, cleanup compact
- offset connect-stg-offsets: 25 partitions, RF 3, cleanup compact
- status connect-stg-status: 5 partitions, RF 3, cleanup compact
Plugins: Kafka 3.6.1; 2 plugins (1 source, 1 converter) (connect_plugins lists them)
Log levels: root INFO`,
	}
	// LSN{3D/4A11C920}, the position task 0 asks for, is 263235684640.
	recStaleOffsets = Recording{
		Tool:    "connect_source_offsets",
		Input:   map[string]any{"connector": "pg-orders-connector"},
		Summary: "pg-orders-connector partition server=pg: lsn=263235684640, lsn_proc=263235684640, ts_usec=1792735512000000, txId=918223",
		Data: map[string]any{"connectors": []any{map[string]any{
			"connector": "pg-orders-connector",
			"offsets": []any{map[string]any{
				"partition": map[string]any{"server": "pg"},
				"offset":    map[string]any{"lsn": 263235684640, "lsn_proc": 263235684640, "ts_usec": 1792735512000000, "txId": 918223},
			}},
		}}},
	}
	recValidateRenamed = Recording{
		Tool:  "connect_validate_config",
		Input: map[string]any{"class": "io.debezium.connector.postgresql.PostgresConnector"},
		Summary: `1 error in io.debezium.connector.postgresql.PostgresConnector:
  topic.prefix: The 'topic.prefix' value is invalid: A value is required
unknown key "database.server.name": not a known key of this plugin`,
		Data: map[string]any{
			"class": "io.debezium.connector.postgresql.PostgresConnector", "base_connector": "pg-orders-connector",
			"error_count": 1, "errors": map[string]any{"topic.prefix": []any{"The 'topic.prefix' value is invalid: A value is required"}},
			"unknown_keys": []any{map[string]any{"key": "database.server.name"}}, "config_keys": 4,
		},
	}
	// The Q1e workers: Kafka 3.7.1 with Debezium 2.7.3.Final. The agent's
	// own version read (Q1d) calls connect_plugins with {} and reads Data.
	recPlugins273 = Recording{
		Tool: "connect_plugins",
		Summary: `Kafka 3.7.1: 3 plugins
1 source, 2 converter
- source io.debezium.connector.postgresql.PostgresConnector 2.7.3.Final
- converter io.confluent.connect.avro.AvroConverter 7.6.1
- converter org.apache.kafka.connect.json.JsonConverter 3.7.1`,
		Data: map[string]any{
			"kafka_version": "3.7.1", "connect_commit": "e2494e6ffb89f828", "plugin_count": 3,
			"by_type": map[string]any{"converter": 2, "source": 1},
			"plugins": []any{
				map[string]any{"class": "io.debezium.connector.postgresql.PostgresConnector", "type": "source", "version": "2.7.3.Final"},
				map[string]any{"class": "io.confluent.connect.avro.AvroConverter", "type": "converter", "version": "7.6.1"},
				map[string]any{"class": "org.apache.kafka.connect.json.JsonConverter", "type": "converter", "version": "3.7.1"},
			},
		},
	}
	recValidateMissingPlugin = Recording{
		Tool:  "connect_validate_config",
		Input: map[string]any{"class": "io.debezium.connector.postgresql.PostgresConnector"},
		Error: "connect_validate_config: io.debezium.connector.postgresql.PostgresConnector: connect: PUT /connector-plugins/io.debezium.connector.postgresql.PostgresConnector/config/validate: not found",
	}
)

// q6Addition is what one scenario gets: recordings, required facts on them
// (each also an evidence tool), a labelled fix, and the data of its
// connect_config recording (the tool's Data is the shown config, as the
// summary lists it), which the reset's Preview reads.
type q6Addition struct {
	recs       []Recording
	facts      []Fact
	fix        []events.ProposalStep
	configData map[string]any
}

var q6Additions = map[string]q6Addition{
	"tolerance-all-skips": {
		recs:  []Recording{recWorkerMetricsSkips},
		facts: []Fact{{ID: "f_records_skipped", Tool: "connect_worker_metrics", Match: []string{"records skipped", "errors tolerance all"}, Required: true}},
	},
	"rebalance-loop": {
		recs: []Recording{recWorkerMetricsRebalance, recWorkerInfoRebalance},
		facts: []Fact{
			{ID: "f_rebalancing", Tool: "connect_worker_metrics", Match: []string{"rebalancing true"}, Required: true},
			{ID: "f_staging_workers", Tool: "connect_worker_info", Match: []string{"5 workers", "connect stg"}},
		},
	},
	"internal-topic-not-compacted": {
		recs:  []Recording{recWorkerInfoNotCompacted},
		facts: []Fact{{ID: "f_offsets_not_compacted", Tool: "connect_worker_info", Match: []string{"connect offsets", "cleanup policy delete"}, Required: true}},
	},
	"stale-source-offsets": {
		recs:  []Recording{recStaleOffsets},
		facts: []Fact{{ID: "f_stored_offset", Tool: "connect_source_offsets", Match: []string{"partition server pg", "lsn 263235684640"}, Required: true}},
		fix: []events.ProposalStep{
			ps("connect_stop_connector", `{"connector":"pg-orders-connector"}`),
			ps("connect_reset_source_offsets", `{"connector":"pg-orders-connector","stop_first":true}`),
		},
		configData: map[string]any{
			"connector.class": "io.debezium.connector.postgresql.PostgresConnector", "database.dbname": "devctl_orders_v2",
			"database.hostname": "postgres", "database.password": "***", "database.port": "5432", "database.user": "piperoom",
			"slot.name": "devctl_orders_v2", "table.include.list": "public.orders", "topic.prefix": "pg",
		},
	},
	"register-rejected-renamed-key": {
		recs:  []Recording{recValidateRenamed},
		facts: []Fact{{ID: "f_validate_unknown_key", Tool: "connect_validate_config", Match: []string{"unknown key", "database server name"}}},
	},
	// The validate read fails (no plugin), so it is a recording, not a fact.
	"register-rejected-missing-plugin": {recs: []Recording{recValidateMissingPlugin}},
	"renamed-property-after-upgrade": {
		recs:  []Recording{recPlugins273},
		facts: []Fact{{ID: "f_debezium_version", Tool: "connect_plugins", Match: []string{"2.7.3.final"}}},
	},
	"removed-decoder-plugin": {
		recs:  []Recording{recPlugins273},
		facts: []Fact{{ID: "f_debezium_version", Tool: "connect_plugins", Match: []string{"2.7.3.final"}}},
	},
}

// withQ6 returns sc with add applied, as the YAML will read once C7 adds it.
func withQ6(sc Scenario, add q6Addition) Scenario {
	sc.Recordings = append(slices.Clone(sc.Recordings), add.recs...)
	if add.configData != nil {
		for i, r := range sc.Recordings {
			if r.Tool == "connect_config" && r.Input["connector"] == "pg-orders-connector" {
				sc.Recordings[i].Data = add.configData
				break
			}
		}
	}
	sc.Expect.ExpectedFacts = append(slices.Clone(sc.Expect.ExpectedFacts), add.facts...)
	sc.Expect.EvidenceTools = slices.Clone(sc.Expect.EvidenceTools)
	for _, f := range add.facts {
		if f.Required && !slices.Contains(sc.Expect.EvidenceTools, f.Tool) {
			sc.Expect.EvidenceTools = append(sc.Expect.EvidenceTools, f.Tool)
		}
	}
	if len(add.fix) > 0 {
		sc.Expect.Fix = nil
		sc.Expect.FixTargets = map[string]map[string]any{}
		for _, st := range add.fix {
			sc.Expect.Fix = append(sc.Expect.Fix, []string{st.Action})
			sc.Expect.FixTargets[st.Action] = map[string]any{"connector": "pg-orders-connector"}
		}
	}
	return sc
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
	for name, add := range q6Additions {
		t.Run(name, func(t *testing.T) {
			sc := withQ6(loadQ2(t, name), add)
			if err := ValidateAll([]Scenario{sc}, defs); err != nil {
				t.Fatalf("scenario with the Q6d additions does not validate: %v", err)
			}
			r, tr := evalOnce(t, sc, canPassScript(t, sc, add.fix))
			if !r.Pass || r.Score != 100 || len(r.Misses) != 0 {
				t.Fatalf("pass=%v score=%v misses=%v failures=%v attempts=%+v", r.Pass, r.Score, r.Misses, r.Failures, tr.Attempts)
			}
			if r.RejectedAttempts != 0 {
				t.Errorf("rejected attempts: %+v", tr.Attempts)
			}
			if len(add.fix) > 0 && (!r.FixExpected || r.RightAction == nil || !r.RightAction.OK) {
				t.Errorf("right action = %+v", r.RightAction)
			}
			for _, f := range add.facts {
				if !slices.Contains(r.FactsFound, f.ID) {
					t.Errorf("fact %s not found: %v", f.ID, r.FactsFound)
				}
			}
		})
	}
}

// A run that skips the new required read no longer passes: the fact is what
// grades the step reader.
func TestQ6RequiredFactsNeedTheReader(t *testing.T) {
	for name, add := range q6Additions {
		var required []Fact
		for _, f := range add.facts {
			if f.Required {
				required = append(required, f)
			}
		}
		if len(required) == 0 {
			continue
		}
		t.Run(name, func(t *testing.T) {
			sc := withQ6(loadQ2(t, name), add)
			full := canPassScript(t, sc, add.fix)
			var script []step
			for _, s := range full {
				if s.tool != required[0].Tool {
					script = append(script, s)
				}
			}
			if r, _ := evalOnce(t, sc, script); r.Pass {
				t.Fatalf("passed without %s", required[0].Tool)
			}
		})
	}
}

// stale-source-offsets' labelled fix on the scenario's own recordings: the
// reset refuses alone (the connector is RUNNING) and passes after the stop
// step in the same proposal; its Preview shows the stored LSN as the rollback.
func TestQ6StaleOffsetsFixValidatesOnReplay(t *testing.T) {
	add := q6Additions["stale-source-offsets"]
	sc := withQ6(loadQ2(t, "stale-source-offsets"), add)
	reg, err := LiveActions(sc.Resources, sc.Recordings)
	if err != nil {
		t.Fatal(err)
	}
	ctx := tools.WithPipeline(context.Background(), sc.Pipeline)
	stop, _ := reg.Get(actions.StopConnectorName)
	if err := stop.Validate(ctx, add.fix[0].Params); err != nil {
		t.Fatalf("stop Validate: %v", err)
	}
	reset, _ := reg.Get(actions.ResetSourceOffsetsName)
	if err := reset.Validate(ctx, json.RawMessage(`{"connector":"pg-orders-connector"}`)); err == nil || !strings.Contains(err.Error(), "stop_first") {
		t.Fatalf("reset without the stop step = %v, want the stop-step refusal", err)
	}
	if err := reset.Validate(ctx, add.fix[1].Params); err != nil {
		t.Fatalf("reset Validate: %v", err)
	}
	pv, err := reset.(actions.Preview).Preview(ctx, add.fix[1].Params)
	if err != nil {
		t.Fatalf("reset Preview: %v", err)
	}
	if !strings.Contains(pv.Current, "263235684640") || !pv.Reversible || !strings.Contains(pv.Rollback, "263235684640") {
		t.Errorf("preview = %+v, want the stored LSN and a rollback to it", pv)
	}

	// Without a connect_source_offsets recording the reset cannot be checked.
	bare := loadQ2(t, "stale-source-offsets")
	reg, err = LiveActions(bare.Resources, bare.Recordings)
	if err != nil {
		t.Fatal(err)
	}
	reset, _ = reg.Get(actions.ResetSourceOffsetsName)
	if err := reset.Validate(ctx, add.fix[1].Params); err == nil || !strings.Contains(err.Error(), "connect_source_offsets") {
		t.Fatalf("reset Validate without the recording = %v", err)
	}
}

// Execute on the replay clients writes nothing: the stop and the reset are
// no-ops recorded in the run's log, and the stop is seen by the reset.
func TestQ6LifecycleWritesAreRecordedNoOps(t *testing.T) {
	add := q6Additions["stale-source-offsets"]
	sc := withQ6(loadQ2(t, "stale-source-offsets"), add)
	reg, wl, err := liveActions(sc.Resources, sc.Recordings)
	if err != nil {
		t.Fatal(err)
	}
	ctx := tools.WithPipeline(context.Background(), sc.Pipeline)
	for _, st := range add.fix {
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
	add := q6Additions["register-rejected-renamed-key"]
	sc := withQ6(loadQ2(t, "register-rejected-renamed-key"), add)
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
