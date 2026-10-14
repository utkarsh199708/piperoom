package evals

import (
	"fmt"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"testing"

	"piperoom/internal/owners"
)

const heldoutDir = "scenarios/heldout"

const ledgerHypothesis = "Task 0 of pg-ledger-connector FAILED on a Kafka producer TimeoutException: 412 records expired " +
	"during the broker restart, and Connect does not restart a failed task by itself. The slot kept every change."

// heldoutScripts are focused, correct investigations of the held-out set. They
// prove each scenario can be solved as written and that its recordings cover
// such a run: every one must pass with no replay miss.
func heldoutScripts(t *testing.T) map[string][]step {
	return map[string][]step{
		"payments-sink-dlq": {
			{tool: "connect_status", input: map[string]any{"connector": "jdbc-payments-sink"}},
			{tool: "kafka_consumer_lag", input: map[string]any{"group": "connect-jdbc-payments-sink"}},
			{tool: "warehouse_freshness", input: map[string]any{"table": "analytics.payments", "ts_column": "updated_at"}},
			{tool: "pg_recent_ddl", input: map[string]any{"since": "2026-10-11T08:00:00Z"}},
			{hypothesis: "The JDBC sink connector jdbc-payments-sink cannot write records that carry the new risk_score column " +
				"(added at 08:52; the warehouse table lacks it), and errors.tolerance=all sends every failing record to the " +
				"dead-letter queue while the task stays RUNNING."},
		},
		"inventory-one-connector-slot-lost": {
			{tool: "connect_status", input: map[string]any{"connector": "pg-inventory-stock"}},
			{tool: "connect_status", input: map[string]any{"connector": "pg-inventory-sites"}},
			{tool: "pg_replication_slots", input: map[string]any{}},
			{hypothesis: "pg-inventory-stock task 0 FAILED because its replication slot debezium_inv_stock was invalidated " +
				"(wal_status lost, max_slot_wal_keep_size exceeded); pg-inventory-sites and its slot are healthy."},
		},
		"users-compat-none-consumer": {
			{tool: "kafka_consumer_lag", input: map[string]any{"group": "user-profile-sync"}},
			{tool: "kafka_consumer_lag", input: map[string]any{"group": "user-profile-sync"}},
			{tool: "schema_registry_versions", input: map[string]any{"subject": "usr.public.users-value"}},
			{tool: "schema_registry_diff", input: map[string]any{"subject": "usr.public.users-value", "from_version": 3}},
			{idle: true},
			{hypothesis: "usr.public.users-value has a subject-level compatibility override NONE, so v4 (loyalty_tier int -> string) " +
				"registered; the user-profile-sync consumer cannot read it and crash-loops."},
		},
		"shipments-lag-burst-healthy": {
			{tool: "kafka_consumer_lag", input: map[string]any{"group": "shipment-tracker"}},
			{tool: "kafka_consumer_lag", input: map[string]any{"group": "shipment-tracker"}},
			{tool: "connect_status", input: map[string]any{"connector": "pg-shipments-connector"}},
			{tool: "warehouse_freshness", input: map[string]any{"table": "analytics.shipments", "ts_column": "updated_at"}},
			{idle: true},
			{message: "The pipeline is healthy; no action is needed. The lag from a burst of updates is draining (48210 -> 31877)."},
		},
		"billing-connect-unreachable": {
			{tool: "connect_status", input: map[string]any{"connector": "pg-billing-connector"}},
			{tool: "pg_replication_slots", input: map[string]any{}},
			{tool: "kafka_topic_offsets", input: map[string]any{"topic": "bill.public.invoices"}},
			{hypothesis: "The root cause cannot be determined: Kafka Connect's REST API times out, so the state of " +
				"pg-billing-connector is unknown. Data stops between the active slot and the topic.", confidence: "low"},
		},
		"ledger-producer-timeout-recovery": {
			{tool: "connect_status", input: map[string]any{"connector": "pg-ledger-connector"}},
			{tool: "warehouse_freshness", input: map[string]any{"table": "analytics.ledger_entries", "ts_column": "posted_at"}},
			{hypothesis: ledgerHypothesis},
			{proposeFix: proposeFixJSON(t, "connect_restart_task", `{"connector":"pg-ledger-connector","task":0}`)},
			{idle: true},
			{tool: "connect_status", input: map[string]any{"connector": "pg-ledger-connector"}},
			{tool: "warehouse_freshness", input: map[string]any{"table": "analytics.ledger_entries", "ts_column": "posted_at"}},
			{message: "recovered: task 0 is RUNNING and new ledger rows reach analytics.ledger_entries again."},
		},
	}
}

func TestHeldoutScenariosValidate(t *testing.T) {
	scs, err := LoadScenarios(heldoutDir)
	if err != nil {
		t.Fatal(err)
	}
	defs, err := LiveTools()
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateAll(scs, defs); err != nil {
		t.Fatal(err)
	}
	if len(scs) < 6 {
		t.Fatalf("%d held-out scenarios, want at least 6", len(scs))
	}
	scripts := heldoutScripts(t)
	for _, sc := range scs {
		if _, ok := scripts[sc.Name]; !ok {
			t.Errorf("%s has no focused-run script in heldoutScripts", sc.Name)
		}
		for _, w := range sc.Warnings() {
			if !strings.HasPrefix(w, "expect: ") {
				t.Errorf("%s: unexpected warning: %s", sc.Name, w)
			}
		}
	}
}

func TestHeldoutFocusedRunsPass(t *testing.T) {
	for name, script := range heldoutScripts(t) {
		t.Run(name, func(t *testing.T) {
			sc := loadScenario(t, filepath.Join(heldoutDir, name+".yaml"))
			r, _ := evalOnce(t, sc, script)
			if !r.Pass {
				t.Fatalf("failures = %v", r.Failures)
			}
			if len(r.Misses) != 0 {
				t.Errorf("misses = %+v", r.Misses)
			}
			if r.Score != 100 {
				t.Errorf("score = %v", r.Score)
			}
			if len(sc.Phases) > 0 && !r.VerifiedAfterFix {
				t.Errorf("phased scenario not verified: %+v", r.ReplayResult)
			}
		})
	}
}

// A schema-change-shaped answer must fail each held-out incident.
func TestHeldoutWrongAnswersFail(t *testing.T) {
	tests := []struct {
		name   string
		script []step
	}{
		{"payments-sink-dlq", []step{
			{tool: "connect_status", input: map[string]any{"connector": "pg-payments-source"}},
			{hypothesis: "The risk_score DDL broke the Debezium source connector; Kafka Connect must be restarted."},
			{proposeFix: proposeFixJSON(t, "connect_restart_task", `{"connector":"pg-payments-source","task":0}`)},
		}},
		{"inventory-one-connector-slot-lost", []step{
			{tool: "kafka_consumer_lag", input: map[string]any{"group": "inventory-sync"}},
			{hypothesis: "inventory-sync consumer is stuck on the shared inv prefix; restart pg-inventory-sites."},
		}},
		{"users-compat-none-consumer", []step{
			{tool: "pg_recent_ddl", input: map[string]any{"since": "2026-10-11T08:00:00Z"}},
			{hypothesis: "The loyalty_tier DDL in Postgres broke the consumer."},
		}},
		{"shipments-lag-burst-healthy", []step{
			{tool: "kafka_consumer_lag", input: map[string]any{"group": "shipment-tracker"}},
			{hypothesis: "shipment-tracker consumer is stuck with lag 48210."},
			{proposeFix: proposeFixJSON(t, "connect_restart_task", `{"connector":"pg-shipments-connector","task":0}`)},
		}},
		{"billing-connect-unreachable", []step{
			{tool: "pg_replication_slots", input: map[string]any{}},
			{hypothesis: "pg-billing-connector is paused: a paused connector keeps its Debezium slot active."},
		}},
		{"ledger-producer-timeout-recovery", []step{
			{tool: "connect_status", input: map[string]any{"connector": "pg-ledger-connector"}},
			{hypothesis: ledgerHypothesis},
			{message: "Restarting task 0 will fix it; the pipeline has recovered."},
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sc := loadScenario(t, filepath.Join(heldoutDir, tt.name+".yaml"))
			if r, _ := evalOnce(t, sc, tt.script); r.Pass {
				t.Fatalf("wrong answer passed: %+v", r)
			}
		})
	}
}

// The default set never includes the held-out one: LoadScenarios does not
// recurse, and no name is shared.
func TestDefaultSetExcludesHeldout(t *testing.T) {
	def, err := LoadScenarios("scenarios")
	if err != nil {
		t.Fatal(err)
	}
	held, err := LoadScenarios(heldoutDir)
	if err != nil {
		t.Fatal(err)
	}
	names := map[string]bool{}
	for _, sc := range def {
		names[sc.Name] = true
	}
	for _, sc := range held {
		if names[sc.Name] {
			t.Errorf("held-out scenario %s is in the default set", sc.Name)
		}
		if sc.Pipeline.ID == "orders" {
			t.Errorf("held-out scenario %s uses the orders pipeline", sc.Name)
		}
	}
}

// The held-out fixtures are the source of truth for their pipelines: every
// scenario's pipeline block is the fixture's, and every owner in a rendered
// map is the fixture's layer owner.
func TestHeldoutFixturesMatchScenarios(t *testing.T) {
	o, err := owners.Load(filepath.Join(heldoutDir, "fixtures", "owners.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	scs, err := LoadScenarios(heldoutDir)
	if err != nil {
		t.Fatal(err)
	}
	ownerRe := regexp.MustCompile(`\[([a-z-]+), owner ([a-z]+)\]`)
	seen := map[string]bool{}
	for _, sc := range scs {
		p, ok := o.Pipeline(sc.Pipeline.ID)
		if !ok {
			t.Errorf("%s: pipeline %q is not in the fixture", sc.Name, sc.Pipeline.ID)
			continue
		}
		seen[p.ID] = true
		got := fmt.Sprint(sc.Pipeline.ID, sc.Pipeline.Title, sc.Pipeline.TopicPrefix, sc.Pipeline.SourceTables, sc.Pipeline.Connectors,
			sc.Pipeline.ConsumerGroups, sc.Pipeline.SinkTables, sc.Pipeline.SinkTSColumn)
		want := fmt.Sprint(p.ID, p.Title, p.TopicPrefix, p.SourceTables, p.Connectors, p.ConsumerGroups, p.SinkTables, p.SinkTSColumn)
		if got != want {
			t.Errorf("%s: pipeline block differs from the fixture:\n got %s\nwant %s", sc.Name, got, want)
		}
		texts := []string{sc.Alert.PipelineMap}
		for _, r := range sc.Recordings {
			if r.Tool == "pipeline_map" {
				texts = append(texts, r.Summary)
			}
		}
		for _, text := range texts {
			for _, m := range ownerRe.FindAllStringSubmatch(text, -1) {
				if want, ok := o.LayerOwner(m[1]); !ok || want != m[2] {
					t.Errorf("%s: owner of %s is %q, the fixture says %q", sc.Name, m[1], m[2], want)
				}
			}
		}
		// The alert map is what pipeline_map returns in the start phase.
		for _, r := range sc.Recordings {
			if r.Tool == "pipeline_map" && reflect.DeepEqual(r.Input, map[string]any{"pipeline_id": sc.Pipeline.ID}) &&
				(r.Phase == "" || r.Phase == sc.startPhase()) && r.Summary != sc.Alert.PipelineMap {
				t.Errorf("%s: alert.pipeline_map differs from the start phase's pipeline_map", sc.Name)
			}
		}
	}
	for _, p := range o.Pipelines {
		if !seen[p.ID] {
			t.Errorf("fixture pipeline %s has no scenario", p.ID)
		}
	}
}
