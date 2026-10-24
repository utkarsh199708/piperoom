package agent_test

import (
	"testing"

	"piperoom/evals"
)

// TestHypothesisFormGradesBothLayers checks rule 4's hypothesis form
// (W4f, KAN-504: `"First failing: <component>. Root cause: <component>."`;
// W4g, KAN-506: `"First failing: <layer> <component>. Root cause: <layer>
// <component>."`) against the eval scorer's own keyword pass
// (evals.GradeKeywords, the pass the evals-live run grades with). A
// hypothesis in that form must be graded as naming the scenario's first
// failing layer and its root-cause layer, with neither item left ambiguous
// for the judge. It also pins the shapes the fix rounds saw fail: the merged
// "First failing and root-cause component: X" names one layer only; a bare
// layer word "kafka" names no layer (the scorer matches Kafka by its parts:
// topic, broker, partition), which is why the form asks for the layer and
// the component; and W4g's run 40077638804 put the root cause in the
// component that failed on it, with no layer word for the real one
// (schema-change: the registry; injection-kafka-record-key: a record key).
// The same stories in the W4g form, the layer named, grade both layers.
//
// The test lives in an external package so the agent package itself never
// imports evals (prompthash.go); the scenarios are read, never written.
func TestHypothesisFormGradesBothLayers(t *testing.T) {
	cases := []struct {
		name, scenario, text          string
		wantLayer, wantRootCauseLayer evals.Grade
	}{
		// The exact sentence form named in the fix-round ruling.
		{"connect first, postgres cause", "setup-wal-level-replica",
			"First failing: kafka-connect task 0. Root cause: postgres.", evals.GradePass, evals.GradePass},
		{"connect first, postgres cause (toast)", "toast-placeholder",
			"First failing: kafka-connect task 0. Root cause: postgres.", evals.GradePass, evals.GradePass},
		{"connect first, postgres cause (unreachable)", "db-unreachable-from-connect",
			"First failing: kafka-connect task 0. Root cause: postgres.", evals.GradePass, evals.GradePass},
		{"connect first, kafka cause", "broker-disk-notenoughreplicas",
			"First failing: kafka-connect task 0. Root cause: broker 2.", evals.GradePass, evals.GradePass},
		{"consumer first, kafka cause", "long-room-compaction",
			"First failing: consumer order-sync. Root cause: topic pg.public.orders partition 1.", evals.GradePass, evals.GradePass},
		{"same component named twice", "dead-slot-wal",
			"First failing: postgres replication slot debezium_orders. Root cause: postgres replication slot debezium_orders.", evals.GradePass, evals.GradePass},
		// The merged form names the consumer only: the root-cause layer fails.
		{"merged form", "long-room-compaction",
			"First failing and root-cause component: consumer group order-sync.", evals.GradePass, evals.GradeFail},
		// A bare layer word is not a Kafka component.
		{"bare kafka layer word", "long-room-compaction",
			"First failing: consumer order-sync. Root cause: kafka.", evals.GradePass, evals.GradeFail},
		// W4g (KAN-506): the <layer> <component> form on the three targets
		// whose root-cause layer went unnamed in evals-live 40077638804.
		{"w4g form: schema-change", "schema-change",
			"First failing: kafka-connect pg-orders-connector task 0 (FAILED, s_1). Root cause: postgres public.orders, where ALTER COLUMN amount TYPE numeric(12,2) changed the value schema (s_2).",
			evals.GradePass, evals.GradePass},
		{"w4g form: injection-kafka-record-key", "injection-kafka-record-key",
			"First failing: consumer order-sync. Root cause: kafka topic pg.public.orders partition 1. A record there has a key that is not in wire format (s_2); order-sync crash-loops on it (s_1).",
			evals.GradePass, evals.GradePass},
		// The form ends the label at the component: a negated description
		// in the label's own clause leaves the layer to the judge.
		{"w4g form, description in the label's clause", "injection-kafka-record-key",
			"First failing: consumer order-sync. Root cause: kafka topic pg.public.orders partition 1, a record whose key is not in wire format (s_2).",
			evals.GradePass, evals.GradeAmbiguous},
		{"w4g form: tolerance-all-skips", "tolerance-all-skips",
			"First failing: kafka-connect pg-orders-connector task 0, records skipped (s_1). Root cause: kafka-connect pg-orders-connector, whose error setting skips every record that fails conversion since the amount type change (s_2).",
			evals.GradePass, evals.GradePass},
		// The run's own shapes, without the real root-cause layer.
		{"run 40077638804: registry named as the cause", "schema-change",
			"First failing: pg-orders-connector task 0 (FAILED, s_1). Root cause: Schema Registry rejects the new value schema after the ALTER COLUMN amount type change (s_2).",
			evals.GradePass, evals.GradeFail},
		{"run 40077638804: record key only", "injection-kafka-record-key",
			"First failing: consumer order-sync (s_1). Root cause: a record key that is not in wire format (s_2).",
			evals.GradePass, evals.GradeFail},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			sc, err := evals.LoadScenario("../../evals/scenarios/" + c.scenario + ".yaml")
			if err != nil {
				t.Fatal(err)
			}
			g := evals.GradeKeywords(sc, c.text)
			if g.Layer != c.wantLayer {
				t.Errorf("first failing layer %q: grade %s, want %s (%q; reasons %v)", sc.Expect.FirstFailingLayer, g.Layer, c.wantLayer, c.text, g.Ambiguity.Reasons)
			}
			if g.RootCauseLayer != c.wantRootCauseLayer {
				t.Errorf("root-cause layer %q: grade %s, want %s (%q; reasons %v)", sc.Expect.RootCauseLayer, g.RootCauseLayer, c.wantRootCauseLayer, c.text, g.Ambiguity.Reasons)
			}
		})
	}
}
