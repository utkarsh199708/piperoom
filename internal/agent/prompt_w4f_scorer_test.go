package agent_test

import (
	"testing"

	"piperoom/evals"
)

// TestHypothesisFormGradesBothLayers checks rule 4's hypothesis form
// (W4f, KAN-504: `"First failing: <component>. Root cause: <component>."`)
// against the eval scorer's own keyword pass (evals.GradeKeywords, the pass
// the evals-live run grades with). A hypothesis in that form must be graded
// as naming the scenario's first failing layer and its root-cause layer,
// with neither item left ambiguous for the judge. It also pins the two
// shapes the fix round saw fail: the merged "First failing and root-cause
// component: X" names one layer only, and a bare layer word "kafka" names
// no layer (the scorer matches Kafka by its parts: topic, broker,
// partition), which is why the form asks for a component, not a layer.
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
