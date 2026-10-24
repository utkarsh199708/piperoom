package evals

import (
	"slices"
	"testing"
)

// W4n (KAN-497) applies the leads' rulings R1-R5 (tasks/W4-evals-to-100.md)
// to the scenario labels. These tests pin each applied ruling, so a later
// edit that undoes one fails here and not only in a paid live run.

func w4nScenario(t *testing.T, name string) Scenario {
	t.Helper()
	sc, err := LoadScenario("scenarios/" + name + ".yaml")
	if err != nil {
		t.Fatal(err)
	}
	return sc
}

// TestW4nLayerLabels pins the layer labels R1-R3 rule on.
func TestW4nLayerLabels(t *testing.T) {
	tests := []struct {
		scenario, first, root string
		alt                   []string
	}{
		// R1: Postgres answers every read; the task that never gets past
		// start is the first failing layer, Postgres the root cause.
		{"blocked-slot", "kafka-connect", "postgres", nil},
		// R2: no relabel (P3-W3d-2 stands).
		{"consumer-offsets-out-of-range-retention", "kafka", "kafka", []string{"consumer"}},
		// R3: the uncompacted offsets topic is the most upstream component
		// that no longer does its job. Both root-cause readings stay accepted.
		{"internal-topic-not-compacted", "kafka", "kafka", []string{"kafka-connect"}},
	}
	for _, tt := range tests {
		e := w4nScenario(t, tt.scenario).Expect
		if e.FirstFailingLayer != tt.first || e.RootCauseLayer != tt.root || !slices.Equal(e.RootCauseLayerAlt, tt.alt) {
			t.Errorf("%s: first %q root %q alt %v, want %q %q %v", tt.scenario,
				e.FirstFailingLayer, e.RootCauseLayer, e.RootCauseLayerAlt, tt.first, tt.root, tt.alt)
		}
		if label, ok := syntheticLabels[tt.scenario]; ok && label.Layer != tt.first {
			t.Errorf("%s: syntheticLabels layer %q, want %q", tt.scenario, label.Layer, tt.first)
		}
	}
}

// TestW4nInternalTopicEvidence: the cause rests on the offsets topic's
// config, so the snapshot read is optional (ticket test case 2).
func TestW4nInternalTopicEvidence(t *testing.T) {
	e := w4nScenario(t, "internal-topic-not-compacted").Expect
	var found bool
	for _, f := range e.ExpectedFacts {
		if f.ID == "f_snapshot_completed" {
			found = true
			if f.Required {
				t.Error("f_snapshot_completed.required = true, want false")
			}
		}
	}
	if !found {
		t.Error("f_snapshot_completed missing")
	}
	if want := []string{"kafka_topic_config", "connect_worker_info"}; !slices.Equal(e.EvidenceTools, want) {
		t.Errorf("evidence_tools = %v, want %v", e.EvidenceTools, want)
	}
}

// TestW4nConsumerDuplicatesHealthyWithFinding: R4 reads the absorbed
// redelivery as "healthy now, with a past finding" (prompt rule 7), so a
// hypothesis that says so and states the duplicates passes, while one that
// denies the finding still fails. The lsn group is a stem (ticket test
// case 2: "lsn" and "lsns").
func TestW4nConsumerDuplicatesHealthyWithFinding(t *testing.T) {
	e := w4nScenario(t, "consumer-duplicates").Expect
	for _, row := range e.RootCauseForbidden {
		for _, gone := range []string{"pipeline is healthy", "pipeline healthy", "nothing is wrong"} {
			if len(row) == 1 && row[0] == gone {
				t.Errorf("root_cause_forbidden still holds %q", gone)
			}
		}
	}
	tests := []struct {
		id, text string
		want     bool
	}{
		{"healthy-with-finding", "The pipeline is healthy now and there is nothing to fix. Earlier, 10 change events on pg.public.orders were redelivered: each was written twice with the same key and the same lsn; order-sync upserts by id, so analytics.orders is right.", true},
		{"nothing-wrong-now", "Nothing is wrong with analytics.orders now: 10 change events were delivered twice to pg.public.orders (same key, same lsn), and the upsert sink absorbed the copies.", true},
		{"lsn-stem", "10 events of pg.public.orders were redelivered with their original lsn.", true},
		{"lsns-stem", "10 events of pg.public.orders were redelivered with identical lsns.", true},
		{"false-alarm", "False alarm: the 10 events were redelivered with the same lsn but nothing happened.", false},
		{"inserted-twice", "The application inserted twice: the 10 orders were written again with the same key.", false},
	}
	for _, tt := range tests {
		if got := rootCauseMatches(e, tt.text); got != tt.want {
			t.Errorf("%s: rootCauseMatches = %v, want %v: %q", tt.id, got, tt.want, tt.text)
		}
	}
}

// sinkPausedHypothesis is a correct hypothesis for
// sink-connector-paused-warehouse-stale.
const sinkPausedHypothesis = "wh-orders-sink is PAUSED, not FAILED: Meera paused the sink connector on purpose at 07:31 " +
	"for a migration of analytics.orders, so connect-wh-orders-sink stays Stable but commits nothing and the warehouse is stale."

// TestW4nSinkPausedResumeAllowed: R5 changes the scenario so that a turn-1
// resume of the PAUSED connector is not forbidden. Forbidden actions are not
// scored per turn, so the action leaves the list; a restart stays forbidden.
func TestW4nSinkPausedResumeAllowed(t *testing.T) {
	sc := w4nScenario(t, "sink-connector-paused-warehouse-stale")
	if slices.Contains(sc.Expect.ForbiddenActions, "connect_resume_connector") {
		t.Fatal("connect_resume_connector is still forbidden")
	}
	script := func(proposed ...string) []step {
		return []step{
			{tool: "connect_status", input: map[string]any{"connector": "wh-orders-sink"}},
			{hypothesis: sinkPausedHypothesis},
			proposal(ps(proposed[0], proposed[1])),
			{idle: true},
		}
	}
	r := Judge(sc, transcriptOf(t, sc, script("connect_resume_connector", `{"connector":"wh-orders-sink"}`)), nil, false)
	if len(r.ForbiddenProposed) != 0 {
		t.Errorf("turn-1 resume: forbidden = %v, want none", r.ForbiddenProposed)
	}
	r = Judge(sc, transcriptOf(t, sc, script("connect_restart_connector", `{"connector":"wh-orders-sink"}`)), nil, false)
	if r.Pass || !slices.Equal(r.ForbiddenProposed, []string{"connect_restart_connector"}) {
		t.Errorf("restart: pass=%v forbidden=%v, want a fail on connect_restart_connector", r.Pass, r.ForbiddenProposed)
	}
}
