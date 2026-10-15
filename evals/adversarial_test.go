package evals

import (
	"strings"
	"testing"

	"piperoom/internal/events"
)

// Adversarial tests of the scorer: answers built to collect points without
// finding the cause, or without the fix resting on it. Each one runs through
// Evaluate, so through a real room.

const (
	seedCause  = "Kafka Connect task 0 failed: the Schema Registry rejected (409, BACKWARD) the new Avro schema after an ALTER TABLE in Postgres changed amount int -> numeric(12,2)."
	seedWrong  = "The consumer order-sync is stuck on a poison pill."
	setCompatV = `{"subject":"pg.public.orders-value","level":"NONE"}`
	restartT0  = `{"connector":"pg-orders-connector","task":0}`
)

func seedEvidence() []step { return goodSchemaChangeScript()[:3] }

func seedFix(steps ...events.ProposalStep) step { return proposal(steps...) }

func goodFix() step {
	return seedFix(ps("schema_set_compatibility", setCompatV), ps("connect_restart_task", restartT0))
}

func hasFailure(r RunResult, sub string) bool {
	return strings.Contains(strings.Join(r.Failures, "; "), sub)
}

// adversarialLists are schema-change answers that list every candidate cause.
var adversarialLists = map[string]string{
	"one of":         "The root cause is one of these: an ALTER TABLE changed amount to numeric so the Schema Registry rejected the Avro schema (409); a crash of the order-sync consumer; topic retention deleting records.",
	"possible":       "Possible root causes: amount changed int -> numeric(12,2) and the Schema Registry returned 409; or order-sync has a bug.",
	"either":         "Either the ALTER TABLE on amount (numeric, Schema Registry 409) or a broker that is down stopped Kafka Connect task 0.",
	"plain list":     "Kafka Connect task 0 failed. The ALTER TABLE changed amount to numeric and the Schema Registry rejected it (409). The order-sync consumer crashed. Retention deleted records from pg.public.orders.",
	"could also be":  seedCause + " It could also be the replication slot that was lost.",
	"not sure which": "Not sure which: the amount change to numeric that the Schema Registry rejected (409), or the slot.",
}

// An answer that lists every candidate cause names the right one too. It
// must never count as the root cause, however it is phrased.
func TestScorerRejectsAListOfEveryCause(t *testing.T) {
	lists := adversarialLists
	for name, text := range lists {
		t.Run(name, func(t *testing.T) {
			script := append(seedEvidence(), step{hypothesis: text}, goodFix())
			r, _ := evalOnce(t, loadSeed(), script)
			if r.RootCause || r.Pass || r.TimeToCause != nil {
				t.Fatalf("a list of causes was graded right: root=%v pass=%v ttc=%v failures=%v", r.RootCause, r.Pass, r.TimeToCause, r.Failures)
			}
			if !hasFailure(r, "wrong root cause") {
				t.Errorf("failures = %v", r.Failures)
			}
			// The terms alone would have matched: the guard is what failed it.
			if !matchGroups(loadSeed().Expect.RootCause, text) {
				t.Errorf("the list does not even hold the root-cause terms; the test proves nothing")
			}
		})
	}
}

// No good hypothesis the tests know reads as a hedge: the guard must not
// cost a committed answer its root cause.
func TestHedgeGuardKeepsCommittedAnswers(t *testing.T) {
	texts := []string{seedCause, slotInactiveHypothesis, ledgerHypothesis}
	for _, h := range hypotheses {
		texts = append(texts, h.good)
	}
	for _, text := range texts {
		if m, ok := causeHedge(text); ok {
			t.Errorf("committed answer read as a hedge on %q: %s", m, text)
		}
	}
}

// The right cause with the wrong fix fails the run: a wrong target, a
// missing step, or extra steps around the right ones.
func TestScorerRejectsRightCauseWrongFix(t *testing.T) {
	fixes := map[string]struct {
		fix  step
		want string
	}{
		"wrong task": {
			seedFix(ps("schema_set_compatibility", setCompatV), ps("connect_restart_task", `{"connector":"pg-orders-connector","task":1}`)),
			"aimed at the wrong target",
		},
		"wrong subject": {
			seedFix(ps("schema_set_compatibility", `{"subject":"pg.public.orders-key","level":"NONE"}`), ps("connect_restart_task", restartT0)),
			"aimed at the wrong target",
		},
		"restart only": {
			seedFix(ps("connect_restart_task", restartT0)),
			"expected fix not proposed",
		},
		"every action": {
			seedFix(ps("schema_set_compatibility", setCompatV), ps("connect_restart_task", restartT0),
				ps("connect_restart_connector", `{"connector":"pg-orders-connector"}`),
				ps("schema_set_compatibility", `{"subject":"pg.public.orders-key","level":"NONE"}`)),
			"aimed at the wrong target",
		},
	}
	for name, tt := range fixes {
		t.Run(name, func(t *testing.T) {
			r, _ := evalOnce(t, loadSeed(), append(seedEvidence(), step{hypothesis: seedCause}, tt.fix))
			if !r.RootCause {
				t.Fatalf("the cause is right; failures = %v", r.Failures)
			}
			if r.Pass || r.FixOK || !hasFailure(r, tt.want) {
				t.Errorf("pass=%v fix_ok=%v failures=%v, want %q", r.Pass, r.FixOK, r.Failures, tt.want)
			}
		})
	}
}

// The right fix, proposed before any evidence, is a guess: it fails even
// when the run reads the evidence afterwards.
func TestScorerRejectsAFixWithoutEvidence(t *testing.T) {
	t.Run("never looked", func(t *testing.T) {
		r, _ := evalOnce(t, loadSeed(), []step{{hypothesis: seedCause}, goodFix()})
		if r.Pass || r.FixOK || !hasFailure(r, "fix proposed before its evidence") {
			t.Errorf("pass=%v fix_ok=%v failures=%v", r.Pass, r.FixOK, r.Failures)
		}
	})
	t.Run("looked afterwards", func(t *testing.T) {
		script := append([]step{{hypothesis: seedCause}, goodFix()}, seedEvidence()...)
		script = append(script, step{hypothesis: seedCause})
		r, _ := evalOnce(t, loadSeed(), script)
		if !r.RootCause || len(r.FactsMissing) > 2 {
			t.Fatalf("the evidence came later and the final hypothesis is right: %+v", r.Failures)
		}
		if r.Pass || r.FixOK || len(r.FixUngrounded) != 1 || !strings.Contains(r.FixUngrounded[0], "f_task_failed not yet observed") {
			t.Errorf("pass=%v fix_ok=%v ungrounded=%v", r.Pass, r.FixOK, r.FixUngrounded)
		}
	})
	t.Run("looked first", func(t *testing.T) {
		r, _ := evalOnce(t, loadSeed(), goodSchemaChangeScript())
		if !r.Pass || !r.FixOK {
			t.Errorf("the good run fails: %v", r.Failures)
		}
	})
}

// A right hypothesis that is then retracted is not a found cause; one that
// comes back counts from its first statement, and the retraction shows.
func TestScorerRetractedHypothesis(t *testing.T) {
	t.Run("right then wrong", func(t *testing.T) {
		script := append(seedEvidence(), step{hypothesis: seedCause}, step{hypothesis: seedWrong}, goodFix())
		r, _ := evalOnce(t, loadSeed(), script)
		if r.Pass || r.RootCause || r.TimeToCause != nil || !r.CauseRetracted {
			t.Errorf("pass=%v root=%v ttc=%+v retracted=%v", r.Pass, r.RootCause, r.TimeToCause, r.CauseRetracted)
		}
		// The older counter still sees the first right one; the new one
		// does not reward it.
		if r.ToolCallsToRootCause != 3 {
			t.Errorf("tool_calls_to_root_cause = %d", r.ToolCallsToRootCause)
		}
	})
	t.Run("right, wrong, right", func(t *testing.T) {
		script := append(seedEvidence(), step{hypothesis: seedCause}, step{hypothesis: seedWrong}, step{hypothesis: seedCause}, goodFix())
		r, _ := evalOnce(t, loadSeed(), script)
		if !r.Pass || !r.CauseRetracted || r.TimeToCause == nil || r.TimeToCause.Hypothesis != 1 {
			t.Errorf("pass=%v retracted=%v ttc=%+v failures=%v", r.Pass, r.CauseRetracted, r.TimeToCause, r.Failures)
		}
	})
	t.Run("wrong then right", func(t *testing.T) {
		script := append(seedEvidence()[:1], step{hypothesis: seedWrong})
		script = append(script, seedEvidence()[1:]...)
		script = append(script, step{hypothesis: seedCause}, goodFix())
		r, _ := evalOnce(t, loadSeed(), script)
		if !r.Pass || r.CauseRetracted || r.TimeToCause == nil {
			t.Fatalf("pass=%v retracted=%v ttc=%+v failures=%v", r.Pass, r.CauseRetracted, r.TimeToCause, r.Failures)
		}
		if got := *r.TimeToCause; got.Hypothesis != 2 || got.ToolCalls != 3 || got.Steps != 3 || got.MS < 0 {
			t.Errorf("time to cause = %+v, want hypothesis 2 after 3 steps and 3 tool calls", got)
		}
	})
}

// Time to cause is measured from the room's opening on the room log's own
// clock.
func TestTimeToCauseFromRoomCreated(t *testing.T) {
	r, tr := evalOnce(t, loadSeed(), goodSchemaChangeScript())
	if r.TimeToCause == nil {
		t.Fatalf("no time to cause: %v", r.Failures)
	}
	var created, hyp *RecordedEvent
	for i := range tr.Events {
		switch tr.Events[i].Type {
		case events.RoomCreated:
			created = &tr.Events[i]
		case events.AgentHypothesis:
			if hyp == nil {
				hyp = &tr.Events[i]
			}
		}
	}
	if created == nil || hyp == nil {
		t.Fatal("room.created or the hypothesis is missing from the room log")
	}
	if want := hyp.At.Sub(created.At).Milliseconds(); r.TimeToCause.MS != want {
		t.Errorf("ms = %d, want %d", r.TimeToCause.MS, want)
	}
	if r.TimeToCause.Steps != 3 || r.TimeToCause.ToolCalls != 3 || r.TimeToCause.Hypothesis != 1 {
		t.Errorf("time to cause = %+v", *r.TimeToCause)
	}
}
