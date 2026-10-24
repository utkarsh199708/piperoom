package agent

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"piperoom/internal/events"
	"piperoom/internal/setup/plan"
)

// The unit tests of the setup mode's bounds and counters (setup.go); the
// prompt frame is pinned by setup_prompt_test.go, the loop behaviour by
// setup_recorded_test.go.

// TestSetupBoundsFillAndValidate: nil bounds mean the design's defaults, a
// negative bound is refused (a caller's bug, never silently corrected).
func TestSetupBoundsFillAndValidate(t *testing.T) {
	s, err := newSetupRun(nil)
	if err != nil {
		t.Fatalf("newSetupRun(nil): %v", err)
	}
	d := plan.DefaultBounds()
	if s.bounds != d {
		t.Fatalf("nil bounds = %+v, want the design defaults %+v", s.bounds, d)
	}
	if _, err := newSetupRun(&SetupBounds{AttemptsPerStep: -1}); err == nil {
		t.Fatal("negative AttemptsPerStep accepted")
	}
	if _, err := newSetupRun(&SetupBounds{StepsPerPlan: -1}); err == nil {
		t.Fatal("negative StepsPerPlan accepted")
	}
	if _, err := newSetupRun(&SetupBounds{WallClock: -time.Minute}); err == nil {
		t.Fatal("negative WallClock accepted")
	}
	if _, err := newSetupRun(&SetupBounds{TokenBudget: -1}); err == nil {
		t.Fatal("negative TokenBudget accepted")
	}
	// A caller's bounds fill only the fields it set.
	s, err = newSetupRun(&SetupBounds{AttemptsPerStep: 5})
	if err != nil {
		t.Fatalf("newSetupRun(attempts 5): %v", err)
	}
	if s.bounds.AttemptsPerStep != 5 || s.bounds.StepsPerPlan != d.StepsPerPlan || s.bounds.WallClock != d.WallClock || s.bounds.TokenBudget != d.TokenBudget {
		t.Fatalf("partial bounds = %+v, want defaults filled", s.bounds)
	}
}

// TestSetupTokenBudget: the endpoint's cap is the setup's token budget, the
// default when the bounds are unset, a negative value refused.
func TestSetupTokenBudget(t *testing.T) {
	got, err := setupTokenBudget(nil)
	if err != nil || got != plan.DefaultTokenBudget {
		t.Fatalf("setupTokenBudget(nil) = %d, %v; want the default %d", got, err, plan.DefaultTokenBudget)
	}
	got, err = setupTokenBudget(&SetupBounds{})
	if err != nil || got != plan.DefaultTokenBudget {
		t.Fatalf("setupTokenBudget(zero) = %d, %v; want the default", got, err)
	}
	got, err = setupTokenBudget(&SetupBounds{TokenBudget: 700})
	if err != nil || got != 700 {
		t.Fatalf("setupTokenBudget(700) = %d, %v", got, err)
	}
	if _, err := setupTokenBudget(&SetupBounds{TokenBudget: -3}); err == nil {
		t.Fatal("negative setup token budget accepted")
	}
}

// TestSetupObserve counts executed steps once per (proposal, step), resets the
// attempts on a success and counts only explicit failures.
func TestSetupObserve(t *testing.T) {
	s, err := newSetupRun(nil)
	if err != nil {
		t.Fatalf("newSetupRun: %v", err)
	}
	step0 := 0
	step1 := 1
	ok, bad := true, false
	s.observe([]HumanInput{
		{Kind: "action_result", ProposalID: "p_1", StepIndex: &step0, OK: &bad},
		{Kind: "action_result", ProposalID: "p_1", StepIndex: &step0, OK: &bad}, // a repeated result: counted once
		{Kind: "action_result", ProposalID: "p_2", StepIndex: &step0, OK: &bad},
		{Kind: "action_result", ProposalID: "p_3", StepIndex: &step1, OK: &ok}, // a success resets the attempts
		{Kind: "action_result", ProposalID: "p_4", StepIndex: nil},             // unknown outcome: executed, neither failure nor success
		{Kind: "steer"}, // not an execution
	})
	if s.executed != 4 {
		t.Fatalf("executed = %d, want 4 (the repeated result counted once)", s.executed)
	}
	if s.attempts != 0 {
		t.Fatalf("attempts = %d, want 0 after a success", s.attempts)
	}
	// Consecutive failures of different steps stop the run at the same bound
	// as two of one step, never later.
	s.observe([]HumanInput{
		{Kind: "action_result", ProposalID: "p_5", StepIndex: &step0, OK: &bad},
		{Kind: "action_result", ProposalID: "p_6", StepIndex: &step0, OK: &bad},
		{Kind: "action_result", ProposalID: "p_7", StepIndex: &step0, OK: &bad},
	})
	if s.attempts != 3 {
		t.Fatalf("attempts = %d, want 3", s.attempts)
	}
	if reason := s.exhausted(); reason != events.SetupBlockedAttempts {
		t.Fatalf("exhausted = %q, want %q", reason, events.SetupBlockedAttempts)
	}
}

// TestSetupNotePlan: a new setup_plan restarts both counters; the wall clock
// does not.
func TestSetupNotePlan(t *testing.T) {
	s, err := newSetupRun(nil)
	if err != nil {
		t.Fatalf("newSetupRun: %v", err)
	}
	step0 := 0
	bad := false
	s.observe([]HumanInput{{Kind: "action_result", ProposalID: "p_1", StepIndex: &step0, OK: &bad}})
	start := s.start
	s.notePlan(events.SetupPlanPayload{PlanID: "pl_new"})
	if s.attempts != 0 || s.executed != 0 || len(s.seen) != 0 {
		t.Fatalf("after a new plan: attempts=%d executed=%d seen=%d, want all zero", s.attempts, s.executed, len(s.seen))
	}
	if !s.start.Equal(start) {
		t.Fatal("notePlan moved the run's start")
	}
}

// TestSetupExhaustedOrder: the bounds are checked in the design's order
// (attempts, steps, wall clock) and the wall clock counts from Run's start.
func TestSetupExhaustedOrder(t *testing.T) {
	s, err := newSetupRun(&SetupBounds{AttemptsPerStep: 2, StepsPerPlan: 4, WallClock: time.Hour})
	if err != nil {
		t.Fatalf("newSetupRun: %v", err)
	}
	if s.bounds.AttemptsPerStep != 2 || s.bounds.StepsPerPlan != 4 {
		t.Fatalf("bounds = %+v", s.bounds)
	}
	if reason := s.exhausted(); reason != "" {
		t.Fatalf("a fresh run is exhausted (%s)", reason)
	}
	s.attempts, s.executed = 2, 4 // attempts wins over steps
	if reason := s.exhausted(); reason != events.SetupBlockedAttempts {
		t.Fatalf("exhausted = %q, want attempts first", reason)
	}
	s.attempts = 0
	if reason := s.exhausted(); reason != events.SetupBlockedSteps {
		t.Fatalf("exhausted = %q, want steps second", reason)
	}
	s.executed = 0
	s.start = time.Now().Add(-2 * time.Hour)
	if reason := s.exhausted(); reason != events.SetupBlockedWallClock {
		t.Fatalf("exhausted = %q, want the wall clock third", reason)
	}
}

// TestSetupBlockedQuestion: each bound's question is a question to the person
// and never names a secret or a request value.
func TestSetupBlockedQuestion(t *testing.T) {
	s, err := newSetupRun(nil)
	if err != nil {
		t.Fatalf("newSetupRun: %v", err)
	}
	for reason, want := range map[string]string{
		events.SetupBlockedAttempts:  "failed",
		events.SetupBlockedSteps:     "plan",
		events.SetupBlockedWallClock: "time budget",
		events.SetupBlockedBudget:    "token budget",
	} {
		q := setupBlockedQuestion(reason, s)
		if !strings.Contains(q, want) {
			t.Errorf("question for %s = %q, want it to name %q", reason, q, want)
		}
		if !strings.Contains(q, "steer") {
			t.Errorf("question for %s = %q does not offer the person a steer", reason, q)
		}
	}
}

// TestSetupStopForBound: a bound stop emits setup.blocked with the reason and
// a question, the message and the idle status, and returns the *BudgetError
// the run wraps.
func TestSetupStopForBound(t *testing.T) {
	sink := &fakeSink{}
	r := &runner{
		setup: &setupRun{bounds: plan.DefaultBounds(), start: time.Now()},
		alert: Alert{RoomID: "r_setup"},
		log:   discardLogger(),
		deps:  Deps{Sink: sink},
	}
	be := r.stopForSetupBound(context.Background(), events.SetupBlockedSteps)
	if be == nil || be.scope != budgetSetupRun || be.setupReason != events.SetupBlockedSteps {
		t.Fatalf("stopForSetupBound returned %+v, want a setup-scope *BudgetError naming the reason", be)
	}
	var blocked []sinkEvent
	for _, e := range sink.events {
		if e.typ == events.SetupBlocked {
			blocked = append(blocked, e)
		}
	}
	if len(blocked) != 1 {
		t.Fatalf("setup.blocked emitted %d times, want once", len(blocked))
	}
	var p events.SetupBlockedPayload
	if err := json.Unmarshal(blocked[0].payload, &p); err != nil {
		t.Fatalf("setup.blocked payload: %v", err)
	}
	if p.Reason != events.SetupBlockedSteps || p.Question == "" {
		t.Fatalf("setup.blocked = %+v, want reason steps and a question", p)
	}
	if len(sink.ofType(events.AgentMessage)) != 1 || len(sink.ofType(events.AgentStatus)) != 1 {
		t.Fatal("a bound stop emits exactly one message and one idle status")
	}
}

// TestSetupDecodeRequest: the room's request decodes from alert.Raw; an
// undecodable or absent Raw leaves the run on the defaults, never an error.
func TestSetupDecodeRequest(t *testing.T) {
	req := events.SetupRequest{PipelineID: "orders", TopicPrefix: "pg", SourceKind: "postgres"}
	raw, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	s, err := newSetupRun(nil)
	if err != nil {
		t.Fatal(err)
	}
	s.decodeRequest(Alert{Mode: ModeSetup, Raw: raw})
	if !s.reqOK || s.req.PipelineID != "orders" {
		t.Fatalf("decoded request = %+v (ok %v), want orders", s.req, s.reqOK)
	}
	s.decodeRequest(Alert{Mode: ModeSetup, Raw: json.RawMessage(`{"pipeline_id":`)})
	if s.reqOK {
		t.Fatal("a broken request must not decode")
	}
	s.decodeRequest(Alert{Mode: ModeSetup})
	if s.reqOK {
		t.Fatal("an absent request must not decode")
	}
}
