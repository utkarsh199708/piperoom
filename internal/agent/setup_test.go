package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"piperoom/internal/events"
	"piperoom/internal/setup/plan"
	setuptools "piperoom/internal/setup/tools"
	"piperoom/internal/tools"
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

// TestSetupNotePlan: a new setup_plan restarts the steps count; the failed
// attempts and the wall clock carry over.
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
	if s.executed != 0 || len(s.seen) != 0 {
		t.Fatalf("after a new plan: executed=%d seen=%d, want both zero", s.executed, len(s.seen))
	}
	if s.attempts != 1 {
		t.Fatalf("after a new plan: attempts=%d, want the failure kept (1)", s.attempts)
	}
	if !s.start.Equal(start) {
		t.Fatal("notePlan moved the run's start")
	}
}

// TestSetupAttemptsSurviveReplans: the room makes the model re-plan after
// every failure, so the attempts bound must be reachable through re-plans:
// fail, re-plan, fail, re-plan, fail stops the run on attempts, and only a
// success clears the count.
func TestSetupAttemptsSurviveReplans(t *testing.T) {
	s, err := newSetupRun(nil)
	if err != nil {
		t.Fatalf("newSetupRun: %v", err)
	}
	step0 := 0
	ok, bad := true, false
	for i := 1; i <= plan.DefaultAttemptsPerStep; i++ {
		if reason := s.exhausted(); reason != "" {
			t.Fatalf("before failure %d: exhausted = %q", i, reason)
		}
		s.observe([]HumanInput{{Kind: "action_result", ProposalID: fmt.Sprintf("p_%d", i), StepIndex: &step0, OK: &bad}})
		s.notePlan(events.SetupPlanPayload{PlanID: fmt.Sprintf("pl_%d", i)})
	}
	if reason := s.exhausted(); reason != events.SetupBlockedAttempts {
		t.Fatalf("after %d failures and re-plans: exhausted = %q, want %q", plan.DefaultAttemptsPerStep, reason, events.SetupBlockedAttempts)
	}
	s.observe([]HumanInput{{Kind: "action_result", ProposalID: "p_ok", StepIndex: &step0, OK: &ok}})
	if s.attempts != 0 || s.exhausted() != "" {
		t.Fatalf("after a success: attempts=%d exhausted=%q, want the count cleared", s.attempts, s.exhausted())
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
		if reason == events.SetupBlockedBudget {
			// The room's token count outlives the run: a steer only starts a
			// run that stops again, so the question names the setting.
			if !strings.Contains(q, "PIPEROOM_SETUP_TOKEN_BUDGET") || strings.Contains(q, "steer me") {
				t.Errorf("budget question = %q, want the setting named and no steer offered", q)
			}
			continue
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
	// A type error after some fields decoded leaves nothing behind: the
	// plan's class resolver reads req.Targets whatever reqOK says.
	s.decodeRequest(Alert{Mode: ModeSetup, Raw: json.RawMessage(`{"targets":[{"kind":"postgres","address":"db:5432","class":"production"}],"tables":"public.orders"}`)})
	if s.reqOK || len(s.req.Targets) != 0 || s.req.PipelineID != "" {
		t.Fatalf("a partly decoded request = %+v (ok %v), want the zero request", s.req, s.reqOK)
	}
	s.decodeRequest(Alert{Mode: ModeSetup})
	if s.reqOK {
		t.Fatal("an absent request must not decode")
	}
}

// TestSetupToolSinkAllowsOnlyThePlanAndTheRequest: the setup room tools emit
// through a sink that passes setup.plan and the verification request and
// refuses everything else, setup.verified first, whatever the run's own sink
// would accept.
func TestSetupToolSinkAllowsOnlyThePlanAndTheRequest(t *testing.T) {
	inner := &fakeSink{}
	s := setupToolSink{inner: inner}
	ctx := context.Background()
	for _, typ := range []events.Type{events.SetupPlan, setuptools.TypeVerificationRequested} {
		if err := s.Emit(ctx, typ, struct{}{}); err != nil {
			t.Errorf("%s refused: %v", typ, err)
		}
	}
	for _, typ := range []events.Type{events.SetupVerified, events.SetupAwaitingChange, events.SetupBlocked, events.PipelineRegistered, events.AgentMessage} {
		if err := s.Emit(ctx, typ, struct{}{}); err == nil {
			t.Errorf("%s passed the setup tools' sink", typ)
		}
	}
	if n := len(inner.events); n != 2 {
		t.Fatalf("the run's sink got %d events, want only the plan and the request", n)
	}
}

// TestSetupBudgetStopEmitsOneOfEach: when the setup's token budget stops a
// setup room's run, the log holds one setup.blocked (reason budget, the
// setup's own question), one agent message and one idle status, and Run
// returns a *BudgetError.
func TestSetupBudgetStopEmitsOneOfEach(t *testing.T) {
	ep := testEndpoint(&scriptedModel{}, 0)
	ep.budget("r_setup").record(usageTokens{input: 500}, true) // already over the setup budget below
	sink := &fakeSink{}
	err := ep.Run(context.Background(), Alert{RoomID: "r_setup", Title: "set up orders", PipelineID: "orders", Mode: ModeSetup},
		Deps{Tools: tools.NewReadOnlyRegistry(), Inbox: &fakeInbox{waitErr: context.Canceled}, Sink: sink, Logger: discardLogger()},
		Options{Setup: &SetupBounds{TokenBudget: 100}})
	if _, ok := asBudgetError(err); !ok {
		t.Fatalf("Run = %v, want a *BudgetError", err)
	}
	blocked := sink.ofType(events.SetupBlocked)
	if len(blocked) != 1 {
		t.Fatalf("setup.blocked emitted %d times, want once", len(blocked))
	}
	var p events.SetupBlockedPayload
	if err := json.Unmarshal(blocked[0].payload, &p); err != nil {
		t.Fatal(err)
	}
	if p.Reason != events.SetupBlockedBudget || !strings.Contains(p.Question, "PIPEROOM_SETUP_TOKEN_BUDGET") {
		t.Fatalf("setup.blocked = %+v, want reason budget and the setup budget's question", p)
	}
	if n := len(sink.ofType(events.AgentMessage)); n != 1 {
		t.Fatalf("agent messages = %d, want one", n)
	}
	idle := 0
	for _, e := range sink.ofType(events.AgentStatus) {
		var st events.AgentStatusPayload
		if json.Unmarshal(e.payload, &st) == nil && st.State == "idle" {
			idle++
		}
	}
	if idle != 1 {
		t.Fatalf("idle statuses = %d, want one", idle)
	}
}

// TestSetupBudgetBlockedNamesTheLimitReached: a spend limit that is not the
// setup's token budget (a pipeline's daily budget, the spend store) is named
// as itself in setup.blocked, not as PIPEROOM_SETUP_TOKEN_BUDGET.
func TestSetupBudgetBlockedNamesTheLimitReached(t *testing.T) {
	sink := &fakeSink{}
	r := &runner{
		setup: &setupRun{bounds: plan.DefaultBounds(), start: time.Now()},
		alert: Alert{RoomID: "r_setup"},
		log:   discardLogger(),
		deps:  Deps{Sink: sink},
	}
	be := &BudgetError{scope: budgetPipelineTokens, pipeline: "orders", day: "2026-10-24", Used: 9, Limit: 5}
	r.setupBudgetBlocked(context.Background(), be)
	blocked := sink.ofType(events.SetupBlocked)
	if len(blocked) != 1 {
		t.Fatalf("setup.blocked emitted %d times, want once", len(blocked))
	}
	var p events.SetupBlockedPayload
	if err := json.Unmarshal(blocked[0].payload, &p); err != nil {
		t.Fatal(err)
	}
	if p.Reason != events.SetupBlockedBudget || p.Question != be.Error() {
		t.Fatalf("setup.blocked = %+v, want reason budget and the pipeline budget's own text", p)
	}
}

// TestEndpointRunCallsUsageOnARefusedSetupBudget: Deps.Usage is called once
// however Run ends, including the refusal of a negative setup token budget.
func TestEndpointRunCallsUsageOnARefusedSetupBudget(t *testing.T) {
	ep := testEndpoint(&scriptedModel{}, 0)
	ep.budget("r_setup").record(usageTokens{input: 7}, true)
	u := &usageRecorder{}
	err := ep.Run(context.Background(), Alert{RoomID: "r_setup", Mode: ModeSetup},
		Deps{Tools: tools.NewReadOnlyRegistry(), Sink: &fakeSink{}, Usage: u.fn, Logger: discardLogger()},
		Options{Setup: &SetupBounds{TokenBudget: -1}})
	if err == nil || !strings.Contains(err.Error(), "negative") {
		t.Fatalf("Run = %v, want the negative budget refused", err)
	}
	var be *BudgetError
	if errors.As(err, &be) {
		t.Fatalf("Run = %v: a refused setting is not a budget stop", err)
	}
	u.once(t, 7)
}
