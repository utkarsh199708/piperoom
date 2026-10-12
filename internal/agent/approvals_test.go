package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"piperoom/internal/events"
)

// ---- wait for step results after a full approval ----

// approvalInput is the approval_result the room writes when a layer owner
// approves (see internal/room feedInbox).
func approvalInput(layer, proposal string) HumanInput {
	approved := true
	return HumanInput{
		Kind: "approval_result", Text: fmt.Sprintf("layer %q approved proposal %s", layer, proposal),
		ProposalID: proposal, Layer: layer, Approved: &approved,
	}
}

// rejectionInput is the approval_result of a rejecting layer owner.
func rejectionInput(layer, proposal string) HumanInput {
	approved := false
	return HumanInput{
		Kind: "approval_result", Text: fmt.Sprintf("layer %q rejected proposal %s: no", layer, proposal),
		ProposalID: proposal, Layer: layer, Approved: &approved,
	}
}

// firstWaitBlocks is an inbox whose first Wait blocks until its context ends,
// like the room inbox with nothing pending, and whose later Waits report the room
// closed. The first Wait is the bounded wait for step results; the later one
// ends the run after the model's turn.
type firstWaitBlocks struct {
	*fakeInbox
	mu    sync.Mutex
	waits int
}

func (f *firstWaitBlocks) Wait(ctx context.Context) error {
	f.mu.Lock()
	f.waits++
	n := f.waits
	f.mu.Unlock()
	if n == 1 {
		<-ctx.Done()
		return ctx.Err()
	}
	return context.Canceled
}

func sleptFor(c *fakeClock, d time.Duration) bool {
	for _, got := range c.durations() {
		if got == d {
			return true
		}
	}
	return false
}

// The last owner approves, the executor reports a moment later: the first turn
// sees the approvals and both step results together, with no pending note.
func TestApprovalFullThenResultsGivesOneTurnBoth(t *testing.T) {
	sink := &fakeSink{}
	inbox := &fakeInbox{
		drains: [][]HumanInput{
			{approvalInput("schema-registry", "p_1"), approvalInput("kafka-connect", "p_1")},
			{
				actionInput("proposal p_1 step 0: "+resultSetNoneFromGlobal, okPtr(true)),
				actionInput("proposal p_1 step 1: restarted task 0 (RUNNING)", okPtr(true)),
			},
		},
		waitErr: context.Canceled,
	}
	model := &scriptedModel{}
	r, _ := newCompatRunner(t, model, inbox, sink, compatSteps("BACKWARD"), compatEnv{})
	clk := &fakeClock{}
	r.clk = clk

	if err := r.run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	if len(model.calls) != 1 {
		t.Fatalf("one turn must see approvals and results: want 1 model call, got %d", len(model.calls))
	}
	text := requestText(model.calls[0])
	for _, want := range []string{`layer "schema-registry" approved proposal p_1`, `layer "kafka-connect" approved proposal p_1`, "proposal p_1 step 1:", verifyTag + "recovered:"} {
		if !strings.Contains(text, want) {
			t.Fatalf("the first turn must carry %q, got:\n%s", want, text)
		}
	}
	if strings.Contains(text, "have not arrived") {
		t.Fatalf("the results arrived, so no pending note is expected, got:\n%s", text)
	}
}

// The executor reports nothing within the bound: the turn starts anyway, with
// one note that the results are pending, so the model claims nothing about the
// steps.
func TestApprovalFullNoResultsBeforeTimeoutGetsPendingNote(t *testing.T) {
	sink := &fakeSink{}
	inbox := &firstWaitBlocks{fakeInbox: &fakeInbox{
		drains: [][]HumanInput{{approvalInput("schema-registry", "p_1"), approvalInput("kafka-connect", "p_1")}},
	}}
	model := &scriptedModel{}
	r, _ := newCompatRunner(t, model, inbox, sink, compatSteps("BACKWARD"), compatEnv{})
	clk := &fakeClock{}
	r.clk = clk

	if err := r.run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	if !sleptFor(clk, defaultStepResultWait) {
		t.Fatalf("the wait must be bounded by the step-result timeout (%v), slept %v", defaultStepResultWait, clk.durations())
	}
	if len(model.calls) != 1 {
		t.Fatalf("want 1 model call, got %d", len(model.calls))
	}
	text := requestText(model.calls[0])
	if n := strings.Count(text, "have not arrived yet"); n != 1 {
		t.Fatalf("want exactly one pending note, got %d in:\n%s", n, text)
	}
	if !strings.Contains(text, verifyTag+"approval is complete for proposal p_1") || !strings.Contains(text, "waited 45s") {
		t.Fatalf("the note must name the proposal and the wait, got:\n%s", text)
	}
	for _, m := range messageTexts(sink) {
		if strings.HasPrefix(m, "recovered:") || strings.HasPrefix(m, "NOT recovered:") {
			t.Fatalf("no step ran as far as the agent knows, so no verdict is expected, got %q", m)
		}
	}
}

// A step result that lands while waiting counts, even when other input wakes the
// wait first. A failed step settles the proposal at once: the executor stops at
// the first failure and reports nothing more.
func TestApprovalFullWaitEndsOnFailedStep(t *testing.T) {
	sink := &fakeSink{}
	inbox := &fakeInbox{
		drains: [][]HumanInput{
			{approvalInput("schema-registry", "p_1"), approvalInput("kafka-connect", "p_1")},
			{{Kind: "steer", Text: "check the connector too"}},
			{actionInput("proposal p_1 step 0: boom", okPtr(false))},
		},
		waitErr: context.Canceled,
	}
	model := &scriptedModel{}
	r, _ := newCompatRunner(t, model, inbox, sink, compatSteps("BACKWARD"), compatEnv{})
	clk := &fakeClock{}
	r.clk = clk

	if err := r.run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	text := requestText(model.calls[0])
	for _, want := range []string{"check the connector too", "proposal p_1 step 0: boom"} {
		if !strings.Contains(text, want) {
			t.Fatalf("the first turn must carry %q, got:\n%s", want, text)
		}
	}
	if strings.Contains(text, "have not arrived") {
		t.Fatalf("a failed step is a result: no pending note, got:\n%s", text)
	}
}

// One owner of two approves: the proposal is not complete, so there is no wait
// and the turn starts as it always did.
func TestApprovalPartialDoesNotWait(t *testing.T) {
	sink := &fakeSink{}
	inbox := &fakeInbox{
		drains:  [][]HumanInput{{approvalInput("schema-registry", "p_1")}},
		waitErr: context.Canceled,
	}
	model := &scriptedModel{}
	r, _ := newCompatRunner(t, model, inbox, sink, compatSteps("BACKWARD"), compatEnv{})
	clk := &fakeClock{}
	r.clk = clk

	if err := r.run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	if sleptFor(clk, defaultStepResultWait) {
		t.Fatalf("a partial approval must not wait, slept %v", clk.durations())
	}
	if len(model.calls) != 1 {
		t.Fatalf("want 1 model call, got %d", len(model.calls))
	}
	text := requestText(model.calls[0])
	if !strings.Contains(text, `layer "schema-registry" approved proposal p_1`) {
		t.Fatalf("the approval must reach the model, got:\n%s", text)
	}
	if strings.Contains(text, "have not arrived") {
		t.Fatalf("no pending note is expected for a partial approval, got:\n%s", text)
	}
}

// An approval for a proposal the agent did not create (for example after a
// restart) cannot be judged complete: no wait.
func TestApprovalForUnknownProposalDoesNotWait(t *testing.T) {
	sink := &fakeSink{}
	inbox := &fakeInbox{
		drains:  [][]HumanInput{{approvalInput("schema-registry", "p_other")}},
		waitErr: context.Canceled,
	}
	r, _ := newCompatRunner(t, &scriptedModel{}, inbox, sink, compatSteps("BACKWARD"), compatEnv{})
	clk := &fakeClock{}
	r.clk = clk

	if err := r.run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	if sleptFor(clk, defaultStepResultWait) {
		t.Fatalf("an unknown proposal must not wait, slept %v", clk.durations())
	}
}

// Only the fields identify an approval. The same text without fields (an old
// event replayed after a restart) completes nothing, so there is no wait.
func TestApprovalWithoutFieldsDoesNotComplete(t *testing.T) {
	tr := newProposalTracker()
	tr.record("p_1", compatSteps("BACKWARD"))
	for _, layer := range []string{"schema-registry", "kafka-connect"} {
		tr.observe(HumanInput{Kind: "approval_result", Text: fmt.Sprintf("layer %q approved proposal p_1", layer)})
	}
	if len(tr.awaiting()) != 0 {
		t.Fatal("approval text without fields must not complete a proposal")
	}
	// Approved unset, layer unset or proposal unset: ignored as well.
	yes := true
	for _, in := range []HumanInput{
		{Kind: "approval_result", ProposalID: "p_1", Layer: "schema-registry"},
		{Kind: "approval_result", ProposalID: "p_1", Approved: &yes},
		{Kind: "approval_result", Layer: "schema-registry", Approved: &yes},
	} {
		tr.observe(in)
	}
	tr.observe(approvalInput("kafka-connect", "p_1"))
	if len(tr.awaiting()) != 0 {
		t.Fatal("one valid approval of two layers must not complete the proposal")
	}
}

// The flow state machine: complete only when every required layer approved, wait
// once, settle on the last step or a failure.
func TestApprovalFlowStates(t *testing.T) {
	newTracker := func() *proposalTracker {
		tr := newProposalTracker()
		tr.record("p_1", compatSteps("BACKWARD")) // layers: schema-registry, kafka-connect; last step 1
		return tr
	}
	pending := func(tr *proposalTracker) bool { return len(tr.awaiting()) == 1 }

	t.Run("partial then full", func(t *testing.T) {
		tr := newTracker()
		tr.observe(approvalInput("schema-registry", "p_1"))
		if pending(tr) {
			t.Fatal("one of two layers must not wait")
		}
		tr.observe(approvalInput("warehouse", "p_1")) // not a required layer
		if pending(tr) {
			t.Fatal("a layer the proposal does not require must not complete it")
		}
		tr.observe(approvalInput("kafka-connect", "p_1"))
		if !pending(tr) {
			t.Fatal("every required layer approved: must wait for results")
		}
	})
	t.Run("intermediate result keeps waiting, last result settles", func(t *testing.T) {
		tr := newTracker()
		tr.observe(approvalInput("schema-registry", "p_1"))
		tr.observe(approvalInput("kafka-connect", "p_1"))
		tr.observe(actionInput("proposal p_1 step 0: ok", okPtr(true)))
		if !pending(tr) {
			t.Fatal("step 0 of 2 is not the end of the results")
		}
		tr.observe(actionInput("proposal p_1 step 1: ok", okPtr(true)))
		if pending(tr) {
			t.Fatal("the last step settles the proposal")
		}
	})
	t.Run("unknown outcome settles", func(t *testing.T) {
		tr := newTracker()
		tr.observe(approvalInput("schema-registry", "p_1"))
		tr.observe(approvalInput("kafka-connect", "p_1"))
		tr.observe(actionInput("proposal p_1 step 0: ?", nil))
		if pending(tr) {
			t.Fatal("an unknown outcome is a result")
		}
	})
	t.Run("a wait is given up once", func(t *testing.T) {
		tr := newTracker()
		tr.observe(approvalInput("schema-registry", "p_1"))
		tr.observe(approvalInput("kafka-connect", "p_1"))
		if ids := tr.giveUpWaiting(); len(ids) != 1 || ids[0] != "p_1" {
			t.Fatalf("giveUpWaiting = %v", ids)
		}
		tr.observe(approvalInput("kafka-connect", "p_1")) // a repeated input
		if pending(tr) {
			t.Fatal("a proposal already waited on must not wait again")
		}
	})
	t.Run("a rejection never completes", func(t *testing.T) {
		tr := newTracker()
		tr.observe(approvalInput("schema-registry", "p_1"))
		tr.observe(rejectionInput("kafka-connect", "p_1"))
		if pending(tr) {
			t.Fatal("a rejected proposal never runs")
		}
	})
}

// ---- config-only verdict only for tracked NONE subjects ----

const otherSubject = "pg.public.customers-value"

func compatStep(subject, level string) events.ProposalStep {
	return events.ProposalStep{
		Action: "schema_set_compatibility", Layer: "schema-registry",
		Params:      json.RawMessage(`{"subject":"` + subject + `","level":"` + level + `"}`),
		Description: "set " + subject + " to " + level,
	}
}

// After a re-break in the same room, a compat-only change of a subject no
// verified fix left at NONE (FULL to BACKWARD) is not the restore: the data-flow
// rule judges it, so a stale warehouse is NOT recovered. The same goes for a
// proposal that restores a tracked subject and also touches an untracked one.
func TestCompatUntrackedChangeWhileRecoveredIdleUsesDataFlowRule(t *testing.T) {
	cases := map[string]struct {
		steps   []events.ProposalStep
		tracked []string
		text    string
	}{
		"nothing tracked":            {restoreSteps(), nil, "proposal p_1 step 0: " + compatSubject + " compatibility FULL → BACKWARD"},
		"another subject is tracked": {restoreSteps(), []string{otherSubject}, "proposal p_1 step 0: " + resultRestoreBackward},
		"tracked plus untracked":     {[]events.ProposalStep{compatStep(compatSubject, "BACKWARD"), compatStep(otherSubject, "BACKWARD")}, []string{compatSubject}, "proposal p_1 step 1: " + otherSubject + " compatibility NONE → BACKWARD"},
		"tracked loosened is no fix": {compatSteps("NONE")[:1], []string{compatSubject}, "proposal p_1 step 0: " + resultSetNoneFromSubject},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			sink := &fakeSink{}
			inbox := &fakeInbox{drains: [][]HumanInput{nil, lastActionResult(c.text)}, waitErr: context.Canceled}
			env := compatEnv{warehouse: staleWarehouseTool()}
			if _, final := compatFinalLevels(c.steps); final[compatSubject] == "NONE" {
				env.level = "NONE"
			}
			r, _ := newCompatRunner(t, &scriptedModel{}, inbox, sink, c.steps, env)
			r.recoveredIdle = true
			for _, s := range c.tracked {
				r.noneSubjects[s] = true
			}

			if err := r.run(context.Background()); err != nil {
				t.Fatalf("run: %v", err)
			}
			var verdict string
			for _, m := range messageTexts(sink) {
				if strings.HasPrefix(m, "NOT recovered:") {
					verdict = m
				}
			}
			if !strings.Contains(verdict, "no new rows since the fix") {
				t.Fatalf("the data-flow rule must decide, got %q", messageTexts(sink))
			}
			for _, m := range messageTexts(sink) {
				if strings.Contains(m, "information only") {
					t.Fatalf("data flow must not be information only here, got %q", m)
				}
			}
			if r.recoveredIdle {
				t.Fatal("recoveredIdle must be cleared by the NOT recovered verdict")
			}
		})
	}
}

// The story with tracking: the fix leaves the subject at NONE and recovers (the
// subject is tracked), the restore verifies on a quiet pipeline as config-only,
// and the tracked set is empty afterwards.
func TestCompatRestoreOfTrackedSubjectIsConfigOnlyAndClearsTracking(t *testing.T) {
	t0 := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	// Reads: p_1 check baseline, p_1 window, p_2 check (information only).
	warehouse := warehouseSeq(t0, t0.Add(time.Second), t0.Add(time.Second))
	sink := &fakeSink{}
	inbox := &fakeInbox{
		drains: [][]HumanInput{
			nil,
			lastActionResult("proposal p_1 step 1: restarted task 0 (RUNNING)"),
			lastActionResult("proposal p_2 step 0: " + resultRestoreBackward),
		},
		waitErr: context.Canceled,
	}
	model := &scriptedModel{}
	level := "NONE"
	var trackedAfterFix map[string]bool
	var r *runner
	hook := &hookModel{scriptedModel: model, onTurn: func(call int) {
		if call == 1 { // the turn that follows p_1's verdict
			trackedAfterFix = map[string]bool{}
			for k, v := range r.noneSubjects {
				trackedAfterFix[k] = v
			}
			level = "BACKWARD"
		}
	}}
	r, _ = newCompatRunner(t, hook, inbox, sink, compatSteps("NONE"), compatEnv{warehouse: warehouse, levelFn: func() string { return level }})
	r.proposals.record("p_2", restoreSteps())

	if err := r.run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	if !trackedAfterFix[compatSubject] || len(trackedAfterFix) != 1 {
		t.Fatalf("p_1's recovered verdict must track the subject it left at NONE, got %v", trackedAfterFix)
	}
	var verdicts []string
	for _, m := range messageTexts(sink) {
		if strings.HasPrefix(m, "recovered:") {
			verdicts = append(verdicts, m)
		}
		if strings.HasPrefix(m, "NOT recovered:") {
			t.Fatalf("unexpected NOT recovered verdict: %q", m)
		}
	}
	if len(verdicts) != 2 || !strings.Contains(verdicts[1], "information only") {
		t.Fatalf("the restore must be judged config-only, got %q", verdicts)
	}
	if len(r.noneSubjects) != 0 {
		t.Fatalf("the tracked set must be empty after the restore verifies, got %v", r.noneSubjects)
	}
	if !r.recoveredIdle {
		t.Fatal("the room must stay recovered")
	}
}

func TestCompatTrackNoneSubjects(t *testing.T) {
	r := &runner{}
	r.trackNoneSubjects([]events.ProposalStep{compatStep(compatSubject, "NONE"), compatStep(otherSubject, "NONE")})
	if !r.noneSubjects[compatSubject] || !r.noneSubjects[otherSubject] {
		t.Fatalf("both subjects must be tracked, got %v", r.noneSubjects)
	}
	// Only one of them is restored: the other stays tracked.
	r.trackNoneSubjects([]events.ProposalStep{compatStep(compatSubject, "backward")})
	if r.noneSubjects[compatSubject] || !r.noneSubjects[otherSubject] {
		t.Fatalf("restoring one subject must untrack only it, got %v", r.noneSubjects)
	}
	// A later step in the same proposal wins: NONE then BACKWARD leaves nothing at NONE.
	r.trackNoneSubjects([]events.ProposalStep{compatStep(compatSubject, "NONE"), compatStep(compatSubject, "BACKWARD")})
	if r.noneSubjects[compatSubject] {
		t.Fatalf("the final level of a subject decides, got %v", r.noneSubjects)
	}
	// The remaining tracked subject may be restored alone; a mix with an untracked one may not.
	if !r.restoresTrackedSubjects([]events.ProposalStep{compatStep(otherSubject, "BACKWARD")}) {
		t.Fatal("restoring a tracked subject must qualify")
	}
	if r.restoresTrackedSubjects([]events.ProposalStep{compatStep(otherSubject, "BACKWARD"), compatStep(compatSubject, "BACKWARD")}) {
		t.Fatal("a proposal that also moves an untracked subject must not qualify")
	}
	if r.restoresTrackedSubjects(nil) {
		t.Fatal("an empty proposal must not qualify")
	}
}
