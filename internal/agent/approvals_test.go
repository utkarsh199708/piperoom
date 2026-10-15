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

func sleptFor(c *fakeClock, d time.Duration) bool {
	for _, got := range c.durations() {
		if got == d {
			return true
		}
	}
	return false
}

// gatedClock blocks the step-result timer until the test releases it, so the
// test decides when the wait times out. Every other sleep (the verification
// windows) returns at once. It honours the context like the real clock, so a
// wait that ends early leaves no goroutine behind.
type gatedClock struct {
	mu     sync.Mutex
	slept  []time.Duration
	gate   chan struct{}
	opened sync.Once
}

func newGatedClock() *gatedClock { return &gatedClock{gate: make(chan struct{})} }

func (c *gatedClock) Sleep(ctx context.Context, d time.Duration) error {
	c.mu.Lock()
	c.slept = append(c.slept, d)
	c.mu.Unlock()
	if d != defaultStepResultWait {
		return ctx.Err()
	}
	select {
	case <-c.gate:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// release fires the step-result timer.
func (c *gatedClock) release() { c.opened.Do(func() { close(c.gate) }) }

// timers counts the step-result timers started.
func (c *gatedClock) timers() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	var n int
	for _, d := range c.slept {
		if d == defaultStepResultWait {
			n++
		}
	}
	return n
}

// pushInbox is an inbox where each Wait runs the next scripted step, which
// pushes input (or blocks, or fails) the way the room does. A Wait with pending
// input returns at once. When the script is spent, Wait runs rest if set and
// otherwise reports the room closed, which ends the run after the model's turn.
type pushInbox struct {
	mu      sync.Mutex
	pending []HumanInput
	steps   []func(ctx context.Context) error
	rest    func(ctx context.Context) error
	waits   int
}

func (p *pushInbox) push(in ...HumanInput) {
	p.mu.Lock()
	p.pending = append(p.pending, in...)
	p.mu.Unlock()
}

func (p *pushInbox) Drain(context.Context) ([]HumanInput, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := p.pending
	p.pending = nil
	return out, nil
}

func (p *pushInbox) Wait(ctx context.Context) error {
	p.mu.Lock()
	p.waits++
	if len(p.pending) > 0 {
		p.mu.Unlock()
		return nil
	}
	var step func(ctx context.Context) error
	switch {
	case len(p.steps) > 0:
		step, p.steps = p.steps[0], p.steps[1:]
	case p.rest != nil:
		step = p.rest
	}
	p.mu.Unlock()
	if step == nil {
		return context.Canceled
	}
	return step(ctx)
}

// deliver is a step where input arrives while the agent waits.
func (p *pushInbox) deliver(in ...HumanInput) func(context.Context) error {
	return func(context.Context) error {
		p.push(in...)
		return nil
	}
}

// timeout is a step where nothing more arrives and the timer fires: Wait blocks
// until the wait context ends, as the room inbox does.
func timeoutStep(clk *gatedClock) func(context.Context) error {
	return func(ctx context.Context) error {
		clk.release()
		<-ctx.Done()
		return ctx.Err()
	}
}

// bothApprove is the batch where the last owner approves proposal id (steps in
// schema-registry and kafka-connect).
func bothApprove(id string) []HumanInput {
	return []HumanInput{approvalInput("schema-registry", id), approvalInput("kafka-connect", id)}
}

func bothResults(id string) []HumanInput {
	return []HumanInput{
		actionInput("proposal "+id+" step 0: "+resultSetNoneFromGlobal, okPtr(true)),
		actionInput("proposal "+id+" step 1: restarted task 0 (RUNNING)", okPtr(true)),
	}
}

// waitRunner is a compat runner on a gated clock, fed by a push inbox.
func waitRunner(t *testing.T, model model, inbox Inbox) (*runner, *gatedClock, *fakeSink) {
	t.Helper()
	sink := &fakeSink{}
	r, _ := newCompatRunner(t, model, inbox, sink, compatSteps("BACKWARD"), compatEnv{})
	clk := newGatedClock()
	r.clk = clk
	return r, clk, sink
}

// noteLine is the pending-results note in a request, or "".
func noteLine(text string) string {
	for _, line := range strings.Split(text, "\n") {
		if strings.HasPrefix(line, verifyTag+"approval is complete") {
			return line
		}
	}
	return ""
}

// The last owner approves, the executor reports while the agent waits: Wait
// blocks, the results are pushed, the next drain gets them, and the first turn
// sees the approvals and both step results together, with no pending note.
func TestApprovalFullThenResultsGivesOneTurnBoth(t *testing.T) {
	inbox := &pushInbox{pending: bothApprove("p_1")}
	inbox.steps = []func(context.Context) error{inbox.deliver(bothResults("p_1")...)}
	model := &scriptedModel{}
	r, clk, sink := waitRunner(t, model, inbox)

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
	if noteLine(text) != "" {
		t.Fatalf("the results arrived, so no pending note is expected, got:\n%s", text)
	}
	if clk.timers() != 1 {
		t.Fatalf("want one timer, got %d", clk.timers())
	}
	st := statusList(sink)
	if !containsString(st, "working/"+waitingDetailResults) {
		t.Fatalf("the wait must show a status, got %v", st)
	}
}

func containsString(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

// The executor reports nothing within the bound: the turn starts anyway, with
// one note that names the proposal and every missing step and says why the wait
// ended.
func TestApprovalFullNoResultsBeforeTimeoutGetsPendingNote(t *testing.T) {
	inbox := &pushInbox{pending: bothApprove("p_1")}
	model := &scriptedModel{}
	r, clk, sink := waitRunner(t, model, inbox)
	inbox.steps = []func(context.Context) error{timeoutStep(clk)}

	if err := r.run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	if len(model.calls) != 1 {
		t.Fatalf("want 1 model call, got %d", len(model.calls))
	}
	text := requestText(model.calls[0])
	if n := strings.Count(text, "not every step result has arrived"); n != 1 {
		t.Fatalf("want exactly one pending note, got %d in:\n%s", n, text)
	}
	note := noteLine(text)
	for _, want := range []string{"proposal p_1 (missing: steps 1, 2)", "the wait limit of 45s was reached"} {
		if !strings.Contains(note, want) {
			t.Fatalf("the note must contain %q, got %q", want, note)
		}
	}
	for _, m := range messageTexts(sink) {
		if strings.HasPrefix(m, "recovered:") || strings.HasPrefix(m, "NOT recovered:") {
			t.Fatalf("no step ran as far as the agent knows, so no verdict is expected, got %q", m)
		}
	}
	if st := statusList(sink); !containsString(st, "working/") {
		t.Fatalf("the waiting detail must be cleared after the wait, got %v", st)
	}
}

// Some results arrive and the last step is late: the note names only the steps
// still missing and does not contradict the results shown with it. The result
// is in the room's format: StepIndex 0, text "step 1: ..." (#423).
func TestApprovalPartialResultsThenTimeoutNamesMissingStepsOnly(t *testing.T) {
	idx := 0
	first := HumanInput{Kind: "action_result", ProposalID: "p_1", StepIndex: &idx, OK: okPtr(true), Text: "step 1: " + resultSetNoneFromGlobal}
	inbox := &pushInbox{pending: append(bothApprove("p_1"), first)}
	model := &scriptedModel{}
	r, clk, _ := waitRunner(t, model, inbox)
	inbox.steps = []func(context.Context) error{timeoutStep(clk)}

	if err := r.run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	text := requestText(model.calls[0])
	if !strings.Contains(text, "step 1: "+resultSetNoneFromGlobal) {
		t.Fatalf("the step 1 result must reach the model, got:\n%s", text)
	}
	note := noteLine(text)
	if !strings.Contains(note, "proposal p_1 (missing: step 2)") {
		t.Fatalf("the note must name step 2 as missing, got %q", note)
	}
	if strings.Contains(note, "step 1") || strings.Contains(note, "steps 1") {
		t.Fatalf("the note must not mention the step that arrived, got %q", note)
	}
	if strings.Contains(note, "Do not say that any step ran") {
		t.Fatalf("the note must not deny steps that the message shows ran, got %q", note)
	}
}

// Two proposals are fully approved in one batch. One settles while waiting, the
// other times out: the note names only the late one.
func TestApprovalTwoProposalsOneSettlesOneTimesOut(t *testing.T) {
	inbox := &pushInbox{pending: append(bothApprove("p_1"), bothApprove("p_2")...)}
	model := &scriptedModel{}
	r, clk, _ := waitRunner(t, model, inbox)
	r.proposals.record("p_2", compatSteps("BACKWARD"))
	inbox.steps = []func(context.Context) error{inbox.deliver(bothResults("p_1")...), timeoutStep(clk)}

	if err := r.run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	text := requestText(model.calls[0])
	note := noteLine(text)
	if !strings.Contains(note, "proposal p_2 (missing: steps 1, 2)") {
		t.Fatalf("the note must name p_2, got %q", note)
	}
	if strings.Contains(note, "p_1") {
		t.Fatalf("p_1 settled and must not be named, got %q", note)
	}
	if !strings.Contains(text, "proposal p_1 step 1:") {
		t.Fatalf("p_1's results must reach the model, got:\n%s", text)
	}
	if clk.timers() != 1 {
		t.Fatalf("both proposals share one timer, got %d", clk.timers())
	}
}

// The outer context ends during the wait: the run stops cleanly with no model
// turn, and the timer goroutine ends.
func TestApprovalWaitStopsWhenContextEnds(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	inbox := &pushInbox{pending: bothApprove("p_1")}
	inbox.steps = []func(context.Context) error{func(wctx context.Context) error {
		cancel()
		<-wctx.Done()
		return wctx.Err()
	}}
	model := &scriptedModel{}
	r, _, _ := waitRunner(t, model, inbox)

	if err := r.run(ctx); err != nil {
		t.Fatalf("a cancelled wait is a clean stop, got %v", err)
	}
	if len(model.calls) != 0 {
		t.Fatalf("no model turn after the context ended, got %d", len(model.calls))
	}
}

// The inbox closing during the wait ends it, and the note says so instead of
// blaming the time limit.
func TestApprovalWaitEndsWhenInboxCloses(t *testing.T) {
	inbox := &pushInbox{pending: bothApprove("p_1")}
	inbox.steps = []func(context.Context) error{func(context.Context) error { return ErrInboxClosed }}
	model := &scriptedModel{}
	r, _, _ := waitRunner(t, model, inbox)

	if err := r.run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	note := noteLine(requestText(model.calls[0]))
	if !strings.Contains(note, "the inbox closed while waiting") || strings.Contains(note, "wait limit") {
		t.Fatalf("the note must give the real cause, got %q", note)
	}
}

// A step result seen before the last approval settles the proposal: when the
// approvals complete there is nothing left to wait for.
func TestApprovalResultSeenBeforeFinalApprovalDoesNotWait(t *testing.T) {
	last := actionInput("proposal p_1 step 1: restarted task 0 (RUNNING)", okPtr(true))
	inbox := &pushInbox{pending: []HumanInput{approvalInput("schema-registry", "p_1"), last, approvalInput("kafka-connect", "p_1")}}
	model := &scriptedModel{}
	r, clk, _ := waitRunner(t, model, inbox)

	if err := r.run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	if clk.timers() != 0 {
		t.Fatalf("a settled proposal must not wait, %d timers started", clk.timers())
	}
	if noteLine(requestText(model.calls[0])) != "" {
		t.Fatal("no pending note is expected")
	}
}

// Steers that arrive during the wait wake it but do not restart the timer: one
// timer for the whole wait.
func TestApprovalSteerDuringWaitDoesNotRestartTimer(t *testing.T) {
	inbox := &pushInbox{pending: bothApprove("p_1")}
	model := &scriptedModel{}
	r, clk, _ := waitRunner(t, model, inbox)
	inbox.steps = []func(context.Context) error{
		inbox.deliver(HumanInput{Kind: "steer", Text: "first steer"}),
		inbox.deliver(HumanInput{Kind: "steer", Text: "second steer"}),
		timeoutStep(clk),
	}

	if err := r.run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	if clk.timers() != 1 {
		t.Fatalf("want exactly one step-result timer, got %d", clk.timers())
	}
	text := requestText(model.calls[0])
	for _, want := range []string{"first steer", "second steer"} {
		if !strings.Contains(text, want) {
			t.Fatalf("the steer %q must reach the model, got:\n%s", want, text)
		}
	}
	if noteLine(text) == "" {
		t.Fatal("the wait timed out, so the note is expected")
	}
}

// Input that keeps coming cannot extend the wait: once the timer has fired, the
// loop stops waiting even though every Wait would return input at once.
func TestApprovalInputFloodCannotExtendWait(t *testing.T) {
	inbox := &pushInbox{pending: bothApprove("p_1")}
	model := &scriptedModel{}
	turned := false // set once the model's turn starts, i.e. after the wait
	r, clk, _ := waitRunner(t, &hookModel{scriptedModel: model, onTurn: func(int) { turned = true }}, inbox)
	var more int
	inbox.steps = []func(context.Context) error{func(ctx context.Context) error {
		inbox.push(HumanInput{Kind: "steer", Text: "steer 0"})
		clk.release()
		<-ctx.Done() // the timer has fired by the time this Wait returns input
		return nil
	}}
	inbox.rest = func(context.Context) error {
		if turned || more > 100 {
			return context.Canceled // after the wait: end the run
		}
		more++
		inbox.push(HumanInput{Kind: "steer", Text: "more steer"})
		return nil
	}

	if err := r.run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	if more > 0 {
		t.Fatalf("the wait kept waiting after the timer fired: %d more Waits", more)
	}
	text := requestText(model.calls[0])
	if !strings.Contains(text, "steer 0") || noteLine(text) == "" {
		t.Fatalf("the steer and the pending note must reach the model, got:\n%s", text)
	}
}

// Results that land as the timer fires still count: the final drain after the
// timeout picks them up and no note is added.
func TestApprovalResultsLandingAsTimerFiresCount(t *testing.T) {
	inbox := &pushInbox{pending: bothApprove("p_1")}
	model := &scriptedModel{}
	r, clk, _ := waitRunner(t, model, inbox)
	inbox.steps = []func(context.Context) error{func(ctx context.Context) error {
		inbox.push(bothResults("p_1")...)
		clk.release()
		<-ctx.Done()
		return ctx.Err()
	}}

	if err := r.run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	text := requestText(model.calls[0])
	if !strings.Contains(text, "proposal p_1 step 1:") {
		t.Fatalf("the late results must reach the model, got:\n%s", text)
	}
	if noteLine(text) != "" {
		t.Fatalf("the results arrived, so no pending note is expected, got:\n%s", text)
	}
}

// A failed step settles the proposal at once: the executor stops at the first
// failure and reports nothing more. A steer that arrives first only wakes the
// wait.
func TestApprovalFullWaitEndsOnFailedStep(t *testing.T) {
	inbox := &pushInbox{pending: bothApprove("p_1")}
	model := &scriptedModel{}
	r, _, _ := waitRunner(t, model, inbox)
	inbox.steps = []func(context.Context) error{
		inbox.deliver(HumanInput{Kind: "steer", Text: "check the connector too"}),
		inbox.deliver(actionInput("proposal p_1 step 0: boom", okPtr(false))),
	}

	if err := r.run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	text := requestText(model.calls[0])
	for _, want := range []string{"check the connector too", "proposal p_1 step 0: boom"} {
		if !strings.Contains(text, want) {
			t.Fatalf("the first turn must carry %q, got:\n%s", want, text)
		}
	}
	if noteLine(text) != "" {
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
		if ids := tr.giveUpWaiting(); len(ids) != 1 || ids[0].id != "p_1" || len(ids[0].missing) != 2 {
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
	vs := verifications(sink)
	if len(verdicts) != 2 || len(vs) != 2 || vs[1].DataFlow == nil || !strings.Contains(vs[1].DataFlow.Detail, "information only") {
		t.Fatalf("the restore must be judged config-only, got %q %+v", verdicts, vs)
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

// missingSteps numbers steps from 1, as the room's action results do.
func TestMissingStepsIsOneBased(t *testing.T) {
	cases := map[string]struct {
		in   []int
		want string
	}{
		"one":      {[]int{0}, "missing: step 1"},
		"two":      {[]int{0, 1}, "missing: steps 1, 2"},
		"overflow": {[]int{0, 1, 2, 3, 4, 5, 6, 7}, "missing: steps 1, 2, 3, 4, 5, 6 and 2 more"},
	}
	for name, c := range cases {
		if got := missingSteps(c.in); got != c.want {
			t.Errorf("%s: missingSteps(%v) = %q, want %q", name, c.in, got, c.want)
		}
	}
}
