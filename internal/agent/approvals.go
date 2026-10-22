package agent

import (
	"context"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"piperoom/internal/events"
)

// Wait for step results after a full approval.
//
// When the last required layer approves a proposal, the room delivers the
// approval_result at once and the executor delivers the action_results a moment
// later. A model turn that started on the approval alone had to guess about the
// steps, and a live run showed it guessing wrong ("arjun still needs to approve",
// "no action result yet") while the steps were running. So a batch that completes
// a proposal's approvals holds the turn until that proposal's step results have
// arrived, for at most timing.stepResultWait. On a timeout the turn starts with a
// note that the results are pending.
//
// The room names the proposal, the layer and the step in HumanInput fields, so
// nothing here parses message text. The agent compares the approved layers with
// the layers it stored when it created the proposal. Those are the layers the
// room requires: the union of the step layers. An input with a field missing (an
// event replayed from before the fields existed) is ignored, and the turn then
// starts as it always did.

// execFlow follows one proposal from its approvals to its executed steps.
type execFlow struct {
	required []string
	approved map[string]bool
	// lastStep is the index of the proposal's last step.
	lastStep int
	// complete is set once every required layer has approved.
	complete bool
	// settled is set once the executor has reported what it will report: the last
	// step, or the first step that failed or has an unknown outcome (the executor
	// stops at the first failure).
	settled bool
	// waiting is set when the approvals completed and the results are still
	// owed. It is cleared when they arrive and when a wait gives up, so a proposal
	// is waited on once.
	waiting bool
	// arrived holds the step indexes whose result has been seen.
	arrived map[int]bool
	// seenAt is when the agent first drained each step's result: the
	// stand-in for the action.executed time a recovery's Verify needs
	// (verify_recovery.go), since HumanInput carries no event time.
	seenAt map[int]time.Time
}

func newExecFlow(steps []events.ProposalStep) *execFlow {
	f := &execFlow{approved: map[string]bool{}, arrived: map[int]bool{}, seenAt: map[int]time.Time{}, lastStep: len(steps) - 1}
	seen := map[string]bool{}
	for _, s := range steps {
		if s.Layer != "" && !seen[s.Layer] {
			seen[s.Layer] = true
			f.required = append(f.required, s.Layer)
		}
	}
	return f
}

// observe updates the flow of the proposal an input is about. Inputs about
// proposals the agent did not create are ignored: it knows no required layers
// for them, so it cannot tell when they are fully approved.
func (t *proposalTracker) observe(in HumanInput) {
	if t == nil {
		return
	}
	switch in.Kind {
	case "approval_result":
		if in.Approved == nil || !*in.Approved || in.Layer == "" || in.ProposalID == "" {
			return
		}
		layer, id := in.Layer, in.ProposalID
		t.mu.Lock()
		defer t.mu.Unlock()
		f := t.flows[id]
		if f == nil || f.complete || len(f.required) == 0 || !contains(f.required, layer) {
			return
		}
		f.approved[layer] = true
		for _, l := range f.required {
			if !f.approved[l] {
				return
			}
		}
		f.complete = true
		f.waiting = !f.settled
	case "action_result":
		id, step, ok := actionResultFields(in)
		if !ok {
			return
		}
		t.mu.Lock()
		defer t.mu.Unlock()
		f := t.flows[id]
		if f == nil {
			return
		}
		f.arrived[step] = true
		if in.OK == nil || !*in.OK || step >= f.lastStep {
			f.settled = true
			f.waiting = false
		}
	}
}

// markSeen records when the result of a proposal step was first drained. A
// repeated result keeps the first time.
func (t *proposalTracker) markSeen(id string, step int, at time.Time) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	f := t.flows[id]
	if f == nil {
		return
	}
	if _, ok := f.seenAt[step]; !ok {
		f.seenAt[step] = at
	}
}

// seenAt returns when the result of a proposal step was first drained.
func (t *proposalTracker) seenAt(id string, step int) (time.Time, bool) {
	if t == nil {
		return time.Time{}, false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	f := t.flows[id]
	if f == nil {
		return time.Time{}, false
	}
	at, ok := f.seenAt[step]
	return at, ok
}

// observe updates the proposal tracker with one drained input and stamps an
// action_result with the time it was drained.
func (r *runner) observe(in HumanInput) {
	r.proposals.observe(in)
	if in.Kind != "action_result" {
		return
	}
	if id, step, ok := actionResultFields(in); ok {
		r.proposals.markSeen(id, step, r.nowTime())
	}
}

// awaiting returns the IDs of proposals approved in full whose step results are
// still owed and not yet waited on.
func (t *proposalTracker) awaiting() []string {
	if t == nil {
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	var ids []string
	for id, f := range t.flows {
		if f.waiting && !f.settled {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	return ids
}

// lateProposal is a proposal approved in full whose wait ended without every step
// result: the steps still missing.
type lateProposal struct {
	id      string
	missing []int
}

// giveUpWaiting ends the wait for every proposal still awaiting and returns
// them, each with the steps whose result has not arrived. A step result that did
// arrive is not listed, so the note never contradicts what the model can read.
func (t *proposalTracker) giveUpWaiting() []lateProposal {
	ids := t.awaiting()
	if t == nil {
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	late := make([]lateProposal, 0, len(ids))
	for _, id := range ids {
		f := t.flows[id]
		f.waiting = false
		lp := lateProposal{id: id}
		for i := 0; i <= f.lastStep; i++ {
			if !f.arrived[i] {
				lp.missing = append(lp.missing, i)
			}
		}
		late = append(late, lp)
	}
	return late
}

func contains(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

// handleInputs is the one place a drained batch turns into a model input. It
// first holds the batch while a fully approved proposal still owes its step
// results, then runs verify-after-fix on everything it collected. The inputs it
// returns are the batch plus whatever arrived while it waited; the outcome notes
// carry the pending-results note when the wait ended without every result.
func (r *runner) handleInputs(ctx context.Context, inputs []HumanInput) ([]HumanInput, actionOutcome) {
	inputs, pending := r.awaitStepResults(ctx, inputs)
	out := r.handleActionResults(ctx, inputs)
	if pending != "" {
		out.notes = append(out.notes, pending)
	}
	return inputs, out
}

// waitingDetailResults is the status detail while a turn waits for step results.
const waitingDetailResults = "waiting for step results"

// awaitStepResults returns inputs once no proposal approved in full still owes
// its step results, or once timing.stepResultWait has passed. Approvals that do
// not complete a proposal never wait. When the wait ends with results missing,
// the returned note says which, and why the wait ended; otherwise it is empty.
func (r *runner) awaitStepResults(ctx context.Context, inputs []HumanInput) ([]HumanInput, string) {
	for _, in := range inputs {
		r.observe(in)
	}
	if len(r.proposals.awaiting()) == 0 {
		return inputs, ""
	}

	r.emit(ctx, events.AgentStatus, events.AgentStatusPayload{State: "working", Detail: waitingDetailResults})
	defer func() {
		// The detail must not outlive the wait.
		r.emit(ctx, events.AgentStatus, events.AgentStatusPayload{State: "working"})
	}()

	// One timer covers the whole wait: it cancels wctx when it fires, which ends
	// the blocking Inbox.Wait below. Other input waking the wait does not restart
	// it, and a steady stream of input cannot extend it: wctx is checked on every
	// round.
	wctx, cancel := context.WithCancel(ctx)
	var timedOut atomic.Bool
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := r.clk.Sleep(wctx, r.timing.stepResultWait); err == nil {
			timedOut.Store(true)
			cancel()
		}
	}()
	defer func() {
		cancel()
		<-done
	}()

	closed := false
	for len(r.proposals.awaiting()) > 0 && wctx.Err() == nil {
		if more := r.drain(ctx); len(more) > 0 {
			for _, in := range more {
				r.observe(in)
			}
			inputs = append(inputs, more...)
			continue
		}
		if err := r.deps.Inbox.Wait(wctx); err != nil {
			closed = wctx.Err() == nil && ctx.Err() == nil
			break
		}
	}
	if timedOut.Load() {
		// Results that landed as the timer fired still count.
		for _, in := range r.drain(ctx) {
			r.observe(in)
			inputs = append(inputs, in)
		}
	}

	late := r.proposals.giveUpWaiting()
	if len(late) == 0 || ctx.Err() != nil {
		return inputs, ""
	}
	why := "the wait limit of " + r.timing.stepResultWait.String() + " was reached"
	if closed {
		why = "the inbox closed while waiting"
	}
	return inputs, pendingResultsNote(late, why)
}

// maxMissingListed caps how many missing steps the note names per proposal.
const maxMissingListed = 6

// pendingResultsNote tells the model that approval is complete but not every
// step result has arrived. The results that did arrive are in the same message,
// so the note only asks the model to report those and to say nothing about the
// steps still missing. The IDs and indexes are the agent's own, not human text.
func pendingResultsNote(late []lateProposal, why string) string {
	parts := make([]string, 0, len(late))
	for _, lp := range late {
		parts = append(parts, "proposal "+lp.id+" ("+missingSteps(lp.missing)+")")
	}
	return verifyTag + "approval is complete for " + strings.Join(parts, "; ") + ", but not every step result has arrived: " + why + ". " +
		"Report only the step results shown in this message. Say nothing about whether the missing steps ran, and do not say approval is missing. Say only that their results are pending."
}

// missingSteps renders 0-based step indexes 1-based, as the room numbers
// action results (StepIndex+1): indexes 0 and 1 read "missing: steps 1, 2".
func missingSteps(missing []int) string {
	shown := missing
	extra := 0
	if len(shown) > maxMissingListed {
		shown, extra = shown[:maxMissingListed], len(missing)-maxMissingListed
	}
	nums := make([]string, len(shown))
	for i, n := range shown {
		nums[i] = strconv.Itoa(n + 1)
	}
	label := "missing: step "
	if len(missing) > 1 {
		label = "missing: steps "
	}
	s := label + strings.Join(nums, ", ")
	if extra > 0 {
		s += " and " + strconv.Itoa(extra) + " more"
	}
	return s
}
