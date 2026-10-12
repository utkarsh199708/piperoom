package agent

import (
	"context"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"

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
// HumanInput carries no structured proposal or layer, so the agent reads both
// back from the approval text the room writes (see internal/room feedInbox) and
// compares the approved layers with the layers it stored when it created the
// proposal. Those are the layers the room requires: the union of the step layers.

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
}

func newExecFlow(steps []events.ProposalStep) *execFlow {
	f := &execFlow{approved: map[string]bool{}, lastStep: len(steps) - 1}
	seen := map[string]bool{}
	for _, s := range steps {
		if s.Layer != "" && !seen[s.Layer] {
			seen[s.Layer] = true
			f.required = append(f.required, s.Layer)
		}
	}
	return f
}

// parseApproval splits the room's approval text back into its parts:
//
//	layer "<layer>" approved proposal <id>
//
// A rejection ("... rejected proposal <id>: <reason>") is not an approval and
// does not parse. The layer is quoted with %q, so it is read back with Unquote.
func parseApproval(text string) (layer, proposalID string, ok bool) {
	rest, found := strings.CutPrefix(text, "layer ")
	if !found {
		return "", "", false
	}
	quoted, err := strconv.QuotedPrefix(rest)
	if err != nil {
		return "", "", false
	}
	layer, err = strconv.Unquote(quoted)
	if err != nil || layer == "" {
		return "", "", false
	}
	id, found := strings.CutPrefix(rest[len(quoted):], " approved proposal ")
	if !found || id == "" || strings.ContainsAny(id, " \t\r\n:") {
		return "", "", false
	}
	return layer, id, true
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
		layer, id, ok := parseApproval(in.Text)
		if !ok {
			return
		}
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
		id, step, ok := parseActionResult(in.Text)
		if !ok {
			return
		}
		t.mu.Lock()
		defer t.mu.Unlock()
		f := t.flows[id]
		if f == nil {
			return
		}
		if in.OK == nil || !*in.OK || step >= f.lastStep {
			f.settled = true
			f.waiting = false
		}
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

// giveUpWaiting ends the wait for every proposal still awaiting and returns
// their IDs.
func (t *proposalTracker) giveUpWaiting() []string {
	ids := t.awaiting()
	if t == nil {
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, id := range ids {
		t.flows[id].waiting = false
	}
	return ids
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
// carry the pending-results note when the wait timed out.
func (r *runner) handleInputs(ctx context.Context, inputs []HumanInput) ([]HumanInput, actionOutcome) {
	inputs, pending := r.awaitStepResults(ctx, inputs)
	out := r.handleActionResults(ctx, inputs)
	if pending != "" {
		out.notes = append(out.notes, pending)
	}
	return inputs, out
}

// awaitStepResults returns inputs once no proposal approved in full still owes
// its step results, or once timing.stepResultWait has passed. Approvals that do
// not complete a proposal never wait. On a timeout the returned note says the
// results are pending; otherwise it is empty.
func (r *runner) awaitStepResults(ctx context.Context, inputs []HumanInput) ([]HumanInput, string) {
	for _, in := range inputs {
		r.proposals.observe(in)
	}
	if len(r.proposals.awaiting()) == 0 {
		return inputs, ""
	}

	// One timer covers the whole wait: it cancels wctx when it fires, which ends
	// the blocking Inbox.Wait below. Other input waking the wait does not restart
	// it.
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

	for len(r.proposals.awaiting()) > 0 {
		if more := r.drain(ctx); len(more) > 0 {
			for _, in := range more {
				r.proposals.observe(in)
			}
			inputs = append(inputs, more...)
			continue
		}
		if err := r.deps.Inbox.Wait(wctx); err != nil {
			break
		}
	}
	if timedOut.Load() {
		// Results that landed as the timer fired still count.
		for _, in := range r.drain(ctx) {
			r.proposals.observe(in)
			inputs = append(inputs, in)
		}
	}

	ids := r.proposals.giveUpWaiting()
	if len(ids) == 0 {
		return inputs, ""
	}
	return inputs, pendingResultsNote(ids, r.timing.stepResultWait.String())
}

// pendingResultsNote tells the model that approval is complete but the executor
// has not reported yet, so it claims neither that steps ran nor that they did
// not. The IDs are the agent's own proposal IDs, not human text.
func pendingResultsNote(ids []string, waited string) string {
	return verifyTag + "approval is complete for " + proposalList(ids) + ", but the step results have not arrived yet (waited " + waited + "). Do not say that any step ran or did not run, and do not say approval is missing. Say the results are pending."
}

func proposalList(ids []string) string {
	if len(ids) == 1 {
		return "proposal " + ids[0]
	}
	return "proposals " + strings.Join(ids, ", ")
}
