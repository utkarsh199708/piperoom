package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"sync/atomic"
	"time"

	"piperoom/internal/actions"
	"piperoom/internal/agent"
	"piperoom/internal/events"
	"piperoom/internal/owners"
)

// stepTimeout bounds one executed action step.
const stepTimeout = 30 * time.Second

// approveProposals is the -approve-all Proposals. It acts as every layer owner
// and as the executor: it approves each required layer, then runs the steps in
// order, stopping at the first failure like the room's executor does, and
// hands the outcomes to the agent as approval_result / action_result inputs.
// It exists for baseline and eval runs against the local dev stack; main
// builds it only after checkApproveAll said yes.
type approveProposals struct {
	acts     *actions.Registry
	pipe     owners.Pipeline
	havePipe bool
	inbox    pushInbox
	out      io.Writer
	ctx      context.Context
	counter  atomic.Uint64
}

// pushInbox is what approveProposals needs from the inbox.
type pushInbox interface {
	push(agent.HumanInput)
	hold()
	release()
}

func (a *approveProposals) Create(_ context.Context, p events.ProposalCreatedPayload) (string, error) {
	id := "p_" + strconv.FormatUint(a.counter.Add(1), 10)
	b, _ := json.MarshalIndent(p, "", "  ")
	fmt.Fprintf(a.out, "PROPOSAL %s (AUTO-APPROVED by -approve-all, will execute):\n%s\n", id, truncatePrinted(sanitizeTerminal(string(b))))
	// The hold keeps the inbox open while the steps run: with stdin at EOF the
	// run would otherwise end before the action results arrive.
	a.inbox.hold()
	go func() {
		defer a.inbox.release()
		a.approveAndRun(id, p)
	}()
	return id, nil
}

func (a *approveProposals) approveAndRun(id string, p events.ProposalCreatedPayload) {
	system := events.Actor{Kind: events.ActorSystem, ID: "approve-all", Name: "approve-all"}
	yes := true
	for _, layer := range p.RequiredLayers {
		a.inbox.push(agent.HumanInput{
			Actor: system, Kind: "approval_result", ProposalID: id, Layer: layer,
			Approved: &yes, Text: layer + " approved (-approve-all)",
		})
	}
	for i, st := range p.Steps {
		if a.ctx.Err() != nil {
			return
		}
		msg, err := a.runStep(st)
		ok := err == nil
		if err != nil {
			msg = err.Error()
		}
		idx := i
		a.inbox.push(agent.HumanInput{
			Actor: system, Kind: "action_result", ProposalID: id, StepIndex: &idx,
			OK: &ok, Text: fmt.Sprintf("step %d %s: %s", i, st.Action, msg),
		})
		if !ok {
			return
		}
	}
}

// runStep re-validates and scope-checks the step, then executes it. An action
// that cannot be scope-checked is refused, as in the room service.
func (a *approveProposals) runStep(st events.ProposalStep) (string, error) {
	act, ok := a.acts.Get(st.Action)
	if !ok {
		return "", fmt.Errorf("unknown action %q", st.Action)
	}
	if err := act.Validate(a.ctx, st.Params); err != nil {
		return "", fmt.Errorf("validate: %w", err)
	}
	sc, ok := act.(actions.Scoped)
	if !ok {
		return "", fmt.Errorf("action %q does not implement scope checks: refused", st.Action)
	}
	if !a.havePipe {
		return "", fmt.Errorf("pipeline is not in an owners file: cannot scope-check, refused")
	}
	if err := sc.CheckScope(a.pipe, st.Params); err != nil {
		return "", fmt.Errorf("scope: %w", err)
	}
	ctx, cancel := context.WithTimeout(a.ctx, stepTimeout)
	defer cancel()
	msg, err := act.Execute(ctx, st.Params)
	if err != nil {
		return "", fmt.Errorf("execute: %w", err)
	}
	return msg, nil
}
