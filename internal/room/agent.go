package room

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"piperoom/internal/actions"
	"piperoom/internal/agent"
	"piperoom/internal/events"
	"piperoom/internal/owners"
)

var errRoomClosed = errors.New("room: closed")

// inbox queues human input for one room's agent. It implements agent.Inbox.
type inbox struct {
	mu    sync.Mutex
	queue []agent.HumanInput
	wake  chan struct{}
}

func newInbox() *inbox { return &inbox{wake: make(chan struct{}, 1)} }

func (b *inbox) push(in agent.HumanInput) {
	b.mu.Lock()
	b.queue = append(b.queue, in)
	b.mu.Unlock()
	select {
	case b.wake <- struct{}{}:
	default:
	}
}

// Drain returns and clears the pending inputs.
func (b *inbox) Drain(context.Context) ([]agent.HumanInput, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := b.queue
	b.queue = nil
	return out, nil
}

// Wait blocks until an input is pending or ctx ends.
func (b *inbox) Wait(ctx context.Context) error {
	for {
		b.mu.Lock()
		n := len(b.queue)
		b.mu.Unlock()
		if n > 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-b.wake:
		}
	}
}

// feedAgent turns the events the agent must read into inbox entries.
func (s *Service) feedAgent(r *roomEntry, e events.Event) {
	in := agent.HumanInput{Seq: e.Seq, Actor: e.Actor}
	switch e.Type {
	case events.SteerSent:
		p, err := events.Decode[events.SteerSentPayload](e)
		if err != nil {
			return
		}
		in.Kind, in.Text = "steer", p.Text
	case events.CommentAdded:
		p, err := events.Decode[events.CommentAddedPayload](e)
		if err != nil || !p.MentionsAgent {
			return
		}
		in.Kind, in.Text, in.StepID, in.CommentID = "comment", p.Text, p.StepID, p.CommentID
	case events.ApprovalGiven:
		p, err := events.Decode[events.ApprovalGivenPayload](e)
		if err != nil {
			return
		}
		in.Kind, in.Text = "approval_result", fmt.Sprintf("The %s owner approved proposal %s.", p.Layer, p.ProposalID)
	case events.ApprovalRejected:
		p, err := events.Decode[events.ApprovalRejectedPayload](e)
		if err != nil {
			return
		}
		in.Kind, in.Text = "approval_result", fmt.Sprintf("The %s owner rejected proposal %s. Reason: %s", p.Layer, p.ProposalID, p.Reason)
	case events.ActionExecuted:
		p, err := events.Decode[events.ActionExecutedPayload](e)
		if err != nil {
			return
		}
		res := "succeeded"
		if !p.OK {
			res = "FAILED"
		}
		in.Kind, in.Text = "action_result", fmt.Sprintf("Step %d of proposal %s %s: %s", p.StepIndex, p.ProposalID, res, p.Output)
	default:
		return
	}
	r.inbox.push(in)
}

// roomSink is the agent's Sink: it appends to the room log as the agent actor.
type roomSink struct {
	s *Service
	r *roomEntry
}

func (k roomSink) Emit(ctx context.Context, typ events.Type, payload any) error {
	k.r.mu.Lock()
	defer k.r.mu.Unlock()
	if k.r.st.snap.Status != StatusOpen {
		return errRoomClosed
	}
	_, err := k.s.appendLocked(ctx, k.r, typ, events.AgentActor, payload)
	return err
}

// roomProposals is the agent's Proposals.
type roomProposals struct {
	s *Service
	r *roomEntry
}

func (p roomProposals) Create(ctx context.Context, pl events.ProposalCreatedPayload) (string, error) {
	return p.s.createProposal(ctx, p.r, pl)
}

// startAgent launches the supervised agent goroutine for a new room.
func (s *Service) startAgent(r *roomEntry, title, pipelineID string, raw json.RawMessage) {
	if s.cfg.Runner == nil {
		return
	}
	ctx, cancel := context.WithCancel(s.base)
	r.mu.Lock()
	r.cancelAgent = cancel
	r.mu.Unlock()
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer cancel()
		alert := agent.Alert{RoomID: r.id, Title: title, PipelineID: pipelineID, Raw: raw, PipelineMap: s.pipelineMap(ctx, pipelineID)}
		deps := agent.Deps{
			Tools: s.cfg.Tools, Actions: s.cfg.Actions, Inbox: r.inbox,
			Sink: roomSink{s, r}, Proposals: roomProposals{s, r}, Logger: s.log,
		}
		err := s.cfg.Runner(ctx, alert, deps, s.cfg.AgentOptions)
		if err == nil || ctx.Err() != nil {
			return
		}
		// One failure is reported to the room; the agent is not restarted in a loop.
		s.log.Error("agent stopped", "room", r.id, "err", err)
		_ = roomSink{s, r}.Emit(s.base, events.AgentStatus, events.AgentStatusPayload{State: "error", Detail: "the agent stopped unexpectedly"})
	}()
}

func (s *Service) pipelineMap(ctx context.Context, pipelineID string) string {
	if s.cfg.Topology == nil {
		return ""
	}
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	g, err := s.cfg.Topology.Build(ctx, pipelineID)
	if err != nil {
		s.log.Warn("pipeline map unavailable", "pipeline", pipelineID, "err", err)
		return ""
	}
	return s.cfg.Topology.Render(g)
}

// createProposal validates a proposal from the agent and appends it. Params are
// frozen here: the executor runs exactly what was approved.
func (s *Service) createProposal(ctx context.Context, r *roomEntry, p events.ProposalCreatedPayload) (string, error) {
	r.mu.Lock()
	snap := r.st.Snapshot()
	r.mu.Unlock()
	if snap.Status != StatusOpen {
		return "", errRoomClosed
	}
	pipe, ok := s.cfg.Owners.Pipeline(snap.PipelineID)
	if !ok {
		return "", errors.New("proposals are disabled: this room's pipeline is unknown")
	}
	if p.Title == "" || len(p.Steps) == 0 || len(p.Steps) > 10 {
		return "", errors.New("a proposal needs a title and 1-10 steps")
	}
	steps := make([]events.ProposalStep, 0, len(p.Steps))
	var layers []string
	for i, st := range p.Steps {
		a, ok := s.cfg.Actions.Get(st.Action)
		if !ok {
			return "", fmt.Errorf("step %d: unknown action %q", i, st.Action)
		}
		if st.Layer != "" && st.Layer != a.Layer() {
			return "", fmt.Errorf("step %d: action %q belongs to layer %q, not %q", i, st.Action, a.Layer(), st.Layer)
		}
		if err := checkStep(a, pipe, st.Params); err != nil {
			return "", fmt.Errorf("step %d: %w", i, err)
		}
		st.Layer = a.Layer()
		steps = append(steps, st)
		if !slices.Contains(layers, st.Layer) {
			layers = append(layers, st.Layer)
		}
	}
	// The server derives the required layers; the proposer cannot choose them.
	slices.SortFunc(layers, func(a, b string) int { return slices.Index(events.Layers, a) - slices.Index(events.Layers, b) })
	p.Steps, p.RequiredLayers = steps, layers
	p.ProposalID = newID("p_", s.now())

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.st.snap.Status != StatusOpen {
		return "", errRoomClosed
	}
	if _, err := s.appendLocked(ctx, r, events.ProposalCreated, events.AgentActor, p); err != nil {
		return "", err
	}
	return p.ProposalID, nil
}

// checkStep runs Validate and the pipeline scope check (docs/api.md §Proposals).
// An action that does not implement actions.Scoped is refused.
func checkStep(a actions.Action, pipe owners.Pipeline, params json.RawMessage) error {
	if err := a.Validate(params); err != nil {
		return fmt.Errorf("action %s: %w", a.Name(), err)
	}
	sc, ok := a.(actions.Scoped)
	if !ok {
		return fmt.Errorf("action %s cannot be scoped to a pipeline and is refused", a.Name())
	}
	if err := sc.CheckScope(pipe, params); err != nil {
		return fmt.Errorf("action %s: %w", a.Name(), err)
	}
	return nil
}
