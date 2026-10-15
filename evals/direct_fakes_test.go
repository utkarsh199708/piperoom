package evals

// A minimal agent.Sink, agent.Proposals and agent.Inbox for unit tests that
// call a runner directly, without a room. Evaluate never uses them: every
// evaluated run goes through room.Service (room_path.go).

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"time"

	"piperoom/internal/agent"
	"piperoom/internal/events"
)

// recorder is the run's Sink and Proposals: it keeps every event in order,
// and every proposal.
type recorder struct {
	mu        sync.Mutex
	events    []RecordedEvent
	proposals []events.ProposalCreatedPayload
	nextID    int
	now       func() time.Time
}

func newRecorder() *recorder { return &recorder{now: time.Now} }

// Emit implements agent.Sink.
func (r *recorder) Emit(_ context.Context, typ events.Type, payload any) error {
	b, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("evals: marshal %s payload: %w", typ, err)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, RecordedEvent{Type: typ, Payload: b, At: r.now()})
	return nil
}

// Create implements agent.Proposals. It stores the proposal and never runs
// anything; the agent sees the same kind of reply the room service gives.
func (r *recorder) Create(ctx context.Context, p events.ProposalCreatedPayload) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	r.mu.Lock()
	r.nextID++
	p.ProposalID = "p_" + strconv.Itoa(r.nextID)
	r.mu.Unlock()
	if err := r.Emit(ctx, events.ProposalCreated, p); err != nil {
		return "", err
	}
	r.mu.Lock()
	r.proposals = append(r.proposals, p)
	r.mu.Unlock()
	return p.ProposalID, nil
}

func (r *recorder) snapshot() []RecordedEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]RecordedEvent(nil), r.events...)
}

// count is how many events have been recorded so far.
func (r *recorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.events)
}

// createdProposals returns the proposals created so far, oldest first.
func (r *recorder) createdProposals() []events.ProposalCreatedPayload {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]events.ProposalCreatedPayload(nil), r.proposals...)
}

// scriptedInbox gives the agent the scenario's inputs one per idle. Once they
// run out it reports agent.ErrInboxClosed, which ends agent.Run cleanly.
type scriptedInbox struct {
	mu      sync.Mutex
	queue   []scriptedInput
	pending []agent.HumanInput
	idles   int
	// log, when set, records each steer and comment as the room log would
	// (steer.sent, comment.added) at the moment the agent receives it, so the
	// transcript shows what the agent had been told when it spoke (the ask
	// outcome counts only questions after the first one). Approval and
	// action results have no event of their own here: theirs need a proposal.
	log agent.Sink
}

// scriptedInput is one scenario input waiting for its idle.
type scriptedInput struct {
	index int // position in the scenario's inputs
	in    agent.HumanInput
}

func newScriptedInbox(inputs []agent.HumanInput, log agent.Sink) *scriptedInbox {
	in := &scriptedInbox{log: log}
	for i, h := range inputs {
		in.queue = append(in.queue, scriptedInput{index: i, in: h})
	}
	return in
}

// Drain implements agent.Inbox.
func (in *scriptedInbox) Drain(ctx context.Context) ([]agent.HumanInput, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	in.mu.Lock()
	out := in.pending
	in.pending = nil
	in.mu.Unlock()
	if in.log != nil {
		for _, h := range out {
			if err := in.logInput(ctx, h); err != nil {
				return nil, err
			}
		}
	}
	return out, nil
}

// logInput records one delivered input in the room log's shape.
func (in *scriptedInbox) logInput(ctx context.Context, h agent.HumanInput) error {
	switch h.Kind {
	case "steer":
		return in.log.Emit(ctx, events.SteerSent, events.SteerSentPayload{Text: h.Text})
	case "comment":
		return in.log.Emit(ctx, events.CommentAdded, events.CommentAddedPayload{
			CommentID: h.CommentID, StepID: h.StepID, Text: h.Text, MentionsAgent: true,
		})
	}
	return nil
}

// Wait implements agent.Inbox. The agent calls it only when it is idle with
// nothing pending, so each call is one idle.
func (in *scriptedInbox) Wait(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	in.mu.Lock()
	defer in.mu.Unlock()
	if len(in.pending) > 0 {
		return nil
	}
	in.idles++
	if len(in.queue) > 0 {
		in.pending = append(in.pending, in.queue[0].in)
		in.queue = in.queue[1:]
		return nil
	}
	return agent.ErrInboxClosed
}

func (in *scriptedInbox) idleCount() int {
	in.mu.Lock()
	defer in.mu.Unlock()
	return in.idles
}
