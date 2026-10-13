package evals

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

// RecordedEvent is one event the agent produced during a run.
type RecordedEvent struct {
	Type    events.Type     `json:"type"`
	Payload json.RawMessage `json:"payload"`
	At      time.Time       `json:"at"`
}

// Transcript is everything one run produced, in order. Proposals appear in
// Events as proposal.created, the way the room service would log them.
type Transcript struct {
	Events []RecordedEvent `json:"events"`
	Misses []Miss          `json:"misses,omitempty"`
	// Attempts are every action Validate call, including the rejected ones
	// that never became a proposal.
	Attempts []ActionAttempt `json:"action_attempts,omitempty"`
	Idles    int             `json:"idles"`
}

// recorder is the run's Sink and Proposals: it keeps every event in order.
type recorder struct {
	mu     sync.Mutex
	events []RecordedEvent
	nextID int
	now    func() time.Time
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
	return p.ProposalID, nil
}

func (r *recorder) snapshot() []RecordedEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]RecordedEvent(nil), r.events...)
}

// scriptedInbox gives the agent the scenario's inputs one per idle. Once they
// run out it reports agent.ErrInboxClosed, which ends agent.Run cleanly.
type scriptedInbox struct {
	mu      sync.Mutex
	queue   []agent.HumanInput
	pending []agent.HumanInput
	idles   int
	// log, when set, records each steer and comment as the room log would
	// (steer.sent, comment.added) at the moment the agent receives it, so the
	// transcript shows what the agent had been told when it spoke (the ask
	// outcome counts only questions after the first one). Approval and
	// action results have no event of their own here: theirs need a proposal.
	log agent.Sink
}

func newScriptedInbox(inputs []agent.HumanInput, log agent.Sink) *scriptedInbox {
	return &scriptedInbox{queue: append([]agent.HumanInput(nil), inputs...), log: log}
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
	if len(in.queue) == 0 {
		return agent.ErrInboxClosed
	}
	in.pending = append(in.pending, in.queue[0])
	in.queue = in.queue[1:]
	return nil
}

func (in *scriptedInbox) idleCount() int {
	in.mu.Lock()
	defer in.mu.Unlock()
	return in.idles
}
