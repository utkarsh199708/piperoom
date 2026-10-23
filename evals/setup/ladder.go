package setup

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"piperoom/internal/events"
	"piperoom/internal/setup/plan"
)

// ErrAgentVerified is what AgentSink returns for setup.verified: the type is
// written by the server's ladder runner only, never by the agent
// (docs/design/setup-agent.md §3.4; events.go's setup block).
var ErrAgentVerified = errors.New("evals/setup: only the ladder runner writes setup.verified, never the agent")

// ReadRequest is one read the fake ladder asks for: the tool the rung names
// and its input (nil when the rung's tool takes no input in this replay).
type ReadRequest struct {
	Tool  string          `json:"tool"`
	Input json.RawMessage `json:"input,omitempty"`
}

// Reader answers a ladder read with the tool's structured Data. In a run it
// is the room's read tools (replayed here); the fake never judges a Summary.
type Reader interface {
	Read(ctx context.Context, req ReadRequest) (json.RawMessage, error)
}

// ReadFunc adapts a function to Reader.
type ReadFunc func(ctx context.Context, req ReadRequest) (json.RawMessage, error)

// Read implements Reader.
func (f ReadFunc) Read(ctx context.Context, req ReadRequest) (json.RawMessage, error) {
	return f(ctx, req)
}

// Event is one event the fake ladder or the agent-side sink writes. The actor
// is part of the event so a test can prove setup.verified came from the
// server side: the room service's Sink has no actor field, but the evals sink
// does, and the check is the point.
type Event struct {
	Type    events.Type
	Payload any
	Actor   events.Actor
}

// Sink receives the fake ladder's events. A memory sink records them; the
// room's own sink is not used here because no setup room path exists yet.
type Sink interface {
	Emit(ctx context.Context, ev Event) error
}

// SinkFunc adapts a function to Sink.
type SinkFunc func(ctx context.Context, ev Event) error

// Emit implements Sink.
func (f SinkFunc) Emit(ctx context.Context, ev Event) error { return f(ctx, ev) }

// MemorySink is a Sink that keeps every event, in order.
type MemorySink struct {
	events []Event
}

// Emit implements Sink.
func (m *MemorySink) Emit(_ context.Context, ev Event) error {
	m.events = append(m.events, ev)
	return nil
}

// Events returns the recorded events.
func (m *MemorySink) Events() []Event { return append([]Event(nil), m.events...) }

// AgentSink wraps s as the agent's side of the room: every event is
// attributed to the agent, and setup.verified is refused before it can reach
// the sink, as production's Sink refuses it. Refused attempts are counted so
// a test can see the agent tried.
func AgentSink(s Sink) *AgentEvents { return &AgentEvents{next: s} }

// AgentEvents is AgentSink's sink.
type AgentEvents struct {
	next      Sink
	refused   int
	forwarded int
}

// Emit implements Sink.
func (a *AgentEvents) Emit(ctx context.Context, ev Event) error {
	if ev.Type == events.SetupVerified {
		a.refused++
		return ErrAgentVerified
	}
	a.forwarded++
	ev.Actor = events.Actor{Kind: events.ActorAgent, ID: "eval-agent", Name: "agent"}
	return a.next.Emit(ctx, ev)
}

// Refused is how many setup.verified events the agent tried to write.
func (a *AgentEvents) Refused() int { return a.refused }

// Forwarded is how many other events reached the wrapped sink.
func (a *AgentEvents) Forwarded() int { return a.forwarded }

// LadderConfig is what a FakeLadder needs to run one setup's ladder: the
// target shape plan.Ladder keys on, the plan the run verified, and rung 8's
// table.
type LadderConfig struct {
	Source string // postgres | mysql | ...
	Target string // compose | host | strimzi | ...
	Sink   string // a warehouse kind, or "" / SinkNone
	PlanID string
	// NonceHash is SHA-256 of the canary nonce, as setup.verified carries it.
	NonceHash string
	// Table and ExampleSQL are rung 8's payload: the table the person makes
	// a change in and the statement the UI shows.
	Table      string
	ExampleSQL string
	// WaitForChange makes rung 8 read for the person's change once. The
	// default reports it as still awaited and writes setup.awaiting_change,
	// which is what a replay does: no person acts in an eval.
	WaitForChange bool
}

// FakeLadder is the server's ladder runner for evals (U1-20): it issues each
// rung's read as a request through a Reader, evaluates plan.Ladder's
// predicate on the structured Data, and writes setup.verified itself, as the
// server does. It never accepts a verified event from the agent: the agent's
// sink is AgentSink, which refuses one.
type FakeLadder struct {
	cfg  LadderConfig
	r    Reader
	sink Sink

	requests []ReadRequest
	rungs    []events.SetupRung
}

// NewFakeLadder builds the ladder runner for cfg. r answers the rung reads
// (NewRecordingReader replays a recording); sink receives the events.
func NewFakeLadder(cfg LadderConfig, r Reader, sink Sink) *FakeLadder {
	return &FakeLadder{cfg: cfg, r: r, sink: sink}
}

// Requests returns the read requests the ladder issued, in order.
func (l *FakeLadder) Requests() []ReadRequest {
	return append([]ReadRequest(nil), l.requests...)
}

// Rungs returns the last verification's rungs.
func (l *FakeLadder) Rungs() []events.SetupRung {
	return append([]events.SetupRung(nil), l.rungs...)
}

// Verify runs the ladder: the rungs of plan.Ladder for the config's target
// shape, each read issued through the Reader until its predicate decides.
// Rungs 1 to 7 are the ladder's; rung 8 is the person's, reported as awaited
// (and with setup.awaiting_change) unless WaitForChange asks the reader once.
// It writes one setup.verified with every rung's outcome and returns it.
func (l *FakeLadder) Verify(ctx context.Context) (events.SetupVerifiedPayload, error) {
	if l.r == nil {
		return events.SetupVerifiedPayload{}, errors.New("evals/setup: a ladder needs a reader")
	}
	if l.sink == nil {
		return events.SetupVerifiedPayload{}, errors.New("evals/setup: a ladder needs a sink")
	}
	rungs, err := plan.Ladder(l.cfg.Source, l.cfg.Target, l.cfg.Sink)
	if err != nil {
		return events.SetupVerifiedPayload{}, fmt.Errorf("evals/setup: ladder: %w", err)
	}
	out := events.SetupVerifiedPayload{PlanID: l.cfg.PlanID, NonceHash: l.cfg.NonceHash}
	var awaiting bool
	for _, rung := range rungs {
		gr := events.SetupRung{Name: rung.Name}
		switch {
		case rung.NotApplicable != "":
			// A rung this setup does not have is green with the note and
			// issues no read, as the design says.
			gr.OK, gr.Note = true, rung.NotApplicable
		case rung.Name == plan.RungPersonChange && !l.cfg.WaitForChange:
			// Rung 8 waits for the person. An eval has no person: report it
			// awaited and ask for the change, as the room does.
			gr.OK, gr.Note = false, "awaiting the person's change"
			awaiting = true
		default:
			gr.OK, gr.Note = l.runRung(ctx, rung)
		}
		out.Rungs = append(out.Rungs, gr)
	}
	l.rungs = out.Rungs
	if awaiting {
		if err := l.sink.Emit(ctx, Event{
			Type:    events.SetupAwaitingChange,
			Payload: events.SetupAwaitingChangePayload{Table: l.cfg.Table, ExampleSQL: l.cfg.ExampleSQL},
			Actor:   events.Actor{Kind: events.ActorSystem, ID: "setup-ladder"},
		}); err != nil {
			return out, fmt.Errorf("evals/setup: write setup.awaiting_change: %w", err)
		}
	}
	if err := l.sink.Emit(ctx, Event{
		Type:    events.SetupVerified,
		Payload: out,
		Actor:   events.Actor{Kind: events.ActorSystem, ID: "setup-ladder"},
	}); err != nil {
		return out, fmt.Errorf("evals/setup: write setup.verified: %w", err)
	}
	return out, nil
}

// runRung issues rung.Reads in order until a predicate decides. The last
// read's note is the rung's when none passes.
func (l *FakeLadder) runRung(ctx context.Context, rung plan.Rung) (bool, string) {
	note := "the rung issued no read"
	for _, tool := range rung.Reads {
		req := ReadRequest{Tool: tool}
		l.requests = append(l.requests, req)
		data, err := l.r.Read(ctx, req)
		if err != nil {
			note = "the read " + tool + " failed: " + err.Error()
			continue
		}
		ok, n := rung.Predicate(data)
		if ok {
			return true, n
		}
		note = n
	}
	return false, note
}
