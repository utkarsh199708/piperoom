package demo

import (
	"context"
	"sync"
	"testing"
	"time"

	"piperoom/internal/agent"
	"piperoom/internal/events"
)

type recSink struct {
	mu  sync.Mutex
	evs []events.Type
	st  []events.AgentStatusPayload
}

func (r *recSink) Emit(_ context.Context, typ events.Type, payload any) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.evs = append(r.evs, typ)
	if p, ok := payload.(events.AgentStatusPayload); ok {
		r.st = append(r.st, p)
	}
	return nil
}

// closedInbox is a shadow room's inbox: nothing to drain, Wait says closed.
type closedInbox struct{ waits int }

func (c *closedInbox) Drain(context.Context) ([]agent.HumanInput, error) { return nil, nil }
func (c *closedInbox) Wait(context.Context) error {
	c.waits++
	return agent.ErrInboxClosed
}

// With Proposals == nil (a shadow room) the scripted run emits its five steps
// and the hypothesis, says idle, and ends on the closed inbox with nil.
func TestRunWithoutProposalsEndsOnClosedInbox(t *testing.T) {
	t.Parallel()
	sink := &recSink{}
	in := &closedInbox{}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	err := Run(ctx, agent.Alert{RoomID: "r", PipelineID: "orders"}, agent.Deps{Sink: sink, Inbox: in}, agent.Options{})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if ctx.Err() != nil {
		t.Fatal("Run ended by the test deadline, not by the closed inbox")
	}
	count := map[events.Type]int{}
	for _, e := range sink.evs {
		count[e]++
	}
	if count[events.AgentStepStart] != 5 || count[events.AgentToolResult] != 5 || count[events.AgentHypothesis] != 1 {
		t.Fatalf("events = %v", count)
	}
	last := sink.st[len(sink.st)-1]
	if last.State != "idle" {
		t.Fatalf("last status = %+v, want idle", last)
	}
	for _, s := range sink.st {
		if s.State == "waiting_approval" {
			t.Fatal("waiting_approval without a proposal")
		}
	}
	if in.waits != 1 {
		t.Fatalf("inbox waits = %d, want 1", in.waits)
	}
}
