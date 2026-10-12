package room_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"piperoom/internal/actions"
	"piperoom/internal/agent"
	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/room"
	"piperoom/internal/store/memstore"
)

// fakeAnthropic is a stand-in for the Messages API that streams scripted
// turns. It records how many requests are in flight, which is the number the
// agent cap is meant to bound.
type fakeAnthropic struct {
	*httptest.Server
	mu       sync.Mutex
	requests int
	inflight int
	maxIn    int
	// script returns the turn for request n (1-based) and may block to hold the
	// request open.
	script func(n int) fakeTurn
}

// fakeTurn is one assistant turn: a text reply, or a single tool call.
type fakeTurn struct {
	text     string
	toolName string
	toolJSON string
}

func newFakeAnthropic(t *testing.T, script func(n int) fakeTurn) *fakeAnthropic {
	t.Helper()
	f := &fakeAnthropic{script: script}
	f.Server = httptest.NewServer(http.HandlerFunc(f.handle))
	t.Cleanup(f.Close)
	t.Setenv("ANTHROPIC_BASE_URL", f.URL)
	t.Setenv("ANTHROPIC_API_KEY", "sk-ant-test-not-real")
	return f
}

func (f *fakeAnthropic) counts() (requests, maxInflight int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.requests, f.maxIn
}

func (f *fakeAnthropic) handle(w http.ResponseWriter, r *http.Request) {
	_, _ = io.Copy(io.Discard, r.Body)
	f.mu.Lock()
	f.requests++
	n := f.requests
	f.inflight++
	if f.inflight > f.maxIn {
		f.maxIn = f.inflight
	}
	f.mu.Unlock()
	defer func() {
		f.mu.Lock()
		f.inflight--
		f.mu.Unlock()
	}()

	turn := f.script(n)

	w.Header().Set("Content-Type", "text/event-stream")
	fl, _ := w.(http.Flusher)
	send := func(event string, data any) {
		b, _ := json.Marshal(data)
		fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event, b)
		if fl != nil {
			fl.Flush()
		}
	}
	send("message_start", map[string]any{"type": "message_start", "message": map[string]any{
		"id": fmt.Sprintf("msg_%d", n), "type": "message", "role": "assistant", "model": "claude-opus-5-5",
		"content": []any{}, "stop_reason": nil, "stop_sequence": nil,
		"usage": map[string]any{"input_tokens": 1, "output_tokens": 1},
	}})
	stop := "end_turn"
	if turn.toolName != "" {
		stop = "tool_use"
		send("content_block_start", map[string]any{"type": "content_block_start", "index": 0, "content_block": map[string]any{
			"type": "tool_use", "id": fmt.Sprintf("toolu_%d", n), "name": turn.toolName, "input": map[string]any{},
		}})
		send("content_block_delta", map[string]any{"type": "content_block_delta", "index": 0, "delta": map[string]any{
			"type": "input_json_delta", "partial_json": turn.toolJSON,
		}})
	} else {
		send("content_block_start", map[string]any{"type": "content_block_start", "index": 0, "content_block": map[string]any{
			"type": "text", "text": "",
		}})
		send("content_block_delta", map[string]any{"type": "content_block_delta", "index": 0, "delta": map[string]any{
			"type": "text_delta", "text": turn.text,
		}})
	}
	send("content_block_stop", map[string]any{"type": "content_block_stop", "index": 0})
	send("message_delta", map[string]any{"type": "message_delta",
		"delta": map[string]any{"stop_reason": stop, "stop_sequence": nil},
		"usage": map[string]any{"output_tokens": 1}})
	send("message_stop", map[string]any{"type": "message_stop"})
}

// T6a: the real agent.Run, two rooms, cap 1. Each agent really goes idle after
// its turn (Inbox.Wait), which is what frees the slot; the next steers wake
// both at once and only one model call is ever in flight.
func TestAgentCapRealAgentIdleAndWake(t *testing.T) {
	gate1, gate3 := make(chan struct{}), make(chan struct{})
	f := newFakeAnthropic(t, func(n int) fakeTurn {
		switch n {
		case 1:
			<-gate1
		case 3:
			<-gate3
		}
		return fakeTurn{text: "looking into it"}
	})
	c := &clock{t: time.Date(2026, 10, 11, 10, 0, 0, 0, time.UTC)}
	svc := room.New(room.Deps{
		Store: memstore.NewWithClock(c.Now), Users: room.DefaultUsers(), Now: c.Now, MaxAgents: 1,
		Run: agent.Run, WriteHandoff: fakeWrite,
	})
	t.Cleanup(svc.Close)
	arjun := mustUser(t, svc, "arjun")

	a := createRoom(t, svc, "arjun").ID
	waitFor(t, 5*time.Second, func() bool { n, _ := f.counts(); return n == 1 })
	b := createRoom(t, svc, "arjun").ID
	waitQueued(t, svc, b)
	close(gate1)

	// A finishes its turn and waits, so B gets the slot; both end idle.
	idle := func(id string) func() bool {
		return func() bool { return agentState(t, svc, id).State == "idle" }
	}
	waitFor(t, 10*time.Second, idle(a))
	waitFor(t, 10*time.Second, idle(b))
	if n, m := f.counts(); n != 2 || m != 1 {
		t.Fatalf("after first turns: requests=%d max in flight=%d, want 2 and 1", n, m)
	}

	// Wake both at once. One model call is held open; the other room queues.
	for _, id := range []string{a, b} {
		if _, err := svc.Steer(context.Background(), arjun, id, "keep going"); err != nil {
			t.Fatalf("steer: %v", err)
		}
	}
	waitFor(t, 10*time.Second, func() bool { n, _ := f.counts(); return n == 3 })
	waitFor(t, 10*time.Second, func() bool {
		return agentState(t, svc, a).State == "queued" || agentState(t, svc, b).State == "queued"
	})
	settle()
	if n, _ := f.counts(); n != 3 {
		t.Fatalf("the queued room called the model: requests=%d, want 3", n)
	}
	close(gate3)
	waitFor(t, 10*time.Second, func() bool { n, _ := f.counts(); return n == 4 })
	waitFor(t, 10*time.Second, idle(a))
	waitFor(t, 10*time.Second, idle(b))
	if _, m := f.counts(); m != 1 {
		t.Fatalf("max model calls in flight = %d, want 1", m)
	}
}

// gatedAction is a write action that runs only when the test lets it, so the
// agent can be held in its wait for step results.
type gatedAction struct {
	release chan struct{}
}

func (a *gatedAction) Name() string     { return "gated_fix" }
func (a *gatedAction) Layer() string    { return "postgres" }
func (a *gatedAction) Describe() string { return "a fix the test releases" }
func (a *gatedAction) ParamsSchema() map[string]any {
	return map[string]any{"type": "object", "additionalProperties": false, "properties": map[string]any{}}
}
func (a *gatedAction) Validate(json.RawMessage) error                    { return nil }
func (a *gatedAction) CheckScope(owners.Pipeline, json.RawMessage) error { return nil }
func (a *gatedAction) Execute(ctx context.Context, _ json.RawMessage) (string, error) {
	select {
	case <-a.release:
		return "done", nil
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

// T6b: the real agent.Run through proposal, full approval and the wait for
// step results, at cap 1 with a second room. While the agent waits for the
// executor it holds no slot (the other room runs); when the result arrives it
// queues for a slot before its next model call.
func TestAgentCapRealAgentApprovalWait(t *testing.T) {
	f := newFakeAnthropic(t, func(n int) fakeTurn {
		if n == 1 {
			return fakeTurn{
				toolName: "propose_fix",
				toolJSON: `{"title":"restart the stalled task","steps":[{"action":"gated_fix","params":{},"description":"run the fix"}],` +
					`"risk":"a short pause while the task restarts","rollback":"stop the task and restore the old settings"}`,
			}
		}
		return fakeTurn{text: "all noted"}
	})
	act := &gatedAction{release: make(chan struct{})}
	reg := actions.NewRegistry()
	if err := reg.Register(act); err != nil {
		t.Fatalf("register: %v", err)
	}
	cr := newCapRunner()
	var first sync.Once
	run := func(ctx context.Context, a agent.Alert, d agent.Deps, o agent.Options) error {
		real := false
		first.Do(func() { real = true })
		if real {
			return agent.Run(ctx, a, d, o)
		}
		return cr.run(ctx, a, d, o)
	}
	c := &clock{t: time.Date(2026, 10, 11, 10, 0, 0, 0, time.UTC)}
	svc := room.New(room.Deps{
		Store: memstore.NewWithClock(c.Now), Actions: reg, Owners: testOwners(), Users: room.DefaultUsers(),
		Now: c.Now, MaxAgents: 1, Run: run, WriteHandoff: fakeWrite,
	})
	t.Cleanup(svc.Close)

	a := createRoom(t, svc, "arjun").ID
	// The agent proposes, then ends its turn and goes idle.
	waitFor(t, 10*time.Second, func() bool {
		s, err := svc.GetRoom(context.Background(), a)
		return err == nil && len(s.OpenProposals) == 1 && s.AgentStatus.State == "idle"
	})
	snap, _ := svc.GetRoom(context.Background(), a)
	pid := snap.OpenProposals[0].ProposalID

	// The last owner approves. The agent wakes, sees the proposal is fully
	// approved with results owed, and waits for them: no slot while it waits.
	if _, err := svc.ApproveProposal(context.Background(), mustUser(t, svc, "priya"), pid, "postgres"); err != nil {
		t.Fatalf("approve: %v", err)
	}
	waitFor(t, 10*time.Second, func() bool {
		s := agentState(t, svc, a)
		return s.State == "working" && s.Detail == "waiting for step results"
	})
	b := createRoom(t, svc, "arjun").ID
	cr.waitStarted(t, 1) // room B runs although A's agent is mid-wait
	if got := cr.startedIDs(); got[0] != b {
		t.Fatalf("started %v, want room B", got)
	}
	if n, _ := f.counts(); n != 2 {
		t.Fatalf("model requests = %d before the step result, want 2", n)
	}

	// The executor reports. A must queue behind B for its next model call.
	close(act.release)
	waitQueued(t, svc, a)
	settle()
	if n, _ := f.counts(); n != 2 {
		t.Fatalf("A called the model without a slot: requests=%d, want 2", n)
	}
	cr.finish(b)
	waitFor(t, 10*time.Second, func() bool { n, _ := f.counts(); return n == 3 })
	waitFor(t, 10*time.Second, func() bool { return agentState(t, svc, a).State == "idle" })
	if _, m := f.counts(); m != 1 {
		t.Fatalf("max model calls in flight = %d, want 1", m)
	}
	if m := cr.maxConcurrent(); m != 1 {
		t.Fatalf("max concurrent = %d, want 1", m)
	}
	for _, e := range allEventsFromSvc(t, svc, a) {
		if e.Type == events.AgentMessage && strings.Contains(string(e.Payload), "agent slot") {
			t.Fatalf("slot bookkeeping leaked into a message: %s", e.Payload)
		}
	}
}

func allEventsFromSvc(t *testing.T, svc *room.Service, roomID string) []events.Event {
	t.Helper()
	evs, _, err := svc.Events(context.Background(), roomID, 0, 1000)
	if err != nil {
		t.Fatalf("events: %v", err)
	}
	return evs
}
