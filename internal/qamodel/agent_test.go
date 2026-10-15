package qamodel_test

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"piperoom/internal/agent"
	"piperoom/internal/agent/fake"
	"piperoom/internal/events"
	"piperoom/internal/qamodel"
	"piperoom/internal/tools"
)

// emitted is one event the agent sent to its sink.
type emitted struct {
	typ     events.Type
	payload any
}

// collectSink records the agent's events.
type collectSink struct {
	mu  sync.Mutex
	evs []emitted
}

func (s *collectSink) Emit(_ context.Context, typ events.Type, payload any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.evs = append(s.evs, emitted{typ, payload})
	return nil
}

func (s *collectSink) snapshot() []emitted {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]emitted(nil), s.evs...)
}

// messages returns the agent.message texts so far.
func (s *collectSink) messages() []string {
	var out []string
	for _, e := range s.snapshot() {
		if p, ok := e.payload.(events.AgentMessagePayload); ok && e.typ == events.AgentMessage {
			out = append(out, p.Text)
		}
	}
	return out
}

func (s *collectSink) toolCalls() []events.AgentToolCallPayload {
	var out []events.AgentToolCallPayload
	for _, e := range s.snapshot() {
		if p, ok := e.payload.(events.AgentToolCallPayload); ok && e.typ == events.AgentToolCall {
			out = append(out, p)
		}
	}
	return out
}

func (s *collectSink) toolResults() []events.AgentToolResultPayload {
	var out []events.AgentToolResultPayload
	for _, e := range s.snapshot() {
		if p, ok := e.payload.(events.AgentToolResultPayload); ok && e.typ == events.AgentToolResult {
			out = append(out, p)
		}
	}
	return out
}

func (s *collectSink) statuses() []events.AgentStatusPayload {
	var out []events.AgentStatusPayload
	for _, e := range s.snapshot() {
		if p, ok := e.payload.(events.AgentStatusPayload); ok && e.typ == events.AgentStatus {
			out = append(out, p)
		}
	}
	return out
}

// idleInbox has no human input; Wait blocks until the run is stopped, which is
// how a parked agent waits for a person.
type idleInbox struct{}

func (idleInbox) Drain(context.Context) ([]agent.HumanInput, error) { return nil, nil }
func (idleInbox) Wait(ctx context.Context) error {
	<-ctx.Done()
	return ctx.Err()
}

// agentRun is the real agent loop (agent.Endpoint.Run, the path cmd/server
// takes) running against the puppet.
type agentRun struct {
	sink   *collectSink
	cancel context.CancelFunc
	done   chan error
}

// startAgent points the SDK at the puppet through the environment, as the
// server is pointed at it, and starts one investigation of room r_qa1.
func startAgent(t *testing.T, baseURL string) *agentRun {
	t.Helper()
	t.Setenv("ANTHROPIC_BASE_URL", baseURL)
	t.Setenv("ANTHROPIC_API_KEY", "qa-puppet")
	t.Setenv("ANTHROPIC_AUTH_TOKEN", "")

	reg := tools.NewReadOnlyRegistry()
	if err := fake.Register(reg); err != nil {
		t.Fatalf("register fake tools: %v", err)
	}
	ep, err := agent.NewEndpoint(context.Background(), agent.EndpointConfig{}, quietLogger())
	if err != nil {
		t.Fatalf("NewEndpoint: %v", err)
	}
	sink := &collectSink{}
	ctx, cancel := context.WithCancel(context.Background())
	run := &agentRun{sink: sink, cancel: cancel, done: make(chan error, 1)}
	go func() {
		run.done <- ep.Run(ctx, agent.Alert{
			RoomID:      "r_qa1",
			Title:       "orders stale",
			PipelineID:  "orders",
			PipelineMap: "postgres: public.orders -> connect: pg-orders-connector -> topic: pg.public.orders",
		}, agent.Deps{Tools: reg, Inbox: idleInbox{}, Sink: sink, Logger: quietLogger()}, agent.Options{MaxTurns: 10})
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-run.done:
		case <-time.After(10 * time.Second):
			t.Error("the agent run did not stop within 10s of its context ending")
		}
	})
	return run
}

// stop ends the run and returns what Run returned.
func (r *agentRun) stop(t *testing.T) error {
	t.Helper()
	r.cancel()
	select {
	case err := <-r.done:
		r.done <- err // for the cleanup
		return err
	case <-time.After(10 * time.Second):
		t.Fatal("the agent run did not stop within 10s of its context ending")
		return nil
	}
}

// waitFor polls cond for up to 10 seconds.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestRealAgentLoopAgainstThePuppet is the whole QA flow: the real agent loop
// asks the model what to do, a tester answers through the control API with one
// tool call, the agent runs the tool and asks again with the result in its
// history, and the tester ends the turn with text.
func TestRealAgentLoopAgainstThePuppet(t *testing.T) {
	_, srv := newPuppet(t, qamodel.Config{})
	x := tester{t, srv.URL}
	run := startAgent(t, srv.URL)

	// 1. The agent's first request: the opening message of the room.
	list := x.pending(1)
	first := list[0]
	if first.MessagesCount != 1 || !strings.Contains(first.RoomHint, "r_qa1") || !strings.Contains(first.RoomHint, "orders stale") {
		t.Fatalf("first turn = %+v, want 1 message from room r_qa1", first)
	}
	if first.Structured {
		t.Error("an investigation turn is not a structured request")
	}
	for _, want := range []string{"connect_status", "propose_fix"} {
		if !contains(first.Tools, want) {
			t.Errorf("tools %v lack %q", first.Tools, want)
		}
	}
	text := x.turnText(first.ID, "?system=0")
	// The agent adds a required "why" to every tool's schema.
	if !strings.Contains(text, "Room: r_qa1") || !strings.Contains(text, "pg-orders-connector") || !strings.Contains(text, "connect_status(connector*, why*)") {
		t.Fatalf("the rendered first turn lacks the alert or the tool list:\n%s", text)
	}

	// 2. The tester calls connect_status (a typo is refused first).
	call := map[string]any{"name": "connect_status", "input": map[string]any{"connector": "pg-orders-connector", "why": "alert names the orders connector"}}
	typo := map[string]any{"name": "connect_stauts", "input": map[string]any{}}
	if code, _ := x.reply(first.ID, map[string]any{"tool_calls": []any{typo}}); code != 400 {
		t.Fatalf("a mistyped tool name got status %d, want 400", code)
	}
	x.mustReply(first.ID, map[string]any{
		"thinking":   "The alert names the orders connector; start with its status.",
		"text":       "Checking the connector status first.",
		"tool_calls": []any{call},
	})

	// 3. The agent emitted the call and ran the real (fake-data) tool.
	waitFor(t, "the agent.tool_call event", func() bool { return len(run.sink.toolCalls()) == 1 })
	tc := run.sink.toolCalls()[0]
	if tc.Tool != "connect_status" || tc.ToolUseID != "toolu_qa_t1_0" || string(tc.Input) != `{"connector":"pg-orders-connector","why":"alert names the orders connector"}` {
		t.Errorf("tool call event = %+v", tc)
	}

	// 4. The next turn carries the tool_result (and the thinking block the
	// SDK sent back with its signature).
	second := waitNewTurn(t, x, first.ID)
	if second.MessagesCount != 3 {
		t.Errorf("second turn has %d messages, want 3 (opening, assistant, tool result)", second.MessagesCount)
	}
	if second.RoomHint != first.RoomHint {
		t.Errorf("room hint changed between turns: %q vs %q", first.RoomHint, second.RoomHint)
	}
	text = x.turnText(second.ID, "?system=0&full=1")
	for _, want := range []string{
		"[thinking]", "start with its status", "[text]\nChecking the connector status first.",
		"[tool_use toolu_qa_t1_0 connect_status]", "[tool_result toolu_qa_t1_0 ok]", "task 0 FAILED", "bytes(decimal 12,2)",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("second turn lacks %q:\n%s", want, text)
		}
	}
	if res := run.sink.toolResults(); len(res) != 1 || res[0].IsError || !strings.Contains(res[0].Summary, "task 0 FAILED") {
		t.Errorf("tool result events = %+v", res)
	}

	// 5. The tester ends the turn with text; the agent says it and parks.
	const verdict = "The connector task failed on an incompatible schema change of orders.amount."
	x.mustReply(second.ID, map[string]any{"text": verdict, "stop": "end_turn"})
	waitFor(t, "the final agent.message", func() bool { return contains(run.sink.messages(), verdict) })
	if msgs := run.sink.messages(); msgs[0] != "Checking the connector status first." {
		t.Errorf("agent messages = %q", msgs)
	}
	waitFor(t, "the agent to go idle", func() bool {
		st := run.sink.statuses()
		return len(st) > 0 && st[len(st)-1].State == "idle"
	})
	if got := x.pendingNow(); len(got) != 0 {
		t.Errorf("turns still pending after the agent parked: %+v", got)
	}
	if err := run.stop(t); err != nil {
		t.Errorf("Run returned %v after a clean stop", err)
	}

	// The log is the QA report: two replied turns, the tool call in the first.
	l := x.log()
	if len(l) != 2 || l[0].Reply == nil || l[0].Reply.Stop != qamodel.StopToolUse || l[1].Reply == nil || l[1].Reply.Text != verdict {
		b, _ := json.MarshalIndent(l, "", " ")
		t.Errorf("log = %s", b)
	}
	if l[0].Reply != nil && (len(l[0].Reply.ToolCalls) != 1 || l[0].Reply.ToolCalls[0].Name != "connect_status") {
		t.Errorf("logged tool calls = %+v", l[0].Reply.ToolCalls)
	}
}

// TestAgentParksWhenNobodyReplies is the deadline fallback seen from the agent:
// it gets plain text and goes idle, it does not report an error.
func TestAgentParksWhenNobodyReplies(t *testing.T) {
	_, srv := newPuppet(t, qamodel.Config{ReplyTimeout: 150 * time.Millisecond})
	run := startAgent(t, srv.URL)

	waitFor(t, "the fallback message", func() bool { return contains(run.sink.messages(), qamodel.FallbackText) })
	waitFor(t, "the agent to go idle", func() bool {
		st := run.sink.statuses()
		return len(st) > 0 && st[len(st)-1].State == "idle"
	})
	for _, s := range run.sink.statuses() {
		if s.State == "error" {
			t.Errorf("the agent reported an error: %+v", s)
		}
	}
	select {
	case err := <-run.done:
		t.Fatalf("the agent stopped (%v), it should wait for a human", err)
	default:
	}
}

// TestAgentSeesAnInjectedModelError shows the failure path: the tester answers
// with an HTTP error and the agent reports it.
func TestAgentSeesAnInjectedModelError(t *testing.T) {
	_, srv := newPuppet(t, qamodel.Config{})
	x := tester{t, srv.URL}
	run := startAgent(t, srv.URL)

	x.mustReply(x.pending(1)[0].ID, map[string]any{"error": map[string]any{"status": 529, "message": "overloaded, try later"}})
	select {
	case err := <-run.done:
		if err == nil || !strings.Contains(err.Error(), "model turn") {
			t.Errorf("Run returned %v, want a model turn error", err)
		}
		run.done <- err
	case <-time.After(10 * time.Second):
		t.Fatal("the agent did not stop on a model error")
	}
	var sawError bool
	for _, s := range run.sink.statuses() {
		sawError = sawError || (s.State == "error" && s.Detail == "model request failed")
	}
	if !sawError {
		t.Errorf("no agent.status error in %+v", run.sink.statuses())
	}
	if l := x.log(); len(l) != 1 {
		t.Errorf("the agent retried an injected error: %d turns", len(l))
	}
}

// waitNewTurn returns the pending turn that is not oldID.
func waitNewTurn(t *testing.T, x tester, oldID string) qamodel.PendingTurn {
	t.Helper()
	var found qamodel.PendingTurn
	waitFor(t, "the agent's next model request", func() bool {
		for _, p := range x.pendingNow() {
			if p.ID != oldID {
				found = p
				return true
			}
		}
		return false
	})
	return found
}

func contains(list []string, want string) bool {
	for _, s := range list {
		if s == want {
			return true
		}
	}
	return false
}
