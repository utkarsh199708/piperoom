package agent

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/anthropics/anthropic-sdk-go"

	"piperoom/internal/events"
	"piperoom/internal/tools"
)

// usageRecorder is a Deps.Usage that remembers every call.
type usageRecorder struct{ calls []int64 }

func (u *usageRecorder) fn(used int64) { u.calls = append(u.calls, used) }

func (u *usageRecorder) once(t *testing.T, want int64) {
	t.Helper()
	if len(u.calls) != 1 || u.calls[0] != want {
		t.Fatalf("Usage calls = %v, want exactly one call with %d", u.calls, want)
	}
}

// failingModel reports some usage on a failed call, like a broken stream.
type failingModel struct{ err error }

func (m *failingModel) Turn(context.Context, anthropic.BetaMessageNewParams) (anthropic.BetaMessage, error) {
	return anthropic.BetaMessage{Usage: anthropic.BetaUsage{InputTokens: 40, OutputTokens: 2}}, m.err
}

// panickingModel stands in for a bug inside the loop.
type panickingModel struct{}

func (panickingModel) Turn(context.Context, anthropic.BetaMessageNewParams) (anthropic.BetaMessage, error) {
	panic("boom")
}

func runEndpoint(ep *Endpoint, roomID string, deps Deps, opts Options) error {
	if deps.Tools == nil {
		deps.Tools = tools.NewRegistry()
	}
	if deps.Inbox == nil {
		deps.Inbox = &fakeInbox{waitErr: context.Canceled}
	}
	if deps.Sink == nil {
		deps.Sink = &fakeSink{}
	}
	deps.Logger = discardLogger()
	return ep.Run(context.Background(), Alert{RoomID: roomID, Title: "orders stale", PipelineID: "orders"}, deps, opts)
}

func TestEndpointRunCallsUsageOnceOnNil(t *testing.T) {
	turn := mustMessage(t, `{"stop_reason":"end_turn","content":[{"type":"text","text":"done"}],"usage":{"input_tokens":80,"output_tokens":30}}`)
	ep := testEndpoint(&scriptedModel{turns: []anthropic.BetaMessage{turn}}, 0)
	u := &usageRecorder{}
	if err := runEndpoint(ep, "r", Deps{Usage: u.fn}, Options{}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	u.once(t, 110)
}

func TestEndpointRunCallsUsageOnceOnError(t *testing.T) {
	t.Run("model error", func(t *testing.T) {
		ep := testEndpoint(&failingModel{err: errors.New("stream reset")}, 0)
		u := &usageRecorder{}
		err := runEndpoint(ep, "r", Deps{Usage: u.fn}, Options{})
		if err == nil || !strings.Contains(err.Error(), "stream reset") {
			t.Fatalf("Run = %v, want the model error", err)
		}
		u.once(t, 42) // the partial usage of the failed call is reported
	})
	t.Run("bad deps", func(t *testing.T) {
		ep := testEndpoint(&scriptedModel{}, 0)
		ep.budget("r").record(usageTokens{input: 7}, true) // e.g. an earlier hand-off
		u := &usageRecorder{}
		err := ep.Run(context.Background(), Alert{RoomID: "r"}, Deps{Usage: u.fn}, Options{})
		if err == nil || !strings.Contains(err.Error(), "Deps.Tools is nil") {
			t.Fatalf("Run = %v, want the deps error", err)
		}
		u.once(t, 7)
	})
}

func TestEndpointRunCallsUsageOnceOnPanicAndRepanics(t *testing.T) {
	ep := testEndpoint(panickingModel{}, 0)
	ep.budget("r").record(usageTokens{input: 50, output: 5}, true)
	u := &usageRecorder{}
	func() {
		defer func() {
			if p := recover(); p != "boom" {
				t.Fatalf("recovered %v, want the run's panic to propagate", p)
			}
		}()
		_ = runEndpoint(ep, "r", Deps{Usage: u.fn}, Options{})
		t.Fatal("Run returned; want a panic")
	}()
	u.once(t, 55)
}

func TestEndpointRunWithoutUsageIsFine(t *testing.T) {
	ep := testEndpoint(&scriptedModel{}, 0)
	if err := runEndpoint(ep, "r", Deps{}, Options{}); err != nil {
		t.Fatalf("Run with nil Usage: %v", err)
	}
}

func TestEndpointRunReturnsBudgetErrorAfterIdleAndMessage(t *testing.T) {
	ep := testEndpoint(&scriptedModel{}, 100)
	ep.budget("r").record(usageTokens{input: 120}, true)
	sink := &fakeSink{}
	u := &usageRecorder{}
	err := runEndpoint(ep, "r", Deps{Sink: sink, Usage: u.fn}, Options{})
	var be *BudgetError
	if !errors.As(err, &be) || be.Used != 120 || be.Limit != 100 {
		t.Fatalf("Run = %v, want an error wrapping *BudgetError 120 of 100", err)
	}
	u.once(t, 120)
	// The message and the idle status are written before Run returns, and no
	// error status: the supervisor treats the return as a stop.
	if n := len(sink.ofType(events.AgentMessage)); n != 1 {
		t.Fatalf("agent messages = %d, want the budget message", n)
	}
	statuses := sink.ofType(events.AgentStatus)
	last := statuses[len(statuses)-1]
	if !strings.Contains(string(last.payload), `"idle"`) || !strings.Contains(string(last.payload), "token budget reached (120 of 100)") {
		t.Fatalf("last status = %s, want idle with the budget text", last.payload)
	}
	for _, s := range statuses {
		if strings.Contains(string(s.payload), `"error"`) {
			t.Fatalf("budget stop wrote an error status: %s", s.payload)
		}
	}
}

func TestEndpointRunTokenBudgetOnANewEntry(t *testing.T) {
	// The endpoint has no cap; the run asks for 100. Turn 1 uses 70 and its
	// input (60) is the estimate for turn 2: 70+60 >= 100 stops the run.
	scripted := &scriptedModel{turns: []anthropic.BetaMessage{toolTurnN(t, 1), toolTurnN(t, 2)}}
	ep := testEndpoint(scripted, 0)
	u := &usageRecorder{}
	err := runEndpoint(ep, "r", Deps{Usage: u.fn}, Options{TokenBudget: 100})
	var be *BudgetError
	if !errors.As(err, &be) || be.Used != 70 || be.Limit != 100 {
		t.Fatalf("Run = %v, want *BudgetError 70 of 100", err)
	}
	if len(scripted.calls) != 1 {
		t.Fatalf("model calls = %d, want 1", len(scripted.calls))
	}
	if got := ep.budget("r").Limit(); got != 100 {
		t.Fatalf("room limit = %d, want 100", got)
	}
	u.once(t, 70)

	// Another room on the same endpoint keeps the endpoint default.
	if got := ep.budget("other").Limit(); got != 0 {
		t.Fatalf("other room limit = %d, want the endpoint default 0", got)
	}
}

func TestEndpointRunTokenBudgetOnAnExistingEntryKeepsUsed(t *testing.T) {
	scripted := &scriptedModel{}
	ep := testEndpoint(scripted, 1000)
	// A hand-off written while the run was queued created the entry with the
	// endpoint default.
	b := ep.budget("r")
	b.record(usageTokens{input: 90}, true)
	if got := b.Limit(); got != 1000 {
		t.Fatalf("limit before the run = %d, want 1000", got)
	}

	err := runEndpoint(ep, "r", Deps{}, Options{TokenBudget: 100})
	var be *BudgetError
	if !errors.As(err, &be) || be.Used != 90 || be.Limit != 100 {
		t.Fatalf("Run = %v, want *BudgetError 90 of 100 (count kept, limit replaced)", err)
	}
	if len(scripted.calls) != 0 {
		t.Fatalf("model calls = %d, want 0", len(scripted.calls))
	}
	if ep.budget("r") != b || b.Limit() != 100 || b.Used() != 90 {
		t.Fatalf("entry = limit %d used %d (same=%v), want the same entry at 100 with 90 used", b.Limit(), b.Used(), ep.budget("r") == b)
	}
}

func TestEndpointRunZeroTokenBudgetUsesTheEndpointDefault(t *testing.T) {
	ep := testEndpoint(&scriptedModel{}, 500)
	if err := runEndpoint(ep, "r", Deps{}, Options{}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := ep.budget("r").Limit(); got != 500 {
		t.Fatalf("limit = %d, want the endpoint default 500", got)
	}
}

func TestPlainRunNeverReportsUsage(t *testing.T) {
	// The runner itself never touches Deps.Usage; only Endpoint.Run does.
	u := &usageRecorder{}
	r := newTestRunner(t, &scriptedModel{}, &fakeInbox{waitErr: context.Canceled}, &fakeSink{}, &recordingTool{name: "connect_status", schema: testSchema("connector")}, Options{})
	r.deps.Usage = u.fn
	if err := r.run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	if len(u.calls) != 0 {
		t.Fatalf("Usage calls = %v, want none outside Endpoint.Run", u.calls)
	}
}
