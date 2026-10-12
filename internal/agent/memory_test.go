package agent

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/anthropics/anthropic-sdk-go"

	"piperoom/internal/tools"
)

// fakeClock records every requested sleep duration and returns at once, so
// backoff tests are deterministic and instant instead of waiting on a real
// timer.
type fakeClock struct {
	mu    sync.Mutex
	slept []time.Duration
}

func (c *fakeClock) Sleep(_ context.Context, d time.Duration) error {
	c.mu.Lock()
	c.slept = append(c.slept, d)
	c.mu.Unlock()
	return nil
}

func (c *fakeClock) durations() []time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]time.Duration(nil), c.slept...)
}

// ---- server-side compaction (beta compact-2026-01-12) ----

// TestRunCompactionBlockCarriesToNextRequest verifies that when the model
// returns a compaction content block alongside its normal output, Run sends
// that block back untouched in the next request's history — the same
// append-only treatment as every other content block. Compaction blocks are
// never special-cased or dropped.
func TestRunCompactionBlockCarriesToNextRequest(t *testing.T) {
	rec := &recordingTool{name: "connect_status", schema: testSchema("connector"), result: tools.Result{Summary: "ok"}}
	model := &scriptedModel{turns: []anthropic.BetaMessage{
		mustMessage(t, `{"stop_reason":"tool_use","content":[
			{"type":"compaction","content":"summary of older turns","encrypted_content":"opaque-blob","signature":"sig"},
			{"type":"tool_use","id":"tu1","name":"connect_status","input":{"why":"check connect","connector":"pg-orders-connector"}}
		]}`),
	}}
	sink := &fakeSink{}
	inbox := &fakeInbox{waitErr: context.Canceled}
	r := newTestRunner(t, model, inbox, sink, rec, Options{MaxTurns: 3})

	if err := r.run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	if len(model.calls) < 2 {
		t.Fatalf("expected at least 2 model calls, got %d", len(model.calls))
	}
	// The second call's history is the first call's history plus the
	// assistant turn: resp.ToParam() must carry the compaction block along
	// with the tool_use block, byte for byte, into that next request.
	js := messagesJSON(model.calls[1])
	if !strings.Contains(js, `"type":"compaction"`) {
		t.Fatalf("compaction block missing from next request: %s", js)
	}
	if !strings.Contains(js, "summary of older turns") || !strings.Contains(js, "opaque-blob") {
		t.Fatalf("compaction block content/metadata missing from next request: %s", js)
	}
}

// ---- Inbox.Drain backoff ----

// TestWaitForInputBacksOffOnDrainError covers the case the A4 review flagged:
// Inbox.Drain fails repeatedly while Wait returns at once with nothing to
// drain. Without a backoff that combination spins the loop at full speed.
// waitForInput must back off between failed drains (250ms, doubling, capped
// at 5s) and recover once Drain succeeds again.
func TestWaitForInputBacksOffOnDrainError(t *testing.T) {
	inbox := &fakeInbox{
		drainErr:      errors.New("inbox store unavailable"),
		drainErrTimes: 3,
		drains:        [][]HumanInput{{{Kind: "steer", Text: "keep going"}}},
		waitImmediate: true,
	}
	rec := &recordingTool{name: "connect_status", schema: testSchema("connector"), result: tools.Result{Summary: "ok"}}
	r := newTestRunner(t, &scriptedModel{}, inbox, &fakeSink{}, rec, Options{MaxTurns: 3})
	fc := &fakeClock{}
	r.clk = fc

	inputs, err := r.waitForInput(context.Background())
	if err != nil {
		t.Fatalf("waitForInput: %v", err)
	}
	if len(inputs) != 1 || inputs[0].Text != "keep going" {
		t.Fatalf("expected the staged input once Drain recovers, got %v", inputs)
	}

	slept := fc.durations()
	if len(slept) != 3 {
		t.Fatalf("expected one backoff sleep per failed drain (3), got %d: %v", len(slept), slept)
	}
	want := []time.Duration{defaultDrainBackoffMin, defaultDrainBackoffMin * 2, defaultDrainBackoffMin * 4}
	for i, d := range want {
		if slept[i] != d {
			t.Fatalf("sleep %d = %v, want %v (all: %v)", i, slept[i], d, slept)
		}
	}
}

// TestWaitForInputBackoffCapsAtMax checks the backoff never exceeds
// defaultDrainBackoffMax even after many consecutive drain failures.
func TestWaitForInputBackoffCapsAtMax(t *testing.T) {
	inbox := &fakeInbox{
		drainErr:      errors.New("inbox store unavailable"),
		drainErrTimes: 8,
		waitImmediate: true,
		waitErr:       nil,
	}
	// After the scripted failures, waitErr still nil means Wait keeps
	// returning immediately forever; give waitForInput an input to end on.
	inbox.drains = [][]HumanInput{{{Kind: "steer", Text: "done"}}}

	rec := &recordingTool{name: "connect_status", schema: testSchema("connector"), result: tools.Result{Summary: "ok"}}
	r := newTestRunner(t, &scriptedModel{}, inbox, &fakeSink{}, rec, Options{MaxTurns: 3})
	fc := &fakeClock{}
	r.clk = fc

	if _, err := r.waitForInput(context.Background()); err != nil {
		t.Fatalf("waitForInput: %v", err)
	}
	slept := fc.durations()
	if len(slept) != 8 {
		t.Fatalf("expected 8 backoff sleeps, got %d: %v", len(slept), slept)
	}
	for _, d := range slept {
		if d > defaultDrainBackoffMax {
			t.Fatalf("backoff exceeded the max: %v > %v (all: %v)", d, defaultDrainBackoffMax, slept)
		}
	}
	if slept[len(slept)-1] != defaultDrainBackoffMax {
		t.Fatalf("backoff should have reached the max by the last failure, got %v", slept)
	}
}
