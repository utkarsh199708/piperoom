package agent

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"piperoom/internal/actions"
	"piperoom/internal/clients"
	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
)

// ---- X6c throughput verification ----

// fakePerfAction is a performance action whose Verify answers from a script
// (the last answer repeats), recording the clock time of every call.
type fakePerfAction struct {
	*fakeRecoveryAction
	clk      *tickClock
	mu       sync.Mutex
	calledAt []time.Time
}

func (a *fakePerfAction) Class() string { return actions.ClassPerformance }

func (a *fakePerfAction) Preview(context.Context, json.RawMessage) (actions.PreviewResult, error) {
	return actions.PreviewResult{}, nil
}

func (a *fakePerfAction) Verify(ctx context.Context, raw json.RawMessage, executedAt time.Time) (actions.VerifyResult, error) {
	a.mu.Lock()
	a.calledAt = append(a.calledAt, a.clk.Now())
	a.mu.Unlock()
	return a.fakeRecoveryAction.Verify(ctx, raw, executedAt)
}

func throughput(before, after int64) verifyAnswer {
	return verifyAnswer{res: actions.VerifyResult{Before: before, After: after, Unit: events.PerformanceUnitRecordsPerMinute, Detail: "sink orders-sink (pg.public.orders)"}}
}

func tuneStep() events.ProposalStep {
	return events.ProposalStep{Action: "connect_tune_throughput", Layer: "kafka-connect",
		Params: json.RawMessage(`{"connector":"orders-sink","changes":{"tasks.max":"3"}}`), Description: "add tasks (s_01JAXXXXXXXXXXXXXXXXXXXXXX)"}
}

// newPerfRunner registers a performance action answering answers and a
// connect_status that passes, and wires the action's clock to the runner's.
func newPerfRunner(t *testing.T, m model, inbox Inbox, sink *fakeSink, answers ...verifyAnswer) (*runner, *tickClock, *fakePerfAction) {
	t.Helper()
	act := &fakePerfAction{fakeRecoveryAction: &fakeRecoveryAction{fakeAction: fakeAction{name: "connect_tune_throughput", layer: "kafka-connect"}, answers: answers}}
	connect := &fakeVerifyTool{name: "connect_status", schema: connectSchema(), result: connectRecoveredResult()}
	r, clk := newRecoveryRunner(t, m, inbox, sink, []tools.Tool{connect}, []events.ProposalStep{tuneStep()}, act)
	act.clk = clk
	// Direct verifyActionResult calls need the read tools run() would list.
	r.toolList = buildToolList(r.deps.Tools, nil)
	return r, clk, act
}

func perfMessages(s *fakeSink) []string {
	var out []string
	for _, e := range s.ofType(events.AgentMessage) {
		var p events.AgentMessagePayload
		if json.Unmarshal(e.payload, &p) == nil && strings.HasPrefix(p.Text, "Throughput check") {
			out = append(out, p.Text)
		}
	}
	return out
}

// TC3 (agent side): the verify table through the real loop. Verify is called
// once, when the after window has closed (17 min after the step), and the
// verdict follows events.PerformanceOutcomeFor; worse names the rollback.
func TestPerformanceVerifyTable(t *testing.T) {
	cases := []struct {
		name          string
		before, after int64
		outcome       string
		done          bool
		msg           string
	}{
		{"+15 % improved", 1000, 1150, events.PerformanceImproved, true, "throughput 1000 → 1150 records/min (+15 %)"},
		{"+2 % no change", 1000, 1020, events.PerformanceNoChange, false, "no change"},
		{"-20 % worse", 1000, 800, events.PerformanceWorse, false, "offers the values from before the step as a rollback"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sink := &fakeSink{}
			inbox := &fakeInbox{drains: [][]HumanInput{nil, lastActionResult("proposal p_1 step 0: orders-sink tasks.max: 1 → 3")}, waitErr: context.Canceled}
			r, clk, act := newPerfRunner(t, &scriptedModel{}, inbox, sink, throughput(tc.before, tc.after))
			start := clk.Now()
			if err := r.run(tools.WithPipeline(context.Background(), owners.Pipeline{ID: "orders"})); err != nil {
				t.Fatalf("run: %v", err)
			}
			if len(act.calledAt) != 1 || !act.calledAt[0].Equal(start.Add(17*time.Minute)) || !act.execAt[0].Equal(start) || act.pipelines[0] != "orders" {
				t.Fatalf("Verify calls at %v (executedAt %v, pipeline %v), want one at +17m", act.calledAt, act.execAt, act.pipelines)
			}
			v := onlyVerification(t, sink)
			if v.Outcome != tc.outcome || v.Before == nil || *v.Before != tc.before || *v.After != tc.after || v.Unit != events.PerformanceUnitRecordsPerMinute {
				t.Fatalf("verification = %+v", v)
			}
			if !v.Recovered {
				t.Fatal("every layer check passed: recovered keeps its F4 meaning")
			}
			if len(recoveryChecks(sink)) != 0 {
				t.Fatal("a tuning writes no agent.recovery_check")
			}
			if hasStatus(sink, "done") != tc.done {
				t.Fatalf("done = %v, want %v", hasStatus(sink, "done"), tc.done)
			}
			msgs := perfMessages(sink)
			if len(msgs) != 1 || !strings.Contains(msgs[0], tc.msg) {
				t.Fatalf("messages = %q, want one containing %q", msgs, tc.msg)
			}
			if tc.outcome == events.PerformanceWorse && !strings.HasPrefix(v.Verdict, "worse:") {
				t.Fatalf("verdict %q", v.Verdict)
			}
		})
	}
}

// A failed call is retried every minute; lag slopes reach the event.
func TestPerformanceVerifyRetriesThenMeasures(t *testing.T) {
	lb, la := int64(600), int64(-120)
	ok := verifyAnswer{res: actions.VerifyResult{Before: 900, After: 950, Unit: events.PerformanceUnitRecordsPerMinute, LagSlopeBefore: &lb, LagSlopeAfter: &la}}
	sink := &fakeSink{}
	r, clk, act := newPerfRunner(t, &scriptedModel{}, &fakeInbox{}, sink, verifyAnswer{err: errors.New("waiting for the watcher's next group_lag sample")}, ok)
	start := clk.Now()
	out := r.verifyActionResult(context.Background(), actionInput("proposal p_1 step 0: tuned", okPtr(true)))
	if len(act.calledAt) != 2 || act.calledAt[1].Sub(act.calledAt[0]) != time.Minute || !act.calledAt[0].Equal(start.Add(17*time.Minute)) {
		t.Fatalf("calls at %v", act.calledAt)
	}
	v := onlyVerification(t, sink)
	// +5 % throughput, but the growing lag now shrinks: improved.
	if v.Outcome != events.PerformanceImproved || v.LagSlopeBefore == nil || *v.LagSlopeBefore != 600 || *v.LagSlopeAfter != -120 || !out.recovered {
		t.Fatalf("verification %+v, outcome %+v", v, out)
	}
	if !strings.Contains(out.notes[0], "lag slope +600 → -120 per min") {
		t.Fatalf("note %q", out.notes[0])
	}
}

// Unverifiable: a reader that cannot measure ends at once; errors until the
// timeout end at 30 min. Neither carries numbers or claims an improvement.
func TestPerformanceVerifyUnverifiable(t *testing.T) {
	t.Run("not supported", func(t *testing.T) {
		sink := &fakeSink{}
		r, _, act := newPerfRunner(t, &scriptedModel{}, &fakeInbox{}, sink, verifyAnswer{err: clients.ErrNotSupported})
		out := r.verifyActionResult(context.Background(), actionInput("proposal p_1 step 0: tuned", okPtr(true)))
		v := onlyVerification(t, sink)
		if act.callCount() != 1 || v.Outcome != events.PerformanceUnverifiable || v.Before != nil || v.After != nil || out.recovered || !out.broken {
			t.Fatalf("calls %d verification %+v outcome %+v", act.callCount(), v, out)
		}
		if hasStatus(sink, "done") || !strings.HasPrefix(v.Verdict, "NOT verified (unverifiable)") {
			t.Fatalf("verdict %q", v.Verdict)
		}
	})
	t.Run("timeout", func(t *testing.T) {
		sink := &fakeSink{}
		r, clk, act := newPerfRunner(t, &scriptedModel{}, &fakeInbox{}, sink, verifyAnswer{err: errors.New("broker down")})
		start := clk.Now()
		r.verifyActionResult(context.Background(), actionInput("proposal p_1 step 0: tuned", okPtr(true)))
		// 17 min, then every minute up to the 30 min timeout.
		if n := act.callCount(); n != 14 {
			t.Fatalf("calls = %d, want 14", n)
		}
		if last := act.calledAt[len(act.calledAt)-1]; last.Sub(start) > actions.PerformanceVerifyTimeout {
			t.Fatalf("called %v after the step, past the timeout", last.Sub(start))
		}
		v := onlyVerification(t, sink)
		if v.Outcome != events.PerformanceUnverifiable || !strings.Contains(v.Checks[len(v.Checks)-1].Detail, "broker down") {
			t.Fatalf("verification %+v", v)
		}
	})
}

// A layer check that fails after the restart: not recovered, not done, even
// with throughput up.
func TestPerformanceVerifyLayerCheckFails(t *testing.T) {
	sink := &fakeSink{}
	act := &fakePerfAction{fakeRecoveryAction: &fakeRecoveryAction{fakeAction: fakeAction{name: "connect_tune_throughput", layer: "kafka-connect"}, answers: []verifyAnswer{throughput(1000, 1500)}}}
	connect := &fakeVerifyTool{name: "connect_status", schema: connectSchema(), result: connectBrokenResult()}
	r, clk := newRecoveryRunner(t, &scriptedModel{}, &fakeInbox{}, sink, []tools.Tool{connect}, []events.ProposalStep{tuneStep()}, act)
	act.clk = clk
	r.toolList = buildToolList(r.deps.Tools, nil)
	out := r.verifyActionResult(context.Background(), actionInput("proposal p_1 step 0: tuned", okPtr(true)))
	v := onlyVerification(t, sink)
	if v.Recovered || out.recovered || v.Outcome != events.PerformanceImproved || !strings.HasPrefix(v.Verdict, "NOT healthy") || hasStatus(sink, "done") {
		t.Fatalf("verification %+v outcome %+v", v, out)
	}
}
