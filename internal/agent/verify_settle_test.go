package agent

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"piperoom/internal/events"
	"piperoom/internal/tools"
)

// ---- helpers ----

// connectStatus is a connect_status result: the connector state and one entry
// per task state.
func connectStatus(state string, taskStates ...string) tools.Result {
	tasks := make([]map[string]any, len(taskStates))
	for i, s := range taskStates {
		tasks[i] = map[string]any{"id": i, "state": s}
	}
	return tools.Result{
		Summary: "pg-orders-connector (source): " + state,
		Data:    map[string]any{"name": "pg-orders-connector", "state": state, "tasks": tasks},
	}
}

// connectSeq answers connect_status from a script, one entry per read: a
// tools.Result or an error. Past the end it repeats the last entry.
func connectSeq(script ...any) *fakeVerifyTool {
	var i int
	return &fakeVerifyTool{name: "connect_status", schema: connectSchema(), runFn: func() (tools.Result, error) {
		e := script[min(i, len(script)-1)]
		i++
		if err, ok := e.(error); ok {
			return tools.Result{}, err
		}
		return e.(tools.Result), nil
	}}
}

// settleRunner is a verify runner with a connect_status tool, a one-sink
// pipeline and a warehouse that keeps receiving rows, on an instant clock.
func settleRunner(t *testing.T, connect *fakeVerifyTool, sink Sink, inbox Inbox) (*runner, *fakeClock) {
	t.Helper()
	r := newVerifyRunner(t, &scriptedModel{}, inbox, sink, []tools.Tool{connect, pipelineMapTool("analytics.orders"), freshWarehouseTool()}, recoveredSteps())
	clk := &fakeClock{}
	r.clk = clk
	r.toolList = buildToolList(r.deps.Tools, nil)
	return r, clk
}

func connectOnlyChecks() []verifyCheck {
	return verifyChecks(recoveredSteps()) // the warehouse step has no layer check
}

func totalSlept(c *fakeClock) time.Duration {
	var d time.Duration
	for _, s := range c.durations() {
		d += s
	}
	return d
}

func toolCallsOf(s *fakeSink, tool string) int {
	var n int
	for _, e := range s.ofType(events.AgentToolCall) {
		if strings.Contains(string(e.payload), `"tool":"`+tool+`"`) {
			n++
		}
	}
	return n
}

// cancelClock cancels the run's context on its first sleep, as a room closing
// in the middle of a settle wait would.
type cancelClock struct{ cancel func() }

func (c cancelClock) Sleep(ctx context.Context, _ time.Duration) error {
	c.cancel()
	return ctx.Err()
}

// deadlineClock ends every sleep with a deadline error while the run's own
// context stays alive: the wall-clock backstop of the settle wait.
type deadlineClock struct{}

func (deadlineClock) Sleep(context.Context, time.Duration) error { return context.DeadlineExceeded }

// refuseTypeSink refuses one event type, like a room service built before the
// type existed.
type refuseTypeSink struct {
	*fakeSink
	refuse events.Type
}

func (s refuseTypeSink) Emit(ctx context.Context, typ events.Type, payload any) error {
	if typ == s.refuse {
		return errors.New("room: agent sink refused event type")
	}
	return s.fakeSink.Emit(ctx, typ, payload)
}

func lastStep() []HumanInput {
	return lastActionResult("proposal p_1 step 1: backfilled analytics.orders")
}

// ---- settleLayers ----

func TestSettleDefaultWindowIsNinetySeconds(t *testing.T) {
	tm := defaultTiming()
	if got := tm.verifySettleDeadline(); got != 90*time.Second {
		t.Fatalf("settle window = %s, want 1m30s (18 polls, 5s apart)", got)
	}
}

func TestSettleLayersTable(t *testing.T) {
	boom := errors.New("connect: 409 conflict, rebalance in progress")
	window := defaultTiming().verifySettleDeadline()
	cases := []struct {
		name       string
		script     []any
		wantOK     bool
		wantTimed  bool
		wantReads  int
		wantRounds int
		wantWaited time.Duration
		wantNote   string // "" = the evidence has no note line (a one-read verdict)
	}{
		{
			name:   "passes on the first read: as before, two reads, no note",
			script: []any{connectStatus("RUNNING", "RUNNING")},
			wantOK: true, wantReads: 2, wantRounds: 1, wantWaited: 5 * time.Second,
		},
		{
			name: "restarting, then no task yet, then up: the QA race",
			script: []any{
				connectStatus("RUNNING", "UNASSIGNED"),
				connectStatus("RUNNING"),
				connectStatus("RUNNING", "RUNNING"),
			},
			wantOK: true, wantReads: 4, wantRounds: 3, wantWaited: 15 * time.Second,
			wantNote: "layer checks settled on read 3, 15s after the first",
		},
		{
			name: "connector RESTARTING counts as not settled",
			script: []any{
				connectStatus("RESTARTING", "UNASSIGNED"),
				connectStatus("RUNNING", "RESTARTING"),
				connectStatus("RUNNING", "RUNNING"),
			},
			wantOK: true, wantReads: 4, wantRounds: 3, wantWaited: 15 * time.Second,
			wantNote: "settled on read 3",
		},
		{
			name: "up, then lost again during the settle read, then up for good",
			script: []any{
				connectStatus("RUNNING", "RUNNING"),    // round 1 first read
				connectStatus("RUNNING", "UNASSIGNED"), // round 1 settle read: the restart began
				connectStatus("RUNNING", "RUNNING"),    // round 2
				connectStatus("RUNNING", "RUNNING"),    // round 2 settle read
			},
			wantOK: true, wantReads: 4, wantRounds: 2, wantWaited: 15 * time.Second,
			wantNote: "settled on read 2",
		},
		{
			name: "FAILED left over from before the fix, then up (fewer than the confirming reads)",
			script: []any{
				connectStatus("RUNNING", "FAILED"),
				connectStatus("RUNNING", "FAILED"),
				connectStatus("RUNNING", "RUNNING"),
			},
			wantOK: true, wantReads: 4, wantRounds: 3, wantWaited: 15 * time.Second,
			wantNote: "settled on read 3",
		},
		{
			name:      "FAILED on every read: final once confirmed, without waiting out the window",
			script:    []any{connectStatus("RUNNING", "FAILED")},
			wantReads: connectFailConfirm, wantRounds: connectFailConfirm, wantWaited: time.Duration(connectFailConfirm-1) * 5 * time.Second,
			wantNote: "layer checks failed on 3 reads in a row",
		},
		{
			name: "a FAILED task next to a restarting one is still settling",
			script: []any{
				connectStatus("RUNNING", "FAILED", "UNASSIGNED"),
				connectStatus("RUNNING", "FAILED", "UNASSIGNED"),
				connectStatus("RUNNING", "FAILED", "UNASSIGNED"),
				connectStatus("RUNNING", "RUNNING", "RUNNING"),
			},
			wantOK: true, wantReads: 5, wantRounds: 4, wantWaited: 20 * time.Second,
			wantNote: "settled on read 4",
		},
		{
			name:      "restarting forever: one verdict at the end of the window",
			script:    []any{connectStatus("RUNNING", "UNASSIGNED")},
			wantTimed: true, wantReads: verifySettleRounds + 1, wantRounds: verifySettleRounds + 1, wantWaited: window,
			wantNote: "layer checks did not settle within 1m30s (19 reads)",
		},
		{
			name: "REST errors while the cluster rebalances are not failures",
			script: []any{
				boom,
				boom,
				connectStatus("RUNNING", "RUNNING"),
			},
			wantOK: true, wantReads: 4, wantRounds: 3, wantWaited: 15 * time.Second,
			wantNote: "settled on read 3",
		},
		{
			name:      "errors forever time out, not fail",
			script:    []any{boom},
			wantTimed: true, wantReads: verifySettleRounds + 1, wantRounds: verifySettleRounds + 1, wantWaited: window,
			wantNote: "did not settle within 1m30s",
		},
		{
			name:      "a paused connector is a failure, not a transition",
			script:    []any{connectStatus("PAUSED", "RUNNING")},
			wantReads: connectFailConfirm, wantRounds: connectFailConfirm, wantWaited: time.Duration(connectFailConfirm-1) * 5 * time.Second,
			wantNote: "failed on 3 reads in a row",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			connect := connectSeq(c.script...)
			sink := &fakeSink{}
			r, clk := settleRunner(t, connect, sink, &fakeInbox{})

			got := r.settleLayers(context.Background(), connectOnlyChecks())

			if got.aborted {
				t.Fatal("aborted without a cancelled context")
			}
			if got.ok != c.wantOK || got.timedOut != c.wantTimed {
				t.Fatalf("ok=%v timedOut=%v, want ok=%v timedOut=%v (lines %q)", got.ok, got.timedOut, c.wantOK, c.wantTimed, got.lines)
			}
			if connect.calls != c.wantReads || got.rounds != c.wantRounds {
				t.Fatalf("reads=%d rounds=%d, want reads=%d rounds=%d", connect.calls, got.rounds, c.wantReads, c.wantRounds)
			}
			if got.waited != c.wantWaited {
				t.Fatalf("waited %s, want %s", got.waited, c.wantWaited)
			}
			// Every pause goes through the injected clock: nothing really sleeps,
			// and the clock saw the time the outcome reports.
			if totalSlept(clk) != got.waited {
				t.Fatalf("clock slept %s, outcome says %s", totalSlept(clk), got.waited)
			}
			if len(got.checks) != 1 || got.checks[0].Layer != "kafka-connect" || got.checks[0].Name != "connect_status" || got.checks[0].OK != c.wantOK {
				t.Fatalf("checks = %+v", got.checks)
			}
			joined := strings.Join(got.lines, "\n")
			if c.wantNote == "" {
				// The line names the room steps of its reads, then the evidence.
				if len(got.lines) != 1 || !strings.HasPrefix(got.lines[0], "connect_status (step s_") {
					t.Fatalf("a one-read verdict keeps the evidence it had before polling, got %q", got.lines)
				}
			} else if !strings.Contains(joined, c.wantNote) {
				t.Fatalf("evidence lacks %q:\n%s", c.wantNote, joined)
			}
		})
	}
}

// Only the first round writes its reads to the room: a layer that takes a
// minute to restart does not fill the log with identical steps.
func TestSettleLayersWritesOnlyTheFirstRoundToTheRoom(t *testing.T) {
	connect := connectSeq(
		connectStatus("RUNNING", "UNASSIGNED"),
		connectStatus("RUNNING", "UNASSIGNED"),
		connectStatus("RUNNING", "UNASSIGNED"),
		connectStatus("RUNNING", "RUNNING"),
	)
	sink := &fakeSink{}
	r, _ := settleRunner(t, connect, sink, &fakeInbox{})
	got := r.settleLayers(context.Background(), connectOnlyChecks())
	if !got.ok || connect.calls != 5 {
		t.Fatalf("ok=%v reads=%d, want a pass after 5 reads", got.ok, connect.calls)
	}
	if n := toolCallsOf(sink, "connect_status"); n != 1 {
		t.Fatalf("room tool calls = %d, want only the first read", n)
	}
	var waiting int
	for _, e := range sink.ofType(events.AgentStatus) {
		if strings.Contains(string(e.payload), "waiting for the layers to settle") {
			waiting++
		}
	}
	if waiting != 1 {
		t.Fatalf("waiting statuses = %d, want 1: the room is told once that the agent is waiting", waiting)
	}
}

func TestSettleLayersWithoutLayerChecks(t *testing.T) {
	r, clk := settleRunner(t, connectSeq(connectStatus("RUNNING", "RUNNING")), &fakeSink{}, &fakeInbox{})
	got := r.settleLayers(context.Background(), nil)
	if !got.ok || len(got.lines) != 0 || len(clk.durations()) != 0 {
		t.Fatalf("a proposal with no layer check passes at once, got %+v", got)
	}
}

// A check that cannot run is final at once: waiting does not install a tool.
func TestSettleLayersUnrunnableCheckFailsAtOnce(t *testing.T) {
	sink := &fakeSink{}
	other := &fakeVerifyTool{name: "pipeline_map", schema: connectSchema()}
	r := newVerifyRunner(t, &scriptedModel{}, &fakeInbox{}, sink, []tools.Tool{other}, recoveredSteps())
	clk := &fakeClock{}
	r.clk = clk
	r.toolList = buildToolList(r.deps.Tools, nil)

	got := r.settleLayers(context.Background(), connectOnlyChecks())
	if got.ok || got.timedOut || got.rounds != 1 || len(clk.durations()) != 0 {
		t.Fatalf("want one round and no wait, got %+v (slept %v)", got, clk.durations())
	}
	if !strings.Contains(strings.Join(got.lines, "\n"), "check cannot run") {
		t.Fatalf("evidence must say the check cannot run: %q", got.lines)
	}
}

func TestSettleLayersContextEndsMidWait(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	connect := connectSeq(connectStatus("RUNNING", "UNASSIGNED"))
	r, _ := settleRunner(t, connect, &fakeSink{}, &fakeInbox{})
	r.clk = cancelClock{cancel: cancel}

	got := r.settleLayers(ctx, connectOnlyChecks())
	if !got.aborted || got.ok {
		t.Fatalf("a cancelled run has no verdict, got %+v", got)
	}
}

// The wall-clock backstop ends the wait without a verdict of failure either:
// the window ran out.
func TestSettleLayersBackstopIsATimeoutNotAnAbort(t *testing.T) {
	connect := connectSeq(connectStatus("RUNNING", "UNASSIGNED"))
	r, _ := settleRunner(t, connect, &fakeSink{}, &fakeInbox{})
	r.clk = deadlineClock{}

	got := r.settleLayers(context.Background(), connectOnlyChecks())
	if got.aborted || got.ok || !got.timedOut {
		t.Fatalf("want a timeout, got %+v", got)
	}
}

func TestConnectStateTable(t *testing.T) {
	cases := []struct {
		name string
		res  tools.Result
		want checkState
	}{
		{"all running", connectStatus("RUNNING", "RUNNING", "RUNNING"), checkPass},
		{"connector unassigned", connectStatus("UNASSIGNED"), checkPending},
		{"connector restarting", connectStatus("RESTARTING", "RUNNING"), checkPending},
		{"task unassigned", connectStatus("RUNNING", "RUNNING", "UNASSIGNED"), checkPending},
		{"task restarting", connectStatus("RUNNING", "RESTARTING"), checkPending},
		{"running, no task yet", connectStatus("RUNNING"), checkPending},
		{"task failed", connectStatus("RUNNING", "RUNNING", "FAILED"), checkFail},
		{"connector failed", connectStatus("FAILED"), checkFail},
		{"connector failed with no tasks", connectStatus("FAILED"), checkFail},
		{"task paused", connectStatus("RUNNING", "PAUSED"), checkFail},
		{"task stopped", connectStatus("RUNNING", "STOPPED"), checkFail},
		{"a state nobody knows", connectStatus("RUNNING", "WEDGED"), checkFail},
		{"restarting wins over failed", connectStatus("RUNNING", "FAILED", "RESTARTING"), checkPending},
		{"undecodable payload", tools.Result{Summary: "pg-orders-connector: RUNNING"}, checkFail},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := connectState(c.res); got != c.want {
				t.Fatalf("connectState = %v, want %v", got, c.want)
			}
		})
	}
}

func TestPgSlotsStateTable(t *testing.T) {
	cases := []struct {
		name string
		res  tools.Result
		want checkState
	}{
		{"active slot", tools.Result{Data: []map[string]any{{"name": "s", "active": true}}}, checkPass},
		{"slot not active yet", tools.Result{Data: []map[string]any{{"name": "s", "active": false}}}, checkPending},
		{"no slot yet", tools.Result{Data: []map[string]any{}}, checkPending},
		{"unreadable", tools.Result{Summary: "slot x: active=true"}, checkFail},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := pgSlotsState(c.res); got != c.want {
				t.Fatalf("pgSlotsState = %v, want %v", got, c.want)
			}
		})
	}
}

// ---- through the loop: one verdict ----

func runSettleLoop(t *testing.T, connect *fakeVerifyTool, sink Sink, inbox Inbox) (*runner, *fakeClock) {
	t.Helper()
	r, clk := settleRunner(t, connect, sink, inbox)
	if err := r.run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	return r, clk
}

// QA G6: right after the change the connector is still restarting. The verdict
// waits and says "recovered" once, not "NOT recovered" first.
func TestSettleLoopFailsThenPassesGivesOneRecoveredVerdict(t *testing.T) {
	connect := connectSeq(
		connectStatus("RUNNING", "UNASSIGNED"),
		connectStatus("RUNNING"),
		connectStatus("RUNNING", "RUNNING"),
	)
	sink := &fakeSink{}
	inbox := &fakeInbox{drains: [][]HumanInput{nil, lastStep()}, waitErr: context.Canceled}
	_, _ = runSettleLoop(t, connect, sink, inbox)

	v := verifications(t, sink)
	if len(v) != 1 {
		t.Fatalf("verdicts = %d, want exactly one: %+v", len(v), v)
	}
	if !v[0].Recovered || v[0].Repeat || v[0].ProposalID != "p_1" || !strings.HasPrefix(v[0].Verdict, "recovered:") {
		t.Fatalf("verdict = %+v, want one recovered verdict for p_1", v[0])
	}
	if len(v[0].Checks) != 1 || !v[0].Checks[0].OK || v[0].Checks[0].Layer != "kafka-connect" {
		t.Fatalf("checks = %+v", v[0].Checks)
	}
	if v[0].DataFlow == nil || !v[0].DataFlow.Checked || !v[0].DataFlow.Fresh {
		t.Fatalf("data flow = %+v, want checked and fresh", v[0].DataFlow)
	}
	for _, m := range messageTexts(sink) {
		if strings.Contains(m, "NOT recovered") {
			t.Fatalf("no NOT recovered may precede the recovery: %q", messageTexts(sink))
		}
	}
	if !hasStatus(sink, "done") {
		t.Fatal("the recovery must park the episode as done")
	}
	// The verdict is the only announcement: no verdict-looking agent.message.
	for _, e := range sink.ofType(events.AgentMessage) {
		if strings.Contains(string(e.payload), "recovered") {
			t.Fatalf("the verdict must not also be a message: %s", e.payload)
		}
	}
}

func TestSettleLoopNeverSettlesGivesOneNotRecoveredVerdict(t *testing.T) {
	connect := connectSeq(connectStatus("RUNNING", "UNASSIGNED"))
	wh := freshWarehouseTool()
	sink := &fakeSink{}
	inbox := &fakeInbox{drains: [][]HumanInput{nil, lastStep()}, waitErr: context.Canceled}
	r := newVerifyRunner(t, &scriptedModel{}, inbox, sink, []tools.Tool{connect, pipelineMapTool("analytics.orders"), wh}, recoveredSteps())
	clk := &fakeClock{}
	r.clk = clk
	if err := r.run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}

	v := verifications(t, sink)
	if len(v) != 1 || v[0].Recovered || v[0].Repeat {
		t.Fatalf("want exactly one NOT recovered verdict, got %+v", v)
	}
	if !strings.HasPrefix(v[0].Verdict, "NOT recovered:") || !strings.Contains(v[0].Verdict, "1m30s") {
		t.Fatalf("verdict = %q, want NOT recovered after the 1m30s window", v[0].Verdict)
	}
	if v[0].DataFlow == nil || v[0].DataFlow.Checked {
		t.Fatalf("data flow must be reported as not checked, got %+v", v[0].DataFlow)
	}
	if connect.calls != verifySettleRounds+1 {
		t.Fatalf("reads = %d, want %d: the first plus one per poll", connect.calls, verifySettleRounds+1)
	}
	if wh.calls != 0 {
		t.Fatalf("data flow must not run when the layers did not settle, got %d reads", wh.calls)
	}
	if hasStatus(sink, "done") {
		t.Fatal("must not reach done")
	}
	if totalSlept(clk) != 90*time.Second {
		t.Fatalf("slept %s, want the 1m30s window", totalSlept(clk))
	}
}

// A repeat of the last step's result after a recovery is the Repeat form and
// nothing else: no second verdict, no second check, no second done.
func TestSettleLoopRepeatedResultGivesTheRepeatForm(t *testing.T) {
	connect := connectSeq(connectStatus("RUNNING", "RUNNING"))
	sink := &fakeSink{}
	inbox := &fakeInbox{drains: [][]HumanInput{nil, lastStep(), lastStep()}, waitErr: context.Canceled}
	_, _ = runSettleLoop(t, connect, sink, inbox)

	v := verifications(t, sink)
	if len(v) != 2 {
		t.Fatalf("events = %d, want the verdict and its Repeat: %+v", len(v), v)
	}
	if v[0].Repeat || !v[0].Recovered || len(v[0].Checks) == 0 {
		t.Fatalf("first = %+v, want the full recovered verdict", v[0])
	}
	if !v[1].Repeat || !v[1].Recovered || len(v[1].Checks) != 0 || v[1].DataFlow != nil || v[1].ProposalID != "p_1" {
		t.Fatalf("second = %+v, want the Repeat form: recovered, no checks, no data flow", v[1])
	}
	if connect.calls != 2 {
		t.Fatalf("reads = %d, want 2 (one settled check): the repeat must not read again", connect.calls)
	}
	var done int
	for _, e := range sink.ofType(events.AgentStatus) {
		// The recovery's own done; the parked episode's later done is "waiting for input".
		if strings.Contains(string(e.payload), `"state":"done"`) && strings.Contains(string(e.payload), `"detail":"pipeline recovered"`) {
			done++
		}
	}
	if done != 1 {
		t.Fatalf("done statuses = %d, want 1", done)
	}
}

// The same result twice in one batch: one verdict, then the Repeat form.
func TestSettleLoopDuplicateInOneBatchIsVerifiedOnce(t *testing.T) {
	connect := connectSeq(connectStatus("RUNNING", "RUNNING"))
	sink := &fakeSink{}
	both := append(lastStep(), lastStep()...)
	inbox := &fakeInbox{drains: [][]HumanInput{nil, both}, waitErr: context.Canceled}
	_, _ = runSettleLoop(t, connect, sink, inbox)

	v := verifications(t, sink)
	if len(v) != 2 || v[0].Repeat || !v[1].Repeat {
		t.Fatalf("want one full verdict then one Repeat, got %+v", v)
	}
	if connect.calls != 2 {
		t.Fatalf("reads = %d, want the checks to run once", connect.calls)
	}
}

// When the run ends in the middle of the wait there is no verdict, and the
// proposal stays open for verification by a later run.
func TestSettleLoopContextEndsMidWaitAnnouncesNothing(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	connect := connectSeq(connectStatus("RUNNING", "UNASSIGNED"))
	sink := &fakeSink{}
	r, _ := settleRunner(t, connect, sink, &fakeInbox{})
	r.clk = cancelClock{cancel: cancel}

	out := r.handleActionResults(ctx, lastStep())
	if out.broken || out.recovered || len(out.notes) != 0 {
		t.Fatalf("outcome = %+v, want none", out)
	}
	if got := verifications(t, sink); len(got) != 0 {
		t.Fatalf("no verdict may be announced, got %+v", got)
	}
	if rec, _ := r.proposals.get("p_1"); rec.verified {
		t.Fatal("the proposal must stay unverified")
	}
	for _, m := range messageTexts(sink) {
		if strings.Contains(m, "recovered") {
			t.Fatalf("no verdict message either: %q", m)
		}
	}
}

// A room service that refuses agent.verification (built before it existed) still
// gets the verdict, as the one plain message the event replaced.
func TestSettleLoopFallsBackToOneMessageWhenTheEventIsRefused(t *testing.T) {
	connect := connectSeq(connectStatus("RUNNING", "RUNNING"))
	inner := &fakeSink{}
	sink := refuseTypeSink{fakeSink: inner, refuse: events.AgentVerification}
	inbox := &fakeInbox{drains: [][]HumanInput{nil, lastStep(), lastStep()}, waitErr: context.Canceled}
	_, _ = runSettleLoop(t, connect, sink, inbox)

	if got := verifications(t, inner); len(got) != 0 {
		t.Fatalf("the refused event must not be stored: %+v", got)
	}
	var verdicts int
	for _, m := range messageTexts(inner) {
		if strings.HasPrefix(m, "Verification of proposal p_1: recovered:") {
			verdicts++
		}
	}
	if verdicts != 1 {
		t.Fatalf("verdict messages = %d, want exactly one (a Repeat is never a message): %q", verdicts, messageTexts(inner))
	}
}

// pg_replication_slots: a slot that turns active a few seconds after the fix is
// a pass, not a failure.
func TestSettleLoopPostgresSlotBecomesActive(t *testing.T) {
	var reads int
	slots := &fakeVerifyTool{
		name:   "pg_replication_slots",
		schema: map[string]any{"type": "object", "properties": map[string]any{}, "additionalProperties": false},
		runFn: func() (tools.Result, error) {
			reads++
			return tools.Result{Summary: "slot orders", Data: []map[string]any{{"name": "orders", "active": reads >= 3}}}, nil
		},
	}
	sink := &fakeSink{}
	inbox := &fakeInbox{drains: [][]HumanInput{nil, lastActionResult("proposal p_1 step 0: done")}, waitErr: context.Canceled}
	steps := recoveredSteps()[:0:0]
	steps = append(steps, events.ProposalStep{Action: "pg_x", Layer: "postgres", Params: []byte(`{}`)})
	r := newVerifyRunner(t, &scriptedModel{}, inbox, sink, []tools.Tool{slots, pipelineMapTool("analytics.orders"), freshWarehouseTool()}, steps)
	if err := r.run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	v := verifications(t, sink)
	if len(v) != 1 || !v[0].Recovered || reads != 3 {
		t.Fatalf("want one recovered verdict after 3 reads, got %+v after %d", v, reads)
	}
}
