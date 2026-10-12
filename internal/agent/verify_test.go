package agent

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/anthropics/anthropic-sdk-go"

	"piperoom/internal/actions"
	"piperoom/internal/events"
	"piperoom/internal/tools"
)

// ---- verify-after-fix test fakes ----

// fakeVerifyTool is a configurable read tool whose result and error a test
// sets, so the verification pass can be driven through the real loop. runFn,
// when set, produces a fresh result per call (for freshness retries).
type fakeVerifyTool struct {
	name   string
	schema map[string]any
	result tools.Result
	err    error
	runFn  func() (tools.Result, error)
	calls  int
}

func (t *fakeVerifyTool) Name() string                { return t.name }
func (t *fakeVerifyTool) Description() string         { return "fake verify tool" }
func (t *fakeVerifyTool) InputSchema() map[string]any { return t.schema }
func (t *fakeVerifyTool) Kind() tools.Kind            { return tools.KindRead }
func (t *fakeVerifyTool) Run(context.Context, json.RawMessage) (tools.Result, error) {
	t.calls++
	if t.runFn != nil {
		return t.runFn()
	}
	return t.result, t.err
}

func connectSchema() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"connector": map[string]any{"type": "string"},
		},
		"additionalProperties": false,
		"required":             []string{"connector"},
	}
}

func whSchema() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"table":     map[string]any{"type": "string"},
			"ts_column": map[string]any{"type": "string"},
		},
		"additionalProperties": false,
		"required":             []string{"table", "ts_column"},
	}
}

func connectRecoveredResult() tools.Result {
	return tools.Result{
		Summary: "pg-orders-connector (source): RUNNING.\nTask 0: RUNNING",
		Data: map[string]any{
			"name":  "pg-orders-connector",
			"state": "RUNNING",
			"tasks": []map[string]any{{"id": 0, "state": "RUNNING"}},
		},
	}
}

func connectBrokenResult() tools.Result {
	return tools.Result{
		Summary: "pg-orders-connector (source): RUNNING.\nTask 0: FAILED ...",
		Data: map[string]any{
			"name":  "pg-orders-connector",
			"state": "RUNNING",
			"tasks": []map[string]any{{"id": 0, "state": "FAILED"}},
		},
	}
}

// newVerifyRunner builds a runner with the given read tools and a seeded
// proposal "p_1", so tests can drive the loop straight into verify-after-fix.
func newVerifyRunner(t *testing.T, model model, inbox Inbox, sink Sink, regTools []tools.Tool, steps []events.ProposalStep) *runner {
	t.Helper()
	reg := tools.NewRegistry()
	for _, tt := range regTools {
		if err := reg.Register(tt); err != nil {
			t.Fatalf("register %s: %v", tt.Name(), err)
		}
	}
	r, err := newRunner(Alert{Title: "orders stale", PipelineID: "orders", PipelineMap: "pg -> connect -> warehouse"}, Deps{
		Tools:  reg,
		Inbox:  inbox,
		Sink:   sink,
		Logger: discardLogger(),
	}, Options{MaxTurns: 5})
	if err != nil {
		t.Fatalf("newRunner: %v", err)
	}
	r.model = model
	if steps != nil {
		r.proposals.record("p_1", steps)
	}
	return r
}

func messageTexts(s *fakeSink) []string {
	var out []string
	for _, e := range s.ofType(events.AgentMessage) {
		var p events.AgentMessagePayload
		if json.Unmarshal(e.payload, &p) == nil {
			out = append(out, p.Text)
		}
	}
	return out
}

func hasStatus(s *fakeSink, state string) bool {
	for _, e := range s.ofType(events.AgentStatus) {
		var p events.AgentStatusPayload
		if json.Unmarshal(e.payload, &p) == nil && p.State == state {
			return true
		}
	}
	return false
}

func assertVerdict(t *testing.T, msgs []string, wantRecovered bool) {
	t.Helper()
	var recovered, notRecovered bool
	for _, m := range msgs {
		if strings.HasPrefix(m, "recovered:") {
			recovered = true
		}
		if strings.HasPrefix(m, "NOT recovered:") {
			notRecovered = true
		}
	}
	if wantRecovered {
		if !recovered || notRecovered {
			t.Fatalf("expected a recovered verdict, got %q", msgs)
		}
		return
	}
	if recovered {
		t.Fatalf("must not claim recovered, got %q", msgs)
	}
	if !notRecovered {
		t.Fatalf("expected a NOT recovered verdict, got %q", msgs)
	}
}

// lastActionResult is an action_result whose step succeeded (OK == true).
func lastActionResult(text string) []HumanInput {
	return []HumanInput{{Kind: "action_result", Text: text, OK: okPtr(true)}}
}

// actionResult is an action_result with an explicit outcome: ok may be nil for
// an unknown outcome.
func actionResult(text string, ok *bool) []HumanInput {
	return []HumanInput{{Kind: "action_result", Text: text, OK: ok}}
}

func okPtr(b bool) *bool { return &b }

// assertNoRecoveryClaim fails when any message claims recovery or reports a
// recovery verdict. Messages that only say recovery is not verified are fine.
func assertNoRecoveryClaim(t *testing.T, msgs []string) {
	t.Helper()
	for _, m := range msgs {
		if strings.Contains(m, "recovered") {
			t.Fatalf("must not claim recovery, got %q", msgs)
		}
	}
}

// ---- unit tests ----

func TestParseActionResult(t *testing.T) {
	cases := []struct {
		in   string
		id   string
		step int
		ok   bool
	}{
		{"proposal p_01JABC step 0: restarted task", "p_01JABC", 0, true},
		{"proposal p_01JABC step 1: done", "p_01JABC", 1, true},
		{"proposal p_01JABC step 12: done", "p_01JABC", 12, true},
		{"proposal p_01JABC step x: done", "", 0, false},
		{"approval p_1 step 0: done", "", 0, false},
		{"proposal p_1 ste 0: done", "", 0, false},
		{"proposal p_1 step 0", "", 0, false},
		{"step 0: done", "", 0, false},
	}
	for _, c := range cases {
		id, step, ok := parseActionResult(c.in)
		if id != c.id || step != c.step || ok != c.ok {
			t.Errorf("parseActionResult(%q) = (%q, %d, %v), want (%q, %d, %v)", c.in, id, step, ok, c.id, c.step, c.ok)
		}
	}
}

func TestVerifyChecksMapLayers(t *testing.T) {
	steps := []events.ProposalStep{
		{Action: "schema_set_compatibility", Layer: "schema-registry", Params: json.RawMessage(`{"subject":"x-value","level":"NONE"}`)},
		{Action: "connect_restart_task", Layer: "kafka-connect", Params: json.RawMessage(`{"connector":"pg-orders-connector","task":0}`)},
		{Action: "connect_restart_task", Layer: "kafka-connect", Params: json.RawMessage(`{"connector":"other-connector","task":0}`)},
	}
	checks := verifyChecks(steps)
	if len(checks) != 1 {
		t.Fatalf("checks = %d, want 1 (schema-registry has no check, kafka-connect deduped)", len(checks))
	}
	if checks[0].name != "connect_status" {
		t.Fatalf("check = %q, want connect_status", checks[0].name)
	}
	if got := string(checks[0].input); !strings.Contains(got, "pg-orders-connector") {
		t.Fatalf("connect_status input should reuse the connector param, got %s", got)
	}

	// warehouse + postgres each add a check, but only up to the cap.
	steps2 := []events.ProposalStep{
		{Action: "connect_restart_task", Layer: "kafka-connect", Params: json.RawMessage(`{"connector":"c"}`)},
		{Action: "pg_something", Layer: "postgres", Params: json.RawMessage(`{}`)},
		{Action: "wh_something", Layer: "warehouse", Params: json.RawMessage(`{"table":"analytics.orders","ts_column":"updated_at"}`)},
	}
	checks2 := verifyChecks(steps2)
	if len(checks2) != maxVerifyChecks {
		t.Fatalf("checks2 = %d, want %d", len(checks2), maxVerifyChecks)
	}
	for i, want := range []string{"connect_status", "pg_replication_slots", "warehouse_freshness"} {
		if checks2[i].name != want {
			t.Fatalf("checks2[%d] = %q, want %q", i, checks2[i].name, want)
		}
	}
	if !checks2[2].retry {
		t.Fatal("warehouse_freshness check must retry")
	}
}

func TestFollowupStep(t *testing.T) {
	marker := events.ProposalStep{Description: "run only after check: restore BACKWARD"}
	plain := events.ProposalStep{Description: "restart the task"}
	if !isFollowupStep(marker) {
		t.Fatal("step marked run-only-after-check must be detected as a follow-up")
	}
	if isFollowupStep(plain) {
		t.Fatal("ordinary step must not be a follow-up")
	}
	if !strings.Contains(followupLine(marker, true), "now safe") {
		t.Fatalf("safe follow-up line = %q", followupLine(marker, true))
	}
	if !strings.Contains(followupLine(marker, false), "not safe") {
		t.Fatalf("unsafe follow-up line = %q", followupLine(marker, false))
	}
}

func TestProposeFixRecordsIntoTracker(t *testing.T) {
	reg := actions.NewRegistry()
	if err := reg.Register(&fakeAction{name: "connect_restart_task", layer: "kafka-connect"}); err != nil {
		t.Fatal(err)
	}
	track := newProposalTracker()
	tool := &proposeFixTool{actions: reg, proposals: &fakeProposals{id: "p_9"}, sink: &fakeSink{}, track: track}

	raw := json.RawMessage(`{"title":"restart task","steps":[{"action":"connect_restart_task","params":{"connector":"c","task":0},"description":"restart"}],"risk":"low","rollback":"restore the old task"}`)
	if _, err := tool.Run(context.Background(), raw); err != nil {
		t.Fatalf("run: %v", err)
	}
	rec, ok := track.get("p_9")
	if !ok {
		t.Fatal("proposal not recorded in the tracker")
	}
	if len(rec.steps) != 1 || rec.steps[0].Layer != "kafka-connect" {
		t.Fatalf("tracked steps = %+v", rec.steps)
	}
}

// ---- loop tests ----

func TestVerifyAfterFixRecoveredEmitsDone(t *testing.T) {
	connect := &fakeVerifyTool{name: "connect_status", schema: connectSchema(), result: connectRecoveredResult()}
	model := &scriptedModel{}
	sink := &fakeSink{}
	inbox := &fakeInbox{
		drains: [][]HumanInput{
			nil,
			lastActionResult("proposal p_1 step 0: restarted pg-orders-connector task 0 (state now RUNNING)"),
		},
		waitErr: context.Canceled,
	}
	steps := []events.ProposalStep{
		{Action: "connect_restart_task", Layer: "kafka-connect", Params: json.RawMessage(`{"connector":"pg-orders-connector","task":0}`), Description: "restart task 0"},
	}
	r := newVerifyRunner(t, model, inbox, sink, []tools.Tool{connect}, steps)

	if err := r.run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	if !hasStatus(sink, "done") {
		t.Fatalf("expected agent.status done, got %v", sink.ofType(events.AgentStatus))
	}
	assertVerdict(t, messageTexts(sink), true)
	if connect.calls != 1 {
		t.Fatalf("expected 1 connect_status verification call, got %d", connect.calls)
	}
	// After recovery the model is not called again until a human speaks.
	if len(model.calls) != 1 {
		t.Fatalf("model must not run again after recovery without human input (1 call), got %d", len(model.calls))
	}
}

func TestVerifyAfterFixNotRecoveredKeepsInvestigating(t *testing.T) {
	connect := &fakeVerifyTool{name: "connect_status", schema: connectSchema(), result: connectBrokenResult()}
	model := &scriptedModel{}
	sink := &fakeSink{}
	inbox := &fakeInbox{
		drains: [][]HumanInput{
			nil,
			lastActionResult("proposal p_1 step 0: restarted pg-orders-connector task 0 (state now RUNNING)"),
		},
		waitErr: context.Canceled,
	}
	steps := []events.ProposalStep{
		{Action: "connect_restart_task", Layer: "kafka-connect", Params: json.RawMessage(`{"connector":"pg-orders-connector","task":0}`), Description: "restart task 0"},
	}
	r := newVerifyRunner(t, model, inbox, sink, []tools.Tool{connect}, steps)

	if err := r.run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	if hasStatus(sink, "done") {
		t.Fatal("must not reach done when verification fails")
	}
	assertVerdict(t, messageTexts(sink), false)
	if len(model.calls) < 2 {
		t.Fatalf("expected the loop to keep investigating (>=2 model calls), got %d", len(model.calls))
	}
	// The action_result must still reach the model so it can keep investigating.
	if !strings.Contains(messagesJSON(model.calls[1]), "proposal p_1 step 0") {
		t.Fatalf("resumed request should carry the action_result: %s", messagesJSON(model.calls[1]))
	}
}

func TestVerifyAfterFixFailedLastStepNoRecoveredClaim(t *testing.T) {
	// The last step reports OK == false, yet the read checks would pass. The
	// agent must say the step failed, must not run recovery checks or claim
	// recovery, and must keep investigating (the model runs again).
	connect := &fakeVerifyTool{name: "connect_status", schema: connectSchema(), result: connectRecoveredResult()}
	model := &scriptedModel{}
	sink := &fakeSink{}
	inbox := &fakeInbox{
		drains: [][]HumanInput{
			nil,
			actionResult("proposal p_1 step 1: task restart failed: connector busy", okPtr(false)),
		},
		waitErr: context.Canceled,
	}
	steps := []events.ProposalStep{
		{Action: "schema_set_compatibility", Layer: "schema-registry", Params: json.RawMessage(`{"subject":"x-value","level":"NONE"}`)},
		{Action: "connect_restart_task", Layer: "kafka-connect", Params: json.RawMessage(`{"connector":"pg-orders-connector","task":0}`)},
	}
	r := newVerifyRunner(t, model, inbox, sink, []tools.Tool{connect}, steps)

	if err := r.run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	if hasStatus(sink, "done") {
		t.Fatal("a failed step must never end with done")
	}
	msgs := messageTexts(sink)
	assertNoRecoveryClaim(t, msgs)
	var sawFailed bool
	for _, m := range msgs {
		if strings.Contains(m, `Fix step failed: proposal "p_1" step 1`) {
			sawFailed = true
		}
	}
	if !sawFailed {
		t.Fatalf("expected a message that the fix step failed, got %q", msgs)
	}
	if connect.calls != 0 {
		t.Fatalf("recovery checks must not run after a failed step, got %d calls", connect.calls)
	}
	if len(model.calls) < 2 {
		t.Fatalf("expected the loop to keep investigating (>=2 model calls), got %d", len(model.calls))
	}
	if !strings.Contains(messagesJSON(model.calls[1]), "proposal p_1 step 1") {
		t.Fatalf("resumed request should carry the failed action_result: %s", messagesJSON(model.calls[1]))
	}
}

func TestVerifyAfterFixUnknownOutcomeNoRecoveredClaim(t *testing.T) {
	// OK == nil means the outcome is unknown. Even with passing read checks the
	// agent must never claim recovery, and it does not run the checks.
	connect := &fakeVerifyTool{name: "connect_status", schema: connectSchema(), result: connectRecoveredResult()}
	model := &scriptedModel{}
	sink := &fakeSink{}
	inbox := &fakeInbox{
		drains: [][]HumanInput{
			nil,
			actionResult("proposal p_1 step 1: done", nil),
		},
		waitErr: context.Canceled,
	}
	steps := []events.ProposalStep{
		{Action: "schema_set_compatibility", Layer: "schema-registry", Params: json.RawMessage(`{"subject":"x-value","level":"NONE"}`)},
		{Action: "connect_restart_task", Layer: "kafka-connect", Params: json.RawMessage(`{"connector":"pg-orders-connector","task":0}`)},
	}
	r := newVerifyRunner(t, model, inbox, sink, []tools.Tool{connect}, steps)

	if err := r.run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	if hasStatus(sink, "done") {
		t.Fatal("an unknown outcome must never end with done")
	}
	assertNoRecoveryClaim(t, messageTexts(sink))
	if connect.calls != 0 {
		t.Fatalf("recovery checks must not run for an unknown outcome, got %d calls", connect.calls)
	}
	if len(model.calls) < 2 {
		t.Fatalf("expected the loop to keep going (>=2 model calls), got %d", len(model.calls))
	}
}

func TestVerifyAfterFixRecoveredThenSteerGetsReply(t *testing.T) {
	// Recovery emits done, but the run stays alive: a steer that arrives later
	// gets a model turn and a reply. The model is not called before the steer.
	connect := &fakeVerifyTool{name: "connect_status", schema: connectSchema(), result: connectRecoveredResult()}
	model := &scriptedModel{turns: []anthropic.BetaMessage{
		mustMessage(t, `{"stop_reason":"end_turn","content":[{"type":"text","text":"Waiting on the fix."}]}`),
		mustMessage(t, `{"stop_reason":"end_turn","content":[{"type":"text","text":"Lag is back to normal."}]}`),
	}}
	sink := &fakeSink{}
	steer := HumanInput{Kind: "steer", Actor: events.Actor{Name: "driver"}, Text: "is the lag back to normal?"}
	inbox := &fakeInbox{
		drains: [][]HumanInput{
			nil,
			lastActionResult("proposal p_1 step 0: restarted pg-orders-connector task 0 (state now RUNNING)"),
			{steer},
		},
		waitErr: context.Canceled,
	}
	steps := []events.ProposalStep{
		{Action: "connect_restart_task", Layer: "kafka-connect", Params: json.RawMessage(`{"connector":"pg-orders-connector","task":0}`), Description: "restart task 0"},
	}
	r := newVerifyRunner(t, model, inbox, sink, []tools.Tool{connect}, steps)

	if err := r.run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	if !hasStatus(sink, "done") {
		t.Fatalf("expected agent.status done, got %v", sink.ofType(events.AgentStatus))
	}
	assertVerdict(t, messageTexts(sink), true)
	if len(model.calls) != 2 {
		t.Fatalf("expected 2 model calls (before and after the steer), got %d", len(model.calls))
	}
	if !strings.Contains(messagesJSON(model.calls[1]), "is the lag back to normal?") {
		t.Fatalf("the steer must reach the model after recovery: %s", messagesJSON(model.calls[1]))
	}
	var replied bool
	for _, m := range messageTexts(sink) {
		if m == "Lag is back to normal." {
			replied = true
		}
	}
	if !replied {
		t.Fatalf("expected the reply to the steer after recovery, got %q", messageTexts(sink))
	}
}

func TestVerifyAfterFixFollowupSafety(t *testing.T) {
	connect := &fakeVerifyTool{name: "connect_status", schema: connectSchema(), result: connectRecoveredResult()}
	model := &scriptedModel{}
	sink := &fakeSink{}
	inbox := &fakeInbox{
		drains: [][]HumanInput{
			nil,
			lastActionResult("proposal p_1 step 1: done"),
		},
		waitErr: context.Canceled,
	}
	steps := []events.ProposalStep{
		{Action: "connect_restart_task", Layer: "kafka-connect", Params: json.RawMessage(`{"connector":"pg-orders-connector","task":0}`), Description: "restart task 0"},
		{Action: "schema_set_compatibility", Layer: "schema-registry", Params: json.RawMessage(`{"subject":"pg.public.orders-value","level":"BACKWARD"}`), Description: "run only after check: restore BACKWARD"},
	}
	r := newVerifyRunner(t, model, inbox, sink, []tools.Tool{connect}, steps)

	if err := r.run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	if !hasStatus(sink, "done") {
		t.Fatalf("expected agent.status done, got %v", sink.ofType(events.AgentStatus))
	}
	var sawSafe bool
	for _, m := range messageTexts(sink) {
		if strings.Contains(m, "run only after check: restore BACKWARD") && strings.Contains(m, "now safe") {
			sawSafe = true
		}
	}
	if !sawSafe {
		t.Fatalf("expected the verdict to say the follow-up is now safe, got %q", messageTexts(sink))
	}
}

func TestVerifyAfterFixNotLastStepDoesNotVerify(t *testing.T) {
	// An action_result for a non-last step must not trigger verification or a
	// done status; the input flows to the model and the loop continues.
	connect := &fakeVerifyTool{name: "connect_status", schema: connectSchema(), result: connectRecoveredResult()}
	model := &scriptedModel{}
	sink := &fakeSink{}
	inbox := &fakeInbox{
		drains: [][]HumanInput{
			nil,
			lastActionResult("proposal p_1 step 0: set compatibility"),
		},
		waitErr: context.Canceled,
	}
	steps := []events.ProposalStep{
		{Action: "schema_set_compatibility", Layer: "schema-registry", Params: json.RawMessage(`{"subject":"x-value","level":"NONE"}`)},
		{Action: "connect_restart_task", Layer: "kafka-connect", Params: json.RawMessage(`{"connector":"pg-orders-connector","task":0}`)},
	}
	r := newVerifyRunner(t, model, inbox, sink, []tools.Tool{connect}, steps)

	if err := r.run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	if hasStatus(sink, "done") {
		t.Fatal("must not end with done before the last step")
	}
	if connect.calls != 0 {
		t.Fatalf("verification must not run before the last step, got %d calls", connect.calls)
	}
}

func TestVerifyAfterFixUnknownProposalDoesNotVerify(t *testing.T) {
	connect := &fakeVerifyTool{name: "connect_status", schema: connectSchema(), result: connectRecoveredResult()}
	model := &scriptedModel{}
	sink := &fakeSink{}
	inbox := &fakeInbox{
		drains: [][]HumanInput{
			nil,
			lastActionResult("proposal p_other step 0: done"),
		},
		waitErr: context.Canceled,
	}
	// No proposal tracked under p_other.
	r := newVerifyRunner(t, model, inbox, sink, []tools.Tool{connect}, nil)

	if err := r.run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	if hasStatus(sink, "done") {
		t.Fatal("must not claim recovery for an unknown proposal")
	}
	if connect.calls != 0 {
		t.Fatalf("no verification should run for an unknown proposal, got %d calls", connect.calls)
	}
}

func TestVerifyAfterFixWarehouseFreshnessRetries(t *testing.T) {
	// warehouse_freshness reports empty on the first two attempts, then rows
	// arrive: the check retries and the proposal recovers.
	attempts := 0
	wh := &fakeVerifyTool{name: "warehouse_freshness", schema: whSchema(), runFn: func() (tools.Result, error) {
		attempts++
		if attempts < freshnessRetries {
			return tools.Result{Summary: "no rows (empty table)", Data: map[string]any{"newest": nil}}, nil
		}
		return tools.Result{
			Summary: "newest=" + time.Now().UTC().Format(time.RFC3339Nano) + " (stale for 3s)",
			Data:    map[string]any{"newest": time.Now().UTC().Format(time.RFC3339Nano)},
		}, nil
	}}
	model := &scriptedModel{}
	sink := &fakeSink{}
	inbox := &fakeInbox{
		drains: [][]HumanInput{
			nil,
			lastActionResult("proposal p_1 step 0: backfilled"),
		},
		waitErr: context.Canceled,
	}
	steps := []events.ProposalStep{
		{Action: "wh_backfill", Layer: "warehouse", Params: json.RawMessage(`{"table":"analytics.orders","ts_column":"updated_at"}`)},
	}
	r := newVerifyRunner(t, model, inbox, sink, []tools.Tool{wh}, steps)
	r.clk = &fakeClock{} // instant retry waits

	if err := r.run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	if !hasStatus(sink, "done") {
		t.Fatalf("expected agent.status done after freshness retries, got %v", sink.ofType(events.AgentStatus))
	}
	if wh.calls != freshnessRetries {
		t.Fatalf("expected %d warehouse_freshness attempts, got %d", freshnessRetries, wh.calls)
	}
}
