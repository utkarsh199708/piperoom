package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/anthropics/anthropic-sdk-go"

	"piperoom/internal/events"
	"piperoom/internal/tools"
)

// ---- R6: every required layer, impacted layers included ----

func decimalPreciseSteps() []events.ProposalStep {
	return []events.ProposalStep{{
		Action: "connect_update_connector_config", Layer: "kafka-connect",
		Params: json.RawMessage(`{"connector":"pg-orders-connector","key":"decimal.handling.mode","value":"precise"}`),
	}}
}

func TestTrackerRequiresImpactedLayers(t *testing.T) {
	tr := newProposalTracker()
	tr.record("p_1", decimalPreciseSteps(), []string{"kafka-connect", "consumer", "warehouse"})
	tr.observe(approvalInput("kafka-connect", "p_1"))
	if len(tr.awaiting()) != 0 {
		t.Fatal("kafka-connect alone approved; consumer and warehouse still owe approval")
	}
	tr.observe(approvalInput("consumer", "p_1"))
	tr.observe(approvalInput("warehouse", "p_1"))
	if len(tr.awaiting()) != 1 {
		t.Fatal("every required layer approved: must wait for step results")
	}
}

func TestProposeFixTracksTheServerApproverSet(t *testing.T) {
	tr := newProposalTracker()
	tool := &proposeFixTool{actions: realActions(t), proposals: &fakeProposals{id: "p_1"}, sink: &fakeSink{}, track: tr}
	raw := json.RawMessage(`{"title":"precise decimals","steps":[{"action":"connect_update_connector_config","params":{"connector":"pg-orders-connector","key":"decimal.handling.mode","value":"precise"},"description":"precise"}],"risk":"consumers see new decimals","rollback":"set it back to double"}`)
	res, err := tool.Run(context.Background(), raw)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	tr.observe(approvalInput("kafka-connect", "p_1"))
	if len(tr.awaiting()) != 0 {
		t.Fatalf("tracker thinks the proposal is fully approved after kafka-connect alone (summary %q)", res.Summary)
	}
}

func TestRejectionOrWithdrawalDropsTheFlow(t *testing.T) {
	tr := newProposalTracker()
	tr.record("p_1", decimalPreciseSteps(), []string{"kafka-connect", "consumer"})
	tr.observe(approvalInput("kafka-connect", "p_1"))
	tr.observe(rejectionInput("consumer", "p_1"))
	tr.observe(approvalInput("consumer", "p_1"))
	if len(tr.awaiting()) != 0 {
		t.Fatal("a rejected or withdrawn proposal never runs: nothing to wait for")
	}
}

// ---- R9: fix notes are critical and labelled in the hand-off input ----

func TestFixNoteSurvivesTheHandoffWindowAndIsLabelled(t *testing.T) {
	evs := []events.Event{pmEvent(1, events.CommentAdded, "Meera",
		events.CommentAddedPayload{CommentID: "c_1", Kind: events.CommentKindFixNote, Text: "restarted order-sync by hand"})}
	for i := 0; i < handoffTailEvents+1; i++ {
		evs = append(evs, pmEvent(int64(i+2), events.AgentMessage, "Agent", events.AgentMessagePayload{Text: fmt.Sprintf("msg %d", i)}))
	}
	out := renderRoomEvents(evs)
	if !strings.Contains(out, "restarted order-sync by hand") {
		t.Fatalf("fix note dropped from the hand-off window:\n%s", out)
	}
	if !strings.Contains(out, "fix note by ") || !strings.Contains(out, "Meera") {
		t.Fatalf("fix note is not labelled with its author:\n%s", out)
	}
	if !strings.Contains(strings.ToLower(handoffPrompt()), "fix note") {
		t.Fatal("hand-off prompt does not explain fix notes")
	}
}

func TestPlainCommentStaysPlain(t *testing.T) {
	out := renderRoomEvents([]events.Event{pmEvent(1, events.CommentAdded, "Bo", events.CommentAddedPayload{CommentID: "c_2", Text: "looks odd"})})
	if strings.Contains(out, "fix note") || !strings.Contains(out, "comment c_2") {
		t.Fatalf("plain comment rendered as %q", out)
	}
}

// ---- R16: a new human input ends the repeat window ----

// limitedWaitInbox returns from Wait n times, then reports the context
// cancelled, so a run ends after its scripted drains.
type limitedWaitInbox struct {
	fakeInbox
	n int
}

func (l *limitedWaitInbox) Wait(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.n > 0 {
		l.n--
		return nil
	}
	return context.Canceled
}

func TestRepeatGuardResetsOnNewSteer(t *testing.T) {
	rec := &recordingTool{name: "connect_status", schema: testSchema("connector"), result: tools.Result{Summary: "ok"}}
	m := &scriptedModel{turns: []anthropic.BetaMessage{
		mustMessage(t, `{"stop_reason":"end_turn","content":[{"type":"text","text":"Yes, the slot is active."}]}`),
		mustMessage(t, `{"stop_reason":"end_turn","content":[{"type":"text","text":"Yes, the slot is active."}]}`),
	}}
	inbox := &limitedWaitInbox{n: 2, fakeInbox: fakeInbox{drains: [][]HumanInput{
		{{Seq: 5, Actor: events.Actor{Kind: "human", Name: "ana"}, Kind: "steer", Text: "is the slot still active?"}},
		{{Seq: 9, Actor: events.Actor{Kind: "human", Name: "bo"}, Kind: "steer", Text: "is the slot still active now?"}},
	}}}
	sink := &fakeSink{}
	r := newTestRunner(t, m, inbox, sink, rec, Options{MaxTurns: 5})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = r.run(ctx)
	n := 0
	for _, x := range guardPostedTexts(sink) {
		if x == "Yes, the slot is active." {
			n++
		}
	}
	if n != 2 {
		t.Fatalf("answers in room = %d, want 2 (one per steer): %q", n, guardPostedTexts(sink))
	}
}

func TestGuardResetAllowsTheSameTextOnce(t *testing.T) {
	s := newGuardedSink(&fakeSink{})
	ctx := context.Background()
	msg := events.AgentMessagePayload{Text: "No."}
	if err := s.Emit(ctx, events.AgentMessage, msg); err != nil {
		t.Fatal(err)
	}
	var dropped *messageDroppedError
	if err := s.Emit(ctx, events.AgentMessage, msg); !errors.As(err, &dropped) {
		t.Fatalf("an unprompted repeat must still be dropped, got %v", err)
	}
	s.reset()
	if err := s.Emit(ctx, events.AgentMessage, msg); err != nil {
		t.Fatalf("after new human input the same answer must post, got %v", err)
	}
	if err := s.Emit(ctx, events.AgentMessage, msg); !errors.As(err, &dropped) {
		t.Fatalf("a repeat after the reset is dropped again, got %v", err)
	}
}

func TestOnlyHumanTurnsResetTheGuard(t *testing.T) {
	cases := []struct {
		in   HumanInput
		want bool
	}{
		{HumanInput{Kind: "steer"}, true},
		{HumanInput{Kind: "comment"}, true},
		{HumanInput{Kind: "alert_update", NewIncident: true}, true},
		{HumanInput{Kind: "alert_update"}, false},
		{HumanInput{Kind: "action_result"}, false},
		{HumanInput{Kind: "approval_result"}, false},
	}
	for _, c := range cases {
		if got := endsRepeatWindow([]HumanInput{c.in}); got != c.want {
			t.Errorf("%+v: endsRepeatWindow = %v, want %v", c.in, got, c.want)
		}
	}
}

// ---- R15 + R20: idle must be proven on the source side ----

func verificationOf(t *testing.T, s *fakeSink) events.AgentVerificationPayload {
	t.Helper()
	for _, e := range s.ofType(events.AgentVerification) {
		var p events.AgentVerificationPayload
		if json.Unmarshal(e.payload, &p) == nil && !p.Repeat {
			return p
		}
	}
	t.Fatal("no agent.verification event")
	return events.AgentVerificationPayload{}
}

func runTopicCaseSink(t *testing.T, slots, topics *fakeVerifyTool) *fakeSink {
	t.Helper()
	sink := &fakeSink{}
	inbox := &fakeInbox{drains: [][]HumanInput{nil, lastActionResult("proposal p_1 step 1: done")}, waitErr: context.Canceled}
	r := newVerifyRunner(t, &scriptedModel{}, inbox, sink,
		[]tools.Tool{&fakeVerifyTool{name: "connect_status", schema: connectSchema(), result: connectRecoveredResult()},
			pipelineMapWithTopics(), staleWarehouseTool(), slots, topics}, recoveredSteps())
	if err := r.run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	return sink
}

// A heartbeat moves the slot (the connector consumes WAL and acknowledges it),
// the data topics stay flat: the source had no change. Recovered, flow
// inconclusive.
func TestIdleSourceWithHeartbeatIsRecoveredFlowInconclusive(t *testing.T) {
	slots := slotsTool([3]any{"0/5000", "0/4000", 100}, [3]any{"0/5100", "0/4100", 100})
	v := verificationOf(t, runTopicCaseSink(t, slots, topicOffsetsTool(t, 42)))
	if !v.Recovered || !strings.HasPrefix(v.Verdict, "recovered:") {
		t.Fatalf("verdict = %+v, want recovered", v)
	}
	if v.DataFlow == nil || v.DataFlow.Checked || !strings.HasPrefix(v.DataFlow.Detail, "inconclusive") {
		t.Fatalf("data flow = %+v, want unchecked and inconclusive", v.DataFlow)
	}
}

// The connector is RUNNING but captures nothing while the source writes: the
// slot stands still and retained WAL grows. NOT recovered.
func TestRunningConnectorNotCapturingIsNotRecovered(t *testing.T) {
	slots := slotsTool([3]any{"0/5000", "0/4000", 100}, [3]any{"0/5000", "0/4000", 900})
	v := verificationOf(t, runTopicCaseSink(t, slots, topicOffsetsTool(t, 42)))
	if v.Recovered || !strings.HasPrefix(v.Verdict, "NOT recovered:") {
		t.Fatalf("verdict = %+v, want NOT recovered", v)
	}
}

// Flat data topics but the slot cannot be read: idleness is not proven, and a
// pure idle doubt is neither recovered nor NOT recovered.
func TestFlatTopicsWithoutSourceProofIsInconclusive(t *testing.T) {
	slots := &fakeVerifyTool{name: "pg_replication_slots", schema: map[string]any{
		"type": "object", "properties": map[string]any{}, "additionalProperties": false,
	}, err: errors.New("source postgres is down")}
	sink := runTopicCaseSink(t, slots, topicOffsetsTool(t, 42))
	v := verificationOf(t, sink)
	if v.Recovered {
		t.Fatalf("an unproven idle source must not be recovered: %+v", v)
	}
	want := "inconclusive: no source changes observed; connector may not be capturing"
	if !strings.HasPrefix(v.Verdict, want) {
		t.Fatalf("verdict = %q, want prefix %q", v.Verdict, want)
	}
	if v.DataFlow == nil || v.DataFlow.Checked {
		t.Fatalf("data flow = %+v, want unchecked", v.DataFlow)
	}
}

func TestJudgeSlots(t *testing.T) {
	pos := func(name, flush string, wal int64) slotPosition {
		return slotPosition{name: name, confirmedFlush: flush, restart: "0/1", retainedWAL: wal}
	}
	cases := []struct {
		name          string
		before, after []slotPosition
		want          sourceState
	}{
		{"heartbeat consumed WAL", []slotPosition{pos("s", "0/5000", 100)}, []slotPosition{pos("s", "0/5100", 120)}, sourceQuiet},
		{"no WAL at all", []slotPosition{pos("s", "0/5000", 100)}, []slotPosition{pos("s", "0/5000", 100)}, sourceQuiet},
		{"stuck slot, WAL grows", []slotPosition{pos("s", "0/5000", 100)}, []slotPosition{pos("s", "0/5000", 900)}, sourceActive},
		{"one of two slots stuck", []slotPosition{pos("a", "0/1", 1), pos("b", "0/2", 1)}, []slotPosition{pos("a", "0/9", 1), pos("b", "0/2", 50)}, sourceActive},
		{"slot set changed", []slotPosition{pos("s", "0/5000", 100)}, []slotPosition{pos("t", "0/5000", 100)}, sourceUnproven},
		{"slot count changed", []slotPosition{pos("s", "0/5000", 100)}, nil, sourceUnproven},
	}
	for _, c := range cases {
		if got, line := judgeSlots(c.before, c.after); got != c.want || line == "" {
			t.Errorf("%s: judgeSlots = %v (%q), want %v", c.name, got, line, c.want)
		}
	}
}

func TestRepeatOfInconclusiveVerdictSaysInconclusive(t *testing.T) {
	tr := newProposalTracker()
	tr.record("p_1", recoveredSteps(), nil)
	v := verifyOutcome{inconclusive: true}
	tr.markVerified("p_1", v.recovered, v.word())
	rec, _ := tr.get("p_1")
	if got := verdictWord(rec); got != "inconclusive" {
		t.Fatalf("verdictWord = %q, want inconclusive", got)
	}
}
