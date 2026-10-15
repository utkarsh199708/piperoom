package agent

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"piperoom/internal/actions"
	"piperoom/internal/events"
	"piperoom/internal/tools"
)

func verifyOnce(t *testing.T, tl []tools.Tool, steps []events.ProposalStep, text string) (*fakeSink, actionOutcome, *runner) {
	t.Helper()
	sink := &fakeSink{}
	r := newVerifyRunner(t, &scriptedModel{}, &fakeInbox{}, sink, tl, steps)
	r.toolList = buildToolList(r.deps.Tools, nil)
	out := r.handleActionResults(context.Background(), lastActionResult(text))
	return sink, out, r
}

func TestVerificationEventRecovered(t *testing.T) {
	connect := &fakeVerifyTool{name: "connect_status", schema: connectSchema(), result: connectRecoveredResult()}
	sink, out, _ := verifyOnce(t, []tools.Tool{connect, pipelineMapTool("analytics.orders"), freshWarehouseTool()}, recoveredSteps(), "proposal p_1 step 1: backfilled")
	vs := verifications(sink)
	if len(vs) != 1 {
		t.Fatalf("want one agent.verification, got %d", len(vs))
	}
	v := vs[0]
	if !v.Recovered || v.Repeat || v.ProposalID != "p_1" || strings.HasSuffix(v.Verdict, "\n") || !strings.HasPrefix(v.Verdict, "recovered:") {
		t.Fatalf("unexpected event: %+v", v)
	}
	if len(v.Checks) != 1 || v.Checks[0].Layer != "kafka-connect" || v.Checks[0].Name != "connect_status" || !v.Checks[0].OK {
		t.Fatalf("checks: %+v", v.Checks)
	}
	if v.DataFlow == nil || !v.DataFlow.Checked || !v.DataFlow.Fresh {
		t.Fatalf("data flow: %+v", v.DataFlow)
	}
	// The verdict is no longer an agent.message.
	for _, m := range sink.ofType(events.AgentMessage) {
		if strings.Contains(string(m.payload), "recovered:") {
			t.Fatalf("verdict still posted as agent.message: %s", m.payload)
		}
	}
	// The model note is the text form, unchanged: tag, head, wrapped evidence.
	if !out.recovered || len(out.notes) != 1 || !strings.HasPrefix(out.notes[0], verifyTag+recoveredHead) || !strings.Contains(out.notes[0], "connect_status:") {
		t.Fatalf("model note changed: %q", out.notes)
	}
}

func TestVerificationEventLayerFailed(t *testing.T) {
	connect := &fakeVerifyTool{name: "connect_status", schema: connectSchema(), result: connectBrokenResult()}
	steps := []events.ProposalStep{{Action: "connect_restart_task", Layer: "kafka-connect", Params: json.RawMessage(`{"connector":"pg-orders-connector","task":0}`)}}
	sink, out, _ := verifyOnce(t, []tools.Tool{connect}, steps, "proposal p_1 step 0: restarted")
	vs := verifications(sink)
	if len(vs) != 1 || vs[0].Recovered || vs[0].Checks[0].OK {
		t.Fatalf("unexpected events: %+v", vs)
	}
	df := vs[0].DataFlow
	if df == nil || df.Checked || df.Detail != "not checked because a layer check did not pass" {
		t.Fatalf("data flow: %+v", df)
	}
	if !out.broken {
		t.Fatal("a failed layer check must be broken")
	}
}

func TestVerificationEventDataFlowSinks(t *testing.T) {
	connect := &fakeVerifyTool{name: "connect_status", schema: connectSchema(), result: connectRecoveredResult()}
	sink, _, _ := verifyOnce(t, []tools.Tool{connect, pipelineMapTool("analytics.orders"), freshWarehouseTool()}, recoveredSteps(), "proposal p_1 step 1: backfilled")
	df := verifications(sink)[0].DataFlow
	if df == nil || len(df.Sinks) != 1 {
		t.Fatalf("want one sink, got %+v", df)
	}
	sc := df.Sinks[0]
	if sc.Table != "analytics.orders" || !sc.Passed || sc.PassedBy == "" || sc.Newest.IsZero() {
		t.Fatalf("sink: %+v", sc)
	}
}

func TestVerificationRepeat(t *testing.T) {
	connect := &fakeVerifyTool{name: "connect_status", schema: connectSchema(), result: connectRecoveredResult()}
	sink, _, r := verifyOnce(t, []tools.Tool{connect, pipelineMapTool("analytics.orders"), freshWarehouseTool()}, recoveredSteps(), "proposal p_1 step 1: backfilled")
	r.handleActionResults(context.Background(), lastActionResult("proposal p_1 step 1: backfilled"))
	vs := verifications(sink)
	if len(vs) != 2 {
		t.Fatalf("want 2 events, got %d", len(vs))
	}
	rep := vs[1]
	if !rep.Repeat || !rep.Recovered || len(rep.Checks) != 0 || rep.DataFlow != nil || rep.ProposalID != "p_1" {
		t.Fatalf("repeat: %+v", rep)
	}
	b, _ := json.Marshal(rep)
	if !strings.Contains(string(b), `"checks":[]`) {
		t.Fatalf("checks must encode as [], got %s", b)
	}
}

func proposeFixTestTool() (*proposeFixTool, *fakeProposals) {
	reg := actions.NewRegistry()
	_ = reg.Register(&fakeAction{name: "connect_restart_task", layer: "kafka-connect"})
	p := &fakeProposals{id: "p_1"}
	return &proposeFixTool{actions: reg, proposals: p, sink: &fakeSink{}}, p
}

func TestProposeFixVerifyField(t *testing.T) {
	tool, p := proposeFixTestTool()
	raw := json.RawMessage(`{"title":"t","steps":[{"action":"connect_restart_task","params":{"connector":"c","task":0},"description":"d","verify":" task 0 RUNNING within 60 s "}],"risk":"low","rollback":"none needed: restart only"}`)
	if _, err := tool.Run(context.Background(), raw); err != nil {
		t.Fatal(err)
	}
	if got := p.created.Steps[0].Verify; got != "task 0 RUNNING within 60 s" {
		t.Fatalf("verify = %q", got)
	}
	if !strings.Contains(tool.Description(), "verify") {
		t.Fatal("the tool description must mention verify")
	}
}

func TestProposeFixVerifyTooLong(t *testing.T) {
	tool, _ := proposeFixTestTool()
	for n, wantErr := range map[int]bool{200: false, 201: true} {
		raw := json.RawMessage(`{"title":"t","steps":[{"action":"connect_restart_task","params":{"connector":"c","task":0},"description":"d","verify":"` + strings.Repeat("é", n) + `"}],"risk":"low","rollback":"none needed: restart only"}`)
		_, err := tool.Run(context.Background(), raw)
		if wantErr != errors.Is(err, tools.ErrBadInput) || (!wantErr && err != nil) {
			t.Fatalf("%d chars: err = %v", n, err)
		}
	}
}

// A data-flow check that could not read a table puts the read error in
// DataFlowCheck.Detail; it is clipped like a per-check detail.
func TestVerificationDataFlowDetailIsClipped(t *testing.T) {
	long := strings.Repeat("x", 3*verifyEvidenceBytes)
	got := (flowResult{detail: long}).dataFlowCheck("").Detail
	if len(got) > verifyEvidenceBytes+len("…") || !strings.HasSuffix(got, "…") {
		t.Fatalf("detail not clipped: %d bytes", len(got))
	}
}

// A recovered verdict with no NONE subject posts no follow-up message.
func TestVerificationNoFollowUpMessageWhenNoneAbsent(t *testing.T) {
	connect := &fakeVerifyTool{name: "connect_status", schema: connectSchema(), result: connectRecoveredResult()}
	sink, _, _ := verifyOnce(t, []tools.Tool{connect, pipelineMapTool("analytics.orders"), freshWarehouseTool()}, recoveredSteps(), "proposal p_1 step 1: backfilled")
	if n := len(sink.ofType(events.AgentMessage)); n != 0 {
		t.Fatalf("no agent.message expected, got %d", n)
	}
}
