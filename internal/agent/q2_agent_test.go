package agent

import (
	"encoding/json"
	"strings"
	"testing"

	"piperoom/internal/events"
)

func TestAlertEvidenceSlotStall(t *testing.T) {
	raw := json.RawMessage(`{"source":"watcher","signals":[{"signal":"slot_stall","severity":"sev2","slot":"debezium_orders",
	 "stalled_seconds":2100,"retained_growth_bytes":1048576,"stall_after_seconds":1800,"stalled_since":"2026-10-14T02:00:00Z",
	 "heartbeat_configured":false,"layer":"postgres","retained_bytes":5,"note":"FREE TEXT"}]}`)
	got := AlertEvidenceText(raw)
	for _, want := range []string{"stalled_seconds=2100", "retained_growth_bytes=1048576", "stall_after_seconds=1800",
		"stalled_since=2026-10-14T02:00:00Z", "heartbeat_configured=false", "layer=postgres", "stalled_for=35m"} {
		if !strings.Contains(got, want) {
			t.Errorf("evidence misses %q:\n%s", want, got)
		}
	}
	if strings.Contains(got, "FREE TEXT") {
		t.Errorf("free text leaked: %s", got)
	}
}

func TestRenderWithdrawnAndVerification(t *testing.T) {
	w := renderOne(t, events.ProposalWithdrawn, events.ProposalWithdrawnPayload{ProposalID: "p_A", Reason: "wrong fix"})
	if !strings.Contains(w, "withdrawal p_A") || !strings.Contains(w, "<untrusted>wrong fix</untrusted>") {
		t.Errorf("withdrawn line = %q", w)
	}
	v := renderOne(t, events.AgentVerification, events.AgentVerificationPayload{ProposalID: "p_1", Verdict: "NOT recovered after 1m30s"})
	if !strings.Contains(v, "verification p_1 (NOT recovered)") || !strings.Contains(v, "NOT recovered after 1m30s") {
		t.Errorf("verification line = %q", v)
	}
	v = renderOne(t, events.AgentVerification, events.AgentVerificationPayload{ProposalID: "p_1", Recovered: true, Verdict: "ok"})
	if !strings.Contains(v, "(recovered)") {
		t.Errorf("recovered line = %q", v)
	}
	for _, ty := range []events.Type{events.ProposalWithdrawn, events.AgentVerification} {
		if !isHandoffCritical(ty) {
			t.Errorf("%s is not hand-off critical", ty)
		}
	}
}

func TestProposeFixSummaryIncludesServerImpact(t *testing.T) {
	store := &withdrawingProposals{}
	tool := roomToolsWithActions(t, Deps{Proposals: store})["propose_fix"]
	in := `{"title":"t",` + connectorFix + `,"risk":"r","rollback":"b"}`
	res, err := runTool(t, tool, in)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(res.Summary, "kafka-connect, consumer, warehouse") {
		t.Fatalf("summary = %q", res.Summary)
	}
}
