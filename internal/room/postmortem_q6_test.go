package room_test

import (
	"context"
	"strings"
	"testing"

	"piperoom/internal/events"
)

// TestPostmortemOutcomeUnknown (Q6 U3): a step whose outcome is unknown is not
// "a step failed"; the reader must check the target.
func TestPostmortemOutcomeUnknown(t *testing.T) {
	svc, st, c := newTestService(t)
	r := createRoom(t, svc, "arjun")
	a := func(typ events.Type, actor events.Actor, p any) { app(t, st, c, r.ID, typ, actor, p) }
	a(events.ProposalCreated, events.AgentActor, events.ProposalCreatedPayload{ProposalID: "p_1", Title: "restart", Steps: []events.ProposalStep{{Action: "connect_restart", Layer: "kafka-connect"}}, RequiredLayers: []string{"kafka-connect"}})
	a(events.ActionExecuted, pmSystem, events.ActionExecutedPayload{ProposalID: "p_1", StepIndex: 0, OutcomeUnknown: true})
	md := renderRoom(t, svc, r.ID)
	if !strings.Contains(md, "Outcome: unknown — the step may or may not have run; check the target") {
		t.Fatalf("no unknown-outcome line:\n%s", md)
	}
	if strings.Contains(md, "a step failed") {
		t.Fatalf("unknown outcome reads as a failure:\n%s", md)
	}
}

// TestPostmortemTimelineWatcherNotes (Q6 U3): watcher joins and all-clear
// notes appear in the timeline.
func TestPostmortemTimelineWatcherNotes(t *testing.T) {
	svc, _, _ := newTestService(t)
	r := createRoom(t, svc, "arjun")
	ctx := context.Background()
	if err := svc.PostRoomEvidence(ctx, r.ID, "watcher: orders: consumer lag joined\nevidence: {}", []byte(`{"source":"watcher","signals":[{"signal":"consumer_lag"}]}`)); err != nil {
		t.Fatal(err)
	}
	if err := svc.PostRoomEvidence(ctx, r.ID, "watcher: orders: all watcher signals are clear", nil); err != nil {
		t.Fatal(err)
	}
	tl := section(t, renderRoom(t, svc, r.ID), "## Timeline")
	for _, want := range []string{"watcher: orders: consumer lag joined", "all watcher signals are clear"} {
		if !strings.Contains(tl, want) {
			t.Errorf("timeline lacks %q:\n%s", want, tl)
		}
	}
	if strings.Contains(tl, "evidence:") {
		t.Errorf("timeline carries the evidence body:\n%s", tl)
	}
}

// TestPostmortemVerificationOneVerdict (Q6 U3): the verdict word is not said
// twice, whichever way the agent's verdict line starts.
func TestPostmortemVerificationOneVerdict(t *testing.T) {
	svc, st, c := newTestService(t)
	r := createRoom(t, svc, "arjun")
	app(t, st, c, r.ID, events.ProposalCreated, events.AgentActor, events.ProposalCreatedPayload{ProposalID: "p_1", Title: "x", Steps: []events.ProposalStep{{Action: "a", Layer: "l"}}, RequiredLayers: []string{"l"}})
	app(t, st, c, r.ID, events.AgentVerification, events.AgentActor, events.AgentVerificationPayload{ProposalID: "p_1", Recovered: false, Verdict: "NOT recovered: lag still growing"})
	md := renderRoom(t, svc, r.ID)
	if !strings.Contains(md, "- `p_1` NOT recovered: lag still growing") {
		t.Fatalf("verification line wrong:\n%s", md)
	}
	if strings.Contains(md, "not recovered; ") {
		t.Fatalf("verdict repeated:\n%s", md)
	}
}
