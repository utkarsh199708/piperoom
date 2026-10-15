package room

import (
	"context"
	"testing"

	"piperoom/internal/events"
	"piperoom/internal/store/memstore"
)

// Every agent.* event type the agent writes goes through its sink; a missing
// one (agent.verification, #269) dropped every fix verdict. Control-plane
// types stay refused.
func TestAgentSinkAcceptsEveryAgentType(t *testing.T) {
	ctx := context.Background()
	c := &testClock{t: leaseT0}
	rig := newLeaseRig(t, memstore.NewWithClock(c.Now), c)
	snap, err := rig.svc.CreateRoom(ctx, leaseUser(t, rig.svc, "arjun"), "r", "sev2", "orders")
	if err != nil {
		t.Fatal(err)
	}
	for _, typ := range []events.Type{
		events.AgentStatus, events.AgentStepStart, events.AgentToolCall, events.AgentToolResult,
		events.AgentMessage, events.AgentHypothesis, events.AgentUsage, events.AgentTriage,
		events.AgentVerification,
	} {
		if !agentEventTypes[typ] {
			t.Errorf("agent sink refuses %s", typ)
		}
	}
	if err := rig.svc.agentEmit(ctx, snap.ID, outsideRun, events.AgentVerification,
		events.AgentVerificationPayload{ProposalID: "p_1"}); err != nil {
		t.Fatalf("agent.verification through the sink: %v", err)
	}
	if err := rig.svc.agentEmit(ctx, snap.ID, outsideRun, events.ControlGranted, events.ControlGrantedPayload{}); err == nil {
		t.Fatal("agent sink accepted control.granted")
	}
}
