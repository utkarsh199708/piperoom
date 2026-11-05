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
		events.AgentVerification, events.AgentRecoveryCheck,
	} {
		if !agentEventTypes[typ] {
			t.Errorf("agent sink refuses %s", typ)
		}
	}
	if err := rig.svc.agentEmit(ctx, snap.ID, outsideRun, events.AgentVerification,
		events.AgentVerificationPayload{ProposalID: "p_1"}); err != nil {
		t.Fatalf("agent.verification through the sink: %v", err)
	}
	if err := rig.svc.agentEmit(ctx, snap.ID, outsideRun, events.AgentRecoveryCheck,
		events.AgentRecoveryCheckPayload{ProposalID: "p_1", Poll: 1, MaxPolls: 3, Before: 20, After: 20, Unit: events.RecoveryUnitRows}); err != nil {
		t.Fatalf("agent.recovery_check through the sink: %v", err)
	}
	if err := rig.svc.agentEmit(ctx, snap.ID, outsideRun, events.ControlGranted, events.ControlGrantedPayload{}); err == nil {
		t.Fatal("agent sink accepted control.granted")
	}
}

// U1-25 item A: the setup run records its plan and its bound stop through
// the agent sink, so both are accepted; the ladder's verdicts are the
// server's own and stay refused, whatever payload the agent sends.
func TestAgentSinkSetupTypes(t *testing.T) {
	ctx := context.Background()
	c := &testClock{t: leaseT0}
	rig := newLeaseRig(t, memstore.NewWithClock(c.Now), c)
	snap, err := rig.svc.CreateRoom(ctx, leaseUser(t, rig.svc, "arjun"), "r", "sev2", "orders")
	if err != nil {
		t.Fatal(err)
	}
	accepted := []struct {
		typ     events.Type
		payload any
	}{
		{events.SetupPlan, events.SetupPlanPayload{PlanID: "pl_1", Phases: []events.SetupPlanPhase{{Name: "stack"}}}},
		{events.SetupBlocked, events.SetupBlockedPayload{Reason: events.SetupBlockedAttempts, Question: "retry?"}},
	}
	for _, a := range accepted {
		if !agentEventTypes[a.typ] {
			t.Errorf("agent sink refuses %s", a.typ)
		}
		if err := rig.svc.agentEmit(ctx, snap.ID, outsideRun, a.typ, a.payload); err != nil {
			t.Errorf("%s through the sink: %v", a.typ, err)
		}
	}
	refused := []struct {
		typ     events.Type
		payload any
	}{
		{events.SetupVerified, events.SetupVerifiedPayload{PlanID: "pl_1", Rungs: []events.SetupRung{{Name: "connector_running", OK: true}}}},
		{events.SetupAwaitingChange, events.SetupAwaitingChangePayload{Table: "public.t", ExampleSQL: "select 1"}},
	}
	for _, r := range refused {
		if agentEventTypes[r.typ] {
			t.Errorf("agent sink accepts %s", r.typ)
		}
		if err := rig.svc.agentEmit(ctx, snap.ID, outsideRun, r.typ, r.payload); err == nil {
			t.Errorf("agent sink accepted %s", r.typ)
		}
	}
	evs, _, err := rig.svc.Events(ctx, snap.ID, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range evs {
		if e.Type == events.SetupVerified || e.Type == events.SetupAwaitingChange {
			t.Fatalf("a refused type reached the log: %s", e.Type)
		}
	}
}
