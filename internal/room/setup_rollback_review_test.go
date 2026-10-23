package room

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"piperoom/internal/events"
	"piperoom/internal/setup/plan"
)

// Review fixes for the setup rollback (U1-14b): what the log walk counts as
// ran, which ran step an undo matches, the open-rollback refusal and the
// superseded plan.

// walkEvent builds one event of the setup log slice.
func walkEvent(t *testing.T, seq int64, typ events.Type, payload any) events.Event {
	t.Helper()
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	return events.Event{Seq: seq, Type: typ, Payload: raw}
}

func walkStep(action, params string) events.ProposalStep {
	return events.ProposalStep{Action: action, Layer: events.LayerEnvironment, Params: json.RawMessage(params)}
}

func walkPreview(pid string, reversible bool, rollback string) events.ProposalPreviewedPayload {
	return events.ProposalPreviewedPayload{ProposalID: pid, Steps: []events.StepPreview{{
		StepIndex: 0, Preview: &events.PreviewResult{Reversible: reversible, Rollback: rollback},
	}}}
}

func walkExecuted(pid string, planIdx *int, ok, unknown bool) events.ActionExecutedPayload {
	return events.ActionExecutedPayload{ProposalID: pid, StepIndex: 0, OK: ok, OutcomeUnknown: unknown, PlanStepIndex: planIdx}
}

func intp(i int) *int { return &i }

// TestSetupWalkRanOutcomes: a step whose outcome is unknown counts as ran and
// is marked Unknown, a refused or failed step does not count, a step whose
// preview is not Reversible carries no rollback, and a preview written after
// its step executed is still read.
func TestSetupWalkRanOutcomes(t *testing.T) {
	evs := []events.Event{
		walkEvent(t, 1, events.SetupPlan, events.SetupPlanPayload{PlanID: "pl_1", Phases: []events.SetupPlanPhase{{
			Name: "stack", Steps: []events.SetupPlanStep{
				{Index: 0, Op: "compose_up", Target: "db.acme:5432"},
				{Index: 1, Op: "connect_create_connector", Target: "db.acme:5432"},
				{Index: 2, Op: "source_create_publication", Target: "db.acme:5432"},
				{Index: 3, Op: "source_create_slot", Target: "db.acme:5432"},
			},
		}}}),
		walkEvent(t, 2, events.ProposalCreated, events.ProposalCreatedPayload{ProposalID: "p_up", Steps: []events.ProposalStep{walkStep("compose_up", `{}`)}}),
		walkEvent(t, 3, events.ActionExecuted, walkExecuted("p_up", intp(0), true, false)),
		// The preview lands after the step executed.
		walkEvent(t, 4, events.ProposalPreviewed, walkPreview("p_up", true, `compose_down {"file": "compose.yaml"}`)),
		walkEvent(t, 5, events.ProposalCreated, events.ProposalCreatedPayload{ProposalID: "p_conn", Steps: []events.ProposalStep{walkStep("connect_create_connector", `{}`)}}),
		walkEvent(t, 6, events.ProposalPreviewed, walkPreview("p_conn", true, `connect_delete_connector {"connector": "a"}`)),
		walkEvent(t, 7, events.ActionExecuted, walkExecuted("p_conn", intp(1), false, true)),
		walkEvent(t, 8, events.ProposalCreated, events.ProposalCreatedPayload{ProposalID: "p_pub", Steps: []events.ProposalStep{walkStep("source_create_publication", `{}`)}}),
		walkEvent(t, 9, events.ProposalPreviewed, walkPreview("p_pub", false, `source_drop_publication {"publication": "x"}`)),
		walkEvent(t, 10, events.ActionExecuted, walkExecuted("p_pub", intp(2), true, false)),
		walkEvent(t, 11, events.ProposalCreated, events.ProposalCreatedPayload{ProposalID: "p_slot", Steps: []events.ProposalStep{walkStep("source_create_slot", `{}`)}}),
		walkEvent(t, 12, events.ProposalPreviewed, walkPreview("p_slot", true, `source_drop_slot {"slot": "s"}`)),
		walkEvent(t, 13, events.ActionExecuted, walkExecuted("p_slot", intp(3), false, false)),
	}
	ran, executed := setupWalkRan(evs)
	if executed != 3 {
		t.Fatalf("executed = %d, want 3: the failed step changed nothing, the unknown one may have", executed)
	}
	if len(ran) != 3 {
		t.Fatalf("ran = %+v, want 3 steps", ran)
	}
	if ran[0].Rollback != `compose_down {"file": "compose.yaml"}` {
		t.Fatalf("compose_up rollback = %q, want the late preview's record", ran[0].Rollback)
	}
	if ran[0].PlanTarget != "db.acme:5432" {
		t.Fatalf("plan target = %q, want the plan step's target", ran[0].PlanTarget)
	}
	if !ran[1].Unknown || ran[0].Unknown || ran[2].Unknown {
		t.Fatalf("unknown marks = %v %v %v, want only the connector step unknown", ran[0].Unknown, ran[1].Unknown, ran[2].Unknown)
	}
	if ran[2].Rollback != "" {
		t.Fatalf("publication rollback = %q, want none: its preview is not Reversible", ran[2].Rollback)
	}
}

// TestSetupWalkUndoesNewest: of two identical steps, an executed undo removes
// the one that ran last (the rollback order), and a failed undo removes none.
func TestSetupWalkUndoesNewest(t *testing.T) {
	record := `source_drop_publication {"publication": "x"}`
	var evs []events.Event
	seq := int64(0)
	add := func(typ events.Type, payload any) {
		seq++
		evs = append(evs, walkEvent(t, seq, typ, payload))
	}
	for i, pid := range []string{"p_a", "p_b"} {
		add(events.ProposalCreated, events.ProposalCreatedPayload{ProposalID: pid, Steps: []events.ProposalStep{walkStep("source_create_publication", fmt.Sprintf(`{"n":%d}`, i))}})
		add(events.ProposalPreviewed, walkPreview(pid, true, record))
		add(events.ActionExecuted, walkExecuted(pid, intp(i), true, false))
	}
	add(events.ProposalCreated, events.ProposalCreatedPayload{ProposalID: "p_undo_failed", Steps: []events.ProposalStep{walkStep("source_drop_publication", `{"publication": "x"}`)}})
	add(events.ActionExecuted, walkExecuted("p_undo_failed", nil, false, false))
	ran, _ := setupWalkRan(evs)
	if len(ran) != 2 {
		t.Fatalf("ran = %d, want 2: a failed undo undoes nothing", len(ran))
	}
	add(events.ProposalCreated, events.ProposalCreatedPayload{ProposalID: "p_undo", Steps: []events.ProposalStep{walkStep("source_drop_publication", `{"publication":"x"}`)}})
	add(events.ActionExecuted, walkExecuted("p_undo", nil, true, false))
	ran, executed := setupWalkRan(evs)
	if executed != 2 {
		t.Fatalf("executed = %d, want 2", executed)
	}
	if len(ran) != 1 || ran[0].PlanIndex != 0 {
		t.Fatalf("ran = %+v, want only plan step 0 left: the undo matches the newest step", ran)
	}
}

// TestSetupRollbackRefusesWhileOneIsOpen: a second click while the first
// rollback proposal is still open is refused and creates nothing.
func TestSetupRollbackRefusesWhileOneIsOpen(t *testing.T) {
	r := newTierRig(t, tierOpts{local: true})
	ctx := context.Background()
	roomID := rollbackSetup(t, r, setupRollbackLocalTarget, plan.ClassLocal)
	ev, err := r.svc.CreateSetupRollback(ctx, r.user("alice"), roomID)
	if err != nil {
		t.Fatalf("CreateSetupRollback: %v", err)
	}
	first := rollbackProposal(t, ev)
	before := proposalCount(t, r.svc, roomID)
	_, err = r.svc.CreateSetupRollback(ctx, r.user("alice"), roomID)
	if err == nil || !strings.Contains(err.Error(), "already open") || !strings.Contains(err.Error(), first.ProposalID) {
		t.Fatalf("err = %v, want the open rollback %s named", err, first.ProposalID)
	}
	if got := proposalCount(t, r.svc, roomID); got != before {
		t.Fatalf("the second click created %d proposal(s)", got-before)
	}
}

// TestSetupRollbackSupersededPlanKeepsRanSteps: a superseding plan keeps the
// steps the plan it replaced already ran (the walk starts at the room's first
// setup.plan, not the current one).
func TestSetupRollbackSupersededPlanKeepsRanSteps(t *testing.T) {
	r := newTierRig(t, tierOpts{local: true})
	ctx := context.Background()
	target := setupRollbackLocalTarget
	roomID := r.openSetup("shipments-setup", "a", "alice", target, "")
	r.classify(roomID, target, plan.ClassLocal)
	r.plan(roomID, target, "compose_up", "connect_create_connector")
	r.acts["compose_up"].setRollback(`compose_down {"file": "compose.yaml"}`)
	r.acts["connect_create_connector"].setRollback(`connect_delete_connector {"connector": "shipments-src"}`)
	runStep(t, r, roomID, "compose_up", events.LayerEnvironment, fmt.Sprintf(`{"target":%q}`, target), "alice")
	waitPlanSteps(t, r.svc, roomID, 1)

	// A replacement plan after a failed step: the compose_up it did not list
	// again still ran and is still the setup's to undo.
	if err := r.svc.EmitSetup(ctx, roomID, events.SetupPlan, events.SetupPlanPayload{
		PlanID: "pl_2", Supersedes: "pl_1",
		Phases: []events.SetupPlanPhase{{Name: "connector", Steps: []events.SetupPlanStep{{Index: 0, Op: "connect_create_connector", Target: target}}}},
	}); err != nil {
		t.Fatalf("emit superseding plan: %v", err)
	}
	runStep(t, r, roomID, "connect_create_connector", "kafka-connect",
		fmt.Sprintf(`{"target":%q,"connector":"shipments-src"}`, target), "bob")

	var p events.ProposalCreatedPayload
	waitForCond(t, 5*time.Second, func() bool {
		ev, err := r.svc.CreateSetupRollback(ctx, r.user("alice"), roomID)
		if err != nil {
			return false
		}
		p = rollbackProposal(t, ev)
		return true
	}, "the rollback was never derived")
	got := make([]string, len(p.Steps))
	for i, s := range p.Steps {
		got[i] = s.Action
	}
	if strings.Join(got, ",") != "connect_delete_connector,compose_down" {
		t.Fatalf("rollback steps = %v, want connect_delete_connector, compose_down: the superseded plan's step still ran", got)
	}
}

// TestSetupUndoneIndexCanonicalParams: an executed undo matches its recorded
// rollback by canonical JSON, so a store that re-serialises params (key order,
// spacing) still marks the step undone.
func TestSetupUndoneIndexCanonicalParams(t *testing.T) {
	ran := []setupRanStep{{PlanIndex: 0, Action: "source_create_publication", Rollback: `source_drop_publication {"publication": "x", "schema": "public"}`}}
	if i := setupUndoneIndex(ran, walkStep("source_drop_publication", `{ "schema":"public","publication":"x" }`)); i != 0 {
		t.Fatalf("index = %d, want 0: the same params in another key order are the same undo", i)
	}
	if i := setupUndoneIndex(ran, walkStep("source_drop_publication", `{"publication":"y","schema":"public"}`)); i != -1 {
		t.Fatalf("index = %d, want -1 for other params", i)
	}
}

// TestSetupWalkKeepsStepWithoutProposal: a plan step whose proposal is not in
// the log slice stays in ran with no action, so the derivation refuses it by
// name instead of dropping it.
func TestSetupWalkKeepsStepWithoutProposal(t *testing.T) {
	evs := []events.Event{walkEvent(t, 1, events.ActionExecuted, walkExecuted("p_missing", intp(0), true, false))}
	ran, executed := setupWalkRan(evs)
	if executed != 1 || len(ran) != 1 || ran[0].Action != "" {
		t.Fatalf("ran = %+v, executed = %d; want the step kept with no action", ran, executed)
	}
}

// TestSetupRoomNotClosedWhenNothingRan: a proposal that executes in a setup
// room whose plan ran no step does not close the room "rolled back".
func TestSetupRoomNotClosedWhenNothingRan(t *testing.T) {
	r := newTierRig(t, tierOpts{local: true})
	ctx := context.Background()
	target := setupRollbackLocalTarget
	roomID := r.openSetup("idle-setup", "a", "alice", target, "")
	r.classify(roomID, target, plan.ClassLocal)
	r.plan(roomID, target, "connect_create_connector")
	ev, err := r.svc.CreateDriverProposal(ctx, r.user("alice"), roomID, events.ProposalCreatedPayload{
		Title: "restart the stack", Risk: "low", Rollback: "compose_down",
		Steps: []events.ProposalStep{walkStep("compose_up", fmt.Sprintf(`{"target":%q}`, target))},
	})
	if err != nil {
		t.Fatalf("CreateDriverProposal: %v", err)
	}
	p := rollbackProposal(t, ev)
	if err := r.approve(roomID, p.ProposalID, events.LayerEnvironment, "alice"); err != nil {
		t.Fatalf("approve: %v", err)
	}
	waitPayload[events.ActionExecutedPayload](t, r.svc, roomID, events.ActionExecuted, p.ProposalID)
	time.Sleep(100 * time.Millisecond)
	if snap := mustRoom(t, r.svc, roomID); snap.Status != "open" || snap.Resolution != "" {
		t.Fatalf("room = %s/%q, want open: no plan step ran, so nothing was rolled back", snap.Status, snap.Resolution)
	}
}
