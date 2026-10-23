package room

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"piperoom/internal/actions"
	"piperoom/internal/events"
	"piperoom/internal/setup/plan"
)

// "Roll back this setup" (U1-14b): one proposal built from the rollbacks the
// room recorded for the steps the current plan executed, in reverse order. The
// fake runner (tierAction) records its undo in Preview, exactly as a setup
// action adapter takes it from the runner's preview op; every step then runs
// through the room's ordinary proposal, approval and executor machinery.
//
// These tests cover the "Done when" of the row: a three-step setup rolled back
// newest first on a fake runner, the guard refusing a rollback step at Create
// (nothing created) and at Execute (nothing run), the compose_up rollback as a
// compose_down that keeps its volumes, and the room closing "rolled back".

// setupRollbackTarget is the shared endpoint the three-step setups use: a
// shared target routes their T2 steps to the tenant admin (design §3.6).
const setupRollbackTarget = "db.acme:5432"

// setupRollbackLocalTarget is a loopback endpoint the test classifies local, so
// its T1 steps are the driver's fallback.
const setupRollbackLocalTarget = "127.0.0.1:5432"

// rollbackSetup runs the three steps of the U1-14b setup — compose_up,
// connect_create_connector, source_create_publication — with a recorded
// rollback for each, and returns the room id.
func rollbackSetup(t *testing.T, r *tierRig, target, class string) string {
	t.Helper()
	roomID := r.openSetup("shipments-setup", "a", "alice", target, "")
	r.classify(roomID, target, class)
	r.plan(roomID, target, "compose_up", "connect_create_connector", "source_create_publication")
	r.acts["compose_up"].setRollback(`compose_down {"file": "compose.yaml", "services": ["kafka"]}`)
	r.acts["connect_create_connector"].setRollback(`connect_delete_connector {"connector": "shipments-src"}`)
	r.acts["source_create_publication"].setRollback(`source_drop_publication {"publication": "dbz_shipments"}`)
	runStep(t, r, roomID, "compose_up", events.LayerEnvironment,
		fmt.Sprintf(`{"target":%q}`, target), stepApprover(class))
	waitPlanSteps(t, r.svc, roomID, 1)
	runStep(t, r, roomID, "connect_create_connector", "kafka-connect",
		fmt.Sprintf(`{"target":%q,"connector":"shipments-src"}`, target), "bob")
	waitPlanSteps(t, r.svc, roomID, 2)
	runStep(t, r, roomID, "source_create_publication", "postgres",
		fmt.Sprintf(`{"target":%q,"publication":"dbz_shipments"}`, target), "bob")
	waitPlanSteps(t, r.svc, roomID, 3)
	return roomID
}

// stepApprover is who approves a step of the setup: the driver where the step
// routes T1 (its fallback layers), the tenant admin where it routes T2 or T3.
func stepApprover(class string) string {
	if class == plan.ClassLocal {
		return "alice"
	}
	return "bob"
}

// runStep proposes op as the agent, approves it as who and waits until the
// action ran.
func runStep(t *testing.T, r *tierRig, roomID, op, layer, params, who string) string {
	t.Helper()
	pid := r.proposeOK(roomID, op, layer, params)
	if err := r.approve(roomID, pid, layer, who); err != nil {
		t.Fatalf("approve %s: %v", op, err)
	}
	waitForCond(t, 5*time.Second, func() bool { return r.acts[op].count() >= 1 }, op+" never ran")
	return pid
}

// waitPlanSteps waits until the room's snapshot says n plan steps executed, so
// the next proposal resolves the next plan step.
func waitPlanSteps(t *testing.T, svc *Service, roomID string, n int) {
	t.Helper()
	waitForCond(t, 5*time.Second, func() bool {
		snap := mustRoom(t, svc, roomID)
		return snap.Setup != nil && len(snap.Setup.executed) == n
	}, fmt.Sprintf("the room never recorded %d executed plan steps", n))
}

// RollbackProposal is CreateSetupRollback's answer, decoded.
func rollbackProposal(t *testing.T, ev events.Event) events.ProposalCreatedPayload {
	t.Helper()
	if ev.Type != events.ProposalCreated {
		t.Fatalf("event type = %s, want %s", ev.Type, events.ProposalCreated)
	}
	var p events.ProposalCreatedPayload
	if err := json.Unmarshal(ev.Payload, &p); err != nil {
		t.Fatalf("decode proposal.created: %v", err)
	}
	return p
}

// proposalCount is how many proposals the room's log holds.
func proposalCount(t *testing.T, svc *Service, roomID string) int {
	t.Helper()
	evs, _, err := svc.Events(context.Background(), roomID, 0, 1000)
	if err != nil {
		t.Fatalf("events: %v", err)
	}
	n := 0
	for _, e := range evs {
		if e.Type == events.ProposalCreated {
			n++
		}
	}
	return n
}

// approveRollback approves every layer of a rollback proposal as bob (the
// tenant admin), which is who a T2 rollback of a team server needs.
func approveRollback(t *testing.T, r *tierRig, roomID, pid string, layers ...string) {
	t.Helper()
	for _, layer := range layers {
		if err := r.approve(roomID, pid, layer, "bob"); err != nil {
			t.Fatalf("approve rollback layer %s: %v", layer, err)
		}
	}
}

// TestSetupRollbackThreeSteps is the row's fake-runner test: a three-step
// setup rolled back newest first, each step on its original target, the
// compose_up undone by a compose_down that keeps its volumes, and the room
// closed "rolled back".
func TestSetupRollbackThreeSteps(t *testing.T) {
	// SetupLocal lets the room register its own loopback endpoint, which the
	// env_detect classifier (U1-8) then makes local.
	r := newTierRig(t, tierOpts{local: true})
	ctx := context.Background()
	target := setupRollbackLocalTarget
	roomID := rollbackSetup(t, r, target, plan.ClassLocal)

	ev, err := r.svc.CreateSetupRollback(ctx, r.user("alice"), roomID)
	if err != nil {
		t.Fatalf("CreateSetupRollback: %v", err)
	}
	p := rollbackProposal(t, ev)
	if p.Origin != events.ProposalOriginDriver {
		t.Fatalf("origin = %q, want driver: a person clicked it", p.Origin)
	}
	want := []string{"source_drop_publication", "connect_delete_connector", "compose_down"}
	if len(p.Steps) != len(want) {
		t.Fatalf("steps = %d, want %d: %+v", len(p.Steps), len(want), p.Steps)
	}
	for i := range want {
		if p.Steps[i].Action != want[i] {
			t.Fatalf("step %d = %q, want %q (newest first)", i, p.Steps[i].Action, want[i])
		}
	}

	// Every rollback step carries exactly what its record says: the recorded
	// rollbacks name only the object, and the room adds no endpoint of its
	// own (the steps' params decode strictly). With no endpoint named, the
	// step routes at the room's highest class (routeSetupSteps).
	for i, step := range p.Steps {
		var params map[string]any
		if err := json.Unmarshal(step.Params, &params); err != nil {
			t.Fatalf("step %d params: %v", i, err)
		}
		if _, ok := params["target"]; ok {
			t.Fatalf("step %d (%s) params = %v, want no target the record did not name", i, step.Action, params)
		}
	}

	// The rollback of a compose_up is a compose_down without volumes: the
	// volumes-kept mark is the guard's exception, and nothing in the params
	// can ask compose to remove them.
	down := p.Steps[len(p.Steps)-1]
	var params map[string]any
	if err := json.Unmarshal(down.Params, &params); err != nil {
		t.Fatalf("compose_down params: %v", err)
	}
	if params["volumes_kept"] != true {
		t.Fatalf("compose_down params = %v, want volumes_kept true", params)
	}
	for _, key := range []string{"args", "argv", "volumes", "remove_volumes"} {
		if _, ok := params[key]; ok {
			t.Fatalf("compose_down params = %v, want no %q: the undo never removes volumes", params, key)
		}
	}

	// On a team server a T2 rollback is not the driver's to approve alone: the
	// driver's own write needs a non-self approver (design §3.6).
	if err := r.approve(roomID, p.ProposalID, "kafka-connect", "alice"); err == nil {
		t.Fatal("the driver approved a T2 rollback alone")
	} else {
		var re *Error
		if !errors.As(err, &re) || re.Code != "not_admin" {
			t.Fatalf("err = %v, want 403 not_admin", err)
		}
	}

	approveRollback(t, r, roomID, p.ProposalID, "environment", "kafka-connect", "postgres")
	for _, op := range want {
		op := op
		waitForCond(t, 10*time.Second, func() bool { return r.acts[op].count() == 1 }, op+" never ran as a rollback")
	}
	waitForCond(t, 10*time.Second, func() bool { return mustRoom(t, r.svc, roomID).Status == "closed" },
		"the room never closed after the rollback")
	closed := mustRoom(t, r.svc, roomID)
	if closed.Resolution != setupRolledBackResolution {
		t.Fatalf("resolution = %q, want %q", closed.Resolution, setupRolledBackResolution)
	}
	if closed.Setup == nil || !closed.Setup.Mode {
		t.Fatalf("the closed room is no longer a setup room: %+v", closed.Setup)
	}
}

// TestSetupRollbackGuardRefusesAtCreate: a rollback step the guard forbids on
// a production target stops the proposal at Create with the guard row's
// reason, and nothing is created.
func TestSetupRollbackGuardRefusesAtCreate(t *testing.T) {
	r := newTierRig(t, tierOpts{})
	ctx := context.Background()
	roomID := r.openSetup("customers-setup", "a", "alice", "db.prod.acme:5432", plan.ClassProduction)
	r.plan(roomID, "db.prod.acme:5432", "connect_create_connector")
	// Deleting the connector it created is the only undo, and that is what the
	// guard refuses outright on a production target.
	r.acts["connect_create_connector"].setRollback(`connect_delete_connector {"connector": "customers-src"}`)
	runStep(t, r, roomID, "connect_create_connector", "kafka-connect",
		`{"target":"db.prod.acme:5432","connector":"customers-src"}`, "bob")
	waitPlanSteps(t, r.svc, roomID, 1)

	before := proposalCount(t, r.svc, roomID)
	_, err := r.svc.CreateSetupRollback(ctx, r.user("alice"), roomID)
	if !errors.Is(err, plan.ErrGuard) {
		t.Fatalf("err = %v, want plan.ErrGuard", err)
	}
	if !strings.Contains(err.Error(), plan.ReasonDeleteConnector) {
		t.Fatalf("err = %q, want the guard's reason %q", err, plan.ReasonDeleteConnector)
	}
	var re *Error
	if !errors.As(err, &re) || re.Status != 400 || re.Code != codeValidation {
		t.Fatalf("error = %#v, want 400 validation", err)
	}
	if got := proposalCount(t, r.svc, roomID); got != before {
		t.Fatalf("the refused rollback created %d proposal(s)", got-before)
	}
	if got := r.acts["connect_delete_connector"].count(); got != 0 {
		t.Fatalf("the refused rollback ran %d step(s)", got)
	}
	if snap := mustRoom(t, r.svc, roomID); snap.Status != "open" {
		t.Fatalf("status = %q, want the room still open", snap.Status)
	}
}

// TestSetupRollbackGuardRefusesAtExecute: a target whose class rises to
// production between Create and Execute makes the executor refuse the rollback
// step, and it never runs.
func TestSetupRollbackGuardRefusesAtExecute(t *testing.T) {
	r := newTierRig(t, tierOpts{})
	ctx := context.Background()
	roomID := r.openSetup("customers-setup", "a", "alice", "db.acme:5432", plan.ClassShared)
	r.plan(roomID, "db.acme:5432", "connect_create_connector")
	r.acts["connect_create_connector"].setRollback(`connect_delete_connector {"connector": "customers-src"}`)
	runStep(t, r, roomID, "connect_create_connector", "kafka-connect",
		`{"target":"db.acme:5432","connector":"customers-src"}`, "bob")
	waitPlanSteps(t, r.svc, roomID, 1)

	ev, err := r.svc.CreateSetupRollback(ctx, r.user("alice"), roomID)
	if err != nil {
		t.Fatalf("CreateSetupRollback: %v", err)
	}
	p := rollbackProposal(t, ev)
	// The class rises after the proposal exists: the executor's re-check is
	// what refuses the step (setup_route.go, design §3.5).
	if _, err := r.svc.RaiseSetupTargetClass(ctx, r.user("alice"), roomID, "db.acme:5432", plan.ClassProduction); err != nil {
		t.Fatalf("raise class: %v", err)
	}
	if err := r.approve(roomID, p.ProposalID, "kafka-connect", "bob"); err != nil {
		t.Fatalf("approve: %v", err)
	}
	executed := waitPayload[events.ActionExecutedPayload](t, r.svc, roomID, events.ActionExecuted, p.ProposalID)
	if executed.OK {
		t.Fatalf("the rollback ran on a production target: %+v", executed)
	}
	if !strings.Contains(executed.Output, "guard table forbids") ||
		!strings.Contains(executed.Output, plan.ReasonDeleteConnector) {
		t.Fatalf("output = %q, want the guard refusal with %q", executed.Output, plan.ReasonDeleteConnector)
	}
	if got := r.acts["connect_delete_connector"].count(); got != 0 {
		t.Fatalf("the refused rollback ran %d step(s)", got)
	}
	if snap := mustRoom(t, r.svc, roomID); snap.Status != "open" {
		t.Fatalf("status = %q, want the room still open: the setup was not rolled back", snap.Status)
	}
}

// TestSetupRollbackNeedsRecordedRollback: a step whose undo the room did not
// record stops the rollback proposal with that step named, never a silent
// skip.
func TestSetupRollbackNeedsRecordedRollback(t *testing.T) {
	r := newTierRig(t, tierOpts{})
	ctx := context.Background()
	target := setupRollbackTarget
	roomID := r.openSetup("shipments-setup", "a", "alice", target, plan.ClassShared)
	r.plan(roomID, target, "compose_up", "connect_create_connector")
	// compose_up's undo is recorded; the connector's is not (the room cannot
	// delete a connector on its own).
	r.acts["compose_up"].setRollback(`compose_down {"file": "compose.yaml"}`)
	runStep(t, r, roomID, "compose_up", events.LayerEnvironment, fmt.Sprintf(`{"target":%q}`, target), "bob")
	waitPlanSteps(t, r.svc, roomID, 1)
	runStep(t, r, roomID, "connect_create_connector", "kafka-connect",
		fmt.Sprintf(`{"target":%q,"connector":"shipments-src"}`, target), "bob")
	waitPlanSteps(t, r.svc, roomID, 2)

	before := proposalCount(t, r.svc, roomID)
	_, err := r.svc.CreateSetupRollback(ctx, r.user("alice"), roomID)
	if err == nil {
		t.Fatal("a step with no recorded rollback did not stop the proposal")
	}
	if !strings.Contains(err.Error(), "no rollback") || !strings.Contains(err.Error(), "connect_create_connector") {
		t.Fatalf("err = %q, want the step named with its missing rollback", err)
	}
	if got := proposalCount(t, r.svc, roomID); got != before {
		t.Fatalf("the refused rollback created %d proposal(s)", got-before)
	}
	// The step with a recorded rollback is not run either: the derivation
	// refuses before any step exists.
	if got := r.acts["compose_down"].count(); got != 0 {
		t.Fatalf("a refused rollback ran compose_down %d time(s)", got)
	}
}

// TestSetupRollbackPartialThenRetry: a failed rollback step leaves the room
// open with the steps it did not undo, and a second click rolls back only what
// is left.
func TestSetupRollbackPartialThenRetry(t *testing.T) {
	r := newTierRig(t, tierOpts{})
	ctx := context.Background()
	target := setupRollbackTarget
	roomID := rollbackSetup(t, r, target, plan.ClassShared)

	r.acts["connect_delete_connector"].setExecErr(errors.New("connect refused the delete"))
	ev, err := r.svc.CreateSetupRollback(ctx, r.user("alice"), roomID)
	if err != nil {
		t.Fatalf("CreateSetupRollback: %v", err)
	}
	first := rollbackProposal(t, ev)
	if len(first.Steps) != 3 {
		t.Fatalf("steps = %d, want 3", len(first.Steps))
	}
	approveRollback(t, r, roomID, first.ProposalID, "environment", "kafka-connect", "postgres")
	failed := waitPayload[events.ActionExecutedPayload](t, r.svc, roomID, events.ActionExecuted, first.ProposalID)
	_ = failed // the first action.executed is the one that succeeded; wait for the stop below

	waitForCond(t, 10*time.Second, func() bool {
		return r.acts["source_drop_publication"].count() == 1 && r.acts["connect_delete_connector"].count() == 1
	}, "the rollback never reached the failing step")
	if got := r.acts["compose_down"].count(); got != 0 {
		t.Fatalf("compose_down ran %d time(s) after an earlier step failed", got)
	}
	// The failed rollback leaves the room open: the setup is only partly back.
	waitForCond(t, 5*time.Second, func() bool {
		snap := mustRoom(t, r.svc, roomID)
		_, open := openSetupRollback(snap)
		return !open
	}, "the failed rollback proposal is still open")
	if snap := mustRoom(t, r.svc, roomID); snap.Status != "open" {
		t.Fatalf("status = %q, want open after a failed rollback step", snap.Status)
	}

	// The retry covers what the log still says ran: the two steps the failed
	// rollback did not undo, newest first.
	r.acts["connect_delete_connector"].setExecErr(nil)
	ev, err = r.svc.CreateSetupRollback(ctx, r.user("alice"), roomID)
	if err != nil {
		t.Fatalf("retry CreateSetupRollback: %v", err)
	}
	retry := rollbackProposal(t, ev)
	if len(retry.Steps) != 2 || retry.Steps[0].Action != "connect_delete_connector" || retry.Steps[1].Action != "compose_down" {
		t.Fatalf("retry steps = %+v, want connect_delete_connector, compose_down", retry.Steps)
	}
	approveRollback(t, r, roomID, retry.ProposalID, "kafka-connect", "environment")
	waitForCond(t, 10*time.Second, func() bool { return mustRoom(t, r.svc, roomID).Status == "closed" },
		"the room never closed after the retry")
	if got := mustRoom(t, r.svc, roomID).Resolution; got != setupRolledBackResolution {
		t.Fatalf("resolution = %q, want %q", got, setupRolledBackResolution)
	}
	if got := r.acts["source_drop_publication"].count(); got != 1 {
		t.Fatalf("the retry ran the undo of a step that was already undone (%d times)", got)
	}
}

// TestSetupRollbackRecord is the parse of a recorded rollback: "<action> {json
// params}" or a refusal naming why the step cannot be undone by an action.
func TestSetupRollbackRecord(t *testing.T) {
	cases := []struct {
		name        string
		record      string
		orig        string
		wantOp      string
		wantParams  map[string]any
		wantTarget  string
		wantErrText string
	}{
		{
			// The undo carries what the record says and nothing the room
			// guessed: a target taken from the original step or the plan could
			// point the undo at a different endpoint than the one the record
			// names, and the steps' params decode strictly.
			name:       "action and params",
			record:     `connect_delete_connector {"connector": "orders-src"}`,
			orig:       `{"target": "db.acme:5432", "connector": "orders-src"}`,
			wantOp:     "connect_delete_connector",
			wantParams: map[string]any{"connector": "orders-src"},
		},
		{
			name:       "the record's own endpoint stays",
			record:     `connect_delete_connector {"connector": "orders-src", "target": "db.prod.acme:5432"}`,
			orig:       `{"target": "db.acme:5432"}`,
			wantOp:     "connect_delete_connector",
			wantParams: map[string]any{"connector": "orders-src"},
			wantTarget: "db.prod.acme:5432",
		},
		{
			name:       "compose_down keeps its volumes",
			record:     `compose_down {"file": "compose.yaml", "services": ["kafka"], "volumes": true, "args": ["-v"]}`,
			orig:       `{"target": "127.0.0.1:5432"}`,
			wantOp:     "compose_down",
			wantParams: map[string]any{"file": "compose.yaml", "volumes_kept": true},
		},
		{name: "a sentence", record: `Undo it by hand, there is no action`, wantErrText: "not an action and params"},
		{name: "no params", record: `compose_down`, wantErrText: "carries no params object"},
		{name: "params are not JSON", record: `compose_down {"file":`, wantErrText: "not a JSON object"},
		{name: "params are a list", record: `compose_down ["-v"]`, wantErrText: "carries no params object"},
		{name: "a name that is not an action", record: `Compose_Down {}`, wantErrText: "not an action and params"},
		{name: "empty", record: ``, wantErrText: "no rollback"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ran := setupRanStep{PlanIndex: 0, Action: "step", Params: json.RawMessage(c.orig), Rollback: c.record}
			op, params, err := setupRollbackOf(ran)
			if c.wantErrText != "" {
				if err == nil || !strings.Contains(err.Error(), c.wantErrText) {
					t.Fatalf("err = %v, want %q", err, c.wantErrText)
				}
				return
			}
			if err != nil {
				t.Fatalf("err = %v", err)
			}
			if op != c.wantOp {
				t.Fatalf("op = %q, want %q", op, c.wantOp)
			}
			var got map[string]any
			if err := json.Unmarshal(params, &got); err != nil {
				t.Fatalf("params: %v", err)
			}
			for key, want := range c.wantParams {
				if got[key] != want {
					t.Fatalf("params = %v, want %s=%v", got, key, want)
				}
			}
			if c.wantTarget == "" {
				if _, ok := got["target"]; ok {
					t.Fatalf("params = %v, want no target the record did not carry", got)
				}
			} else if got["target"] != c.wantTarget {
				t.Fatalf("params = %v, want target %q", got, c.wantTarget)
			}
			for _, key := range []string{"args", "volumes", "remove_volumes"} {
				if _, ok := got[key]; ok {
					t.Fatalf("params = %v, want no %q", got, key)
				}
			}
		})
	}
}

// TestSetupRouteComposeDownVolumesKept: the room's volumes-kept mark is the
// guard's exception for a compose_down, so the rollback of a compose setup on
// a production target is allowed while a bare compose_down stays refused
// (TestSetupRouteTiers).
func TestSetupRouteComposeDownVolumesKept(t *testing.T) {
	reg := actions.NewRegistry()
	if err := reg.Register(&tierAction{name: plan.OpComposeDown, layer: events.LayerEnvironment, floor: plan.TierT1}); err != nil {
		t.Fatal(err)
	}
	snap := &RoomSnapshot{Setup: &SetupSnapshot{Mode: true, Targets: []SetupTargetState{
		{Kind: "compose", Address: "db.prod.acme:5432", Class: plan.ClassProduction},
	}}}
	step := []events.ProposalStep{{
		Action: plan.OpComposeDown, Layer: events.LayerEnvironment,
		Params: json.RawMessage(`{"target":"db.prod.acme:5432","volumes_kept":true}`),
	}}
	route, err := resolveSetupRoute(snap, reg, step, false, 0, time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("a volumes-keeping compose_down was refused: %v", err)
	}
	if route.Class != plan.ClassProduction || route.Tier != plan.TierT3 {
		t.Fatalf("route = %s/%s, want production/T3", route.Class, route.Tier)
	}
}

// TestSetupKeepsVolumesMark: the volumes-kept mark is a keep signal for the
// guard, and a removal signal beside it still wins.
func TestSetupKeepsVolumesMark(t *testing.T) {
	cases := []struct {
		params string
		want   bool
	}{
		{`{"volumes_kept": true}`, true},
		{`{"volumes_kept": false}`, false},
		{`{"volumes_kept": true, "volumes": true}`, false},
		{`{"volumes_kept": true, "remove_volumes": true}`, false},
		{`{"volumes_kept": true, "args": ["-v"]}`, false},
		{`{"volumes_kept": true, "args": ["--volumes"]}`, false},
		{`{"volumes_kept": true, "args": ["down"]}`, true},
		{`{"target": "db.prod.acme:5432"}`, false},
		{`{"volumes": false}`, true},
		// A removal signal in another letter case is the one the executor's
		// decoder reads: it still wins over a keep flag.
		{`{"remove_volumes": false, "Volumes": true}`, false},
		{`{"VOLUMES_KEPT": true, "ARGS": ["-v"]}`, false},
	}
	for _, c := range cases {
		obj, ok := setupParamsObject(json.RawMessage(c.params))
		if !ok {
			t.Fatalf("setupParamsObject(%s) refused", c.params)
		}
		if got := setupKeepsVolumes(obj); got != c.want {
			t.Fatalf("setupKeepsVolumes(%s) = %v, want %v", c.params, got, c.want)
		}
	}
}
