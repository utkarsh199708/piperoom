package room

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"piperoom/internal/actions"
	"piperoom/internal/agent"
	"piperoom/internal/config"
	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/setup/plan"
	"piperoom/internal/setup/protocol"
	"piperoom/internal/store/memstore"
)

// Tiered routing for setup rooms (U1-6): the tier table, the guard at Create
// and at Execute, the maintenance window, the plan membership, and the
// approver rules of design §3.6, all on a fake clock.

const tierUsersYAML = `users:
  - {id: alice, name: Alice}
  - {id: bob, name: Bob}
  - {id: carol, name: Carol}
  - {id: dave, name: Dave}
`

// tierOwnersYAML gives tenant a three members and one admin (bob), tenant b
// one member (dave, solo). "existing" overlaps a setup of topic prefix
// "orders"; "unrelated" carries a top-level environment owner (carol), the
// owner discover writes, and overlaps nothing.
func tierOwnersYAML() string {
	return `tenants:
  a: {members: [user:alice, user:bob, user:carol], admins: [user:bob]}
  b: {members: [user:dave]}
pipelines:
  - id: existing
    tenant: a
    topic_prefix: orders
    source_tables: [public.orders]
    connectors: [orders-src]
    connections: {source_db_url: {env: EX_SRC}, warehouse_db_url: {env: EX_WH}, kafka_brokers: {env: EX_K}, connect_url: {env: EX_C}, schema_registry_url: {env: EX_SR}}
    owners:
      kafka-connect: {approvers: [user:carol]}
      environment: {approvers: [user:carol]}
  - id: unrelated
    tenant: a
    topic_prefix: billing
    source_tables: [public.billing]
    connectors: [billing-src]
    connections: {source_db_url: {env: UN_SRC}, warehouse_db_url: {env: UN_WH}, kafka_brokers: {env: UN_K}, connect_url: {env: UN_C}, schema_registry_url: {env: UN_SR}}
    owners:
      environment: {approvers: [user:carol]}
`
}

// tierAction is a fake setup action: Tier is the op's floor raised to the
// class (what a setup adapter does), Preview always passes, and Execute
// records what the executor put in ctx, including the protocol.OpRequest the
// adapter would build from it.
type tierAction struct {
	name  string
	layer string
	floor string

	mu       sync.Mutex
	runs     int
	lastStep SetupStep
	hasStep  bool
	lastReq  *protocol.OpRequest
	// lastLeft is how long the last Execute's context had until its deadline.
	lastLeft time.Duration
	release  chan struct{} // when set, Execute waits for it
	execErr  error
	// rollback is the record this fake's Preview reports as the step's undo,
	// "<action> {json params}" (a setup action adapter takes it from the
	// runner's preview op). Empty: nothing recorded, as for a step nothing can
	// undo. Set before a proposal exists (setRollback).
	rollback string
}

func (a *tierAction) Name() string                 { return a.name }
func (a *tierAction) Layer() string                { return a.layer }
func (a *tierAction) Describe() string             { return a.name }
func (a *tierAction) ParamsSchema() map[string]any { return map[string]any{"type": "object"} }
func (a *tierAction) Validate(context.Context, json.RawMessage) error {
	return nil
}
func (a *tierAction) CheckScope(owners.Pipeline, json.RawMessage) error { return nil }
func (a *tierAction) Preview(context.Context, json.RawMessage) (actions.PreviewResult, error) {
	a.mu.Lock()
	record := a.rollback
	a.mu.Unlock()
	res := actions.PreviewResult{Target: a.name, Current: "absent", Expected: "present"}
	if record != "" {
		// Reversible is what says the rollback is an action and params, as
		// the real previews mark it (actions.Preview).
		res.Reversible = true
		res.Rollback = record
	}
	return res, nil
}
func (a *tierAction) Tier(_ json.RawMessage, class string) (string, error) {
	return plan.TierOf(a.floor, class)
}
func (a *tierAction) Execute(ctx context.Context, _ json.RawMessage) (string, error) {
	a.mu.Lock()
	a.runs++
	if dl, ok := ctx.Deadline(); ok {
		a.lastLeft = time.Until(dl)
	}
	if st, ok := SetupStepFrom(ctx); ok {
		a.lastStep, a.hasStep = st, true
		// What a setup action adapter (U1-7) does with the context the room
		// gives it: every proposal field and the server's class and tier.
		step := st.StepIndex
		a.lastReq = &protocol.OpRequest{
			Op: a.name, Tier: st.Tier, Class: st.Class,
			ProposalID: st.ProposalID, StepIndex: &step, ParamsHash: st.ParamsHash,
		}
	}
	release, execErr := a.release, a.execErr
	a.mu.Unlock()
	if release != nil {
		select {
		case <-release:
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
	if execErr != nil {
		return "", execErr
	}
	return "done", nil
}

// setRollback records the undo this fake reports in its preview.
func (a *tierAction) setRollback(record string) {
	a.mu.Lock()
	a.rollback = record
	a.mu.Unlock()
}

// setExecErr makes the next Execute fail with err ("" clears it).
func (a *tierAction) setExecErr(err error) {
	a.mu.Lock()
	a.execErr = err
	a.mu.Unlock()
}

func (a *tierAction) count() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.runs
}

// plainAction is an action that is not actions.Tiered: T2 in a setup room.
type plainAction struct{ *tierAction }

func (*plainAction) Tier() {} // shadows the embedded method: not actions.Tiered

// tierOps are the actions every test registry holds. The guard table's ops
// all ride one layer here: the guard is checked before any layer decision.
var tierOps = []struct {
	name, layer, floor string
}{
	{"compose_write", events.LayerEnvironment, plan.TierT1},
	{"compose_up", events.LayerEnvironment, plan.TierT1},
	{"env_detect", events.LayerEnvironment, plan.TierT0},
	{"blocker", "kafka-connect", plan.TierT2},
	{"source_create_publication", "postgres", plan.TierT2},
	{"connect_create_connector", "kafka-connect", plan.TierT2},
	{"source_drop_table", "postgres", plan.TierT2},
	{"source_truncate_table", "postgres", plan.TierT2},
	{"source_drop_slot", "postgres", plan.TierT2},
	{"source_drop_publication", "postgres", plan.TierT2},
	{"source_alter_system", "postgres", plan.TierT3},
	{"connect_delete_connector", "kafka-connect", plan.TierT2},
	{"kafka_delete_topic", "kafka", plan.TierT2},
	{"compose_down", events.LayerEnvironment, plan.TierT1},
	{"k8s_delete_namespace", events.LayerEnvironment, plan.TierT2},
	{"msk_delete_connector", events.LayerEnvironment, plan.TierT2},
	{"file_write", events.LayerEnvironment, plan.TierT1},
	{"file_delete", events.LayerEnvironment, plan.TierT1},
}

type tierOpts struct {
	local bool
	delay time.Duration
	own   string
}

type tierRig struct {
	t     *testing.T
	svc   *Service
	clock *testClock
	acts  map[string]*tierAction
	ticks chan time.Time
	done  chan struct{}
}

func newTierRig(t *testing.T, o tierOpts) *tierRig {
	t.Helper()
	users, err := ParseUsers([]byte(tierUsersYAML))
	if err != nil {
		t.Fatal(err)
	}
	ownYAML := o.own
	if ownYAML == "" {
		ownYAML = tierOwnersYAML()
	}
	path := filepath.Join(t.TempDir(), "owners.yaml")
	if err := os.WriteFile(path, []byte(ownYAML), 0o600); err != nil {
		t.Fatal(err)
	}
	own, err := owners.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	reg := actions.NewRegistry()
	acts := map[string]*tierAction{}
	for _, op := range tierOps {
		a := &tierAction{name: op.name, layer: op.layer, floor: op.floor}
		acts[op.name] = a
		if err := reg.Register(a); err != nil {
			t.Fatal(err)
		}
	}
	plain := &plainAction{&tierAction{name: "plain_op", layer: events.LayerEnvironment, floor: plan.TierT1}}
	if err := reg.Register(plain); err != nil {
		t.Fatal(err)
	}
	acts["plain_op"] = plain.tierAction

	r := &tierRig{
		t: t, clock: &testClock{t: time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC)},
		acts: acts, ticks: make(chan time.Time), done: make(chan struct{}),
	}
	r.svc = New(Deps{
		Store: memstore.New(), Actions: reg, Users: users, Owners: own, Run: idleRun, Now: r.clock.Now,
		Setup:      config.Setup{Enabled: true, TokenBudget: 2_000_000, LocalDelay: o.delay},
		SetupLocal: o.local,
		AutoApproveTicker: func(time.Duration) (<-chan time.Time, func()) {
			return r.ticks, func() {}
		},
	})
	r.svc.autoTickDone = func() { r.done <- struct{}{} }
	t.Cleanup(r.svc.Close)
	if err := r.svc.Load(context.Background()); err != nil {
		t.Fatal(err)
	}
	return r
}

// tick runs one check of the policy loop and returns when it finished.
func (r *tierRig) tick() {
	r.t.Helper()
	select {
	case r.ticks <- time.Time{}:
	case <-time.After(5 * time.Second):
		r.t.Fatal("policy loop is not running")
	}
	<-r.done
}

func (r *tierRig) user(id string) User {
	r.t.Helper()
	u, ok := r.svc.LookupUser(id)
	if !ok {
		r.t.Fatalf("no user %s", id)
	}
	return u
}

// openSetup opens a setup room for tenant and target, driven by who.
func (r *tierRig) openSetup(id, tenant, who, target, class string) string {
	r.t.Helper()
	req := events.SetupRequest{
		PipelineID: id, Tenant: tenant, Title: "Setup " + id,
		SourceKind: "postgres", Target: "compose",
		// "orders" overlaps the loaded pipeline "existing"; every other id
		// does not.
		TopicPrefix: strings.TrimSuffix(id, "-setup"),
		Tables:      []string{"public." + strings.ReplaceAll(strings.TrimSuffix(id, "-setup"), "-", "_")},
		Connectors:  []string{strings.TrimSuffix(id, "-setup") + "-src"},
		Targets:     []events.SetupTarget{{Kind: "postgres", Address: target, Class: class}},
	}
	res, err := r.svc.CreateSetupRoom(context.Background(), r.user(who), req)
	if err != nil {
		r.t.Fatalf("CreateSetupRoom: %v", err)
	}
	return res.Room.ID
}

// classify emits the server's class decision for a target, as the Hello and
// env_detect classifier (U1-8) does: only this can make a loopback target
// local, because the server's own derivation at creation is shared.
func (r *tierRig) classify(roomID, address, class string) {
	r.t.Helper()
	if err := r.svc.EmitSetup(context.Background(), roomID, events.SetupTargetClassed, events.SetupTargetClassedPayload{
		Target: address, Class: class, Reason: plan.ReasonLoopback, By: "server",
	}); err != nil {
		r.t.Fatalf("classify %s: %v", address, err)
	}
}

// plan emits a setup.plan holding one step per op, all on target.
func (r *tierRig) plan(roomID, target string, ops ...string) {
	r.t.Helper()
	payload := events.SetupPlanPayload{PlanID: "pl_1"}
	phase := events.SetupPlanPhase{Name: "stack"}
	for i, op := range ops {
		phase.Steps = append(phase.Steps, events.SetupPlanStep{Index: i, Op: op, Target: target})
	}
	payload.Phases = []events.SetupPlanPhase{phase}
	if err := r.svc.EmitSetup(context.Background(), roomID, events.SetupPlan, payload); err != nil {
		r.t.Fatalf("emit plan: %v", err)
	}
}

// propose creates an agent proposal and returns its id and the error.
func (r *tierRig) propose(roomID, action, layer, params string) (string, error) {
	r.t.Helper()
	return r.svc.CreateProposal(context.Background(), roomID, events.ProposalCreatedPayload{
		Title: "setup step", Risk: "low", Rollback: "undo it",
		Steps: []events.ProposalStep{{Action: action, Layer: layer, Params: json.RawMessage(params)}},
	}, events.AgentActor)
}

// proposeOK is propose that fails the test on a refusal.
func (r *tierRig) proposeOK(roomID, action, layer, params string) string {
	r.t.Helper()
	pid, err := r.propose(roomID, action, layer, params)
	if err != nil {
		r.t.Fatalf("CreateProposal(%s): %v", action, err)
	}
	return pid
}

// settle waits for the proposal's preview (and, when it qualifies, its
// countdown) to be folded.
func (r *tierRig) settle(roomID, pid string) Proposal {
	r.t.Helper()
	waitForCond(r.t, 5*time.Second, func() bool {
		p, ok := openProposal(mustRoom(r.t, r.svc, roomID), pid)
		return ok && p.Preview != nil
	}, "preview never folded")
	p, ok := openProposal(mustRoom(r.t, r.svc, roomID), pid)
	if !ok {
		r.t.Fatalf("proposal %s is not open", pid)
	}
	return p
}

// approve approves one layer as who at the proposal's current hash.
func (r *tierRig) approve(roomID, pid, layer, who string) error {
	r.t.Helper()
	p := r.settle(roomID, pid)
	_, err := r.svc.ApproveProposal(context.Background(), r.user(who), pid, layer, p.ParamsHash)
	return err
}

// waitPayload waits for one event type of a room whose payload has
// proposal_id pid, and returns it.
func waitPayload[T any](t *testing.T, svc *Service, roomID string, typ events.Type, pid string) T {
	t.Helper()
	var out T
	waitForCond(t, 5*time.Second, func() bool {
		evs, _, err := svc.Events(context.Background(), roomID, 0, 1000)
		if err != nil {
			return false
		}
		for _, e := range evs {
			if e.Type != typ {
				continue
			}
			var probe struct {
				ProposalID string `json:"proposal_id"`
			}
			if json.Unmarshal(e.Payload, &probe) != nil || probe.ProposalID != pid {
				continue
			}
			if json.Unmarshal(e.Payload, &out) == nil {
				return true
			}
		}
		return false
	}, "event "+string(typ)+" of "+pid+" never arrived")
	return out
}

// guardReason is the guard table's reason code for op.
func guardReason(op string) string {
	for _, row := range plan.GuardRows() {
		if row.Op == op {
			return row.Reason
		}
	}
	return ""
}

// TestSetupRouteTiers is the tier table: floor raised to class, T0 refused,
// an action that is not Tiered is T2, and the guard on a production target.
func TestSetupRouteTiers(t *testing.T) {
	reg := actions.NewRegistry()
	for _, op := range tierOps {
		if err := reg.Register(&tierAction{name: op.name, layer: op.layer, floor: op.floor}); err != nil {
			t.Fatal(err)
		}
	}
	if err := reg.Register(&plainAction{&tierAction{name: "plain_op", layer: events.LayerEnvironment, floor: plan.TierT1}}); err != nil {
		t.Fatal(err)
	}
	snap := &RoomSnapshot{
		PipelineID: "p",
		Setup: &SetupSnapshot{Mode: true, Targets: []SetupTargetState{
			{Kind: "postgres", Address: "127.0.0.1:5432", Class: plan.ClassLocal},
			{Kind: "postgres", Address: "db.acme:5432", Class: plan.ClassShared},
			{Kind: "postgres", Address: "db.prod.acme:5432", Class: plan.ClassProduction},
		}},
	}
	step := func(action, params string) []events.ProposalStep {
		return []events.ProposalStep{{Action: action, Layer: events.LayerEnvironment, Params: json.RawMessage(params)}}
	}
	cases := []struct {
		name      string
		action    string
		params    string
		wantTier  string
		wantClass string
		wantErr   string
	}{
		{"T1 local", "compose_write", `{"target":"127.0.0.1:5432"}`, plan.TierT1, plan.ClassLocal, ""},
		{"T2 shared", "compose_write", `{"target":"db.acme:5432"}`, plan.TierT2, plan.ClassShared, ""},
		{"T3 production", "compose_write", `{"target":"db.prod.acme:5432"}`, plan.TierT3, plan.ClassProduction, ""},
		// A driver step (no plan) that names no registered endpoint may reach
		// any of the room's endpoints: it routes at the room's highest class.
		{"unregistered endpoint is the room's highest class", "compose_write", `{"target":"elsewhere:5432"}`, plan.TierT3, plan.ClassProduction, ""},
		{"no target is the room's highest class", "compose_write", `{}`, plan.TierT3, plan.ClassProduction, ""},
		{"not Tiered is T2", "plain_op", `{"target":"127.0.0.1:5432"}`, plan.TierT2, plan.ClassLocal, ""},
		{"T0 is refused", "env_detect", `{"target":"127.0.0.1:5432"}`, "", "", "T0 ops are tools"},
		{"guard on production", "source_drop_table", `{"target":"db.prod.acme:5432"}`, "", "", "(drop_table)"},
		{"compose_down keeps nothing", "compose_down", `{"target":"db.prod.acme:5432"}`, "", "", "(compose_down_volumes)"},
		{"alter_system needs its allow-list param", "source_alter_system", `{"target":"db.prod.acme:5432"}`, "", "", "(alter_system)"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			route, err := resolveSetupRoute(snap, reg, step(c.action, c.params), false, 0, time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC))
			if c.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), c.wantErr) {
					t.Fatalf("err = %v, want %q", err, c.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("resolve: %v", err)
			}
			if route.Tier != c.wantTier || route.Class != c.wantClass {
				t.Fatalf("route = %s/%s, want %s/%s", route.Tier, route.Class, c.wantTier, c.wantClass)
			}
		})
	}
}

// TestSetupRouteUnknownClassFailsClosed: a target whose class is not a class
// is read as shared, never as local, so it cannot be scheduled by policy.
func TestSetupRouteUnknownClassFailsClosed(t *testing.T) {
	reg := actions.NewRegistry()
	if err := reg.Register(&tierAction{name: "compose_write", layer: events.LayerEnvironment, floor: plan.TierT1}); err != nil {
		t.Fatal(err)
	}
	snap := &RoomSnapshot{Setup: &SetupSnapshot{Mode: true, Targets: []SetupTargetState{
		{Kind: "postgres", Address: "db.acme:5432", Class: "bogus"},
	}}}
	route, err := resolveSetupRoute(snap, reg, []events.ProposalStep{{
		Action: "compose_write", Layer: events.LayerEnvironment, Params: json.RawMessage(`{"target":"db.acme:5432"}`),
	}}, false, 0, time.Now())
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if route.Class != plan.ClassShared || route.Tier != plan.TierT2 {
		t.Fatalf("route = %s/%s, want shared/T2", route.Class, route.Tier)
	}
}

// TestSetupPlanMembership: an agent's step must be in the latest setup.plan,
// and a plan step that already ran cannot be proposed again.
func TestSetupPlanMembership(t *testing.T) {
	r := newTierRig(t, tierOpts{})
	ctx := context.Background()
	roomID := r.openSetup("orders-setup", "a", "alice", "db.acme:5432", "")
	r.plan(roomID, "db.acme:5432", "compose_up", "connect_create_connector")

	// No plan entry for this op.
	_, err := r.propose(roomID, "source_create_publication", "postgres", `{"target":"db.acme:5432"}`)
	if !errors.Is(err, agent.ErrNotInPlan) {
		t.Fatalf("err = %v, want agent.ErrNotInPlan", err)
	}
	var re *Error
	if !errors.As(err, &re) || re.Code != "not_in_plan" || re.Status != 400 {
		t.Fatalf("error = %#v, want 400 not_in_plan", err)
	}

	// A step that is in the plan is accepted.
	r.proposeOK(roomID, "compose_up", events.LayerEnvironment, `{"target":"db.acme:5432"}`)

	// After the step ran, the same plan step is no longer proposable.
	planStep0 := 0
	if err := r.svc.EmitSetup(ctx, roomID, events.ActionExecuted, events.ActionExecutedPayload{
		ProposalID: "p_old", StepIndex: 0, PlanStepIndex: &planStep0, OK: true, Output: "ok",
	}); err != nil {
		t.Fatal(err)
	}
	_, err = r.propose(roomID, "compose_up", events.LayerEnvironment, `{"target":"db.acme:5432"}`)
	if !errors.Is(err, agent.ErrNotInPlan) {
		t.Fatalf("err = %v, want agent.ErrNotInPlan after the step ran", err)
	}

	// A new plan that supersedes the old one bounds what may be proposed.
	r.plan(roomID, "db.acme:5432", "compose_write")
	_, err = r.propose(roomID, "compose_up", events.LayerEnvironment, `{"target":"db.acme:5432"}`)
	if !errors.Is(err, agent.ErrNotInPlan) {
		t.Fatalf("err = %v, want agent.ErrNotInPlan under the new plan", err)
	}
	r.proposeOK(roomID, "compose_write", events.LayerEnvironment, `{"target":"db.acme:5432"}`)

	// A driver's own proposal carries its own reason to write: no plan entry
	// needed. Its guard and window checks still apply.
	if _, err := r.svc.CreateDriverProposal(ctx, r.user("alice"), roomID, events.ProposalCreatedPayload{
		Title: "manual", Risk: "low", Rollback: "undo",
		Steps: []events.ProposalStep{{Action: "source_create_publication", Layer: "postgres", Params: json.RawMessage(`{"target":"db.acme:5432"}`)}},
	}); err != nil {
		t.Fatalf("driver proposal outside the plan: %v", err)
	}
}

// TestSetupGuardAtCreate: every guard row refuses its op on a production
// target, with the row's fixed reason.
func TestSetupGuardAtCreate(t *testing.T) {
	for _, row := range plan.GuardRows() {
		t.Run(row.Op, func(t *testing.T) {
			r := newTierRig(t, tierOpts{})
			roomID := r.openSetup("orders-"+row.Op, "a", "alice", "db.prod.acme:5432", plan.ClassProduction)
			r.plan(roomID, "db.prod.acme:5432", row.Op)
			_, err := r.propose(roomID, row.Op, tierLayerOf(row.Op), `{"target":"db.prod.acme:5432"}`)
			if !errors.Is(err, plan.ErrGuard) {
				t.Fatalf("err = %v, want plan.ErrGuard", err)
			}
			if !strings.Contains(err.Error(), row.Reason) {
				t.Fatalf("err = %q, want the reason %q", err, row.Reason)
			}
		})
	}
}

// tierLayerOf is the layer the test registry gives an op.
func tierLayerOf(op string) string {
	for _, o := range tierOps {
		if o.name == op {
			return o.layer
		}
	}
	return events.LayerEnvironment
}

// TestSetupGuardAtExecute: a target class raised between approval and the
// write makes the executor refuse every guard row's op, and the action never
// runs.
func TestSetupGuardAtExecute(t *testing.T) {
	for _, row := range plan.GuardRows() {
		op := row.Op
		t.Run(op, func(t *testing.T) {
			r := newTierRig(t, tierOpts{})
			ctx := context.Background()
			roomID := r.openSetup("orders-"+op, "a", "alice", "db.acme:5432", plan.ClassShared)
			r.plan(roomID, "db.acme:5432", "blocker", op)
			// A window so a T3 row is created at all; the guard, not the
			// window, is what the executor must refuse.
			now := r.clock.Now()
			if _, err := r.svc.RecordMaintenanceWindow(ctx, r.user("alice"), roomID, now.Add(-time.Minute), now.Add(time.Hour)); err != nil {
				t.Fatalf("record window: %v", err)
			}

			// The blocker keeps the room's single executor busy, so the
			// guarded proposal is approved before the class is raised.
			held := r.acts["blocker"]
			held.mu.Lock()
			held.release = make(chan struct{})
			release := held.release
			held.mu.Unlock()
			blocker := r.proposeOK(roomID, "blocker", "kafka-connect", `{"target":"db.acme:5432"}`)
			if err := r.approve(roomID, blocker, "kafka-connect", "bob"); err != nil {
				t.Fatalf("approve blocker: %v", err)
			}
			waitForCond(t, 5*time.Second, func() bool { return held.count() == 1 }, "the blocker never started")

			guarded := r.proposeOK(roomID, op, tierLayerOf(op), `{"target":"db.acme:5432"}`)
			if err := r.approve(roomID, guarded, tierLayerOf(op), "bob"); err != nil {
				t.Fatalf("approve %s: %v", op, err)
			}
			// The class rises after the approval: the executor's re-check is
			// what refuses the step.
			if _, err := r.svc.RaiseSetupTargetClass(ctx, r.user("alice"), roomID, "db.acme:5432", plan.ClassProduction); err != nil {
				t.Fatalf("raise class: %v", err)
			}
			close(release)

			executed := waitPayload[events.ActionExecutedPayload](t, r.svc, roomID, events.ActionExecuted, guarded)
			if executed.OK {
				t.Fatalf("%s ran on a production target: %+v", op, executed)
			}
			if !strings.Contains(executed.Output, "guard table forbids") ||
				!strings.Contains(executed.Output, guardReason(op)) {
				t.Fatalf("output = %q, want the guard refusal with %q", executed.Output, guardReason(op))
			}
			if got := r.acts[op].count(); got != 0 {
				t.Fatalf("%s ran %d times, want 0", op, got)
			}
		})
	}
}

// TestSetupT3Window: a T3 window step is refused outside the recorded window
// at Create and again at Execute.
func TestSetupT3Window(t *testing.T) {
	r := newTierRig(t, tierOpts{})
	ctx := context.Background()
	roomID := r.openSetup("altsys-setup", "a", "alice", "db.prod.acme:5432", plan.ClassProduction)
	r.plan(roomID, "db.prod.acme:5432", "source_alter_system")
	params := `{"target":"db.prod.acme:5432","param":"wal_level"}`

	// No window yet: refused at Create.
	if _, err := r.propose(roomID, "source_alter_system", "postgres", params); err == nil ||
		!strings.Contains(err.Error(), "maintenance window") {
		t.Fatalf("err = %v, want a maintenance-window refusal", err)
	}

	// A window that covers now: accepted.
	start := r.clock.Now()
	if _, err := r.svc.RecordMaintenanceWindow(ctx, r.user("alice"), roomID, start.Add(-time.Minute), start.Add(time.Hour)); err != nil {
		t.Fatalf("record window: %v", err)
	}
	pid := r.proposeOK(roomID, "source_alter_system", "postgres", params)

	// The window ended before the step ran: refused at Execute.
	r.clock.Advance(2 * time.Hour)
	if err := r.approve(roomID, pid, "postgres", "bob"); err != nil {
		t.Fatalf("approve: %v", err)
	}
	executed := waitPayload[events.ActionExecutedPayload](t, r.svc, roomID, events.ActionExecuted, pid)
	if executed.OK {
		t.Fatalf("the T3 step ran outside its window: %+v", executed)
	}
	if !strings.Contains(executed.Output, "maintenance window") {
		t.Fatalf("output = %q, want the window refusal", executed.Output)
	}
	if got := r.acts["source_alter_system"].count(); got != 0 {
		t.Fatalf("the T3 step ran %d times, want 0", got)
	}
}

// TestSetupT1ScheduledByPolicy: a T1 proposal is scheduled under policy
// setup_local for PIPEROOM_SETUP_LOCAL_DELAY, then approved by the policy.
func TestSetupT1ScheduledByPolicy(t *testing.T) {
	r := newTierRig(t, tierOpts{local: true, delay: 20 * time.Second})
	roomID := r.openSetup("orders-setup", "a", "alice", "127.0.0.1:5432", "")
	// env_detect (U1-8) is what makes a loopback target local; the server's
	// own derivation at creation is shared.
	r.classify(roomID, "127.0.0.1:5432", plan.ClassLocal)
	r.plan(roomID, "127.0.0.1:5432", "compose_write")
	pid := r.proposeOK(roomID, "compose_write", events.LayerEnvironment, `{"target":"127.0.0.1:5432"}`)

	p := r.settle(roomID, pid)
	waitForCond(t, 5*time.Second, func() bool {
		p, ok := openProposal(mustRoom(t, r.svc, roomID), pid)
		return ok && p.Scheduled != nil
	}, "the T1 countdown never started")
	p, _ = openProposal(mustRoom(t, r.svc, roomID), pid)
	if want := r.clock.Now().Add(20 * time.Second); !p.Scheduled.RunAt.Equal(want) {
		t.Fatalf("run_at = %v, want %v", p.Scheduled.RunAt, want)
	}
	scheduled := waitPayload[events.ProposalScheduledPayload](t, r.svc, roomID, events.ProposalScheduled, pid)
	if scheduled.Policy != events.PolicySetupLocal {
		t.Fatalf("policy = %q, want %q", scheduled.Policy, events.PolicySetupLocal)
	}

	// Before run_at nothing runs.
	r.tick()
	if got := r.acts["compose_write"].count(); got != 0 {
		t.Fatalf("the T1 step ran before its countdown: %d", got)
	}
	// At run_at the policy approves with grant policy and the step runs.
	r.clock.Advance(20 * time.Second)
	r.tick()
	given := waitPayload[events.ApprovalGivenPayload](t, r.svc, roomID, events.ApprovalGiven, pid)
	if given.Grant != events.GrantPolicy || given.Policy != events.PolicySetupLocal {
		t.Fatalf("approval = %+v, want grant policy, policy setup_local", given)
	}
	waitForCond(t, 5*time.Second, func() bool { return r.acts["compose_write"].count() == 1 }, "the T1 step never ran")
}

// TestSetupT1DelayZeroWaits: with PIPEROOM_SETUP_LOCAL_DELAY "0" a T1 step
// waits for a click (the driver's fallback).
func TestSetupT1DelayZeroWaits(t *testing.T) {
	r := newTierRig(t, tierOpts{local: true, delay: 0})
	roomID := r.openSetup("orders-setup", "a", "alice", "127.0.0.1:5432", "")
	// env_detect (U1-8) is what makes a loopback target local; the server's
	// own derivation at creation is shared.
	r.classify(roomID, "127.0.0.1:5432", plan.ClassLocal)
	r.plan(roomID, "127.0.0.1:5432", "compose_write")
	pid := r.proposeOK(roomID, "compose_write", events.LayerEnvironment, `{"target":"127.0.0.1:5432"}`)
	p := r.settle(roomID, pid)
	if p.Scheduled != nil {
		t.Fatalf("a countdown started with the delay off: %+v", p.Scheduled)
	}
	if err := r.approve(roomID, pid, events.LayerEnvironment, "alice"); err != nil {
		t.Fatalf("driver fallback: %v", err)
	}
	waitForCond(t, 5*time.Second, func() bool { return r.acts["compose_write"].count() == 1 }, "the T1 step never ran")
}

// TestSetupT2ApproverTeamServer: on a team server a non-admin driver cannot
// approve a T2 step of a shared target; a tenant admin can, with grant admin.
func TestSetupT2ApproverTeamServer(t *testing.T) {
	r := newTierRig(t, tierOpts{})
	roomID := r.openSetup("shipments-setup", "a", "alice", "db.acme:5432", plan.ClassShared)
	r.plan(roomID, "db.acme:5432", "source_create_publication")
	pid := r.proposeOK(roomID, "source_create_publication", "postgres", `{"target":"db.acme:5432"}`)

	err := r.approve(roomID, pid, "postgres", "alice")
	var re *Error
	if !errors.As(err, &re) || re.Code != "not_admin" || re.Status != 403 {
		t.Fatalf("driver approval = %v, want 403 not_admin", err)
	}
	// The driver's fallback is not offered on a team server.
	p := r.settle(roomID, pid)
	if len(p.FallbackLayers) != 0 {
		t.Fatalf("fallback_layers = %v, want none on a team server", p.FallbackLayers)
	}
	if err := r.approve(roomID, pid, "postgres", "bob"); err != nil {
		t.Fatalf("tenant admin approval: %v", err)
	}
	given := waitPayload[events.ApprovalGivenPayload](t, r.svc, roomID, events.ApprovalGiven, pid)
	if given.Grant != events.GrantAdmin {
		t.Fatalf("grant = %q, want %q", given.Grant, events.GrantAdmin)
	}
	waitForCond(t, 5*time.Second, func() bool { return r.acts["source_create_publication"].count() == 1 }, "the step never ran")
}

// TestSetupEnvironmentOwnerNeverApproves: a top-level environment owner
// written by discover does not approve in a setup room before registration.
func TestSetupEnvironmentOwnerNeverApproves(t *testing.T) {
	r := newTierRig(t, tierOpts{})
	roomID := r.openSetup("shipments-setup", "a", "alice", "db.acme:5432", plan.ClassShared)
	r.plan(roomID, "db.acme:5432", "compose_up")
	pid := r.proposeOK(roomID, "compose_up", events.LayerEnvironment, `{"target":"db.acme:5432"}`)

	snap := mustRoom(t, r.svc, roomID)
	prov, ok := snap.SetupProvisional()
	if !ok || len(prov.Owners) != 0 {
		t.Fatalf("the provisional pipeline carries owners: %+v", prov.Owners)
	}
	// carol owns environment on the loaded pipeline "unrelated".
	if _, ok := r.svc.owners.CanApprove("unrelated", events.LayerEnvironment, "carol", nil); !ok {
		t.Fatal("the fixture does not make carol the environment owner")
	}
	err := r.approve(roomID, pid, events.LayerEnvironment, "carol")
	var re *Error
	if !errors.As(err, &re) || re.Code != "not_admin" {
		t.Fatalf("environment owner approval = %v, want 403 not_admin", err)
	}
}

// TestSetupOverlapOwnersApprove: a T2 step that overlaps an existing pipeline
// needs that pipeline's layers, and its owners approve.
func TestSetupOverlapOwnersApprove(t *testing.T) {
	r := newTierRig(t, tierOpts{})
	roomID := r.openSetup("orders-setup", "a", "alice", "db.acme:5432", plan.ClassShared)
	r.plan(roomID, "db.acme:5432", "compose_up")
	pid := r.proposeOK(roomID, "compose_up", events.LayerEnvironment, `{"target":"db.acme:5432"}`)

	p := r.settle(roomID, pid)
	want := []string{"kafka-connect", events.LayerEnvironment}
	if fmt.Sprint(p.RequiredLayers) != fmt.Sprint(want) {
		t.Fatalf("required_layers = %v, want the overlapping pipeline's %v", p.RequiredLayers, want)
	}
	for _, layer := range want {
		if err := r.approve(roomID, pid, layer, "carol"); err != nil {
			t.Fatalf("overlap owner approval of %s: %v", layer, err)
		}
	}
	waitForCond(t, 5*time.Second, func() bool { return r.acts["compose_up"].count() == 1 }, "the step never ran")
}

// TestSetupSoloRule: the setup room's own solo rule is mode.solo (one person
// is the whole tenant), and the driver's fallback for a T2 step needs solo
// mode on the standalone server.
func TestSetupSoloRule(t *testing.T) {
	r := newTierRig(t, tierOpts{})
	ctx := context.Background()

	// Tenant a has three members: not solo, whatever the caller owns.
	roomA := r.openSetup("orders-setup", "a", "alice", "db.acme:5432", plan.ClassShared)
	view, err := r.svc.RoomFor(ctx, r.user("alice"), roomA)
	if err != nil {
		t.Fatal(err)
	}
	if view.Mode == nil || view.Mode.Solo {
		t.Fatalf("mode.solo = %+v, want false in a three-person tenant", view.Mode)
	}

	// Tenant b has one member: the setup room's rule makes it solo.
	roomB := r.openSetup("shipments-setup", "b", "dave", "db.acme:5432", plan.ClassShared)
	view, err = r.svc.RoomFor(ctx, r.user("dave"), roomB)
	if err != nil {
		t.Fatal(err)
	}
	if view.Mode == nil || !view.Mode.Solo {
		t.Fatalf("mode.solo = %+v, want true for a one-person tenant", view.Mode)
	}

	// mode.solo alone is not enough for an environment step on a team server:
	// the fallback needs the standalone server too.
	r.plan(roomB, "db.acme:5432", "source_create_publication")
	pid := r.proposeOK(roomB, "source_create_publication", "postgres", `{"target":"db.acme:5432"}`)
	err = r.approve(roomB, pid, "postgres", "dave")
	var re *Error
	if !errors.As(err, &re) || re.Code != "not_admin" {
		t.Fatalf("solo driver on a team server = %v, want 403 not_admin", err)
	}

	// On the standalone server the same driver approves with the fallback.
	standalone := newTierRig(t, tierOpts{local: true})
	roomC := standalone.openSetup("shipments-setup", "b", "dave", "db.acme:5432", plan.ClassShared)
	standalone.plan(roomC, "db.acme:5432", "source_create_publication")
	pidC := standalone.proposeOK(roomC, "source_create_publication", "postgres", `{"target":"db.acme:5432"}`)
	if err := standalone.approve(roomC, pidC, "postgres", "dave"); err != nil {
		t.Fatalf("solo fallback on the standalone server: %v", err)
	}
	given := waitPayload[events.ApprovalGivenPayload](t, standalone.svc, roomC, events.ApprovalGiven, pidC)
	if given.Grant != events.GrantFallback {
		t.Fatalf("grant = %q, want %q", given.Grant, events.GrantFallback)
	}
}

// TestSetupStepContextCarriesTheOpRequestFields: the executor puts the
// proposal fields and the server's class and tier into the action's context,
// so every setup OpRequest carries them.
func TestSetupStepContextCarriesTheOpRequestFields(t *testing.T) {
	r := newTierRig(t, tierOpts{})
	roomID := r.openSetup("shipments-setup", "a", "alice", "db.prod.acme:5432", plan.ClassProduction)
	r.plan(roomID, "db.prod.acme:5432", "source_create_publication")
	pid := r.proposeOK(roomID, "source_create_publication", "postgres", `{"target":"db.prod.acme:5432"}`)
	p := r.settle(roomID, pid)
	if err := r.approve(roomID, pid, "postgres", "bob"); err != nil {
		t.Fatalf("approve: %v", err)
	}
	a := r.acts["source_create_publication"]
	waitForCond(t, 5*time.Second, func() bool { return a.count() == 1 }, "the step never ran")

	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.hasStep || a.lastReq == nil {
		t.Fatal("the action's context carried no SetupStep")
	}
	if a.lastReq.ProposalID != pid || a.lastReq.StepIndex == nil || *a.lastReq.StepIndex != 0 {
		t.Fatalf("OpRequest proposal fields = %+v", a.lastReq)
	}
	if a.lastReq.ParamsHash != p.ParamsHash || a.lastReq.ParamsHash == "" {
		t.Fatalf("OpRequest params_hash = %q, want %q", a.lastReq.ParamsHash, p.ParamsHash)
	}
	if a.lastReq.Class != plan.ClassProduction || a.lastReq.Tier != plan.TierT3 {
		t.Fatalf("OpRequest class/tier = %s/%s, want production/T3", a.lastReq.Class, a.lastReq.Tier)
	}
	// The frozen step travels as approved, so a client sends exactly the
	// params the runner binds the request to.
	if a.lastStep.Action != "source_create_publication" || string(a.lastStep.Params) != `{"target":"db.prod.acme:5432"}` {
		t.Fatalf("SetupStep frozen step = %s %s, want the approved action and params", a.lastStep.Action, a.lastStep.Params)
	}
	// A T3 step waits for a person on the runner's terminal: its bound is
	// the op's 5 minute deadline plus the margin, not the 30 s step timeout.
	if a.lastLeft < 5*time.Minute || a.lastLeft > setupT3StepTimeout {
		t.Fatalf("a T3 step ran with %s left, want between 5m and %s", a.lastLeft, setupT3StepTimeout)
	}
}

// TestSetupT0Refused: a T0 op is never a proposal.
func TestSetupT0Refused(t *testing.T) {
	r := newTierRig(t, tierOpts{})
	roomID := r.openSetup("shipments-setup", "a", "alice", "db.acme:5432", plan.ClassShared)
	r.plan(roomID, "db.acme:5432", "env_detect")
	_, err := r.propose(roomID, "env_detect", events.LayerEnvironment, `{"target":"db.acme:5432"}`)
	if err == nil || !strings.Contains(err.Error(), "T0 ops are tools") {
		t.Fatalf("err = %v, want the T0 refusal", err)
	}
}

// driverPropose creates a driver-origin proposal as who and returns its id.
func (r *tierRig) driverPropose(roomID, who, action, layer, params string) string {
	r.t.Helper()
	ev, err := r.svc.CreateDriverProposal(context.Background(), r.user(who), roomID, events.ProposalCreatedPayload{
		Title: "manual fix", Risk: "low", Rollback: "undo",
		Steps: []events.ProposalStep{{Action: action, Layer: layer, Params: json.RawMessage(params)}},
	})
	if err != nil {
		r.t.Fatalf("driver proposal: %v", err)
	}
	var p events.ProposalCreatedPayload
	if err := json.Unmarshal(ev.Payload, &p); err != nil {
		r.t.Fatalf("decode proposal.created: %v", err)
	}
	return p.ProposalID
}

// TestSetupOpMismatchRefused: a params "op" that differs from the step's
// action is refused — the guard table, the window rule and the plan
// membership are keyed on the op, so a spoofed op would route one op's write
// past another op's checks.
func TestSetupOpMismatchRefused(t *testing.T) {
	reg := actions.NewRegistry()
	for _, op := range tierOps {
		if err := reg.Register(&tierAction{name: op.name, layer: op.layer, floor: op.floor}); err != nil {
			t.Fatal(err)
		}
	}
	snap := &RoomSnapshot{Setup: &SetupSnapshot{Mode: true, Targets: []SetupTargetState{
		{Kind: "postgres", Address: "db.prod.acme:5432", Class: plan.ClassProduction},
	}}}
	_, err := resolveSetupRoute(snap, reg, []events.ProposalStep{{
		Action: "source_alter_system", Layer: "postgres",
		Params: json.RawMessage(`{"op":"env_detect","target":"db.prod.acme:5432","param":"wal_level"}`),
	}}, false, 0, time.Now())
	if err == nil || !strings.Contains(err.Error(), "may only repeat the action") {
		t.Fatalf("err = %v, want the op-mismatch refusal", err)
	}
	// An op that repeats the action is fine.
	if _, err := resolveSetupRoute(snap, reg, []events.ProposalStep{{
		Action: "compose_write", Layer: events.LayerEnvironment,
		Params: json.RawMessage(`{"op":"compose_write","target":"db.prod.acme:5432"}`),
	}}, false, 0, time.Now()); err != nil {
		t.Fatalf("matching op refused: %v", err)
	}
}

// TestSetupStepClassMaxOverMatches: a step's class is the highest class of
// every registered target its params name — a decoy low-class field never
// lowers it — an exact address match is never shadowed by another target on
// the same host, and a bare host or a nested field still resolves.
func TestSetupStepClassMaxOverMatches(t *testing.T) {
	reg := actions.NewRegistry()
	for _, op := range tierOps {
		if err := reg.Register(&tierAction{name: op.name, layer: op.layer, floor: op.floor}); err != nil {
			t.Fatal(err)
		}
	}
	snap := &RoomSnapshot{Setup: &SetupSnapshot{Mode: true, Targets: []SetupTargetState{
		{Kind: "postgres", Address: "127.0.0.1:5432", Class: plan.ClassLocal},
		{Kind: "postgres", Address: "db.acme:5432", Class: plan.ClassShared},
		{Kind: "postgres", Address: "db.prod.acme:5432", Class: plan.ClassProduction},
	}}}
	cases := []struct {
		name, params, class string
	}{
		{"decoy local target with a shared url", `{"target":"127.0.0.1:5432","url":"db.acme:5432"}`, plan.ClassShared},
		{"production address nested in a config", `{"target":"127.0.0.1:5432","config":{"database.hostname":"db.prod.acme:5432"}}`, plan.ClassProduction},
		{"bare host of a production target", `{"target":"db.prod.acme"}`, plan.ClassProduction},
		{"local only stays local", `{"target":"127.0.0.1:5432"}`, plan.ClassLocal},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			route, err := resolveSetupRoute(snap, reg, []events.ProposalStep{{
				Action: "compose_write", Layer: events.LayerEnvironment, Params: json.RawMessage(c.params),
			}}, false, 0, time.Now())
			if err != nil {
				t.Fatalf("resolve: %v", err)
			}
			if route.Class != c.class {
				t.Fatalf("class = %s, want %s", route.Class, c.class)
			}
		})
	}
	// Two targets on one host: the exact address match wins, so the shared
	// pgbouncer entry cannot shadow the production primary.
	shadow := &RoomSnapshot{Setup: &SetupSnapshot{Mode: true, Targets: []SetupTargetState{
		{Kind: "postgres", Address: "db.acme:6432", Class: plan.ClassShared},
		{Kind: "postgres", Address: "db.acme:5432", Class: plan.ClassProduction},
	}}}
	route, err := resolveSetupRoute(shadow, reg, []events.ProposalStep{{
		Action: "compose_write", Layer: events.LayerEnvironment, Params: json.RawMessage(`{"target":"db.acme:5432"}`),
	}}, false, 0, time.Now())
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if route.Class != plan.ClassProduction {
		t.Fatalf("class = %s, want production (exact match over host match)", route.Class)
	}
}

// TestSetupKeepsVolumesConflict: conflicting compose_down params never buy
// the keeps-volumes exception — any signal that volumes are removed wins —
// while consistent keep params do.
func TestSetupKeepsVolumesConflict(t *testing.T) {
	reg := actions.NewRegistry()
	for _, op := range tierOps {
		if err := reg.Register(&tierAction{name: op.name, layer: op.layer, floor: op.floor}); err != nil {
			t.Fatal(err)
		}
	}
	snap := &RoomSnapshot{Setup: &SetupSnapshot{Mode: true, Targets: []SetupTargetState{
		{Kind: "postgres", Address: "db.prod.acme:5432", Class: plan.ClassProduction},
	}}}
	resolve := func(params string) error {
		_, err := resolveSetupRoute(snap, reg, []events.ProposalStep{{
			Action: "compose_down", Layer: events.LayerEnvironment, Params: json.RawMessage(params),
		}}, false, 0, time.Now())
		return err
	}
	if err := resolve(`{"target":"db.prod.acme:5432","volumes":false}`); err != nil {
		t.Fatalf("keeps-volumes exception refused: %v", err)
	}
	if err := resolve(`{"target":"db.prod.acme:5432","args":["down"]}`); err != nil {
		t.Fatalf("argv without -v refused: %v", err)
	}
	for _, params := range []string{
		`{"target":"db.prod.acme:5432","volumes":false,"args":["-v"]}`,
		`{"target":"db.prod.acme:5432","volumes":true,"remove_volumes":false}`,
		`{"target":"db.prod.acme:5432","remove_volumes":false,"argv":["--volumes"]}`,
	} {
		if err := resolve(params); !errors.Is(err, plan.ErrGuard) {
			t.Fatalf("params %s: err = %v, want plan.ErrGuard", params, err)
		}
	}
}

// TestSetupPersonClassNotLowered: a class the person typed at registration is
// pinned — a later server (env_detect) decision cannot lower it to local —
// while the server's own derived shared is still refinable to local.
func TestSetupPersonClassNotLowered(t *testing.T) {
	r := newTierRig(t, tierOpts{local: true})

	targetClass := func(roomID, address string) string {
		snap := mustRoom(t, r.svc, roomID)
		for _, tg := range snap.Setup.Targets {
			if tg.Address == address {
				return tg.Class
			}
		}
		t.Fatalf("target %s not found", address)
		return ""
	}

	// The person typed "shared": a server "local" decision must not lower it.
	pinned := r.openSetup("pinned-setup", "a", "alice", "127.0.0.1:5432", plan.ClassShared)
	r.classify(pinned, "127.0.0.1:5432", plan.ClassLocal)
	if got := targetClass(pinned, "127.0.0.1:5432"); got != plan.ClassShared {
		t.Fatalf("class = %s, want shared (the person's class is pinned)", got)
	}

	// The server derived "shared" on its own: env_detect may refine it.
	derived := r.openSetup("derived-setup", "a", "alice", "127.0.0.1:5432", "")
	r.classify(derived, "127.0.0.1:5432", plan.ClassLocal)
	if got := targetClass(derived, "127.0.0.1:5432"); got != plan.ClassLocal {
		t.Fatalf("class = %s, want local (the server refines its own derivation)", got)
	}
}

// TestSetupPlanProgressByPlanIndex: the executed fold is keyed by the PLAN
// step index the executor resolved, not the step's index inside its proposal.
// A proposal whose single step (proposal index 0) executes plan step 1 must
// consume plan step 1 — step 0 stays proposable, step 1 does not.
func TestSetupPlanProgressByPlanIndex(t *testing.T) {
	r := newTierRig(t, tierOpts{})
	roomID := r.openSetup("shipments-setup", "a", "alice", "db.acme:5432", "")
	r.plan(roomID, "db.acme:5432", "compose_write", "compose_up")

	// Propose and run plan step 1 (compose_up) alone: its proposal index is 0.
	pid := r.proposeOK(roomID, "compose_up", events.LayerEnvironment, `{"target":"db.acme:5432"}`)
	if err := r.approve(roomID, pid, events.LayerEnvironment, "bob"); err != nil {
		t.Fatalf("approve: %v", err)
	}
	executed := waitPayload[events.ActionExecutedPayload](t, r.svc, roomID, events.ActionExecuted, pid)
	if !executed.OK {
		t.Fatalf("compose_up failed: %+v", executed)
	}
	if executed.PlanStepIndex == nil || *executed.PlanStepIndex != 1 {
		t.Fatalf("plan_step_index = %v, want 1", executed.PlanStepIndex)
	}

	// Plan step 1 is consumed; plan step 0 is not.
	if _, err := r.propose(roomID, "compose_up", events.LayerEnvironment, `{"target":"db.acme:5432"}`); !errors.Is(err, agent.ErrNotInPlan) {
		t.Fatalf("err = %v, want agent.ErrNotInPlan for the step that ran", err)
	}
	r.proposeOK(roomID, "compose_write", events.LayerEnvironment, `{"target":"db.acme:5432"}`)
}

// TestSetupDriverCannotSelfApprove: a T2/T3 driver-origin proposal needs a
// non-self approver on a team server (design §3.6) — the driver cannot
// approve their own write, even as a tenant admin; another person with the
// §3.6 grant can.
func TestSetupDriverCannotSelfApprove(t *testing.T) {
	r := newTierRig(t, tierOpts{})

	// bob (tenant admin) drives the room and proposes his own fix: his admin
	// grant must not approve his own T2 step.
	roomID := r.openSetup("shipments-setup", "a", "bob", "db.acme:5432", "")
	pid := r.driverPropose(roomID, "bob", "source_create_publication", "postgres", `{"target":"db.acme:5432"}`)
	err := r.approve(roomID, pid, "postgres", "bob")
	var re *Error
	if !errors.As(err, &re) || re.Code != "not_admin" {
		t.Fatalf("self-approval err = %v, want 403 not_admin", err)
	}

	// alice drives, bob approves: a non-self admin approval still works.
	other := r.openSetup("billing2-setup", "a", "alice", "db.acme:5432", "")
	pid2 := r.driverPropose(other, "alice", "source_create_publication", "postgres", `{"target":"db.acme:5432"}`)
	if err := r.approve(other, pid2, "postgres", "bob"); err != nil {
		t.Fatalf("non-self admin approval: %v", err)
	}
}

// TestSetupMixedTierProposalRoutesMax: a proposal runs at the tier of its
// riskiest step — a T1 step next to a T2 step makes the whole proposal T2, so
// nothing is scheduled under setup_local and the T1-style driver approval is
// refused.
func TestSetupMixedTierProposalRoutesMax(t *testing.T) {
	r := newTierRig(t, tierOpts{local: true, delay: 20 * time.Second})
	ctx := context.Background()
	roomID := r.openSetup("shipments-setup", "a", "alice", "127.0.0.1:5432", "")
	r.classify(roomID, "127.0.0.1:5432", plan.ClassLocal)
	if _, err := r.svc.AddSetupTarget(ctx, r.user("alice"), roomID, events.SetupTarget{Kind: "postgres", Address: "db.acme:5432"}); err != nil {
		t.Fatalf("add shared target: %v", err)
	}
	r.plan(roomID, "127.0.0.1:5432", "compose_write", "source_create_publication")

	pid, err := r.svc.CreateProposal(ctx, roomID, events.ProposalCreatedPayload{
		Title: "two steps", Risk: "low", Rollback: "undo it",
		Steps: []events.ProposalStep{
			{Action: "compose_write", Layer: events.LayerEnvironment, Params: json.RawMessage(`{"target":"127.0.0.1:5432"}`)},
			{Action: "source_create_publication", Layer: "postgres", Params: json.RawMessage(`{"target":"db.acme:5432"}`)},
		},
	}, events.AgentActor)
	if err != nil {
		t.Fatalf("CreateProposal: %v", err)
	}
	p := r.settle(roomID, pid)
	if p.Scheduled != nil {
		t.Fatalf("a mixed T1+T2 proposal was scheduled under setup_local: %+v", p.Scheduled)
	}
	// alice is neither a tenant admin nor an overlap owner: the T2 rule holds
	// for the whole proposal, T1 step included.
	err = r.approve(roomID, pid, events.LayerEnvironment, "alice")
	var re *Error
	if !errors.As(err, &re) || re.Code != "not_admin" {
		t.Fatalf("err = %v, want 403 not_admin for the whole proposal", err)
	}
}

// TestSetupT1CountdownInvalidatedByClassRaise: a running setup_local countdown
// dies when the target's class rises — at run_at the fire re-check sees the
// step no longer routes T1 and nothing runs.
func TestSetupT1CountdownInvalidatedByClassRaise(t *testing.T) {
	r := newTierRig(t, tierOpts{local: true, delay: 20 * time.Second})
	ctx := context.Background()
	roomID := r.openSetup("shipments-setup", "a", "alice", "127.0.0.1:5432", "")
	r.classify(roomID, "127.0.0.1:5432", plan.ClassLocal)
	r.plan(roomID, "127.0.0.1:5432", "compose_write")
	pid := r.proposeOK(roomID, "compose_write", events.LayerEnvironment, `{"target":"127.0.0.1:5432"}`)
	r.settle(roomID, pid)
	waitForCond(t, 5*time.Second, func() bool {
		p, ok := openProposal(mustRoom(t, r.svc, roomID), pid)
		return ok && p.Scheduled != nil
	}, "the T1 countdown never started")

	// The class rises mid-countdown: the step is T2 now.
	if _, err := r.svc.RaiseSetupTargetClass(ctx, r.user("alice"), roomID, "127.0.0.1:5432", plan.ClassShared); err != nil {
		t.Fatalf("raise class: %v", err)
	}
	r.clock.Advance(20 * time.Second)
	r.tick()
	if got := r.acts["compose_write"].count(); got != 0 {
		t.Fatalf("the step ran %d times after its class rose, want 0", got)
	}
	p, ok := openProposal(mustRoom(t, r.svc, roomID), pid)
	if !ok {
		t.Fatal("proposal gone")
	}
	for layer, decision := range p.Approvals {
		if decision != "pending" {
			t.Fatalf("layer %s = %s, want pending (the policy must not approve a T2 step)", layer, decision)
		}
	}
}

// TestSetupDriverProposalNeverScheduled: the setup_local policy never
// schedules a driver-origin proposal, whatever its tier.
func TestSetupDriverProposalNeverScheduled(t *testing.T) {
	r := newTierRig(t, tierOpts{local: true, delay: 20 * time.Second})

	roomID := r.openSetup("shipments-setup", "a", "alice", "127.0.0.1:5432", "")
	r.classify(roomID, "127.0.0.1:5432", plan.ClassLocal)
	pid := r.driverPropose(roomID, "alice", "compose_write", events.LayerEnvironment, `{"target":"127.0.0.1:5432"}`)
	p := r.settle(roomID, pid)
	if p.Scheduled != nil {
		t.Fatalf("a driver proposal was scheduled under setup_local: %+v", p.Scheduled)
	}
}

// TestSetupWindowEdges: the maintenance window is [start, end): a T3 window
// op is accepted at exactly start and refused at exactly end.
func TestSetupWindowEdges(t *testing.T) {
	r := newTierRig(t, tierOpts{})
	ctx := context.Background()
	roomID := r.openSetup("altsys2-setup", "a", "alice", "db.prod.acme:5432", plan.ClassProduction)
	r.plan(roomID, "db.prod.acme:5432", "source_alter_system", "source_alter_system", "source_alter_system")
	params := `{"target":"db.prod.acme:5432","param":"wal_level"}`

	start := r.clock.Now().Add(10 * time.Minute)
	end := start.Add(time.Hour)
	if _, err := r.svc.RecordMaintenanceWindow(ctx, r.user("alice"), roomID, start, end); err != nil {
		t.Fatalf("record window: %v", err)
	}
	// Before start: refused.
	if _, err := r.propose(roomID, "source_alter_system", "postgres", params); err == nil ||
		!strings.Contains(err.Error(), "maintenance window") {
		t.Fatalf("before start: err = %v, want the window refusal", err)
	}
	// Exactly at start: allowed.
	r.clock.Advance(10 * time.Minute)
	r.proposeOK(roomID, "source_alter_system", "postgres", params)
	// Exactly at end: refused.
	r.clock.Advance(time.Hour)
	if _, err := r.propose(roomID, "source_alter_system", "postgres", params); err == nil ||
		!strings.Contains(err.Error(), "maintenance window") {
		t.Fatalf("at end: err = %v, want the window refusal", err)
	}
}

// TestSetupAdminGrantScopedToRoomTenant: the admin grant reads the ROOM's
// tenant — tenant b's admin approves tenant b's setup room (nothing else
// grants dave anything there), and cannot even see tenant a's room.
func TestSetupAdminGrantScopedToRoomTenant(t *testing.T) {
	own := strings.Replace(tierOwnersYAML(), "b: {members: [user:dave]}", "b: {members: [user:dave, user:carol], admins: [user:dave]}", 1)
	r := newTierRig(t, tierOpts{own: own})

	// dave, admin of tenant b, approves a T2 step of tenant b's room.
	roomB := r.openSetup("freight-setup", "b", "dave", "db.acme:5432", "")
	r.plan(roomB, "db.acme:5432", "source_create_publication")
	pidB := r.proposeOK(roomB, "source_create_publication", "postgres", `{"target":"db.acme:5432"}`)
	if err := r.approve(roomB, pidB, "postgres", "dave"); err != nil {
		t.Fatalf("tenant b's admin refused on tenant b's room: %v", err)
	}

	// On tenant a's room, dave's admin grant counts for nothing: the room is
	// not even visible to another tenant's user.
	roomA := r.openSetup("shipments-setup", "a", "alice", "db.acme:5432", "")
	r.plan(roomA, "db.acme:5432", "source_create_publication")
	pidA := r.proposeOK(roomA, "source_create_publication", "postgres", `{"target":"db.acme:5432"}`)
	if err := r.approve(roomA, pidA, "postgres", "dave"); err == nil {
		t.Fatal("another tenant's admin approved tenant a's step")
	}
	if err := r.approve(roomA, pidA, "postgres", "bob"); err != nil {
		t.Fatalf("own tenant's admin refused: %v", err)
	}
}
