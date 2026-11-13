package room

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"piperoom/internal/actions"
	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/setup/plan"
	"piperoom/internal/setup/protocol"
)

// Tiered routing for setup rooms (U1-6; docs/design/setup-agent.md §3.3,
// §3.5, §3.6): a proposal of a setup room is routed by the tier of its steps,
// which is the op's floor raised to the class of the endpoint the room
// resolved from the step's params, never a label the model supplied.
//
//	T0  a read: refused as a proposal (tools only);
//	T1  scheduled by the policy setup_local after PIPEROOM_SETUP_LOCAL_DELAY;
//	T2  waits for the approver of §3.6: the owners of an overlapping pipeline,
//	    else a tenant admin (grant "admin"); the driver's fallback only in
//	    solo mode on the standalone server;
//	T3  T2 plus a recorded maintenance window for the ops the guard table ties
//	    to one (ALTER SYSTEM, a database restart).
//
// The guard table (plan.Forbidden) is checked at Create and again before
// Execute, and the latest setup.plan bounds what an agent may propose at all
// (agent.ErrNotInPlan). Every function here is pure — no I/O, no clock of its
// own — so a fake clock drives the countdown in tests and every replica
// reaches the same answer from the room log.

// setupOpSourceRestart is the catalogue name of the restart op that needs a
// maintenance window (plan §3.6 lists ALTER SYSTEM and a database restart).
// It has no plan constant because the op catalogue itself is the runner's
// (internal/setup/ops, A9); the room only needs the name for the window rule.
const setupOpSourceRestart = "source_restart"

// SetupStep is the proposal step identity and the server's routing decision
// that the executor puts into an action call's context in a setup room. A
// setup action adapter (U1-7) reads it to fill protocol.OpRequest: ProposalID,
// StepIndex, ParamsHash, Tier and Class are exactly the fields the runner
// re-checks (Class against its own target floors, Tier against the op floor,
// the params hash over the frozen proposal). Action and Params are the frozen
// step itself, exactly as approved: the runner refuses a request whose op is
// not the step's action or whose params are not canonically the step's
// params, so a client sends Params unchanged rather than rebuilding them from
// its own arguments. Steps outside a setup room carry none.
type SetupStep struct {
	ProposalID string
	StepIndex  int
	ParamsHash string
	Tier       string
	Class      string
	Action     string
	Params     json.RawMessage
}

// setupStepKey carries a SetupStep in an action call's context.
type setupStepKey struct{}

// WithSetupStep returns ctx carrying st. It is the room's side of the
// protocol.OpRequest contract for the setup action adapters; the executor
// adds it to every step of a setup room.
func WithSetupStep(ctx context.Context, st SetupStep) context.Context {
	return context.WithValue(ctx, setupStepKey{}, st)
}

// SetupStepFrom returns the SetupStep carried by ctx, and whether it carries
// one.
func SetupStepFrom(ctx context.Context) (SetupStep, bool) {
	st, ok := ctx.Value(setupStepKey{}).(SetupStep)
	return st, ok
}

// setupRoute is the routing decision of a whole setup proposal: the highest
// tier and class of its steps, because a proposal runs at the tier of its
// riskiest step, plus each step's decision.
type setupRoute struct {
	Tier  string
	Class string
	Steps []setupStepRoute
}

// setupStepRoute is one step's decision.
type setupStepRoute struct {
	Op    string
	Tier  string
	Class string
	// Plan is the plan step the proposal step executes; its zero value for a
	// proposal that does not need to be in a plan.
	Plan events.SetupPlanStep
}

// tierRank is plan.TierRank for a tier that plan.ValidTier accepted.
func tierRank(tier string) int {
	r, _ := plan.TierRank(tier)
	return r
}

// raiseTier returns the higher of two valid tiers.
func raiseTier(a, b string) string {
	if tierRank(b) > tierRank(a) {
		return b
	}
	return a
}

// resolveSetupRoute decides each step's op, class and tier, checks the plan
// (when requirePlan), the guard table and the maintenance window, and returns
// the proposal's route. A step's action that is not actions.Tiered is T2 in a
// setup room (actions/action.go); a T0 op is a tool and never a proposal.
// now is read once, so every step of one resolution sees the same clock.
// base is the first step's index in its proposal, for error messages: the
// executor resolves one step at a time, and its refusal must name the real
// step, not "step 0".
//
// The returned errors are the API's: 400 not_in_plan (wrapping
// agent.ErrNotInPlan), 400 validation for a T0 proposal and a T3 step outside
// its window, and a 400 validation wrapping plan.ErrGuard with the guard row's
// fixed reason code for a forbidden (op, class) pair.
func resolveSetupRoute(snap *RoomSnapshot, acts *actions.Registry, steps []events.ProposalStep, requirePlan bool, base int, now time.Time) (setupRoute, error) {
	return routeSetupSteps(snap, acts, steps, requirePlan, true, base, now)
}

// setupRouteTier is the routing half of resolveSetupRoute: each step's op,
// class and tier — including the plan step's server-filled tier raise and
// target, so the approval path and Create answer the same tier for the same
// proposal — with no plan, guard or window refusal. The approval path uses
// it to decide who approves (T1 versus T2/T3); the guard and the window are
// Create's and Execute's business, so raising a target's class after approval
// does not turn a decision into a refusal — it makes the executor refuse the
// step (design §3.5).
func setupRouteTier(snap *RoomSnapshot, acts *actions.Registry, steps []events.ProposalStep, origin string, now time.Time) (setupRoute, error) {
	return routeSetupSteps(snap, acts, steps, setupNeedsPlan(origin), false, 0, now)
}

// routeSetupSteps is the one resolver behind resolveSetupRoute (enforce) and
// setupRouteTier (route only): both must answer the same tier and class for
// the same proposal, so neither can drift from the other.
func routeSetupSteps(snap *RoomSnapshot, acts *actions.Registry, steps []events.ProposalStep, requirePlan, enforce bool, base int, now time.Time) (setupRoute, error) {
	if snap == nil || snap.Setup == nil {
		return setupRoute{}, validation("this room is not a setup room")
	}
	if len(steps) == 0 {
		return setupRoute{}, validation("a proposal needs at least one step")
	}
	route := setupRoute{Steps: make([]setupStepRoute, len(steps))}
	for i, step := range steps {
		op, err := setupOpOf(step)
		if err != nil {
			return setupRoute{}, validation(fmt.Sprintf("step %d: %v", base+i, err))
		}
		target, class := setupStepResolve(snap.Setup, step.Params)
		if op == setupLadderOpInsertCanary && target == "" {
			// The ladder's canary insert (U1-8b, setup_ladder.go) names no
			// endpoint in its params — the table and the run's nonce only — so
			// nothing in them resolves to a target. It writes into the source
			// this setup created, and its class is that target's, never the
			// "shared" a step naming no endpoint gets: otherwise a local
			// setup's canary could only ever wait for a click.
			if c := setupLadderCanaryClass(snap.Setup); c != "" {
				class = c
			}
		}
		var planStep events.SetupPlanStep
		if requirePlan {
			found, ok := setupPlanStepFor(snap.Setup, op, target)
			if !ok && enforce {
				return setupRoute{}, notInPlan(fmt.Sprintf(
					"step %d (%s) is not in the current setup plan; record a new setup plan (setup_plan) before proposing it",
					base+i, cleanPlanName(op)))
			}
			if ok {
				planStep = found
				if target == "" && found.Target != "" {
					// The plan's target is the server's own record of the
					// endpoint; use it when the params name none. The class
					// only rises: a plan target can never lower what the
					// params already resolved.
					target = found.Target
					class = classAtLeast(class, setupStepClass(snap.Setup, target))
				}
			}
		}
		if !requirePlan && target == "" {
			// A driver step (the setup rollback among them) has no plan step
			// to name its endpoint, and its params name no registered one: the
			// action may reach any endpoint the room has, so it is routed and
			// guarded at the room's highest class. A class raised after Create
			// then still reaches the executor's re-check.
			class = classAtLeast(class, setupRoomClass(snap.Setup))
		}
		tier, err := setupStepTier(acts, step, class)
		if err != nil {
			return setupRoute{}, validation(fmt.Sprintf("step %d (%s): %v", base+i, cleanPlanName(op), err))
		}
		if plan.ValidTier(planStep.Tier) {
			// The plan step's tier is server-filled and can only be raised.
			tier = raiseTier(tier, planStep.Tier)
		}
		if enforce {
			if tier == plan.TierT0 {
				return setupRoute{}, validation(fmt.Sprintf(
					"step %d (%s) is a T0 read: T0 ops are tools, never proposals", base+i, cleanPlanName(op)))
			}
			if row, forbidden := plan.Forbidden(plan.GuardRequest{Op: op, Class: class, Facts: setupGuardFacts(op, step.Params)}); forbidden {
				return setupRoute{}, setupGuardRefused(fmt.Sprintf(
					"step %d (%s): the guard table forbids %s on a %s target (%s)",
					base+i, cleanPlanName(op), cleanPlanName(row.Op), class, row.Reason))
			}
			if tier == plan.TierT3 && setupOpNeedsWindow(op) && !setupWindowAllows(snap.Setup, now) {
				return setupRoute{}, validation(fmt.Sprintf(
					"step %d (%s) is T3 on a %s target and needs a maintenance window: record one with POST /api/rooms/{id}/setup/maintenance-window",
					base+i, cleanPlanName(op), class))
			}
		}
		route.Steps[i] = setupStepRoute{Op: op, Tier: tier, Class: class, Plan: planStep}
		if route.Tier == "" {
			route.Tier = tier
		} else {
			route.Tier = raiseTier(route.Tier, tier)
		}
		if route.Class == "" {
			route.Class = class
		} else {
			route.Class = classAtLeast(route.Class, class)
		}
	}
	return route, nil
}

// setupStepTier asks the action for the step's tier at class, or answers T2
// for an action that does not implement actions.Tiered.
func setupStepTier(acts *actions.Registry, step events.ProposalStep, class string) (string, error) {
	if acts == nil {
		return "", fmt.Errorf("no action registry")
	}
	a, ok := acts.Get(step.Action)
	if !ok {
		return "", fmt.Errorf("unknown action %q", cleanPlanName(step.Action))
	}
	tiered, ok := a.(actions.Tiered)
	if !ok {
		return plan.TierT2, nil
	}
	tier, err := tiered.Tier(step.Params, class)
	if err != nil {
		// The error text can echo model-written params; sanitize it like any
		// other agent text before it lands in a 400 or a log line.
		return "", fmt.Errorf("tier of %q: %s", cleanPlanName(step.Action), sanitizeAgentText(err.Error()))
	}
	if !plan.ValidTier(tier) {
		return "", fmt.Errorf("tier of %q is %q, not T0..T3", cleanPlanName(step.Action), cleanPlanName(tier))
	}
	if isDeferred(a) {
		// Q7k: a Deferred (pull-request) step is never a setup T1 step, so
		// setup_local never schedules it, whatever its Tier says.
		tier = raiseTier(tier, plan.TierT2)
	}
	return tier, nil
}

// setupOpOf is the catalogue op a proposal step executes: the action's name
// (the setup adapters register under the op name). A params "op" field may
// only repeat it: the guard table, the maintenance-window rule and the plan
// membership are keyed on the op, so a params op that differs from the action
// would let the model route one op's write past another op's checks.
func setupOpOf(step events.ProposalStep) (string, error) {
	var params struct {
		Op string `json:"op"`
	}
	if err := protocol.CheckParamKeys(step.Params); err != nil {
		// Two keys the decoder folds together: which "op" runs is the
		// decoder's choice, so none is believed.
		return "", fmt.Errorf("params: %w", err)
	}
	if len(step.Params) > 0 && json.Unmarshal(step.Params, &params) == nil {
		if op := strings.TrimSpace(params.Op); op != "" && op != step.Action {
			return "", fmt.Errorf("params name op %q but the step's action is %q; the op field may only repeat the action",
				cleanPlanName(op), cleanPlanName(step.Action))
		}
	}
	return step.Action, nil
}

// setupStepResolve returns the registered target a step's params name ("" when
// they name none) and the class the step routes at. The setup action adapters
// name the endpoint they reach in a "target" field (or "address"/"host"/"url");
// a value equal to a registered target's address or host resolves to that
// target, preferring an exact address match over a host match so a second
// target on the same host is never shadowed. Any other string in the params
// that equals a registered target also counts, so an adapter that nests the
// endpoint deeper still resolves; nothing is guessed from a host name that is
// not registered.
//
// The class is the HIGHEST class of every registered target any candidate
// matches, not the first match's: a step that names two registered endpoints
// routes at the riskier one, so a decoy low-class field can never lower the
// class below the real endpoint's (design §3.5). No match reads as shared,
// never local (setupStepClass).
func setupStepResolve(setup *SetupSnapshot, params json.RawMessage) (string, string) {
	if setup == nil || len(setup.Targets) == 0 || len(params) == 0 {
		return "", protocolClassShared
	}
	obj, ok := setupParamsObject(params)
	if !ok {
		// Unreadable, or keys the executor's decoder would fold together:
		// no target is believed, and the step routes at the room's highest
		// class.
		return "", classAtLeast(protocolClassShared, setupRoomClass(setup))
	}
	var candidates []string
	for _, key := range []string{"target", "address", "host", "url"} {
		if v, ok := obj[foldKey(key)].(string); ok && strings.TrimSpace(v) != "" {
			candidates = append(candidates, strings.TrimSpace(v))
		}
	}
	collectSetupStrings(obj, &candidates, 0)
	address := ""
	exact := false
	class := ""
	for _, c := range candidates {
		host := setupTargetHost(c)
		for _, t := range setup.Targets {
			exactHere := t.Address == c
			if !exactHere && (host == "" || setupTargetHost(t.Address) != host) {
				continue
			}
			if address == "" || (exactHere && !exact) {
				address = t.Address
				exact = exactHere
			}
			if validSetupClass(t.Class) {
				class = classAtLeast(class, t.Class)
			} else {
				class = classAtLeast(class, protocolClassShared)
			}
		}
	}
	if class == "" {
		class = protocolClassShared
	}
	return address, class
}

// collectSetupStrings appends every string value of obj to out, up to a
// bounded depth and count: a step's params are small, and the resolver must
// not walk a hostile, deeply nested payload.
func collectSetupStrings(v any, out *[]string, depth int) {
	if depth > 4 || len(*out) >= maxSetupParamsStrings {
		return
	}
	switch x := v.(type) {
	case string:
		if s := strings.TrimSpace(x); s != "" {
			*out = append(*out, s)
		}
	case map[string]any:
		keys := make([]string, 0, len(x))
		for k := range x {
			keys = append(keys, k)
		}
		sort.Strings(keys) // stable order, so the answer never depends on map iteration
		for _, k := range keys {
			collectSetupStrings(x[k], out, depth+1)
		}
	case []any:
		for _, item := range x {
			collectSetupStrings(item, out, depth+1)
		}
	}
}

// maxSetupParamsStrings bounds collectSetupStrings.
const maxSetupParamsStrings = 64

// setupLadderCanaryClass is the class the ladder's canary insert routes at: the
// class of the room's source target — the source kind the setup request names
// (a postgres for a postgres setup) — because the canary params hold no endpoint
// to resolve (a table and the run's nonce only) and the row goes into the
// source the setup created. A room whose request names no such target routes at
// its highest class, so the canary is never taken for a lower class than a
// target it may reach; "" when the room has no classed target at all.
func setupLadderCanaryClass(setup *SetupSnapshot) string {
	if setup == nil {
		return ""
	}
	kind := setup.req.SourceKind
	best := ""
	for _, t := range setup.Targets {
		if kind != "" && t.Kind != kind {
			continue
		}
		if validSetupClass(t.Class) {
			best = classAtLeast(best, t.Class)
		}
	}
	if best == "" && kind != "" {
		// No target of the source kind: the canary still reaches a registered
		// endpoint, so the room's highest class is the safe answer.
		return setupRoomClass(setup)
	}
	return best
}

// setupRoomClass is the highest class of the room's targets, or "" when none
// has a valid class.
func setupRoomClass(setup *SetupSnapshot) string {
	class := ""
	if setup == nil {
		return class
	}
	for _, t := range setup.Targets {
		if validSetupClass(t.Class) {
			class = classAtLeast(class, t.Class)
		}
	}
	return class
}

// setupStepClass is the class of the endpoint a step names: the class of the
// registered target it resolves to, or "shared" when it resolves to none. It
// never answers "local" for an endpoint the room did not register, so a step
// whose target is unknown cannot be scheduled by policy; a registered target
// whose class is not a class is read as shared, never as local.
func setupStepClass(setup *SetupSnapshot, address string) string {
	if setup == nil || address == "" {
		return protocolClassShared
	}
	class := ""
	for _, t := range setup.Targets {
		if t.Address != address || !validSetupClass(t.Class) {
			continue
		}
		if class == "" {
			class = t.Class
		} else {
			class = classAtLeast(class, t.Class)
		}
	}
	if class == "" {
		return protocolClassShared
	}
	return class
}

// setupPlanStepFor finds the plan step a proposal step executes: the latest
// plan's step with the same op that has not run yet, preferring the one whose
// target matches the proposal's when both name one. A plan with no such step
// means the model must re-plan (agent.ErrNotInPlan).
func setupPlanStepFor(setup *SetupSnapshot, op, target string) (events.SetupPlanStep, bool) {
	var first *events.SetupPlanStep
	for pi := range setup.phases {
		for si := range setup.phases[pi].Steps {
			planStep := setup.phases[pi].Steps[si]
			if planStep.Op != op || setup.executed[planStep.Index] {
				continue
			}
			if target != "" && planStep.Target == target {
				return planStep, true
			}
			if first == nil {
				p := planStep
				first = &p
			}
		}
	}
	if first != nil {
		return *first, true
	}
	return events.SetupPlanStep{}, false
}

// setupPlanIndexOf resolves, from the room's current snapshot, the plan step a
// proposal step consumes (nil when it consumes none: a driver's own fix, a
// non-setup room, a step that resolves to no plan step). The reconciled runner
// result path uses it: the executor that resolved the route is gone by the
// time the result arrives, and the action.executed event must carry the plan
// index for the fold (ActionExecutedPayload.PlanStepIndex).
func (s *Service) setupPlanIndexOf(ctx context.Context, roomID, proposalID string, stepIndex int) *int {
	snap, err := s.readRoom(ctx, roomID)
	if err != nil || snap == nil || !snap.IsSetupRoom() || snap.Setup == nil {
		return nil
	}
	prop, ok := openProposal(*snap, proposalID)
	if !ok || stepIndex < 0 || stepIndex >= len(prop.Steps) || !setupNeedsPlan(prop.Origin) {
		return nil
	}
	step := prop.Steps[stepIndex]
	op, err2 := setupOpOf(step)
	if err2 != nil {
		return nil
	}
	target, _ := setupStepResolve(snap.Setup, step.Params)
	found, ok := setupPlanStepFor(snap.Setup, op, target)
	if !ok || found.Op == "" {
		return nil
	}
	idx := found.Index
	return &idx
}

// setupGuardFacts are the guard table's per-step inputs, read from the step's
// params: the ALTER SYSTEM parameter name, and whether a compose_down keeps
// its volumes. Anything the params do not clearly answer reads as the
// strictest value (plan.StepFacts), so an unreadable param never buys the
// exception.
//
// Every read goes through the decode the executor uses, never a separate
// exact-key map read: the ALTER SYSTEM name through
// plan.DecodeAlterSystemStep (the struct the setup client binds its write
// to), and the compose_down flags through setupParamsObject, whose keys are
// case folded as encoding/json folds them. Params with two keys that fold
// together are refused by both, so the value the guard judged is the value
// that runs.
func setupGuardFacts(op string, params json.RawMessage) plan.StepFacts {
	var facts plan.StepFacts
	if len(params) == 0 {
		return facts
	}
	switch op {
	case plan.OpSourceAlterSystem:
		p, err := plan.DecodeAlterSystemStep(params)
		if err != nil {
			return facts
		}
		facts.Param = p.Name
	case plan.OpComposeDown:
		obj, ok := setupParamsObject(params)
		if !ok {
			return facts
		}
		facts.KeepsVolumes = setupKeepsVolumes(obj)
	case plan.OpK8sUnapply:
		// The kind the runner's decode binds (rollbackParams.Kind): a key
		// that folds to "kind" is the one encoding/json decodes, and two
		// that fold together are refused by setupParamsObject.
		obj, ok := setupParamsObject(params)
		if !ok {
			return facts
		}
		if k, ok := obj[foldKey("kind")].(string); ok {
			facts.Kind = k
		}
	}
	return facts
}

// setupParamsObject decodes step params as an object whose top-level keys are
// folded (foldKey), so a lookup of foldKey(name) finds the value
// encoding/json would decode into a field tagged name, whatever its case.
// Params whose keys fold together at any depth, or that are not an object,
// are refused (ok false).
func setupParamsObject(params json.RawMessage) (map[string]any, bool) {
	if protocol.CheckParamKeys(params) != nil {
		return nil, false
	}
	var raw map[string]any
	if err := json.Unmarshal(params, &raw); err != nil {
		return nil, false
	}
	obj := make(map[string]any, len(raw))
	for k, v := range raw {
		obj[foldKey(k)] = v
	}
	return obj, true
}

// setupKeepsVolumes reports whether a compose_down's params say volumes are
// kept: "volumes": false, "remove_volumes": false, "volumes_kept": true, or an
// argv-style list without "-v"/"--volumes". A params object that says nothing
// keeps nothing, and params that contradict one another ({"volumes": true,
// "remove_volumes": false}, or a keep flag next to an argv carrying -v) keep
// nothing either: any signal that volumes are removed wins, so a conflicting
// decoy key can never buy the guard exception for a write that removes them.
//
// "volumes_kept": true is the mark the room writes on the rollback of a
// compose_up (setup_rollback.go, U1-14b). It cannot buy an exception for a
// write that removes volumes: the catalogue has no "compose down" form that
// removes them (internal/setup/targets/compose, subcommandFlags), and a
// removal signal in the same params still wins.
func setupKeepsVolumes(obj map[string]any) bool {
	kept := false
	removed := false
	for _, key := range []string{"volumes", "remove_volumes"} {
		if v, ok := obj[foldKey(key)].(bool); ok {
			if v {
				removed = true
			} else {
				kept = true
			}
		}
	}
	if v, ok := obj[foldKey("volumes_kept")].(bool); ok && v {
		kept = true
	}
	for _, key := range []string{"args", "argv"} {
		raw, ok := obj[foldKey(key)].([]any)
		if !ok {
			continue
		}
		kept = true
		for _, item := range raw {
			if s, ok := item.(string); ok && (s == "-v" || s == "--volumes") {
				removed = true
			}
		}
	}
	return kept && !removed
}

// setupOpNeedsWindow reports whether an op at T3 is created and executed only
// inside a recorded maintenance window (design §3.5): changing a database
// setting through the ALTER SYSTEM allow-list, or restarting a database.
func setupOpNeedsWindow(op string) bool {
	return op == plan.OpSourceAlterSystem || op == setupOpSourceRestart
}

// setupWindowAllows reports whether the room has a recorded maintenance
// window covering now. A window that ended does not allow anything; the view
// hides it (fillSetupRuntime) but the log keeps it.
func setupWindowAllows(setup *SetupSnapshot, now time.Time) bool {
	if setup == nil || setup.MaintenanceWindow == nil {
		return false
	}
	w := setup.MaintenanceWindow
	return !now.Before(w.Start) && now.Before(w.End)
}

// setupNeedsPlan reports whether a proposal of this origin must come from the
// current setup.plan: the agent's must; the driver's own fix (POST
// /api/rooms/{id}/proposals) and the server's own proposals (the ladder's
// canary, setup_ladder.go) carry their own reason to write.
func setupNeedsPlan(origin string) bool {
	return origin != events.ProposalOriginDriver && origin != setupProposalOriginSystem
}

// setupLayers returns the required and fallback layers of a setup proposal
// (design §3.6). A T2 or T3 proposal whose endpoint overlaps an existing
// pipeline needs that pipeline's layers, so its owners are the approvers;
// otherwise it needs its steps' layers, and the driver's fallback is offered
// only in solo mode on the standalone server.
func (s *Service) setupLayers(snap *RoomSnapshot, route setupRoute, steps []events.ProposalStep) (required, fallback []string) {
	required = []string{}
	seen := map[string]bool{}
	for _, step := range steps {
		if step.Layer == "" || seen[step.Layer] {
			continue
		}
		seen[step.Layer] = true
		required = append(required, step.Layer)
	}
	fallback = []string{}
	if tierRank(route.Tier) < 2 {
		// A T1 proposal's layers are the driver's: the provisional pipeline
		// owns nothing, so every layer is a fallback layer.
		return required, append(fallback, required...)
	}
	if owned := s.setupOverlapLayers(snap); len(owned) > 0 {
		return owned, fallback
	}
	if s.setupSolo(snap) && s.setupLocal {
		return required, append(fallback, required...)
	}
	return required, fallback
}

// setupOverlapLayers returns the layers of the existing pipelines whose host,
// topic prefix, connector or table the provisional pipeline shares, in
// owners.KnownLayers order: the proposal's approvers are those pipelines' owners
// (design §3.6). A pipeline of another tenant is not included: its owners
// cannot approve here, and its admins are only a contact.
func (s *Service) setupOverlapLayers(snap *RoomSnapshot) []string {
	pipelines := s.setupOverlapPipelines(snap)
	if len(pipelines) == 0 {
		return nil
	}
	seen := map[string]bool{}
	out := []string{}
	for _, layer := range owners.KnownLayers() {
		for _, p := range pipelines {
			if !seen[layer] && s.ownersNow().IsOwned(p.ID, layer) {
				seen[layer] = true
				out = append(out, layer)
			}
		}
	}
	return out
}

// setupOverlapPipelines returns the loaded pipelines of the room's tenant that
// overlap the room's provisional pipeline (owners.Overlap). It is also the
// source of the refusal's wording in pipeline_register (U1-14).
func (s *Service) setupOverlapPipelines(snap *RoomSnapshot) []owners.Pipeline {
	o := s.ownersNow()
	if o == nil || snap == nil || snap.Setup == nil {
		return nil
	}
	prov, ok := snap.SetupProvisional()
	if !ok {
		return nil
	}
	var out []owners.Pipeline
	seen := map[string]bool{}
	for _, ov := range o.Overlap(prov, nil) {
		if ov.PipelineID == "" || seen[ov.PipelineID] {
			continue
		}
		if p, ok := o.Pipeline(ov.PipelineID); ok {
			seen[p.ID] = true
			out = append(out, p)
		}
	}
	return out
}

// setupOverlapApprover reports whether user may approve layer as an owner of
// an overlapping pipeline, and the principal detail ("user:…", "group:…").
func (s *Service) setupOverlapApprover(snap *RoomSnapshot, user User, layer string) (string, bool) {
	for _, p := range s.setupOverlapPipelines(snap) {
		if g, ok := s.ownersNow().CanApprove(p.ID, layer, user.ID, user.Groups); ok {
			return g, true
		}
	}
	return "", false
}

// setupSolo is the setup room's own solo rule (design §3.6): one person is
// the whole tenant, or no tenancy and one user. It replaces the generic
// owners-based mode.solo for a setup room, whose provisional pipeline nobody
// owns, so the room's rule overrides mode.solo for environment steps.
func (s *Service) setupSolo(snap *RoomSnapshot) bool {
	if snap == nil || snap.Setup == nil {
		return false
	}
	// PIPEROOM_SOLO_USER is a one-person install by construction; the
	// operator named the one person the server is for.
	if s.soloUser != "" {
		return true
	}
	if s.users == nil {
		return false
	}
	n := 0
	for _, u := range s.users.All() {
		if s.TenancyOn() && !s.owners.InTenant(snap.SetupTenant(), u.ID, u.Groups) {
			continue
		}
		n++
		if n > 1 {
			return false
		}
	}
	return n == 1
}

// setupSnapshot returns the room's current snapshot when it is a setup room,
// nil otherwise. It never fails the caller: the under-lock checks repeat every
// decision with the authoritative state, so this read only lets a
// before-the-lock Validate see the provisional pipeline.
func (s *Service) setupSnapshot(ctx context.Context, roomID string) *RoomSnapshot {
	snap, err := s.readRoom(ctx, roomID)
	if err != nil || snap == nil || !snap.IsSetupRoom() {
		return nil
	}
	return snap
}

// cleanPlanName quotes a name from a plan or a step for an error message,
// clipped and stripped of control characters: the name is model text.
func cleanPlanName(name string) string {
	name = sanitizeAgentText(name)
	if len(name) > plan.MaxPlanIDBytes {
		name = name[:plan.MaxPlanIDBytes]
		// The clip is a byte count and may have split a rune; drop the
		// incomplete tail so the message stays valid UTF-8.
		for len(name) > 0 && !utf8.ValidString(name) {
			name = name[:len(name)-1]
		}
	}
	return name
}

// setupStepBound is the bound of one setup step: the T3 bound when the tier
// the setup client will send it at is T3, which is the routed tier raised to
// the op's client floor (plan.ClientFloor: source_alter_system is always sent
// T3, even on a step routed T2), and stepTimeout otherwise.
func setupStepBound(op, routedTier string) time.Duration {
	tier := routedTier
	if floor := plan.ClientFloor(op); floor != "" {
		tier = raiseTier(tier, floor)
	}
	if tier == plan.TierT3 {
		return setupT3StepTimeout
	}
	return stepTimeout
}
