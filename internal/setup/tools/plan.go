package setuptools

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"piperoom/internal/events"
	"piperoom/internal/setup/plan"
	"piperoom/internal/tools"
)

// Emitter receives the events a KindRoom setup tool produces. It is the
// agent.Sink contract (Emit) expressed here so this package and the agent
// package do not depend on each other.
//
// The room service's agent sink (room.agentEmit) does not accept these events
// as it stands: it allow-lists agent.* types only. The PR that wires these
// tools must add exactly two types to that allow-list, events.SetupPlan
// (setup.plan, emitted here) and TypeVerificationRequested
// (setup.verification_requested, emitted by setup_request_verification), and
// must never add setup.verified, which only the server's ladder runner writes.
// Until then every Emit through the room's sink is refused and the tool
// reports the error.
type Emitter interface {
	Emit(ctx context.Context, typ events.Type, payload any) error
}

// RoomDeps are the collaborators of the two KindRoom tools.
type RoomDeps struct {
	// Catalogue answers the tier floor of an op: the runner's op catalogue
	// (internal/setup/ops.Registry implements plan.Catalogue with Floor), or a
	// fake in tests. Required: without it no op can be checked. When it also
	// implements LayerCatalogue (ops.Registry does), every stored step's
	// Layer is filled from it; otherwise Layer is left empty.
	Catalogue plan.Catalogue
	// Sink receives setup.plan and the verification request. Required: a tool
	// that cannot record its result must not claim it.
	Sink Emitter
	// ClassOf returns the class of the endpoint a step resolves to
	// (protocol.ClassLocal, ClassShared, ClassProduction). Required: the plan's
	// tiers and the guard check are computed from it, and a class this tool
	// guessed could hide a guarded step. The room re-derives the class when it
	// routes a proposal; a plan whose class is wrong is refused there too.
	ClassOf func(plan.Step) (string, error)
	// Facts answers the guard's exceptions for a step (the ALTER SYSTEM
	// parameter, whether a compose_down keeps its volumes). A plan step has
	// no params, so the agent's wiring leaves it nil, and then a guard row
	// whose verdict depends on those facts is deferred to the room
	// (plan.ForbiddenAtPlan): the room decides it with plan.Forbidden and the
	// real params at Create, before Execute, on rollback and on the runner. A
	// row that refuses the class whatever the params is refused here either
	// way. A non-nil Facts decides every row at plan time from its answers,
	// with the zero StepFacts as the strictest reading.
	Facts func(plan.Step) plan.StepFacts
}

// LayerCatalogue is the optional catalogue lookup of an op's owners layer
// ("environment", ...). ops.Registry implements it with LayerOf.
type LayerCatalogue interface {
	LayerOf(op string) (layer string, ok bool)
}

// RoomToolNames returns the names of the tools RoomTools builds.
func RoomToolNames() []string {
	return []string{"setup_plan", "setup_request_verification"}
}

// RoomTools builds the two KindRoom setup tools. They are not added to the
// read-only registry: the agent adds its own KindRoom tools itself (the
// tools.Registry doc comment says so), and RoomTools is how the setup mode of
// U1-10 gets these two beside update_hypothesis, propose_fix and
// post_message.
func RoomTools(deps RoomDeps) ([]tools.Tool, error) {
	if deps.Catalogue == nil {
		return nil, errors.New("setuptools: setup_plan needs the op catalogue")
	}
	if deps.Sink == nil {
		return nil, errors.New("setuptools: the room tools need a sink")
	}
	if deps.ClassOf == nil {
		return nil, errors.New("setuptools: setup_plan needs the class of each step's endpoint")
	}
	return []tools.Tool{
		&setupPlanTool{cat: deps.Catalogue, sink: deps.Sink, classOf: deps.ClassOf, facts: deps.Facts},
		&requestVerificationTool{sink: deps.Sink},
	}, nil
}

// FixedClass returns a ClassOf that answers class for every step. It is for a
// room with one class floor (a single-target setup, tests); a room resolving
// per-endpoint classes passes its own function.
func FixedClass(class string) func(plan.Step) (string, error) {
	return func(plan.Step) (string, error) { return class, nil }
}

// setupPlanTool is setup_plan (KindRoom): the model records the plan, the
// tool checks every op against the catalogue and fills the server-owned
// fields, then the plan enters the room log as setup.plan.
type setupPlanTool struct {
	cat     plan.Catalogue
	sink    Emitter
	classOf func(plan.Step) (string, error)
	facts   func(plan.Step) plan.StepFacts
}

// Name is the tool name the model calls.
func (t *setupPlanTool) Name() string { return "setup_plan" }

// Description tells the model to plan before it proposes and to re-plan after
// a failure.
func (t *setupPlanTool) Description() string {
	return "Record the setup plan: phases of typed runner ops with one sentence each on how the step will be verified. " +
		"Every op is checked against the runner's catalogue and every step gets its tier. " +
		"Call it before propose_fix, and again (supersedes the old plan) after a failed step; a proposal whose op is not in the current plan is refused."
}

// InputSchema is the plan's schema (plan.Decode is the real validator; the
// schema keeps the model's shape bounded).
func (t *setupPlanTool) InputSchema() map[string]any {
	return map[string]any{
		"type":                 "object",
		"additionalProperties": false,
		"required":             []string{"phases"},
		"properties": map[string]any{
			// plan_id is not here: the server mints it (Run refuses one).
			"supersedes": map[string]any{"type": "string", "maxLength": plan.MaxPlanIDBytes, "pattern": planIDPattern},
			"phases": map[string]any{
				"type": "array", "minItems": 1, "maxItems": plan.MaxPhases,
				"items": map[string]any{
					"type": "object", "additionalProperties": false,
					"required": []string{"name", "steps"},
					"properties": map[string]any{
						"name": map[string]any{"type": "string", "minLength": 1, "maxLength": plan.MaxPhaseNameBytes},
						"steps": map[string]any{
							"type": "array", "minItems": 1, "maxItems": plan.MaxStepsPerPhase,
							"items": map[string]any{
								"type": "object", "additionalProperties": false,
								"required": []string{"op"},
								"properties": map[string]any{
									"op":          map[string]any{"type": "string", "minLength": 1, "maxLength": 64},
									"params_hint": map[string]any{"type": "string", "maxLength": plan.MaxParamsHintBytes},
									"verify":      map[string]any{"type": "string", "maxLength": plan.MaxVerifyBytes},
								},
							},
						},
					},
				},
			},
		},
	}
}

// Kind is KindRoom: the tool writes only to the room log (setup.plan), never
// to a customer system.
func (t *setupPlanTool) Kind() tools.Kind { return tools.KindRoom }

// Run decodes and resolves the plan, emits setup.plan and returns the stored
// payload. An op outside the catalogue (or a step the guard forbids on its
// class) is refused as bad input, so the model re-plans instead of proposing
// a step that could never run.
func (t *setupPlanTool) Run(ctx context.Context, input json.RawMessage) (tools.Result, error) {
	p, err := plan.Decode(input)
	if err != nil {
		return tools.Result{}, fmt.Errorf("%w: %w", tools.ErrBadInput, err)
	}
	// plan_id is the room's key for a plan: the SetupVerified fold drops a
	// verdict whose plan_id is not the current plan's, and pipeline_register
	// keys on a setup.verified for the current plan. A model that chose the
	// id could reuse the id of a plan whose ladder already ran and make that
	// verdict count for steps that never executed, so the id is server-owned
	// like a step's class and tier: refused on input, always minted here.
	if p.PlanID != "" {
		return tools.Result{}, fmt.Errorf("%w: setup_plan: %q is filled by the server, never by the plan's text", tools.ErrBadInput, "plan_id")
	}
	if p.Supersedes != "" && !planIDRe.MatchString(p.Supersedes) {
		return tools.Result{}, fmt.Errorf("%w: setup_plan: supersedes is not a plan id", tools.ErrBadInput)
	}
	id, err := newPlanID()
	if err != nil {
		return tools.Result{}, err
	}
	p.PlanID = id
	// Resolve fills Index and Tier; the class is Resolve's input, so set it
	// from the server's resolver before resolving rather than trusting a
	// step the model wrote (plan.Decode already refused one).
	if err := t.fillClasses(&p); err != nil {
		return tools.Result{}, err
	}
	if err := t.resolve(&p); err != nil {
		return tools.Result{}, fmt.Errorf("%w: %w", tools.ErrBadInput, err)
	}
	t.fillLayers(&p)
	payload := p.Payload()
	if err := t.sink.Emit(ctx, events.SetupPlan, payload); err != nil {
		return tools.Result{}, fmt.Errorf("setuptools: recording setup.plan: %w", err)
	}
	return tools.Result{Summary: planSummary(payload, t.deferredSteps(p)), Data: payload}, nil
}

// resolve fills the plan's indexes and tiers and checks the guard. With no
// Facts the plan step has nothing to answer a row's exception from, so a row
// whose verdict depends on the step's params is left to the room's checks
// with the real params instead of refused here (review of #1012, item E);
// a row that refuses the class unconditionally still refuses the plan.
func (t *setupPlanTool) resolve(p *plan.Plan) error {
	if t.facts == nil {
		return p.ResolveAtPlanTime(t.cat)
	}
	return p.Resolve(t.cat, t.facts)
}

// deferredSteps returns the indexes of the steps whose guard verdict the
// room decides from the step's params, so the summary can tell the model
// that planning such a step is not approval of any params it later proposes.
func (t *setupPlanTool) deferredSteps(p plan.Plan) map[int]bool {
	if t.facts != nil {
		return nil
	}
	var out map[int]bool
	for _, phase := range p.Phases {
		for _, step := range phase.Steps {
			if plan.GuardDeferred(step.Op, step.Class) {
				if out == nil {
					out = map[int]bool{}
				}
				out[step.Index] = true
			}
		}
	}
	return out
}

// fillClasses sets every step's Class from the room's resolver. It runs before
// Resolve (which refuses an unknown class), so a plan can never be stored with
// a class the model supplied or the tool guessed.
func (t *setupPlanTool) fillClasses(p *plan.Plan) error {
	index := 0
	for pi := range p.Phases {
		for si := range p.Phases[pi].Steps {
			step := &p.Phases[pi].Steps[si]
			class, err := t.classOf(*step)
			if err != nil {
				return fmt.Errorf("setuptools: setup_plan: step %d (%s): resolving its class: %w", index, clipText(step.Op), err)
			}
			step.Class = class
			index++
		}
	}
	return nil
}

// fillLayers sets every step's Layer from the catalogue when it can answer
// one. plan.Decode refused a model-written layer, so the stored value is
// always the catalogue's. Target is not filled here: it needs the resolved
// endpoint, which the room (U1-10) owns.
func (t *setupPlanTool) fillLayers(p *plan.Plan) {
	lc, ok := t.cat.(LayerCatalogue)
	if !ok {
		return
	}
	for pi := range p.Phases {
		for si := range p.Phases[pi].Steps {
			step := &p.Phases[pi].Steps[si]
			if layer, ok := lc.LayerOf(step.Op); ok {
				step.Layer = layer
			}
		}
	}
}

// planIDPattern bounds supersedes to the shape of an id this tool mints
// (pl_<hex>) or any short id of letters, digits, '_' and '-': it is stored
// in setup.plan, so it must never carry a control character or markup.
const planIDPattern = `^[A-Za-z0-9_-]{1,64}$`

var planIDRe = regexp.MustCompile(planIDPattern)

// planSummary is the one-paragraph rendering the model reads: the plan id,
// its phases and each step's op with the tier the server assigned. A step in
// deferred is marked: the guard table decides it from the params the
// proposal carries.
func planSummary(p events.SetupPlanPayload, deferred map[int]bool) string {
	var b strings.Builder
	steps := 0
	for _, phase := range p.Phases {
		steps += len(phase.Steps)
	}
	fmt.Fprintf(&b, "plan %s: %d phase(s), %d step(s)", p.PlanID, len(p.Phases), steps)
	for _, phase := range p.Phases {
		b.WriteString("\n")
		b.WriteString(clipText(phase.Name))
		b.WriteString(":")
		for _, step := range phase.Steps {
			if deferred[step.Index] {
				fmt.Fprintf(&b, " %s(%s, guard checked on the proposal's params)", step.Op, step.Tier)
				continue
			}
			fmt.Fprintf(&b, " %s(%s)", step.Op, step.Tier)
		}
	}
	return b.String()
}

// newPlanID returns a fresh plan id. plan.PlanID is at most
// plan.MaxPlanIDBytes and the room keys plans by it, so it is a random hex
// string minted here for every plan, never a timestamp the next plan could
// collide with and never text the model chose.
func newPlanID() (string, error) {
	var b [10]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("setuptools: generating a plan id: %w", err)
	}
	return "pl_" + hex.EncodeToString(b[:]), nil
}
