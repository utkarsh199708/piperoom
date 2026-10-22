// Package plan is the setup plan model of the U wave
// (docs/design/setup-agent.md §2.1, §3.3 to §3.5): the plan the agent records
// with setup_plan, the environment classes and their floors, the tier of a
// step, the guard table, the bounds of a setup run and the verification
// ladder. It holds no I/O and no global mutable state: every function is pure
// and table-tested, so the room service (A5), the agent (A4) and the runner
// (A9) apply one definition.
//
// The plan is the model's text, so it is bounded before it is stored
// (Decode's byte and depth caps, Resolve's size caps) and every field the
// server decides — the step index, the tier — is filled here from the op's
// catalogue floor and the class of the resolved endpoint, never from the
// model. The guard table is checked on every step: a plan cannot carry a step
// the guard forbids on its class.
package plan

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"piperoom/internal/events"
)

// Caps of a plan (design §3.3). The bounds of a run stop it earlier (12
// executed steps); these bound what one setup_plan tool call may carry at
// all, so a plan is never stored larger than a person can read.
const (
	// MaxPlanBytes is the largest setup_plan input Decode accepts: 64 KiB.
	MaxPlanBytes = 64 << 10
	// MaxPlanDepth is the deepest JSON nesting Decode accepts. A plan is
	// three levels (plan, phase, step); the limit leaves room for a step's
	// fields without permitting a deeply nested payload.
	MaxPlanDepth = 8
	// MaxPhases, MaxStepsPerPhase and MaxPlanSteps bound the plan's shape.
	MaxPhases        = 8
	MaxStepsPerPhase = 16
	MaxPlanSteps     = 32
	// MaxPlanIDBytes, MaxPhaseNameBytes, MaxVerifyBytes and
	// MaxParamsHintBytes bound each model-written string. MaxVerifyBytes is
	// events.SetupPlanStep.Verify's cap.
	MaxPlanIDBytes     = 64
	MaxPhaseNameBytes  = 64
	MaxVerifyBytes     = 200
	MaxParamsHintBytes = 512
)

// Errors of Decode and Resolve.
var (
	// ErrPlan: the plan does not decode (bad JSON, an unknown field,
	// trailing data) or a field the server cannot accept.
	ErrPlan = errors.New("plan: invalid plan")
	// ErrEmptyPlan: no phases, or a phase with no steps.
	ErrEmptyPlan = errors.New("plan: a plan needs at least one step")
	// ErrUnknownOp: a step's op is not in the op catalogue.
	ErrUnknownOp = errors.New("plan: op is not in the catalogue")
	// ErrTooLarge: the plan or one of its model-written fields is over its
	// cap.
	ErrTooLarge = errors.New("plan: plan exceeds its size cap")
)

// Catalogue is the part of the op catalogue a plan needs: the op's tier
// floor. internal/setup/ops.Registry implements it with Floor, so this
// package never imports the catalogue and fakes need no runner.
type Catalogue interface {
	Floor(op string) (tier string, ok bool)
}

// Plan is the plan the agent records with setup_plan (design §3.3):
// {"phases": [{name, steps: [{op, params_hint, verify}]}], supersedes?}. The
// server fills each step's Index and Tier with Resolve and stores it as
// events.SetupPlanPayload; PlanID may be empty and the server assigns one.
// Class is the class of the endpoint the step resolved to: the server's
// input to Resolve, not part of the stored payload.
type Plan struct {
	PlanID     string  `json:"plan_id,omitempty"`
	Supersedes string  `json:"supersedes,omitempty"`
	Phases     []Phase `json:"phases"`
}

// Phase groups steps under a name ("prerequisites", "stack", "connector",
// "verify").
type Phase struct {
	Name  string `json:"name"`
	Steps []Step `json:"steps"`
}

// Step is one step of the plan. Op, ParamsHint and Verify are the model's;
// Index, Target, Class, Tier and Layer are filled by the server from the
// resolved endpoint (design §3.3) and never from the model's text: Decode
// refuses input that carries any of them, so they can only be set after the
// plan is parsed. The json tags exist so Payload round-trips in tests, not so
// the wire may fill them.
type Step struct {
	Index      int    `json:"index,omitempty"` // filled by Resolve
	Op         string `json:"op"`
	ParamsHint string `json:"params_hint,omitempty"` // model text, never stored in the event
	Verify     string `json:"verify,omitempty"`      // one sentence; the verify-after-fix check
	Target     string `json:"target,omitempty"`      // filled by the server
	Class      string `json:"class,omitempty"`       // filled by the server; Resolve's input
	Tier       string `json:"tier,omitempty"`        // filled by Resolve
	Layer      string `json:"layer,omitempty"`       // filled by the server
}

// Decode parses a setup_plan tool input. It refuses input over MaxPlanBytes,
// JSON nested deeper than MaxPlanDepth, an unknown field, trailing data and a
// step that carries a server-owned field (index, target, class, tier, layer),
// so what Resolve sees is exactly the plan and nothing hidden beside it, and
// the fields the server decides can never arrive from the model's text. The
// caps are checked before the parse, on the model's bytes.
func Decode(raw []byte) (Plan, error) {
	var p Plan
	if len(raw) == 0 {
		return p, fmt.Errorf("%w: empty input", ErrPlan)
	}
	if len(raw) > MaxPlanBytes {
		return p, fmt.Errorf("%w: %d bytes", ErrTooLarge, len(raw))
	}
	depth, err := jsonDepth(raw)
	if err != nil {
		return p, fmt.Errorf("%w: %v", ErrPlan, err)
	}
	if depth > MaxPlanDepth {
		return p, fmt.Errorf("%w: JSON depth %d", ErrTooLarge, depth)
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&p); err != nil {
		return Plan{}, fmt.Errorf("%w: %v", ErrPlan, err)
	}
	var extra any
	if err := dec.Decode(&extra); err == nil {
		return Plan{}, fmt.Errorf("%w: trailing JSON", ErrPlan)
	}
	for _, phase := range p.Phases {
		for _, step := range phase.Steps {
			if field := step.serverOwnedField(); field != "" {
				return Plan{}, fmt.Errorf("%w: %q is filled by the server, never by the plan's text", ErrPlan, field)
			}
		}
	}
	return p, nil
}

// serverOwnedField names the first server-owned field this step carries, or
// "" when it carries none. Decode calls it on every parsed step: a plan from
// the wire must leave Index, Target, Class, Tier and Layer at their zero
// values, because a model that could write them would choose its own class
// and dodge the guard table and the tier floor.
func (s *Step) serverOwnedField() string {
	switch {
	case s.Index != 0:
		return "index"
	case s.Target != "":
		return "target"
	case s.Class != "":
		return "class"
	case s.Tier != "":
		return "tier"
	case s.Layer != "":
		return "layer"
	}
	return ""
}

// jsonDepth is the deepest bracket nesting of raw, after json.Decoder has
// checked the syntax.
func jsonDepth(raw []byte) (int, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	depth, deepest := 0, 0
	for {
		tok, err := dec.Token()
		if errors.Is(err, io.EOF) {
			return deepest, nil
		}
		if err != nil {
			return 0, err
		}
		switch tok {
		case json.Delim('{'), json.Delim('['):
			depth++
			if depth > deepest {
				deepest = depth
			}
		case json.Delim('}'), json.Delim(']'):
			depth--
		}
	}
}

// Resolve validates the plan and fills what the server decides: each step's
// Index (0-based, across phases) and Tier (TierOf of the op's floor and the
// step's Class). It refuses an empty plan, a model string over its cap, an op
// not in cat, an unknown class, an unknown tier, a tier below what the class
// and the floor require (a tier can be raised, never lowered) and a step the
// guard table forbids on its class.
//
// facts answers the guard's exceptions for a step (the ALTER SYSTEM
// parameter, whether a compose_down keeps its volumes); nil means the
// strictest answer, so a guarded step is refused. A
// returned error wraps ErrEmptyPlan, ErrTooLarge, ErrUnknownOp, ErrClass,
// ErrTier or ErrGuard.
func (p *Plan) Resolve(cat Catalogue, facts func(Step) StepFacts) error {
	if cat == nil {
		return fmt.Errorf("%w: nil catalogue", ErrPlan)
	}
	if err := p.checkSize(); err != nil {
		return err
	}
	total := 0
	for pi := range p.Phases {
		phase := &p.Phases[pi]
		for si := range phase.Steps {
			step := &phase.Steps[si]
			step.Index = total
			total++
			if err := resolveStep(cat, step, facts); err != nil {
				return err
			}
		}
	}
	return nil
}

// checkSize refuses an empty plan and every cap overrun, before any lookup.
func (p *Plan) checkSize() error {
	if len(p.PlanID) > MaxPlanIDBytes {
		return fmt.Errorf("%w: plan_id is %d bytes", ErrTooLarge, len(p.PlanID))
	}
	if len(p.Supersedes) > MaxPlanIDBytes {
		return fmt.Errorf("%w: supersedes is %d bytes", ErrTooLarge, len(p.Supersedes))
	}
	if len(p.Phases) == 0 {
		return ErrEmptyPlan
	}
	if len(p.Phases) > MaxPhases {
		return fmt.Errorf("%w: %d phases (at most %d)", ErrTooLarge, len(p.Phases), MaxPhases)
	}
	steps := 0
	for _, phase := range p.Phases {
		if phase.Name == "" {
			return fmt.Errorf("%w: a phase has no name", ErrPlan)
		}
		if len(phase.Name) > MaxPhaseNameBytes {
			return fmt.Errorf("%w: a phase name is %d bytes", ErrTooLarge, len(phase.Name))
		}
		if len(phase.Steps) == 0 {
			return fmt.Errorf("%w: phase %q has no steps", ErrEmptyPlan, cleanText(phase.Name, MaxPhaseNameBytes))
		}
		if len(phase.Steps) > MaxStepsPerPhase {
			return fmt.Errorf("%w: %d steps in one phase (at most %d)", ErrTooLarge, len(phase.Steps), MaxStepsPerPhase)
		}
		steps += len(phase.Steps)
		for _, step := range phase.Steps {
			if len(step.ParamsHint) > MaxParamsHintBytes {
				return fmt.Errorf("%w: a params_hint is %d bytes", ErrTooLarge, len(step.ParamsHint))
			}
			if len(step.Verify) > MaxVerifyBytes {
				return fmt.Errorf("%w: a verify sentence is %d bytes", ErrTooLarge, len(step.Verify))
			}
		}
	}
	if steps > MaxPlanSteps {
		return fmt.Errorf("%w: %d steps (at most %d)", ErrTooLarge, steps, MaxPlanSteps)
	}
	return nil
}

// resolveStep validates one step and fills its tier.
func resolveStep(cat Catalogue, step *Step, facts func(Step) StepFacts) error {
	if step.Op == "" {
		return fmt.Errorf("%w: step %d has no op", ErrUnknownOp, step.Index)
	}
	floor, ok := cat.Floor(step.Op)
	if !ok {
		return fmt.Errorf("%w: %q", ErrUnknownOp, cleanText(step.Op, 64))
	}
	if !ValidTier(floor) {
		return fmt.Errorf("%w: the catalogue floor of %q is %q", ErrTier, cleanText(step.Op, 64), floor)
	}
	if !ValidClass(step.Class) {
		return fmt.Errorf("%w: step %d (%s) has class %q", ErrClass, step.Index, cleanText(step.Op, 64), cleanText(step.Class, 32))
	}
	want, err := TierOf(floor, step.Class)
	if err != nil {
		return fmt.Errorf("plan: step %d (%s): %w", step.Index, cleanText(step.Op, 64), err)
	}
	if step.Tier != "" {
		if !ValidTier(step.Tier) {
			return fmt.Errorf("%w: step %d has tier %q", ErrTier, step.Index, cleanText(step.Tier, 16))
		}
		if rankOf(step.Tier) < rankOf(want) {
			return fmt.Errorf("%w: step %d (%s) on %s is %s, and cannot be lowered to %s",
				ErrTier, step.Index, cleanText(step.Op, 64), step.Class, want, step.Tier)
		}
		want = step.Tier
	}
	step.Tier = want
	var f StepFacts
	if facts != nil {
		f = facts(*step)
	}
	if row, forbidden := Forbidden(GuardRequest{Op: step.Op, Class: step.Class, Facts: f}); forbidden {
		return fmt.Errorf("%w: step %d: %s on %s (%s)", ErrGuard, step.Index, cleanText(step.Op, 64), step.Class, row.Reason)
	}
	return nil
}

// Payload is the plan as the room stores it (events.SetupPlanPayload). The
// model's ParamsHint and the server's Class are not part of the payload: the
// hint is never stored, and the class is the targets' business
// (setup.target_classed), not the plan's. Every model-written string the
// payload keeps (the phase name, the verify sentence) goes through cleanText,
// so a stored plan never carries a control character or invalid UTF-8 to a
// reader.
func (p Plan) Payload() events.SetupPlanPayload {
	out := events.SetupPlanPayload{PlanID: p.PlanID, Supersedes: p.Supersedes}
	for _, phase := range p.Phases {
		stored := events.SetupPlanPhase{Name: cleanText(phase.Name, MaxPhaseNameBytes)}
		for _, step := range phase.Steps {
			stored.Steps = append(stored.Steps, events.SetupPlanStep{
				Index:  step.Index,
				Op:     step.Op,
				Target: step.Target,
				Tier:   step.Tier,
				Layer:  step.Layer,
				Verify: cleanText(step.Verify, MaxVerifyBytes),
			})
		}
		out.Phases = append(out.Phases, stored)
	}
	return out
}
