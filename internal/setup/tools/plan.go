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
	"piperoom/internal/setup/catalog"
	"piperoom/internal/setup/plan"
	"piperoom/internal/setup/protocol"
	"piperoom/internal/tools"
)

// Names of the KindRoom tools.
const (
	PlanToolName   = "setup_plan"
	VerifyToolName = "setup_request_verification"
)

// Sink is the agent's event sink (agent.Sink has this method set; the
// package does not import internal/agent so the agent can import this one).
type Sink interface {
	Emit(ctx context.Context, typ events.Type, payload any) error
}

// PlanConfig configures setup_plan.
type PlanConfig struct {
	// Sink receives the setup.plan event; required.
	Sink Sink
	// Catalogue lists the ops a plan may name; nil means PlanCatalogue().
	Catalogue *catalog.Catalogue
	// Targets returns the room's targets with their classes (the
	// SetupRequest's and every setup.target_registered so far). The tool
	// resolves each step's endpoint and class from them, never from the
	// model's text. Nil means none: every step then resolves to no target at
	// class shared, the room's own reading of an unregistered endpoint.
	Targets func(ctx context.Context) []events.SetupTarget
}

// planIDRe is the form of a plan id this tool assigns, and so of the only
// supersedes value it accepts.
var planIDRe = regexp.MustCompile(`^plan_[0-9a-f]{16}$`)

// NewPlanTool returns setup_plan.
func NewPlanTool(cfg PlanConfig) (tools.Tool, error) {
	if cfg.Sink == nil {
		return nil, errors.New("setuptools: setup_plan needs the agent's sink")
	}
	cat := cfg.Catalogue
	if cat == nil {
		var err error
		if cat, err = PlanCatalogue(); err != nil {
			return nil, err
		}
	}
	return &planTool{sink: cfg.Sink, cat: cat, targets: cfg.Targets}, nil
}

type planTool struct {
	sink    Sink
	cat     *catalog.Catalogue
	targets func(ctx context.Context) []events.SetupTarget
}

func (t *planTool) Name() string { return PlanToolName }

func (t *planTool) Description() string {
	return "Record the setup plan before proposing any write: phases of steps, each naming one catalogue op " +
		"(the setup ops and the Q actions), a short params_hint and the one-sentence check that proves the step worked. " +
		"The server fills each step's target, tier and layer from the room's registered targets. " +
		"After a failed step, record a new plan with supersedes set to the plan it replaces before the next proposal. " +
		"An op outside the catalogue, or one the guard table forbids on its target's class, is refused."
}

func (t *planTool) InputSchema() map[string]any {
	str := func(max int) map[string]any { return map[string]any{"type": "string", "maxLength": max} }
	step := map[string]any{
		"type": "object", "additionalProperties": false, "required": []string{"op"},
		"properties": map[string]any{
			"op":          str(64),
			"params_hint": str(plan.MaxParamsHintBytes),
			"verify":      str(plan.MaxVerifyBytes),
		},
	}
	phase := map[string]any{
		"type": "object", "additionalProperties": false, "required": []string{"name", "steps"},
		"properties": map[string]any{
			"name":  str(plan.MaxPhaseNameBytes),
			"steps": map[string]any{"type": "array", "minItems": 1, "maxItems": plan.MaxStepsPerPhase, "items": step},
		},
	}
	return map[string]any{
		"type": "object", "additionalProperties": false, "required": []string{"phases"},
		"properties": map[string]any{
			"phases":     map[string]any{"type": "array", "minItems": 1, "maxItems": plan.MaxPhases, "items": phase},
			"supersedes": map[string]any{"type": "string", "pattern": planIDRe.String()},
		},
	}
}

func (t *planTool) Kind() tools.Kind { return tools.KindRoom }

// Run decodes the plan (plan.Decode refuses every server-owned field), fills
// each step's target, class and layer from the catalogue and the room's
// targets, resolves the tiers (plan.Resolve refuses an op outside the
// catalogue, a step the guard forbids and every cap overrun), assigns the plan
// id and emits setup.plan. A plan the model must fix wraps tools.ErrBadInput;
// a sink failure does not.
func (t *planTool) Run(ctx context.Context, input json.RawMessage) (tools.Result, error) {
	p, err := plan.Decode(input)
	if err != nil {
		return tools.Result{}, badPlan(err)
	}
	if p.PlanID != "" {
		return tools.Result{}, badPlan(errors.New("plan_id is assigned by the server; leave it out"))
	}
	if p.Supersedes != "" && !planIDRe.MatchString(p.Supersedes) {
		return tools.Result{}, badPlan(errors.New("supersedes must be the plan_id of an earlier setup_plan result"))
	}
	var targets []events.SetupTarget
	if t.targets != nil {
		targets = t.targets(ctx)
	}
	for pi := range p.Phases {
		for si := range p.Phases[pi].Steps {
			step := &p.Phases[pi].Steps[si]
			spec, ok := t.cat.Spec(step.Op)
			if !ok {
				continue // Resolve refuses it with plan.ErrUnknownOp
			}
			step.Target, step.Class = resolveTarget(spec.Target, targets)
			step.Layer = spec.Layer
		}
	}
	// No guard facts: a plan step carries no params yet, so every guard row's
	// exception reads its strictest answer. The room re-checks the guard with
	// the real params at Create and Execute.
	if err := p.Resolve(t.cat, nil); err != nil {
		return tools.Result{}, badPlan(err)
	}
	if p.PlanID, err = newPlanID(); err != nil {
		return tools.Result{}, err
	}
	payload := p.Payload()
	if err := t.sink.Emit(ctx, events.SetupPlan, payload); err != nil {
		return tools.Result{}, fmt.Errorf("setuptools: %s: recording the plan: %w", PlanToolName, err)
	}
	summary, cut := tools.Truncate(planSummary(payload))
	return tools.Result{Summary: summary, Data: payload, Truncated: cut}, nil
}

// resolveTarget returns the registered target a step of kind reaches and the
// class it runs at. Exactly one target of the kind names it; several leave the
// target to the proposal's params, at the highest of their classes, so a step
// never routes below the riskiest endpoint it might reach. No target of the
// kind (the runner's own project directory, a kind the room has not
// registered) is class shared: the room never reads an unregistered endpoint
// as local, and a person raises a class, never the model.
func resolveTarget(kind string, targets []events.SetupTarget) (target, class string) {
	var matches []events.SetupTarget
	if kind != "" {
		for _, tg := range targets {
			if tg.Kind == kind {
				matches = append(matches, tg)
			}
		}
	}
	if len(matches) == 0 {
		return "", protocol.ClassShared
	}
	class = ""
	for _, tg := range matches {
		c := tg.Class
		if !plan.ValidClass(c) {
			c = protocol.ClassShared
		}
		if class == "" || classRank(c) > classRank(class) {
			class = c
		}
	}
	if len(matches) == 1 {
		target = matches[0].Address
	}
	return target, class
}

func classRank(c string) int {
	r, _ := plan.ClassRank(c)
	return r
}

// planSummary is the model-readable plan: the id to supersede later and one
// line per step with the server's tier and target.
func planSummary(p events.SetupPlanPayload) string {
	var b strings.Builder
	fmt.Fprintf(&b, "setup plan %s recorded", p.PlanID)
	if p.Supersedes != "" {
		fmt.Fprintf(&b, " (supersedes %s)", p.Supersedes)
	}
	b.WriteString(". Propose only these steps; T0 steps are tools, never proposals.\n")
	for _, ph := range p.Phases {
		fmt.Fprintf(&b, "%s:\n", ph.Name)
		for _, s := range ph.Steps {
			target := s.Target
			if target == "" {
				target = "(no registered target)"
			}
			fmt.Fprintf(&b, "  #%d %s %s on %s, layer %s\n", s.Index, s.Op, s.Tier, target, s.Layer)
		}
	}
	return b.String()
}

// badPlan wraps a plan the model must fix.
func badPlan(err error) error {
	return fmt.Errorf("setuptools: %s: %w: %w", PlanToolName, tools.ErrBadInput, err)
}

// newPlanID returns a fresh plan id; the model never chooses one, so a plan id
// in the room log is one this server issued.
func newPlanID() (string, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("setuptools: generating a plan id: %w", err)
	}
	return "plan_" + hex.EncodeToString(b[:]), nil
}
