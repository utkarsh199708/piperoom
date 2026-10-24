package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"piperoom/internal/actions"
	"piperoom/internal/events"
	"piperoom/internal/setup/plan"
	setuptools "piperoom/internal/setup/tools"
	"piperoom/internal/tools"
)

// The setup mode of the run (docs/design/setup-agent.md §3.1): the bounds of
// the run, the counters each bound is checked against, and the two KindRoom
// setup tools (setup_plan, setup_request_verification). The mode switch is
// Alert.Mode == ModeSetup; the prompt frame lives in setup_prompt.go.
//
// The bounds stop the run, never fail it: on any of them Run emits
// setup.blocked with a question, goes idle and returns an error wrapping
// *BudgetError, exactly like a token-budget stop today.

// setupRun is the per-run setup state. Zero outside setup mode.
type setupRun struct {
	// bounds are the filled, validated bounds of the run (plan.Bounds, the
	// one definition shared with the room and the runner; zero fields read
	// as the design's defaults).
	bounds plan.Bounds
	// start is when the run began; the wall clock counts from it, over every
	// turn and wait.
	start time.Time
	// attempts counts consecutive failed executions since the last
	// successful execution. A new setup_plan does not reset it: after a
	// failure the model must re-plan before it proposes again (the room's
	// ErrNotInPlan), so a count that restarted with every plan could never
	// reach the bound and a fail, re-plan, fail loop would run until the wall
	// clock. It is at least as strict as a per-step count: two failures of
	// different steps stop the run at the same bound as two of one step,
	// never later.
	attempts int
	// executed counts the executed steps (every action_result) since the
	// last setup_plan: the steps the current plan spent.
	executed int
	// seen dedupes executed steps by (proposal, step), so a repeated result
	// is counted once.
	seen map[string]bool
	// req is the room's setup request (alert.Raw); reqOK says it decoded,
	// which is how the run renders its opening message and resolves plan
	// classes. A room whose request does not decode (a harness) still runs:
	// the bounds are the defaults and every plan step resolves at the lowest
	// class.
	req   events.SetupRequest
	reqOK bool
}

// newSetupRun builds the setup state of a run. b may be nil: the defaults
// apply (contract: SetupBounds zero values take the design's defaults). A
// negative bound is refused: it is a caller's bug, and a silently corrected
// negative would hide it.
func newSetupRun(b *SetupBounds) (*setupRun, error) {
	p := plan.Bounds{}
	if b != nil {
		p = plan.Bounds{
			AttemptsPerStep: b.AttemptsPerStep,
			StepsPerPlan:    b.StepsPerPlan,
			WallClock:       b.WallClock,
			TokenBudget:     b.TokenBudget,
		}
	}
	p = p.Fill()
	if err := p.Validate(); err != nil {
		return nil, fmt.Errorf("agent: %w", err)
	}
	return &setupRun{
		bounds: p,
		start:  time.Now(),
		seen:   map[string]bool{},
	}, nil
}

// setupTokenBudget is the token cap of a setup room: SetupBounds.TokenBudget,
// or the design's default when the bounds are unset. Endpoint.Run makes it
// the room's cap, so a budget stop in a setup room is the setup's budget.
func setupTokenBudget(b *SetupBounds) (int64, error) {
	if b == nil || b.TokenBudget == 0 {
		return plan.DefaultTokenBudget, nil
	}
	if b.TokenBudget < 0 {
		return 0, fmt.Errorf("agent: Options.Setup.TokenBudget is %d: a negative bound is refused", b.TokenBudget)
	}
	return b.TokenBudget, nil
}

// decodeRequest reads the room's setup request from alert.Raw, best effort:
// an undecodable Raw leaves reqOK false and the run continues with the
// defaults (a room always stores the request it was opened with; only a
// harness omits it).
func (s *setupRun) decodeRequest(alert Alert) {
	s.reqOK = false
	var req events.SetupRequest
	if len(alert.Raw) > 0 && json.Unmarshal(alert.Raw, &req) == nil {
		s.req, s.reqOK = req, true
		return
	}
	// A partial decode (a type error after some fields) must not leave
	// targets behind for the plan's class resolver.
	s.req = events.SetupRequest{}
}

// notePlan restarts the steps count when the model records a new plan
// (setup_plan): the steps bound counts the current plan's executed steps. The
// failed-attempts count is not reset (see attempts): a re-plan is how the
// model must answer a failure, so it cannot also be what clears it.
func (s *setupRun) notePlan(events.SetupPlanPayload) {
	s.executed = 0
	s.seen = map[string]bool{}
}

// observe counts the executed and failed steps of one input batch. An
// action_result is one executed step of the current plan, whoever its
// proposal came from (the ladder's canary among them): the steps bound is a
// stop, so over-counting is safe and under-counting is not. A repeated
// result of the same (proposal, step) is counted once; a retry is a new
// proposal step, so it counts again. Only an explicit failure counts toward
// attempts; an unknown outcome (OK nil) counts the execution and leaves the
// attempts count as it was.
func (s *setupRun) observe(inputs []HumanInput) {
	for _, in := range inputs {
		if in.Kind != "action_result" || in.ProposalID == "" {
			continue
		}
		key := in.ProposalID
		if in.StepIndex != nil {
			key += "/" + strconv.Itoa(*in.StepIndex)
		}
		if s.seen[key] {
			continue
		}
		s.seen[key] = true
		s.executed++
		switch {
		case in.OK == nil:
			// Unknown: neither a failure nor a success.
		case !*in.OK:
			s.attempts++
		default:
			s.attempts = 0
		}
	}
}

// exhausted reports the bound this run has reached, as one of
// events.SetupBlocked* (plan.Bounds.Exhausted, checked in the design's
// order: attempts, steps, wall clock), or "" while the run may go on. The
// token bound is the endpoint's: its stop reaches the run as a *BudgetError,
// which the run reports as setup.blocked with reason "budget".
func (s *setupRun) exhausted() string {
	return s.bounds.Exhausted(s.attempts, s.executed, time.Since(s.start), 0)
}

// stopForSetupBound ends the run on a bound: setup.blocked with a question,
// the agent message and the idle status, and the *BudgetError the run
// returns, which callers treat as a stop, not a failure (agent.Runner
// contract).
func (r *runner) stopForSetupBound(ctx context.Context, reason string) *BudgetError {
	s := r.setup
	question := setupBlockedQuestion(reason, s)
	r.log.Warn("agent: setup bound reached", "room", r.alert.RoomID, "reason", reason)
	r.emit(ctx, events.SetupBlocked, events.SetupBlockedPayload{Reason: reason, Question: question})
	r.emit(ctx, events.AgentMessage, events.AgentMessagePayload{Text: question})
	r.emit(ctx, events.AgentStatus, events.AgentStatusPayload{State: "idle", Detail: question})
	used, limit := s.usedLimit(reason)
	return &BudgetError{Used: used, Limit: limit, scope: budgetSetupRun, setupReason: reason}
}

// setupBudgetBlocked records in a setup room that a spend limit stopped the
// run: setup.blocked with reason budget. The agent message and the idle
// status are stopForBudget's, which the caller emits next, so the room reads
// one of each. The question names the limit that was reached: the setup's own
// token budget (the room's cap in a setup room, Endpoint.Run), or the text of
// a pipeline daily budget or an unavailable spend store, which a person
// raising PIPEROOM_SETUP_TOKEN_BUDGET would not cure.
func (r *runner) setupBudgetBlocked(ctx context.Context, be *BudgetError) {
	question := setupBlockedQuestion(events.SetupBlockedBudget, r.setup)
	if be.scope != budgetRoom {
		question = be.Error()
	}
	r.log.Warn("agent: setup bound reached", "room", r.alert.RoomID, "reason", events.SetupBlockedBudget)
	r.emit(ctx, events.SetupBlocked, events.SetupBlockedPayload{Reason: events.SetupBlockedBudget, Question: question})
}

// usedLimit fills the BudgetError's counts for the reason; the wall clock
// carries no meaningful pair, so it reads as reached.
func (s *setupRun) usedLimit(reason string) (used, limit int64) {
	switch reason {
	case events.SetupBlockedAttempts:
		return int64(s.attempts), int64(s.bounds.AttemptsPerStep)
	case events.SetupBlockedSteps:
		return int64(s.executed), int64(s.bounds.StepsPerPlan)
	default:
		return 1, 1
	}
}

// setupBlockedQuestion is the question setup.blocked carries, per reason.
// It is agent text a person reads; the room sanitizes it. It never names a
// secret or a value from the request.
func setupBlockedQuestion(reason string, s *setupRun) string {
	switch reason {
	case events.SetupBlockedAttempts:
		return fmt.Sprintf("This setup step has failed %d times without success. A person can steer me to a different step, record a new plan, or close the room.", s.attempts)
	case events.SetupBlockedSteps:
		return fmt.Sprintf("This setup plan has executed %d steps, its limit. A person can steer me to record a new plan or close the room.", s.executed)
	case events.SetupBlockedWallClock:
		return "The setup's time budget is spent. A person can steer me to continue or close the room."
	case events.SetupBlockedBudget:
		// The room's token count outlives the run, so a steer would only
		// start a run that stops on the same budget: offer no steer.
		return "The setup's token budget is spent, so a steer cannot restart me. A person can raise PIPEROOM_SETUP_TOKEN_BUDGET or close the room."
	}
	return "The setup run reached a bound. A person can steer me to continue."
}

// setupRoomTools builds the two KindRoom setup tools (setup_plan,
// setup_request_verification) beside update_hypothesis, propose_fix and
// post_message. They emit through the run's sink, so the room log holds the
// plan and the verification request.
//
// The catalogue and the class resolver below derive the plan's tiers from
// what the run holds: the registered actions, which are what the room routes
// a proposal on (actions.Tiered), and the request's targets. The leads kept
// them over a Deps change (review of #1012): the runner's op catalogue would
// be a second tier source beside the one that routes, and a plan step has no
// params, so no per-step endpoint exists to resolve at plan time. Every
// derived value errs on the side the room corrects rather than the side it
// cannot: a tier or class that is too low is raised by the room at proposal
// time from the step's real params, a tier or class that is too high would
// need approvals no step earned.
func (r *runner) setupRoomTools() ([]tools.Tool, error) {
	var sink setuptools.Emitter
	if r.deps.Sink != nil {
		sink = setupToolSink{inner: r.deps.Sink}
	}
	return setuptools.RoomTools(setuptools.RoomDeps{
		Catalogue: setupCatalogue{acts: r.deps.Actions},
		Sink:      sink,
		ClassOf:   setupPlanClassOf(r.setup.req.Targets),
	})
}

// setupToolSink is the Sink the two setup room tools emit through. It passes
// exactly the two setup types the agent may write, setup.plan and the
// verification request, and refuses every other type, setup.verified first
// among them (design §3.4: only the server's ladder runner writes the
// verdict). The room's sink refuses setup.verified too; this makes the rule
// hold in the agent whatever sink the run was given.
type setupToolSink struct {
	inner Sink
}

// Emit implements setuptools.Emitter.
func (s setupToolSink) Emit(ctx context.Context, typ events.Type, payload any) error {
	switch typ {
	case events.SetupPlan, setuptools.TypeVerificationRequested:
		return s.inner.Emit(ctx, typ, payload)
	}
	return fmt.Errorf("agent: a setup room tool may not emit %q", typ)
}

// setupCatalogue answers the tier floor of an op from the room's action
// registry: the floor of a Tiered action at the lowest class (its own floor,
// which TierOf keeps when it is above the class floor), the room's fixed T2
// for an action that is not Tiered, and "unknown" for an op no action can
// execute (propose_fix would refuse it too).
type setupCatalogue struct {
	acts *actions.Registry
}

// Floor implements plan.Catalogue.
func (c setupCatalogue) Floor(op string) (string, bool) {
	if c.acts == nil {
		return "", false
	}
	a, ok := c.acts.Get(op)
	if !ok {
		return "", false
	}
	if t, ok := a.(actions.Tiered); ok {
		if tier, err := t.Tier(json.RawMessage(`{}`), plan.ClassLocal); err == nil && plan.ValidTier(tier) {
			return tier, true
		}
		// The adapter refuses the empty input because its params schema
		// requires fields; every write op's floor is T1 or above, so T1 is
		// the safe under-report.
		return plan.TierT1, true
	}
	return plan.TierT2, true
}

// setupPlanClassOf resolves every plan step's class at the lowest class of
// the request's registered targets (local when none is registered): a
// step's real endpoint class is at least that, so the tier the plan stores
// is never above the tier the room derives from the step's real params at
// proposal time, and a stored tier only raises a route, never lowers one.
// The room re-checks the guard from the real params and class at Create, so
// a plan-time class that is lower than the real one hides nothing.
func setupPlanClassOf(targets []events.SetupTarget) func(plan.Step) (string, error) {
	class := plan.ClassLocal
	best := -1
	for _, t := range targets {
		r, ok := plan.ClassRank(t.Class)
		if !ok {
			continue
		}
		if best < 0 || r < best {
			class, best = t.Class, r
		}
	}
	return setuptools.FixedClass(class)
}
