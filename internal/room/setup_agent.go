package room

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"piperoom/internal/actions"
	"piperoom/internal/agent"
	"piperoom/internal/events"
	"piperoom/internal/setup/plan"
	"piperoom/internal/setup/protocol"
	"piperoom/internal/tools"
)

// Setup rooms' agent runs (U1-25; docs/design/setup-agent.md §3.1, §3.8,
// §3.9). A room whose room.created has mode "setup" runs its agent in setup
// mode: Alert.Mode is agent.ModeSetup with the room's request in Raw,
// Options.Setup holds the room's bounds, and the run's tools and actions are
// the room's setup stack, built over the room's paired runner by
// Deps.SetupStack. The same stack serves the room's proposals, executor and
// verification ladder, so the action a proposal names is the one that runs.

// SetupStackInput is what Deps.SetupStack builds one setup room's registries
// from. Request is the room's request as the log has grown it: the
// provisional pipeline's tables and connectors after every
// setup.pipeline_extended, and every target with its current class. Executor
// reaches the runner paired to RoomID (Service.RunnerExecutor).
type SetupStackInput struct {
	RoomID   string
	Request  events.SetupRequest
	Executor protocol.RunnerExecutor
}

// SetupStackBuilder builds a setup room's read tools and write actions over
// its paired runner (Deps.SetupStack). It must do no I/O: the room calls it
// on the paths that resolve a room's actions.
type SetupStackBuilder func(ctx context.Context, in SetupStackInput) (PipelineStack, error)

// inputSetupVerified is the agent.HumanInput kind that carries the ladder's
// setup.verified for the room's current plan to the agent (design §3.9: on
// rungs 1 to 7 green the agent proposes pipeline_register). OK is true when
// every rung registration needs is green, and Text is the server's rendering
// of the rungs: rung notes come from tool output, so the agent reads it as
// untrusted data. The kind is not in agent.HumanInput's contract comment
// yet; the leads' contract PR U1-26 adds it, and this constant is the
// adapter until then.
const inputSetupVerified = "setup_verified"

// maxSetupVerifiedText bounds the setup_verified input's Text: eight rungs
// with a clipped note each fit well inside it.
const maxSetupVerifiedText = 4096

// setupRegistrationRungs are the rungs pipeline_register needs green: rungs 1
// to 7 of design §3.4. Rung 8 (the person's change) is not one of them.
var setupRegistrationRungs = []string{
	plan.RungConnector, plan.RungTasks, plan.RungCanaryTopic, plan.RungSnapshot,
	plan.RungCanaryEvent, plan.RungCanarySubject, plan.RungSinkRow,
}

// setupStackEntry is one setup room's cached stack. key is the request it
// was built from; a grown request builds a new one.
type setupStackEntry struct {
	roomID string
	key    string
	stack  PipelineStack
}

// setupAgentAllowed reports whether a setup room's agent may run here: setup
// on and write mode on (design §3.8). With either off no setup stack is
// built and no setup run starts.
func (s *Service) setupAgentAllowed() bool {
	return s.setupCfg.Enabled && !s.writeOff
}

// setupBounds are a setup run's bounds (agent.Options.Setup): the fixed
// attempt, step and wall-clock bounds of design §3.1 and the configured
// PIPEROOM_SETUP_TOKEN_BUDGET, the same values the room view shows.
func (s *Service) setupBounds() *agent.SetupBounds {
	budget := s.setupCfg.TokenBudget
	if budget <= 0 {
		budget = defaultSetupTokenBudget
	}
	return &agent.SetupBounds{
		AttemptsPerStep: setupAttemptsPerStep,
		StepsPerPlan:    setupStepsPerPlan,
		WallClock:       setupWallClockSeconds * time.Second,
		TokenBudget:     budget,
	}
}

// setupStackFor returns the setup stack of the open setup room that sets up
// pipelineID. ok is false when there is none to use: no builder, setup or
// write mode off, a pipeline owners.yaml knows (a registered pipeline uses
// its own client set), or no open setup room for it. err is the builder's
// refusal; the caller then fails closed.
func (s *Service) setupStackFor(pipelineID string) (PipelineStack, bool, error) {
	if s.setupStack == nil || pipelineID == "" || !s.setupAgentAllowed() || s.owners.HasPipeline(pipelineID) {
		return PipelineStack{}, false, nil
	}
	var snap *RoomSnapshot
	for _, p := range s.publishedRooms() {
		if p.Status == "open" && p.PipelineID == pipelineID && p.IsSetupRoom() {
			snap = p
			break
		}
	}
	s.setupStackMu.Lock()
	defer s.setupStackMu.Unlock()
	if snap == nil {
		delete(s.setupStacks, pipelineID)
		return PipelineStack{}, false, nil
	}
	req := setupStackRequest(snap)
	raw, err := json.Marshal(req)
	if err != nil {
		return PipelineStack{}, true, fmt.Errorf("room: setup stack of %s: encode the request: %w", snap.ID, err)
	}
	key := string(raw)
	if e, ok := s.setupStacks[pipelineID]; ok && e.roomID == snap.ID && e.key == key {
		return e.stack, true, nil
	}
	stack, err := s.setupStack(s.ctx, SetupStackInput{RoomID: snap.ID, Request: req, Executor: s.runnerExec})
	if err != nil {
		return PipelineStack{}, true, fmt.Errorf("room: setup stack of %s: %w", snap.ID, err)
	}
	if stack.Tools == nil {
		stack.Tools = tools.NewReadOnlyRegistry()
	}
	if stack.Actions == nil {
		stack.Actions = actions.NewRegistry()
	}
	stack.Staging = false // no staging stack exists before registration
	if s.setupStacks == nil {
		s.setupStacks = map[string]setupStackEntry{}
	}
	s.setupStacks[pipelineID] = setupStackEntry{roomID: snap.ID, key: key, stack: stack}
	return stack, true, nil
}

// setupToolsFor is toolsFor for the paths that serve a setup room (its agent
// run and its ladder's reads): the setup stack's read tools when there is
// one, an empty registry when it could not be built (fail closed: never the
// pipeline's client set), and toolsFor otherwise.
func (s *Service) setupToolsFor(pipelineID string) *tools.Registry {
	stack, ok, err := s.setupStackFor(pipelineID)
	switch {
	case err != nil:
		s.log.Warn("room: setup stack not built; the room gets no tools", "pipeline", pipelineID, "err", err)
		return tools.NewReadOnlyRegistry()
	case ok:
		return stack.Tools
	}
	return s.toolsFor(pipelineID)
}

// setupActionsFor is the setup half of actionsFor: the setup stack's actions
// and true for a setup room's pipeline, an empty registry and true when the
// stack could not be built, false for every other pipeline.
func (s *Service) setupActionsFor(pipelineID string) (*actions.Registry, bool) {
	stack, ok, err := s.setupStackFor(pipelineID)
	switch {
	case err != nil:
		s.log.Warn("room: setup stack not built; the room gets no actions", "pipeline", pipelineID, "err", err)
		return actions.NewRegistry(), true
	case ok:
		return stack.Actions, true
	}
	return nil, false
}

// setupRunDeps puts a setup run's registries in deps: the setup stack over
// the room's paired runner. Without a builder (a harness) the room's own
// registries are used; a builder's refusal is returned, and the run does not
// start on any other registry.
func (s *Service) setupRunDeps(deps *agent.Deps, pipelineID string) error {
	stack, ok, err := s.setupStackFor(pipelineID)
	if err != nil {
		return err
	}
	if ok {
		deps.Tools, deps.Actions = stack.Tools, stack.Actions
		return nil
	}
	deps.Tools, deps.Actions = s.toolsFor(pipelineID), s.actionsFor(pipelineID)
	return nil
}

// setupStackRequest is the room's request as the log has grown it: the
// provisional lists after every setup.pipeline_extended and every target
// (the request's and the registered ones) with its current class.
func setupStackRequest(snap *RoomSnapshot) events.SetupRequest {
	req := snap.Setup.req
	req.PipelineID = snap.PipelineID
	req.Tenant = snap.Setup.tenant
	req.TopicPrefix = snap.Setup.Provisional.TopicPrefix
	req.Tables = append([]string{}, snap.Setup.Provisional.Tables...)
	req.Connectors = append([]string{}, snap.Setup.Provisional.Connectors...)
	req.Repos = append([]string{}, snap.Setup.req.Repos...)
	req.Targets = make([]events.SetupTarget, 0, len(snap.Setup.Targets))
	for _, t := range snap.Setup.Targets {
		req.Targets = append(req.Targets, events.SetupTarget{Kind: t.Kind, Address: t.Address, Class: t.Class})
	}
	return req
}

// setupRunCtx scopes a setup run's context to the room and its provisional
// pipeline, as actionCtxFor scopes the room's action calls: the run's
// recovery and verify reads then resolve against the pipeline the person
// described. Without a provisional pipeline it is pipelineCtx.
func (s *Service) setupRunCtx(ctx context.Context, roomID, pipelineID string) context.Context {
	base := WithRoomID(s.pipelineCtx(ctx, pipelineID), roomID)
	if snap := s.published(roomID); snap != nil {
		if prov, ok := snap.SetupProvisional(); ok {
			base = tools.WithPipeline(base, prov)
		}
	}
	return base
}

// setupVerifiedInput is the "setup_verified" input of a setup.verified the
// ladder wrote for the room's current plan (snap is folded through e). A
// verified event of another plan says nothing about the one the agent works
// on and yields none.
func setupVerifiedInput(snap RoomSnapshot, e events.Event) *agent.HumanInput {
	if e.Type != events.SetupVerified || !snap.IsSetupRoom() {
		return nil
	}
	p, ok := decode[events.SetupVerifiedPayload](e)
	if !ok || p.PlanID == "" || p.PlanID != snap.Setup.PlanID {
		return nil
	}
	green := setupRegistrationGreen(p.Rungs)
	return &agent.HumanInput{
		Seq: e.Seq, At: e.At, Actor: e.Actor, Kind: inputSetupVerified,
		Text: setupVerifiedText(p, green), OK: &green,
	}
}

// setupRegistrationGreen reports whether every rung registration needs is in
// rungs and green. A missing rung is not green.
func setupRegistrationGreen(rungs []events.SetupRung) bool {
	ok := map[string]bool{}
	for _, r := range rungs {
		ok[r.Name] = r.OK
	}
	for _, name := range setupRegistrationRungs {
		if !ok[name] {
			return false
		}
	}
	return true
}

// setupVerifiedText renders the ladder's outcome for the agent: one line per
// rung with its verdict and its note. Notes come from tool output and rung
// names from the event, so both are masked and clipped; the agent wraps the
// whole text as untrusted data.
func setupVerifiedText(p events.SetupVerifiedPayload, green bool) string {
	var b strings.Builder
	fmt.Fprintf(&b, "verification of plan %s: ", cleanPlanName(p.PlanID))
	if green {
		b.WriteString("rungs 1 to 7 are green, so registration may be proposed")
	} else {
		b.WriteString("not every rung registration needs (1 to 7) is green")
	}
	for i, r := range p.Rungs {
		verdict := "ok"
		if !r.OK {
			verdict = "FAILED"
		}
		fmt.Fprintf(&b, "\n%d. %s: %s", i+1, cleanPlanName(r.Name), verdict)
		if note := strings.TrimSpace(r.Note); note != "" {
			b.WriteString(" - " + truncateDetail(maskExecOutput(note)))
		}
	}
	return cutUTF8(b.String(), maxSetupVerifiedText)
}
