package room

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/gowebpki/jcs"

	"piperoom/internal/events"
	"piperoom/internal/setup/plan"
)

// "Roll back this setup" (U1-14b; docs/design/setup-agent.md §3.5,
// docs/product/setup-2026-10.md §3.4): one proposal built from the rollback
// the room recorded for every step the current setup plan executed, in reverse
// order. The proposal is an ordinary driver proposal: it is approved layer by
// layer and runs through the room's executor, and every step is routed and
// guarded again at Create and again before Execute exactly like any other
// setup step (setup_route.go). Nothing here is a second write path.
//
// Where the rollbacks come from: a setup action's preview records how to undo
// it as "<action> {json params}" (events.PreviewResult.Rollback, written once
// per proposal in proposal.previewed). The executed step itself is an
// action.executed with the plan step's index. The rollback plan is therefore
// derived from the room log alone — never from model text at rollback time,
// and never from a driver's typed params. A step whose rollback was not
// recorded is a refusal with that step's name, never a silent skip: a partial
// rollback that looks complete is worse than no rollback.
//
// The derivation reads the log from the room's first setup.plan
// (SetupSnapshot.firstPlanSeq): a plan that replaced a failed one (Supersedes)
// does not hide what the earlier plan already ran. Each executed step keeps the
// target of the plan that was current when it ran.

const (
	// setupRolledBackResolution is room.closed.resolution once the last
	// recorded rollback ran (docs/product/setup-2026-10.md §3.4: "the room
	// closes with resolution 'rolled back'").
	setupRolledBackResolution = "rolled back"
	// setupRollbackTitle is the rollback proposal's title. It is written by
	// the server, not by a person or the model.
	setupRollbackTitle = "Roll back this setup"
	// setupRollbackPage is one read of the plan's log slice; the walk stops on
	// a short page.
	setupRollbackPage = 500
	// maxSetupRollbackEvents bounds the events the derivation keeps: only
	// setup.plan, proposal.created, proposal.previewed and action.executed are
	// kept, so comments and other chatter do not count against it.
	maxSetupRollbackEvents = 20_000
	// maxSetupRollbackScan bounds how much of a room's log one derivation
	// reads at all, so a click never reads a whole 490k-event log.
	maxSetupRollbackScan = 100_000
	// maxSetupRollbackRecord caps one recorded rollback's text. It is a
	// preview field the room wrote, and the parse refuses anything larger
	// rather than carry it into params.
	maxSetupRollbackRecord = 8 << 10
)

// setupRollbackActionRe is the shape of an action name a recorded rollback may
// name: the op naming rule the catalogue keeps.
var setupRollbackActionRe = regexp.MustCompile(`^[a-z][a-z0-9_]{0,63}$`)

// setupRanStep is one step the log recorded as executed in the current setup
// plan: what ran, and the rollback the room recorded for it. Action is "" when
// the plan's slice does not hold the proposal the step belonged to (a proposal
// created before the plan), which the derivation reports rather than skips.
type setupRanStep struct {
	PlanIndex int
	// PlanTarget is the target of the plan step in the plan that was current
	// when the step ran (a later plan may renumber or drop it).
	PlanTarget string
	Action     string
	Layer      string
	Params     json.RawMessage
	Rollback   string
	// Unknown marks a step whose outcome is unknown (the runner lost it
	// mid-step): it may have run, so it counts as executed, but nothing
	// undoes it automatically and the room is never closed "rolled back"
	// while it stands.
	Unknown bool
}

// CreateSetupRollback is POST /api/rooms/{id}/setup/rollback: "roll back this
// setup" (U1-14b). It builds one proposal from the recorded rollbacks of the
// steps the current setup plan executed, newest first, and creates it through
// the ordinary proposal path, so it needs the usual approvals and runs through
// the executor. Driver only, on an open setup room, like every setup command.
//
// The guard is Create's business (resolveSetupRoute): a rollback step the
// guard forbids on its class — deleting a connector on a production target,
// say — refuses the whole proposal here, with the row's reason code, and
// nothing is created.
func (s *Service) CreateSetupRollback(ctx context.Context, user User, roomID string) (events.Event, error) {
	// The same answer every setup route gives on a server with setup off. It
	// comes before the room is read, so a disabled server says nothing about
	// any room.
	if !s.setupCfg.Enabled {
		return events.Event{}, setupDisabled()
	}
	// The same pre-check a driver-authored proposal gets, before any target is
	// resolved: a non-driver, a closed room, a shadow room or a room at its
	// driver-proposal cap learns nothing about the setup's steps.
	if err := s.precheckDriverProposal(ctx, user, roomID); err != nil {
		return events.Event{}, err
	}
	snap, err := s.readRoom(ctx, roomID)
	if err != nil {
		return events.Event{}, err
	}
	if snap == nil || !snap.IsSetupRoom() {
		return events.Event{}, notSetupRoom()
	}
	if open, ok := openSetupRollback(*snap); ok {
		return events.Event{}, setupRollbackOpen(open)
	}
	steps, unknown, err := s.setupRollbackPlan(ctx, snap)
	if err != nil {
		return events.Event{}, err
	}
	risk := fmt.Sprintf("Undoes the %d executed step(s) of this setup, newest first, on their own targets: %s. "+
		"Every step passes the guard again before it runs.",
		len(steps), setupRollbackNames(steps))
	if len(unknown) > 0 {
		risk += " Not undone here, because their outcome is unknown (they may or may not have run): " +
			truncateDetail(strings.Join(unknown, ", ")) + "; check those targets by hand. The room stays open until they are settled."
	}
	p := events.ProposalCreatedPayload{
		Title: setupRollbackTitle,
		Risk:  risk,
		Rollback: "This proposal undoes the setup; it does not undo itself. " +
			"Record a new setup plan and run its steps again if the setup is still wanted.",
		// The driver's own write: the rollback ops are not plan steps, so the
		// plan membership check would refuse them, while the guard, the window
		// and the approver rules still apply to every step (resolveSetupRoute).
		Origin: events.ProposalOriginDriver,
		Steps:  steps,
	}
	fence := s.driverProposalFence(user)
	_, ev, err := s.createProposal(ctx, roomID, p, s.humanActor(user), func(st *roomState) error {
		if err := fence(st); err != nil {
			return err
		}
		// Again under the room's lock: two clicks read the snapshot before
		// either created its proposal.
		if open, ok := openSetupRollback(st.snap); ok {
			return setupRollbackOpen(open)
		}
		return nil
	})
	if err != nil {
		return events.Event{}, err
	}
	return ev, nil
}

// driverProposalFence is the check createProposal runs under the room's lock
// before a driver's write lands: the caller drives the room, it is not a
// shadow room, and the room is under its cap of open driver proposals. It is
// shared by POST /api/rooms/{id}/proposals and the rollback proposal so both
// answer the same refusal for the same state.
func (s *Service) driverProposalFence(user User) func(st *roomState) error {
	return func(st *roomState) error {
		if !s.isDriver(st, user.ID) {
			return ErrNotDriver()
		}
		if st.snap.Shadow {
			return validation("a shadow room has no proposals")
		}
		if openDriverProposals(st.snap) >= maxOpenDriverProposals {
			return tooManyProposals()
		}
		return nil
	}
}

// setupRollbackNames lists the rollback ops for the proposal's risk text.
func setupRollbackNames(steps []events.ProposalStep) string {
	names := make([]string, 0, len(steps))
	for _, st := range steps {
		names = append(names, cleanPlanName(st.Action))
	}
	return truncateDetail(strings.Join(names, ", "))
}

// setupRollbackPlan derives the rollback proposal's steps: one step per
// executed setup step that has not been undone yet, newest first, across every
// plan the room recorded. Each step runs the action and params the room
// recorded as that step's rollback. A step whose outcome is unknown is not
// undone automatically; its name comes back in unknown.
func (s *Service) setupRollbackPlan(ctx context.Context, snap *RoomSnapshot) ([]events.ProposalStep, []string, error) {
	if snap == nil || snap.Setup == nil {
		return nil, nil, notSetupRoom()
	}
	if snap.Setup.PlanID == "" || snap.Setup.firstPlanSeq == 0 {
		return nil, nil, validation("nothing to roll back: this room has no setup plan")
	}
	ran, executed, err := s.setupRanSteps(ctx, snap)
	if err != nil {
		return nil, nil, err
	}
	switch {
	case executed == 0:
		return nil, nil, validation("nothing to roll back: no step of a setup plan was executed")
	case len(ran) == 0:
		return nil, nil, validation("nothing to roll back: every executed setup step was already rolled back")
	}
	acts := s.actionsFor(snap.PipelineID)
	steps := make([]events.ProposalStep, 0, len(ran))
	var unknown []string
	for i := len(ran) - 1; i >= 0; i-- {
		r := ran[i]
		if r.Unknown {
			unknown = append(unknown, fmt.Sprintf("step %d (%s)", r.PlanIndex+1, cleanPlanName(r.Action)))
			continue
		}
		if r.Action == "" {
			return nil, nil, validation(fmt.Sprintf(
				"step %d: the room log does not hold the proposal of this executed step, so its rollback is unknown; record a new setup plan before rolling back",
				r.PlanIndex+1))
		}
		op, params, err := setupRollbackOf(r)
		if err != nil {
			return nil, nil, validation(fmt.Sprintf("step %d (%s): %v", r.PlanIndex+1, cleanPlanName(r.Action), err))
		}
		a, ok := acts.Get(op)
		if !ok {
			return nil, nil, validation(fmt.Sprintf(
				"step %d (%s): its recorded rollback names the action %q, which this server does not have",
				r.PlanIndex+1, cleanPlanName(r.Action), cleanPlanName(op)))
		}
		ranClass := setupRanClass(snap.Setup, r)
		undo, err := routeSetupSteps(snap, acts, []events.ProposalStep{{Action: op, Layer: a.Layer(), Params: params}}, false, false, 0, s.now().UTC())
		if err != nil {
			return nil, nil, validation(fmt.Sprintf("step %d (%s): %v", r.PlanIndex+1, cleanPlanName(r.Action), err))
		}
		if undoClass := undo.Class; setupClassRank[undoClass] < setupClassRank[ranClass] {
			// The undo would be guarded at a lower class than the step ran
			// at (its params name no registered endpoint of that class), so
			// the room cannot prove the guard sees it right.
			return nil, nil, validation(fmt.Sprintf(
				"step %d (%s): it ran on a %s target, but its recorded rollback (%s) names no endpoint of that class, so the guard would see it as %s; undo it by hand",
				r.PlanIndex+1, cleanPlanName(r.Action), ranClass, cleanPlanName(op), undoClass))
		}
		steps = append(steps, events.ProposalStep{
			Action: op,
			Layer:  a.Layer(),
			Params: params,
			Description: fmt.Sprintf("undo step %d (%s): %s",
				r.PlanIndex+1, cleanPlanName(r.Action), cleanPlanName(op)),
		})
	}
	if len(steps) == 0 {
		return nil, nil, validation("nothing to roll back automatically: the outcome of " +
			truncateDetail(strings.Join(unknown, ", ")) + " is unknown (it may or may not have run); check those targets by hand")
	}
	return steps, unknown, nil
}

// setupRanClass is the class a step was routed at: the class its params
// resolve to, raised to its plan step's target class when the params name no
// registered endpoint (routeSetupSteps' rule).
func setupRanClass(setup *SetupSnapshot, r setupRanStep) string {
	target, class := setupStepResolve(setup, r.Params)
	if target == "" && r.PlanTarget != "" {
		class = classAtLeast(class, setupStepClass(setup, r.PlanTarget))
	}
	return class
}

// setupRollbackOpen is the refusal while a rollback proposal is still open.
func setupRollbackOpen(id string) error {
	return validation(fmt.Sprintf(
		"a rollback of this setup is already open (proposal %s); let it finish, or reject it, first", id))
}

// setupRanSteps reads the log slice of the current setup plan and returns the
// executed steps that are not undone yet, oldest first, plus how many plan
// steps were executed at all (rolled back ones included). The second number is
// what tells "nothing ran yet" from "everything was rolled back": both leave
// the first list empty.
func (s *Service) setupRanSteps(ctx context.Context, snap *RoomSnapshot) ([]setupRanStep, int, error) {
	evs, err := s.setupLogSince(ctx, snap.ID, snap.Setup.firstPlanSeq-1)
	if err != nil {
		return nil, 0, err
	}
	ran, executed := setupWalkRan(evs)
	return ran, executed, nil
}

// setupLogSince reads a room's events after afterSeq, a page at a time, up to
// maxSetupRollbackEvents. It stops on a short page (the log's head).
func (s *Service) setupLogSince(ctx context.Context, roomID string, afterSeq int64) ([]events.Event, error) {
	var out []events.Event
	scanned := 0
	for {
		page, err := s.store.Since(ctx, roomID, afterSeq, setupRollbackPage)
		if err != nil {
			return nil, fmt.Errorf("room: read the setup plan's log slice: %w", err)
		}
		if len(page) == 0 {
			return out, nil
		}
		scanned += len(page)
		if scanned > maxSetupRollbackScan {
			return nil, validation(fmt.Sprintf(
				"the room log holds more than %d events since the first setup plan; nothing was derived", maxSetupRollbackScan))
		}
		for _, e := range page {
			switch e.Type {
			case events.SetupPlan, events.ProposalCreated, events.ProposalPreviewed, events.ActionExecuted:
				out = append(out, e)
			}
		}
		if len(out) > maxSetupRollbackEvents {
			return nil, validation(fmt.Sprintf(
				"the room log holds more than %d proposal and step events since the first setup plan; nothing was derived", maxSetupRollbackEvents))
		}
		afterSeq = page[len(page)-1].Seq
	}
}

// setupWalkRan walks the setup's log slice and collects what it says about
// the plan steps: a setup.plan sets the plan whose targets the next steps ran
// against, a proposal.created carries a proposal's steps, a proposal.previewed
// the rollback recorded for each of them, an action.executed with a plan
// step's index says the step ran (or may have: OutcomeUnknown), and an
// action.executed outside the plan that is exactly a recorded rollback says
// that step is undone.
//
// Proposals and previews are gathered first, over the whole slice: a preview
// is written asynchronously and may land after its step executed.
//
// Every piece of the answer is server-written: the preview's rollback, the
// plan index and the step's own params. The model writes params, but only a
// proposal that was created (and so validated, guarded and approved) has them.
func setupWalkRan(evs []events.Event) ([]setupRanStep, int) {
	proposals := map[string][]events.ProposalStep{}
	previews := map[string]map[int]string{}
	for _, e := range evs {
		switch e.Type {
		case events.ProposalCreated:
			if p, ok := decode[events.ProposalCreatedPayload](e); ok {
				proposals[p.ProposalID] = p.Steps
			}
		case events.ProposalPreviewed:
			p, ok := decode[events.ProposalPreviewedPayload](e)
			if !ok {
				continue
			}
			recs := make(map[int]string, len(p.Steps))
			for _, sp := range p.Steps {
				// Reversible is what says Rollback is an action and params;
				// the prose of a step nothing can undo is not one.
				if sp.Preview != nil && sp.Preview.Reversible && strings.TrimSpace(sp.Preview.Rollback) != "" {
					recs[sp.StepIndex] = sp.Preview.Rollback
				}
			}
			previews[p.ProposalID] = recs
		}
	}
	var phases []events.SetupPlanPhase
	var ran []setupRanStep
	executed := 0
	for _, e := range evs {
		switch e.Type {
		case events.SetupPlan:
			if p, ok := decode[events.SetupPlanPayload](e); ok {
				phases = p.Phases
			}
		case events.ActionExecuted:
			p, ok := decode[events.ActionExecutedPayload](e)
			if !ok {
				continue
			}
			steps := proposals[p.ProposalID]
			found := p.StepIndex >= 0 && p.StepIndex < len(steps)
			if p.PlanStepIndex != nil {
				if !p.OK && !p.OutcomeUnknown {
					continue // refused or failed before it changed anything
				}
				executed++
				r := setupRanStep{PlanIndex: *p.PlanStepIndex, PlanTarget: planStepTarget(phases, *p.PlanStepIndex), Unknown: !p.OK}
				if found {
					step := steps[p.StepIndex]
					r.Action, r.Layer, r.Params = step.Action, step.Layer, step.Params
					r.Rollback = previews[p.ProposalID][p.StepIndex]
				}
				ran = append(ran, r)
				continue
			}
			// A write outside the plan: when it is exactly the recorded
			// rollback of a step that ran, that step is undone.
			if !p.OK || !found {
				continue
			}
			if i := setupUndoneIndex(ran, steps[p.StepIndex]); i >= 0 {
				ran = append(ran[:i], ran[i+1:]...)
			}
		}
	}
	return ran, executed
}

// setupUndoneIndex reports the newest step of ran that step undoes (its
// recorded rollback is exactly step's action and params), or -1. Newest first
// is the rollback order, so a repeated rollback of two identical steps undoes
// the one that ran last.
func setupUndoneIndex(ran []setupRanStep, step events.ProposalStep) int {
	for i := len(ran) - 1; i >= 0; i-- {
		r := ran[i]
		if r.Action == "" {
			continue
		}
		op, params, err := setupRollbackOf(r)
		if err != nil || op != step.Action {
			continue
		}
		if setupSameParams(params, step.Params) {
			return i
		}
	}
	return -1
}

// setupSameParams reports whether two params documents are the same JSON
// object, in any member order (JCS, the canonical form params_hash uses). A
// document that is not JSON is only equal to itself byte for byte.
func setupSameParams(a, b json.RawMessage) bool {
	ca, errA := jcs.Transform(a)
	cb, errB := jcs.Transform(b)
	if errA != nil || errB != nil {
		return string(a) == string(b)
	}
	return string(ca) == string(cb)
}

// setupRollbackOf resolves one executed step's rollback: the action to run and
// the params it runs with.
func setupRollbackOf(r setupRanStep) (string, json.RawMessage, error) {
	record := strings.TrimSpace(r.Rollback)
	if record == "" {
		return "", nil, errors.New("the room recorded no rollback for it, so no action can undo it")
	}
	op, obj, err := parseSetupRollback(record)
	if err != nil {
		return "", nil, err
	}
	params, err := setupRollbackParams(op, obj)
	if err != nil {
		return "", nil, err
	}
	return op, params, nil
}

// parseSetupRollback splits a recorded rollback, "<action> {json params}", as
// actions.Preview writes it (actions.rollbackText). Anything else — a sentence
// "undo it by hand", a name that is not an action name, params that are not a
// JSON object — is an error, so the rollback proposal names the step instead of
// running something it does not understand.
func parseSetupRollback(record string) (string, map[string]any, error) {
	if len(record) > maxSetupRollbackRecord {
		return "", nil, fmt.Errorf("its recorded rollback is longer than %d bytes", maxSetupRollbackRecord)
	}
	name, rest, _ := strings.Cut(strings.TrimSpace(record), " ")
	name = strings.TrimSpace(name)
	rest = strings.TrimSpace(rest)
	if !setupRollbackActionRe.MatchString(name) {
		return "", nil, fmt.Errorf("its recorded rollback is not an action and params: %q", truncateDetail(sanitizeAgentText(record)))
	}
	if !strings.HasPrefix(rest, "{") {
		return "", nil, fmt.Errorf("its recorded rollback names %q but carries no params object", cleanPlanName(name))
	}
	var obj map[string]any
	if err := json.Unmarshal([]byte(rest), &obj); err != nil || obj == nil {
		return "", nil, fmt.Errorf("its recorded rollback names %q but its params are not a JSON object", cleanPlanName(name))
	}
	return name, obj, nil
}

// setupRollbackParams builds the params the rollback step runs with: the
// recorded ones as written, and for compose_down the room's volumes-kept mark.
// Nothing else is added: an action decodes its params strictly, and an
// injected field would make it refuse its own rollback. The class the undo
// routes at is checked against the class the step ran at instead
// (setupRollbackPlan).
func setupRollbackParams(op string, recorded map[string]any) (json.RawMessage, error) {
	if op == plan.OpComposeDown {
		// The rollback of a compose_up is a compose_down that keeps its
		// volumes (U1-14b, design §2.5: no op deletes data). The catalogue has
		// no down form that removes them; the room says so in the params as
		// well, and setupKeepsVolumes reads the mark, so the guard's exception
		// for a volumes-keeping compose_down is the one that applies. Any
		// removal signal the record carried is dropped: the undo of a setup
		// never removes volumes.
		for _, key := range []string{"args", "argv", "volumes", "remove_volumes"} {
			delete(recorded, key)
		}
		recorded["volumes_kept"] = true
	}
	raw, err := json.Marshal(recorded)
	if err != nil {
		return nil, fmt.Errorf("its recorded rollback cannot be written as params: %v", err)
	}
	return raw, nil
}

// planStepTarget is a plan step's target by index in phases ("" when there is
// none).
func planStepTarget(phases []events.SetupPlanPhase, index int) string {
	for _, phase := range phases {
		for _, step := range phase.Steps {
			if step.Index == index {
				return step.Target
			}
		}
	}
	return ""
}

// openSetupRollback returns an open rollback proposal of this setup, if any.
// Any open one blocks another: while one runs, the walk already counts its
// executed steps as undone, so a second click would queue the rest twice.
func openSetupRollback(snap RoomSnapshot) (string, bool) {
	for _, p := range snap.OpenProposals {
		if p.Title == setupRollbackTitle {
			return p.ProposalID, true
		}
	}
	return "", false
}

// closeRolledBackSetup closes a setup room "rolled back" once the last step
// the current plan executed has been undone. It runs after a proposal executed
// in full (service_executor.go): the proposal itself may be the rollback
// proposal, or a later ordinary step. With steps left undone the room stays
// open — a partial rollback is visible as the rollback proposal's failed step
// and as the steps the snapshot still shows as executed, and a second
// "roll back this setup" covers what remains.
func (s *Service) closeRolledBackSetup(ctx context.Context, roomID string) {
	snap, err := s.readRoom(ctx, roomID)
	if err != nil || snap == nil || !snap.IsSetupRoom() || snap.Setup == nil || snap.Setup.firstPlanSeq == 0 {
		return
	}
	if !s.isRoomOpen(ctx, roomID) {
		return
	}
	ran, executed, err := s.setupRanSteps(ctx, snap)
	if err != nil {
		// The room stays open: a setup the room cannot prove rolled back is
		// not reported as rolled back.
		s.log.Warn("room: cannot read the setup plan's steps; the room stays open", "room", roomID, "err", err)
		return
	}
	if executed == 0 || len(ran) > 0 {
		return
	}
	_, err = s.closeRoom(ctx, roomID, systemActor, func(st *roomState) (string, error) {
		if !st.snap.IsSetupRoom() || st.snap.Setup == nil {
			return "", notSetupRoom()
		}
		return setupRolledBackResolution, nil
	})
	var re *Error
	if errors.As(err, &re) && re.Code == codeRoomClosed {
		return
	}
	if err != nil {
		s.log.Warn("room: closing a rolled-back setup room failed", "room", roomID, "err", err)
		return
	}
	s.log.Info("room: setup rolled back; room closed", "room", roomID)
}
