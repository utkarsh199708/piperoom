package room

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"piperoom/internal/actions"
	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/store"
	"piperoom/internal/tools"
)

// stepTimeout bounds each action execution (rule 4).
const stepTimeout = 30 * time.Second

// placeholderTexts are values a model or a form writes when it has nothing to
// say. They look filled in but say nothing about the risk or the undo plan, so
// a proposal may not carry one. Compared lowercased and trimmed.
var placeholderTexts = map[string]bool{
	"<no value>": true,
	"n/a":        true,
	"none":       true,
	"-":          true,
	"tbd":        true,
}

// checkPlanText rejects a risk or rollback that is empty, whitespace-only or a
// placeholder. Approvers decide on these texts, so a blank one is a validation
// error before anything is stored or logged.
func checkPlanText(field, text string) error {
	norm := strings.ToLower(strings.TrimSpace(text))
	if norm == "" {
		return validation(field + " is required")
	}
	if placeholderTexts[norm] {
		return validation(fmt.Sprintf("%s must describe the real %s, not the placeholder %q", field, field, text))
	}
	return nil
}

// sanitizeProposal applies the agent text policy to every model-written field
// of a proposal. It copies the steps, so the caller's slice is not changed.
func sanitizeProposal(p events.ProposalCreatedPayload) events.ProposalCreatedPayload {
	p.Title = sanitizeAgentText(p.Title)
	p.Risk = sanitizeAgentText(p.Risk)
	p.Rollback = sanitizeAgentText(p.Rollback)
	steps := make([]events.ProposalStep, len(p.Steps))
	for i, st := range p.Steps {
		st.Action = sanitizeAgentText(st.Action)
		st.Layer = sanitizeAgentText(st.Layer)
		st.Description = sanitizeAgentText(st.Description)
		// Verify is optional, so checkPlanText does not apply to it.
		st.Verify = truncateDetail(sanitizeAgentText(st.Verify))
		st.Params = store.SanitizeJSON(st.Params)
		steps[i] = st
	}
	p.Steps = steps
	return p
}

// CreateProposal validates every step against the actions registry and its
// scope, derives the required approval layers from the step layers, and stores
// the proposal (and its proposal.created event). It never executes anything.
func (s *Service) CreateProposal(ctx context.Context, roomID string, p events.ProposalCreatedPayload, actor events.Actor) (string, error) {
	id, _, err := s.createProposal(ctx, roomID, p, actor, nil)
	return id, err
}

// maxOpenDriverProposals caps the open driver-authored proposals of one room
// (docs/api.md: a sixth answers 429 too_many_proposals).
const maxOpenDriverProposals = 5

// CreateDriverProposal is POST /api/rooms/{id}/proposals (P3c): the driver
// authors a fix proposal. Driver only (403 not_driver), open rooms only
// (409 room_closed), never in a shadow room (400 validation), at most
// maxOpenDriverProposals open driver-authored proposals per room
// (429 too_many_proposals). It is validated exactly like an agent proposal
// (createProposal: registered action, layer match, Validate under the room's
// pipeline ctx, param keys, scope, params_hash, preview); only the typed text
// is refused rather than sanitized, and Origin is forced to
// ProposalOriginDriver, which also keeps the auto-approve policy off it
// (scheduleCheckLocked).
func (s *Service) CreateDriverProposal(ctx context.Context, user User, roomID string, p events.ProposalCreatedPayload) (events.Event, error) {
	// The title, risk, rollback and step descriptions are typed by the driver,
	// so they follow the typed-text rule (refused, not sanitized).
	if err := rejectUnstorable(p.Title, p.Risk, p.Rollback); err != nil {
		return events.Event{}, err
	}
	for _, st := range p.Steps {
		if err := rejectUnstorable(st.Description, st.Verify); err != nil {
			return events.Event{}, err
		}
	}
	p.Origin = events.ProposalOriginDriver
	// The fence below re-checks under the room's lock and is authoritative, but
	// createProposal runs the structural and Validate checks first: Validate
	// reads the pipeline's own systems, so a non-driver (or a call to a closed,
	// shadow or capped room) must be refused before that. precheckDriverProposal
	// does those checks under a short lock; the fence repeats them where the
	// write lands.
	if err := s.precheckDriverProposal(ctx, user, roomID); err != nil {
		return events.Event{}, err
	}
	_, ev, err := s.createProposal(ctx, roomID, p, s.humanActor(user), func(st *roomState) error {
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
	})
	return ev, err
}

// precheckDriverProposal refuses, before any action Validate runs, the calls
// createProposal's fence would refuse under the lock: a non-driver, a closed
// room, a shadow room, or a room at its driver-proposal cap. The fence still
// re-checks under the lock (the room may change between the checks); this
// pass only keeps a refused caller from reading the pipeline's systems through
// Validate and seeing their error text.
func (s *Service) precheckDriverProposal(ctx context.Context, user User, roomID string) error {
	s.syncRoom(ctx, roomID)
	st, err := s.lockRoom(ctx, roomID)
	if err != nil {
		return err
	}
	defer s.unlockRoom(st)
	if !s.isDriver(st, user.ID) {
		return ErrNotDriver()
	}
	if !s.roomOpen(st) {
		return roomClosed("room is closed")
	}
	if st.snap.Shadow {
		return validation("a shadow room has no proposals")
	}
	if openDriverProposals(st.snap) >= maxOpenDriverProposals {
		return tooManyProposals()
	}
	return nil
}

// openDriverProposals counts the open driver-authored proposals of a snapshot
// (Origin "driver"; see scheduleCheckLocked, which never schedules them).
func openDriverProposals(snap RoomSnapshot) int {
	n := 0
	for _, p := range snap.OpenProposals {
		if p.Origin == events.ProposalOriginDriver {
			n++
		}
	}
	return n
}

// createProposal is CreateProposal with an optional fence, checked under the
// room's lock before anything is written: the agent's proposals refuse a run
// that lost its lease (fenceLocked), the driver's refuse a non-driver, a
// shadow room or too many open driver proposals. It returns the proposal id
// and the proposal.created event.
func (s *Service) createProposal(ctx context.Context, roomID string, p events.ProposalCreatedPayload, actor events.Actor, fence func(st *roomState) error) (string, events.Event, error) {
	p = sanitizeProposal(p)
	if len(p.Steps) == 0 {
		return "", events.Event{}, validation("proposal requires at least one step")
	}
	if err := checkPlanText("risk", p.Risk); err != nil {
		return "", events.Event{}, err
	}
	if err := checkPlanText("rollback", p.Rollback); err != nil {
		return "", events.Event{}, err
	}
	// The actions of the room's own pipeline (docs/design/tenancy.md): their
	// Validate reads that pipeline's systems only.
	pipelineID := s.roomPipeline(ctx, roomID)
	acts := s.actionsFor(pipelineID)
	// Validate runs under the room's pipeline scope and room id, as the
	// executor and the preview do, so an action reads that pipeline's whole
	// config and that room's per-room client state. A setup room runs before
	// pipeline_register, so its pipeline is the provisional one
	// (owners.Provisional, design §3.2): Validate and the scope check see that
	// pipeline, never a pipeline owners.yaml does not define yet.
	setupSnap := s.setupSnapshot(ctx, roomID)
	vctx := s.actionCtx(ctx, roomID, pipelineID)
	if setupSnap != nil {
		if prov, ok := setupSnap.SetupProvisional(); ok {
			vctx = tools.WithPipeline(vctx, prov)
		}
	}
	// Structural checks first: they do not need the room state.
	// The class comes from the registry, never from the proposer (X2, X6).
	p.Class = ""
	for i, step := range p.Steps {
		a, ok := acts.Get(step.Action)
		if !ok {
			return "", events.Event{}, validation(fmt.Sprintf("unknown action %q", step.Action))
		}
		switch c := actions.ClassOf(a); {
		case c == actions.ClassFix:
		case p.Class != "" && p.Class != c:
			// One verdict per proposal: counts or throughput, not both.
			return "", events.Event{}, validation(fmt.Sprintf("step %d (%s): a proposal cannot mix recovery and performance steps; propose them separately", i, step.Action))
		default:
			p.Class = c
		}
		if step.Layer != a.Layer() {
			return "", events.Event{}, validation(fmt.Sprintf("step %d (%s): layer %q does not match the action's layer %q", i, step.Action, step.Layer, a.Layer()))
		}
		if !isKnownLayer(step.Layer) {
			return "", events.Event{}, validation(fmt.Sprintf("step %d (%s): layer %q is not a known layer", i, step.Action, step.Layer))
		}
		if err := a.Validate(vctx, step.Params); err != nil {
			return "", events.Event{}, validation(fmt.Sprintf("step %d (%s): %v", i, step.Action, err))
		}
		// Exact, case-sensitive keys only: the decoder would fold case and the
		// approver would not see which value ran (see param_keys.go).
		if err := checkParamKeys(a.ParamsSchema(), step.Params); err != nil {
			return "", events.Event{}, validation(fmt.Sprintf("step %d (%s): %v", i, step.Action, err))
		}
	}

	// Shared store: fold what other replicas wrote, reading outside the lock.
	s.syncRoom(ctx, roomID)
	st, err := s.lockRoom(ctx, roomID)
	if err != nil {
		return "", events.Event{}, err
	}
	defer s.unlockRoom(st)
	if fence != nil {
		if err := fence(st); err != nil {
			return "", events.Event{}, err
		}
	}
	if !s.roomOpen(st) {
		return "", events.Event{}, roomClosed("room is closed")
	}
	// A room opened from a webhook with an unknown pipeline has proposals
	// disabled (docs/api.md:103): the agent must not propose a fix against a
	// pipeline nothing can be scoped to.
	if s.proposalsDisabled(st) {
		return "", events.Event{}, errProposalsDisabled(st.snap.PipelineID)
	}

	// Scope check against the room's pipeline (docs/api.md): a step must
	// implement actions.Scoped and must stay inside that pipeline. A scope
	// violation is a validation error, not a server error.
	for i, step := range p.Steps {
		a, _ := acts.Get(step.Action)
		if err := s.scopeCheckRoom(&st.snap, st.snap.PipelineID, a, step.Params); err != nil {
			return "", events.Event{}, validation(fmt.Sprintf("step %d (%s): %v", i, step.Action, err))
		}
	}

	if st.snap.IsSetupRoom() {
		// The tier decides the route (T1 scheduled, T2/T3 an approver), the
		// guard table and the maintenance window are checked here for the
		// first time, and an agent proposal must come from the latest
		// setup.plan (setup_route.go; design §3.3, §3.5).
		route, rerr := resolveSetupRoute(&st.snap, acts, p.Steps, setupNeedsPlan(p.Origin), 0, s.now().UTC())
		if rerr != nil {
			return "", events.Event{}, rerr
		}
		p.RequiredLayers, p.FallbackLayers = s.setupLayers(&st.snap, route, p.Steps)
	} else {
		// The required layers are the union of the step layers: a proposal needs
		// approval from every layer it writes to, never fewer.
		required := make([]string, 0, len(p.Steps))
		seen := map[string]bool{}
		for _, step := range p.Steps {
			if !seen[step.Layer] {
				seen[step.Layer] = true
				required = append(required, step.Layer)
			}
		}
		p.RequiredLayers = required
		// Fixed at creation like the required layers; never set by the caller.
		p.FallbackLayers = s.fallbackLayersFor(st.snap.PipelineID, required)
	}

	p.ProposalID = NewID("p_")
	stepsJSON, err := json.Marshal(p.Steps)
	if err != nil {
		return "", events.Event{}, fmt.Errorf("room: marshal proposal steps: %w", err)
	}
	// The hash is always computed here, over the parsed and sanitized proposal
	// that is about to be stored. A value the caller put in ParamsHash is
	// overwritten: nobody but the server may say what the hash is (design
	// section 3.1). A number a double cannot hold exactly, or a duplicate key,
	// is a 400 now, before anything is stored.
	hash, err := ComputeParamsHash(roomID, p.ProposalID, p.Title, p.Risk, p.Rollback, p.RequiredLayers, p.Steps)
	if err != nil {
		if errors.Is(err, errParamsNumber) {
			return "", events.Event{}, validation(err.Error())
		}
		return "", events.Event{}, validation("proposal cannot be hashed: " + err.Error())
	}
	p.ParamsHash = hash
	// Row first so a crash cannot leave an event with no backing row; if the
	// event append fails we remove the row again (compensation).
	if err := s.store.CreateProposal(ctx, store.ProposalRow{
		ProposalID:     p.ProposalID,
		RoomID:         roomID,
		Title:          p.Title,
		Status:         "open",
		RequiredLayers: p.RequiredLayers,
		Steps:          stepsJSON,
		Risk:           p.Risk,
		Rollback:       p.Rollback,
		ParamsHash:     p.ParamsHash,
	}); err != nil {
		return "", events.Event{}, err
	}
	ev, err := s.emitLocked(ctx, st, roomID, events.ProposalCreated, actor, p)
	if err != nil {
		dctx, cancel := compensateCtx(ctx)
		defer cancel()
		if delErr := s.store.DeleteProposal(dctx, p.ProposalID); delErr != nil {
			s.log.Warn("room: compensate failed proposal row", "proposal", p.ProposalID, "err", delErr)
		}
		return "", events.Event{}, err
	}
	// Preview runs outside the room's lock, on the service lifetime context;
	// it waits for this call to release the lock before it appends anything.
	// A closing service previews nothing.
	if s.addWorker() {
		go func(pipelineID string, steps []events.ProposalStep) {
			defer s.wg.Done()
			s.previewProposal(s.ctx, roomID, pipelineID, p.ProposalID, steps)
		}(st.snap.PipelineID, p.Steps)
	}
	return p.ProposalID, ev, nil
}

// ApproveProposal records a layer owner's approval. When every required layer
// is approved the executor runs the steps in order (asynchronously, on the
// service lifetime context, not the request context).
//
// paramsHash is the params_hash the approver saw (design section 3.2). It is
// required: empty is 400 validation, and a value that is not the proposal's
// hash is 409 params_mismatch.
func (s *Service) ApproveProposal(ctx context.Context, user User, proposalID, layer, paramsHash string) (events.Event, error) {
	return s.ApproveProposalConfirm(ctx, user, proposalID, layer, paramsHash, "")
}

// ApproveProposalConfirm is ApproveProposal with the solo-mode confirm string
// (docs/design/auth.md section 5.3): the solo user must send
// "<pipeline_id>/<layer>". Anyone else's confirm is ignored.
func (s *Service) ApproveProposalConfirm(ctx context.Context, user User, proposalID, layer, paramsHash, confirm string) (events.Event, error) {
	if err := rejectUnstorable(layer, paramsHash); err != nil {
		return events.Event{}, err
	}
	if paramsHash == "" {
		return events.Event{}, validation(paramsRequiredMsg)
	}
	var prow store.ProposalRow
	var ev events.Event
	s.syncProposalRoom(ctx, proposalID)

	if err := func() error {
		p, st, err := s.lockProposalRoom(ctx, user, proposalID)
		if err != nil {
			return err
		}
		defer s.unlockRoom(st)
		prow = p
		if err := s.decisionLockedCheck(ctx, st, p); err != nil {
			return err
		}
		if !s.roomOpen(st) {
			return roomClosed("room is closed")
		}
		// The solo confirm text is checked after the hash (below), so a stale
		// solo user is told params_mismatch first.
		grant, err := s.authorizeProposalLayer(st, p, user, layer, confirm, false)
		if err != nil {
			return err
		}
		if !contains(p.RequiredLayers, layer) {
			return validation(fmt.Sprintf("layer %q is not required for this proposal", layer))
		}
		if approvalFor(st.snap, proposalID, layer) != "pending" {
			return alreadyDecided(fmt.Sprintf("layer %q already decided", layer))
		}
		// The hash the approver echoed must be the proposal's. A proposal
		// from before the hash existed gets its hash here, on first use.
		if err := s.checkParamsHash(ctx, &p, paramsHash, false); err != nil {
			return err
		}
		// X8: a require_staging: step needs a healthy stage of the
		// current params first; nothing is recorded otherwise.
		if err := s.stagingGateLocked(st, proposalID); err != nil {
			return err
		}
		if grant == "solo" {
			if err := checkSoloConfirm(st.snap.PipelineID, layer, confirm); err != nil {
				return err
			}
		}
		prow = p
		if ev, err = s.appendApprovalLocked(ctx, st, p, layer, s.personApprover(st, proposalID, user, layer, grant)); err != nil {
			return err
		}
		if allApproved(st.snap, proposalID) {
			// Approved in full: queue the steps on the room's single executor so
			// no two actions ever run at the same time in one room.
			s.enqueueExecutionLocked(st, prow)
		}
		return nil
	}(); err != nil {
		return events.Event{}, err
	}

	return ev, nil
}

// approver is who gives one approval and on what right: the event actor,
// the identity recorded with it (nil for the policy, which has none),
// authorizeLayer's principal detail ("user:priya", "backup:user:meera",
// "solo", "fallback", GrantAdmin or GrantPolicy for a policy), the person
// (nil for the policy), the policy name for a policy approval (PolicyAutoApprove
// or PolicySetupLocal) and the condition the append is made under (nil: none
// beyond the caller's checks).
type approver struct {
	actor  events.Actor
	ident  *events.Identity
	detail string
	policy string
	user   *User
	check  func() error
}

// personApprover is the approver of a person's approve or approve-all. A
// fallback approval is appended only while checkFallbackLocked still holds,
// so a driver change logged after the caller's check refuses it.
func (s *Service) personApprover(st *roomState, proposalID string, user User, layer, detail string) approver {
	ident := s.identityFor(user, detail)
	a := approver{actor: s.humanActor(user), ident: &ident, detail: detail, user: &user}
	if approvalGrant(detail) == events.GrantFallback {
		a.check = func() error { return s.checkFallbackLocked(st, proposalID, user, layer) }
	}
	return a
}

// appendApprovalLocked records one layer's approval: the approvals row first,
// so the store's one-decision-per-layer rule refuses a second one
// (409 already_decided), then approval.given, removing the row again when the
// event cannot be appended. a.detail is identity.grant, and
// approvalGrant(a.detail) is the event's grant (P0 item 4). A backup's
// approval also gets the room's backup note. ApproveProposalConfirm,
// ApproveAllProposal and the auto-approve policy (firePolicyLocked) all
// record through here, so an approval is the same record, log line and
// metric whichever path gave it. The caller has checked that the proposal is
// open, the layer pending and the params hash; it holds st's lock and queues
// the executor once allApproved. With a.check set the event is appended only
// while it holds (emitCheckedLocked).
//
// A person's approval also stops a pending policy countdown of the proposal:
// the fold of its approval.given does (foldScheduleDecision), on every
// replica, and the policy's fire path re-checks that fold under the room's
// lock, so there is no timer here to stop.
func (s *Service) appendApprovalLocked(ctx context.Context, st *roomState, p store.ProposalRow, layer string, a approver) (events.Event, error) {
	grant := approvalGrant(a.detail)
	rowIdent := events.Identity{Grant: a.detail}
	if a.ident != nil {
		rowIdent = *a.ident
	}
	if err := s.store.RecordApproval(ctx, approvalRow(p.ProposalID, layer, "approved", "", p.ParamsHash, rowIdent)); err != nil {
		if errors.Is(err, store.ErrAlreadyExists) {
			return events.Event{}, alreadyDecided(fmt.Sprintf("layer %q already decided", layer))
		}
		// Named, so a failure part-way through approve-all says which layer.
		return events.Event{}, fmt.Errorf("room: record approval of %s: %w", layer, err)
	}
	payload := events.ApprovalGivenPayload{
		ProposalID: p.ProposalID, Layer: layer, ParamsHash: p.ParamsHash, Identity: a.ident, Grant: grant,
	}
	if grant == events.GrantPolicy {
		payload.Policy = a.policy
		if payload.Policy == "" {
			payload.Policy = events.PolicyAutoApprove
		}
	}
	var ev events.Event
	var err error
	if a.check != nil {
		ev, err = s.emitCheckedLocked(ctx, st, p.RoomID, a.check, events.ApprovalGiven, a.actor, payload)
	} else {
		ev, err = s.emitLocked(ctx, st, p.RoomID, events.ApprovalGiven, a.actor, payload)
	}
	if err != nil {
		dctx, cancel := compensateCtx(ctx)
		defer cancel()
		if delErr := s.store.DeleteApproval(dctx, p.ProposalID, layer); delErr != nil {
			s.log.Warn("room: compensate failed approval", "proposal", p.ProposalID, "layer", layer, "err", delErr)
		}
		return events.Event{}, err
	}
	s.approvals.inc(grant)
	s.log.Info("room: approval given", "proposal", p.ProposalID, "layer", layer, "user", a.actor.ID, "grant", a.detail)
	if a.user != nil && strings.HasPrefix(a.detail, owners.GrantBackupPrefix) {
		_, err := s.emitLocked(ctx, st, p.RoomID, events.AgentMessage, systemActor, events.AgentMessagePayload{Text: backupNote(*a.user, layer, a.detail)})
		s.afterAppend(p.RoomID, "backup approval note", err, "proposal", p.ProposalID)
	}
	return ev, nil
}

// approvalGrant maps authorizeLayer's principal detail to approval.given's
// grant: the solo user's detail is "solo", the driver's on an unowned layer
// is "fallback" (authorizeProposalLayer), a policy's is "policy", a tenant
// admin's is "admin" (Events.GrantAdmin, setup rooms); every other detail is
// an owner right (owners.yaml principal or backup, or the users.yaml layers
// list).
func approvalGrant(detail string) string {
	switch detail {
	case events.GrantSolo:
		return events.GrantSolo
	case events.GrantFallback:
		return events.GrantFallback
	case events.GrantPolicy:
		return events.GrantPolicy
	case events.GrantAdmin:
		return events.GrantAdmin
	}
	return events.GrantOwner
}

// RejectProposal records a layer owner's rejection. paramsHash follows the same
// rule as in ApproveProposal: required, and 409 params_mismatch when it is not
// the proposal's hash.
//
// Whoever may approve a layer may reject it (authorizeProposalLayer), so the
// room's driver may reject a fallback_layers entry (P2b-3b); otherwise a layer
// nobody owns would keep the proposal open until the room closes. As for a
// fallback approval, the fallback rule is checked again when the event is
// appended (emitCheckedLocked), so a driver change logged in between refuses it.
func (s *Service) RejectProposal(ctx context.Context, user User, proposalID, layer, reason, paramsHash string) (events.Event, error) {
	if err := rejectUnstorable(layer, reason, paramsHash); err != nil {
		return events.Event{}, err
	}
	// An empty params_hash is allowed only for a proposal that cannot be
	// hashed; checkParamsHash decides once it has the row.
	s.syncProposalRoom(ctx, proposalID)
	p, st, err := s.lockProposalRoom(ctx, user, proposalID)
	if err != nil {
		return events.Event{}, err
	}
	defer s.unlockRoom(st)
	if err := s.decisionLockedCheck(ctx, st, p); err != nil {
		return events.Event{}, err
	}
	if !s.roomOpen(st) {
		return events.Event{}, roomClosed("room is closed")
	}
	grant, err := s.authorizeProposalLayer(st, p, user, layer, "", false)
	if err != nil {
		return events.Event{}, err
	}
	if !contains(p.RequiredLayers, layer) {
		return events.Event{}, validation(fmt.Sprintf("layer %q is not required for this proposal", layer))
	}
	if approvalFor(st.snap, proposalID, layer) != "pending" {
		return events.Event{}, alreadyDecided(fmt.Sprintf("layer %q already decided", layer))
	}
	if err := s.checkParamsHash(ctx, &p, paramsHash, true); err != nil {
		return events.Event{}, err
	}
	ident := s.identityFor(user, grant)
	if err := s.store.RecordApproval(ctx, approvalRow(proposalID, layer, "rejected", reason, p.ParamsHash, ident)); err != nil {
		if errors.Is(err, store.ErrAlreadyExists) {
			return events.Event{}, alreadyDecided(fmt.Sprintf("layer %q already decided", layer))
		}
		return events.Event{}, err
	}
	payload := events.ApprovalRejectedPayload{
		ProposalID: proposalID, Layer: layer, Reason: reason, ParamsHash: p.ParamsHash, Identity: &ident,
	}
	var ev events.Event
	if grant == events.GrantFallback {
		ev, err = s.emitCheckedLocked(ctx, st, p.RoomID, func() error {
			return s.checkFallbackLocked(st, proposalID, user, layer)
		}, events.ApprovalRejected, s.humanActor(user), payload)
	} else {
		ev, err = s.emitLocked(ctx, st, p.RoomID, events.ApprovalRejected, s.humanActor(user), payload)
	}
	if err != nil {
		dctx, cancel := compensateCtx(ctx)
		defer cancel()
		if delErr := s.store.DeleteApproval(dctx, proposalID, layer); delErr != nil {
			s.log.Warn("room: compensate failed rejection", "proposal", proposalID, "layer", layer, "err", delErr)
		}
		return events.Event{}, err
	}
	s.log.Info("room: rejection given", "proposal", proposalID, "layer", layer, "user", user.ID, "grant", grant)
	s.afterAppend(p.RoomID, "set proposal rejected", s.store.SetProposalStatus(ctx, proposalID, "rejected"), "proposal", proposalID)
	return ev, nil
}

// decisionLockedCheck refuses a decision on a proposal that is already
// decided: by its row's status, or by the room log. The log is the record
// (afterAppend): when a rejection was logged but its "rejected" status write
// was lost, the row still says open, and without the log check a later
// approval would be logged for a rejected proposal. The row is repaired then,
// best effort; the refusal does not depend on it. Caller holds st's lock, so
// the snapshot holds every event of the room this replica has folded.
func (s *Service) decisionLockedCheck(ctx context.Context, st *roomState, p store.ProposalRow) error {
	if p.Status != "open" {
		return alreadyDecided("proposal already decided")
	}
	if !st.snap.rejected[p.ProposalID] {
		return nil
	}
	if err := s.store.SetProposalStatus(ctx, p.ProposalID, "rejected"); err != nil {
		s.log.Warn("room: repair of a rejected proposal's status failed", "room", p.RoomID, "proposal", p.ProposalID, "err", err)
	} else {
		s.log.Info("room: repaired a rejected proposal's status from the log", "room", p.RoomID, "proposal", p.ProposalID)
	}
	return alreadyDecided("proposal already decided")
}

// lockProposalRoom locks the room of a proposal for a decision on it. The row
// is read first to learn the room (and to answer 404 to a person outside its
// tenant before anything else), then read again under the room's lock: every
// decision of the proposal is taken under that lock, so the status and the
// params hash the caller checks are the ones its write lands on. The caller
// releases the room with unlockRoom.
func (s *Service) lockProposalRoom(ctx context.Context, user User, proposalID string) (store.ProposalRow, *roomState, error) {
	p, err := s.store.GetProposal(ctx, proposalID)
	if err != nil {
		return store.ProposalRow{}, nil, mapStoreErr(err)
	}
	if err := s.checkVisible(ctx, user, p.RoomID); err != nil {
		return store.ProposalRow{}, nil, err
	}
	st, err := s.lockRoom(ctx, p.RoomID)
	if err != nil {
		return store.ProposalRow{}, nil, err
	}
	if p, err = s.store.GetProposal(ctx, proposalID); err != nil {
		s.unlockRoom(st)
		return store.ProposalRow{}, nil, mapStoreErr(err)
	}
	return p, st, nil
}

// executeProposal, finishStep, failStep, setProposalStatus, isRoomOpen and the
// step state machine live in service_executor.go.

func isKnownLayer(layer string) bool {
	for _, l := range events.Layers {
		if l == layer {
			return true
		}
	}
	return false
}

func contains(layers []string, layer string) bool {
	for _, l := range layers {
		if l == layer {
			return true
		}
	}
	return false
}

func approvalFor(snap RoomSnapshot, proposalID, layer string) string {
	for _, p := range snap.OpenProposals {
		if p.ProposalID == proposalID {
			if v, ok := p.Approvals[layer]; ok {
				return v
			}
			return "pending"
		}
	}
	return "pending"
}

func allApproved(snap RoomSnapshot, proposalID string) bool {
	for _, p := range snap.OpenProposals {
		if p.ProposalID == proposalID {
			return fullyApproved(p)
		}
	}
	return false
}

// fullyApproved reports whether every required layer of the proposal has
// approved it.
func fullyApproved(p Proposal) bool {
	if len(p.RequiredLayers) == 0 {
		return false
	}
	for _, l := range p.RequiredLayers {
		if p.Approvals[l] != "approved" {
			return false
		}
	}
	return true
}
