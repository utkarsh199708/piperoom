package room

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/store"
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
	p = sanitizeProposal(p)
	if len(p.Steps) == 0 {
		return "", validation("proposal requires at least one step")
	}
	if err := checkPlanText("risk", p.Risk); err != nil {
		return "", err
	}
	if err := checkPlanText("rollback", p.Rollback); err != nil {
		return "", err
	}
	// Structural checks first: they do not need the room.
	for i, step := range p.Steps {
		a, ok := s.actions.Get(step.Action)
		if !ok {
			return "", validation(fmt.Sprintf("unknown action %q", step.Action))
		}
		if step.Layer != a.Layer() {
			return "", validation(fmt.Sprintf("step %d (%s): layer %q does not match the action's layer %q", i, step.Action, step.Layer, a.Layer()))
		}
		if !isKnownLayer(step.Layer) {
			return "", validation(fmt.Sprintf("step %d (%s): layer %q is not a known layer", i, step.Action, step.Layer))
		}
		if err := a.Validate(step.Params); err != nil {
			return "", validation(fmt.Sprintf("step %d (%s): %v", i, step.Action, err))
		}
		// Exact, case-sensitive keys only: the decoder would fold case and the
		// approver would not see which value ran (see param_keys.go).
		if err := checkParamKeys(a.ParamsSchema(), step.Params); err != nil {
			return "", validation(fmt.Sprintf("step %d (%s): %v", i, step.Action, err))
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	st, err := s.getStateLocked(ctx, roomID)
	if err != nil {
		return "", err
	}
	if !s.roomOpen(st) {
		return "", roomClosed("room is closed")
	}
	// A room opened from a webhook with an unknown pipeline has proposals
	// disabled (docs/api.md:103): the agent must not propose a fix against a
	// pipeline nothing can be scoped to.
	if s.proposalsDisabled(st) {
		return "", validation(fmt.Sprintf("pipeline %q is unknown; proposals are disabled for this room", st.snap.PipelineID))
	}

	// Scope check against the room's pipeline (docs/api.md): a step must
	// implement actions.Scoped and must stay inside that pipeline. A scope
	// violation is a validation error, not a server error.
	for i, step := range p.Steps {
		a, _ := s.actions.Get(step.Action)
		if err := s.scopeCheck(st.snap.PipelineID, a, step.Params); err != nil {
			return "", validation(fmt.Sprintf("step %d (%s): %v", i, step.Action, err))
		}
	}

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

	p.ProposalID = NewID("p_")
	stepsJSON, err := json.Marshal(p.Steps)
	if err != nil {
		return "", fmt.Errorf("room: marshal proposal steps: %w", err)
	}
	// The hash is always computed here, over the parsed and sanitized proposal
	// that is about to be stored. A value the caller put in ParamsHash is
	// overwritten: nobody but the server may say what the hash is (design
	// section 3.1). A number a double cannot hold exactly, or a duplicate key,
	// is a 400 now, before anything is stored.
	hash, err := ComputeParamsHash(roomID, p.ProposalID, p.Title, p.Risk, p.Rollback, p.RequiredLayers, p.Steps)
	if err != nil {
		if errors.Is(err, errParamsNumber) {
			return "", validation(err.Error())
		}
		return "", validation("proposal cannot be hashed: " + err.Error())
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
		return "", err
	}
	if _, err := s.emitLocked(ctx, st, roomID, events.ProposalCreated, actor, p); err != nil {
		if delErr := s.store.DeleteProposal(ctx, p.ProposalID); delErr != nil {
			s.log.Warn("room: compensate failed proposal row", "proposal", p.ProposalID, "err", delErr)
		}
		return "", err
	}
	return p.ProposalID, nil
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

	if err := func() error {
		s.mu.Lock()
		defer s.mu.Unlock()

		p, err := s.store.GetProposal(ctx, proposalID)
		if err != nil {
			return mapStoreErr(err)
		}
		prow = p
		if p.Status != "open" {
			return alreadyDecided("proposal already decided")
		}
		st, err := s.getStateLocked(ctx, p.RoomID)
		if err != nil {
			return err
		}
		if !s.roomOpen(st) {
			return roomClosed("room is closed")
		}
		// The solo confirm text is checked after the hash (below), so a stale
		// solo user is told params_mismatch first.
		grant, err := s.authorizeLayer(st.snap.PipelineID, user, layer, confirm, false)
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
		if grant == "solo" {
			if err := checkSoloConfirm(st.snap.PipelineID, layer, confirm); err != nil {
				return err
			}
		}
		prow = p
		ident := identityFor(user, grant)
		if err := s.store.RecordApproval(ctx, approvalRow(proposalID, layer, "approved", "", p.ParamsHash, ident)); err != nil {
			if errors.Is(err, store.ErrAlreadyExists) {
				return alreadyDecided(fmt.Sprintf("layer %q already decided", layer))
			}
			return err
		}
		ev, err = s.emitLocked(ctx, st, p.RoomID, events.ApprovalGiven, humanActor(user), events.ApprovalGivenPayload{
			ProposalID: proposalID, Layer: layer, ParamsHash: p.ParamsHash, Identity: &ident,
		})
		if err != nil {
			if delErr := s.store.DeleteApproval(ctx, proposalID, layer); delErr != nil {
				s.log.Warn("room: compensate failed approval", "proposal", proposalID, "layer", layer, "err", delErr)
			}
			return err
		}
		s.log.Info("room: approval given", "proposal", proposalID, "layer", layer, "user", user.ID, "grant", grant)
		if strings.HasPrefix(grant, owners.GrantBackupPrefix) {
			if _, err := s.emitLocked(ctx, st, p.RoomID, events.AgentMessage, systemActor, events.AgentMessagePayload{Text: backupNote(user, layer, grant)}); err != nil {
				s.log.Warn("room: backup approval note failed", "proposal", proposalID, "err", err)
			}
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

// RejectProposal records a layer owner's rejection. paramsHash follows the same
// rule as in ApproveProposal: required, and 409 params_mismatch when it is not
// the proposal's hash.
func (s *Service) RejectProposal(ctx context.Context, user User, proposalID, layer, reason, paramsHash string) (events.Event, error) {
	if err := rejectUnstorable(layer, reason, paramsHash); err != nil {
		return events.Event{}, err
	}
	// An empty params_hash is allowed only for a proposal that cannot be
	// hashed; checkParamsHash decides once it has the row.
	s.mu.Lock()
	defer s.mu.Unlock()

	p, err := s.store.GetProposal(ctx, proposalID)
	if err != nil {
		return events.Event{}, mapStoreErr(err)
	}
	if p.Status != "open" {
		return events.Event{}, alreadyDecided("proposal already decided")
	}
	st, err := s.getStateLocked(ctx, p.RoomID)
	if err != nil {
		return events.Event{}, err
	}
	if !s.roomOpen(st) {
		return events.Event{}, roomClosed("room is closed")
	}
	grant, err := s.authorizeLayer(st.snap.PipelineID, user, layer, "", false)
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
	ident := identityFor(user, grant)
	if err := s.store.RecordApproval(ctx, approvalRow(proposalID, layer, "rejected", reason, p.ParamsHash, ident)); err != nil {
		if errors.Is(err, store.ErrAlreadyExists) {
			return events.Event{}, alreadyDecided(fmt.Sprintf("layer %q already decided", layer))
		}
		return events.Event{}, err
	}
	ev, err := s.emitLocked(ctx, st, p.RoomID, events.ApprovalRejected, humanActor(user), events.ApprovalRejectedPayload{
		ProposalID: proposalID, Layer: layer, Reason: reason, ParamsHash: p.ParamsHash, Identity: &ident,
	})
	if err != nil {
		if delErr := s.store.DeleteApproval(ctx, proposalID, layer); delErr != nil {
			s.log.Warn("room: compensate failed rejection", "proposal", proposalID, "layer", layer, "err", delErr)
		}
		return events.Event{}, err
	}
	s.log.Info("room: rejection given", "proposal", proposalID, "layer", layer, "user", user.ID, "grant", grant)
	if err := s.store.SetProposalStatus(ctx, proposalID, "rejected"); err != nil {
		s.log.Warn("room: set proposal rejected failed", "proposal", proposalID, "err", err)
	}
	return ev, nil
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
