package room

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"piperoom/internal/events"
	"piperoom/internal/store"
)

// stepTimeout bounds each action execution (rule 4).
const stepTimeout = 30 * time.Second

// CreateProposal validates every step against the actions registry, derives
// the required approval layers from the step layers, and stores the proposal
// (and its proposal.created event). It never executes anything.
func (s *Service) CreateProposal(ctx context.Context, roomID string, p events.ProposalCreatedPayload, actor events.Actor) (string, error) {
	if len(p.Steps) == 0 {
		return "", validation("proposal requires at least one step")
	}
	// The required layers are the union of the step layers: a proposal needs
	// approval from every layer it writes to, never fewer.
	required := make([]string, 0, len(p.Steps))
	seen := map[string]bool{}
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
		if !seen[step.Layer] {
			seen[step.Layer] = true
			required = append(required, step.Layer)
		}
	}
	p.RequiredLayers = required

	s.mu.Lock()
	defer s.mu.Unlock()
	st, err := s.getStateLocked(ctx, roomID)
	if err != nil {
		return "", err
	}
	if !s.roomOpen(st) {
		return "", roomClosed("room is closed")
	}

	p.ProposalID = NewID("p_")
	stepsJSON, err := json.Marshal(p.Steps)
	if err != nil {
		return "", fmt.Errorf("room: marshal proposal steps: %w", err)
	}
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
func (s *Service) ApproveProposal(ctx context.Context, user User, proposalID, layer string) (events.Event, error) {
	var prow store.ProposalRow
	var ev events.Event
	var shouldRun bool

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
		if !user.OwnsLayer(layer) {
			return notLayerOwner(fmt.Sprintf("user %s does not own layer %q", user.ID, layer))
		}
		if !contains(p.RequiredLayers, layer) {
			return validation(fmt.Sprintf("layer %q is not required for this proposal", layer))
		}
		if approvalFor(st.snap, proposalID, layer) != "pending" {
			return alreadyDecided(fmt.Sprintf("layer %q already decided", layer))
		}
		if err := s.store.RecordApproval(ctx, proposalID, layer, "approved", ""); err != nil {
			if errors.Is(err, store.ErrAlreadyExists) {
				return alreadyDecided(fmt.Sprintf("layer %q already decided", layer))
			}
			return err
		}
		ev, err = s.emitLocked(ctx, st, p.RoomID, events.ApprovalGiven, humanActor(user), events.ApprovalGivenPayload{
			ProposalID: proposalID, Layer: layer,
		})
		if err != nil {
			if delErr := s.store.DeleteApproval(ctx, proposalID, layer); delErr != nil {
				s.log.Warn("room: compensate failed approval", "proposal", proposalID, "layer", layer, "err", delErr)
			}
			return err
		}
		if allApproved(st.snap, proposalID) {
			shouldRun = true
		}
		return nil
	}(); err != nil {
		return events.Event{}, err
	}

	if shouldRun {
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.executeProposal(prow.RoomID, prow)
		}()
	}
	return ev, nil
}

// RejectProposal records a layer owner's rejection.
func (s *Service) RejectProposal(ctx context.Context, user User, proposalID, layer, reason string) (events.Event, error) {
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
	if !user.OwnsLayer(layer) {
		return events.Event{}, notLayerOwner(fmt.Sprintf("user %s does not own layer %q", user.ID, layer))
	}
	if !contains(p.RequiredLayers, layer) {
		return events.Event{}, validation(fmt.Sprintf("layer %q is not required for this proposal", layer))
	}
	if approvalFor(st.snap, proposalID, layer) != "pending" {
		return events.Event{}, alreadyDecided(fmt.Sprintf("layer %q already decided", layer))
	}
	if err := s.store.RecordApproval(ctx, proposalID, layer, "rejected", reason); err != nil {
		if errors.Is(err, store.ErrAlreadyExists) {
			return events.Event{}, alreadyDecided(fmt.Sprintf("layer %q already decided", layer))
		}
		return events.Event{}, err
	}
	ev, err := s.emitLocked(ctx, st, p.RoomID, events.ApprovalRejected, humanActor(user), events.ApprovalRejectedPayload{
		ProposalID: proposalID, Layer: layer, Reason: reason,
	})
	if err != nil {
		if delErr := s.store.DeleteApproval(ctx, proposalID, layer); delErr != nil {
			s.log.Warn("room: compensate failed rejection", "proposal", proposalID, "layer", layer, "err", delErr)
		}
		return events.Event{}, err
	}
	if err := s.store.SetProposalStatus(ctx, proposalID, "rejected"); err != nil {
		s.log.Warn("room: set proposal rejected failed", "proposal", proposalID, "err", err)
	}
	return ev, nil
}

// executeProposal runs the approved steps in order, stopping at the first
// failure. Each step is marked "started" in proposal_steps BEFORE it runs so a
// crash or duplicate executor can never run it twice. It runs on the service
// lifetime context with a per-step deadline.
func (s *Service) executeProposal(roomID string, p store.ProposalRow) {
	s.mu.Lock()
	st := s.rooms[roomID]
	if st == nil {
		s.mu.Unlock()
		return
	}
	if st.running[p.ProposalID] {
		s.mu.Unlock()
		return
	}
	st.running[p.ProposalID] = true
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(st.running, p.ProposalID)
		s.mu.Unlock()
	}()

	ctx := s.ctx

	var steps []events.ProposalStep
	if err := json.Unmarshal(p.Steps, &steps); err != nil {
		s.failStep(ctx, roomID, p.ProposalID, 0, "cannot decode steps: "+err.Error())
		s.setProposalStatus(ctx, p.ProposalID, "failed")
		return
	}

	for i, step := range steps {
		if !s.isRoomOpen(ctx, roomID) {
			s.setProposalStatus(ctx, p.ProposalID, "failed")
			return
		}
		// Claim the step before running it; skip an already-started step so it
		// is never re-run.
		if err := s.store.StartProposalStep(ctx, p.ProposalID, i); err != nil {
			if errors.Is(err, store.ErrAlreadyExists) {
				s.log.Warn("room: executor step already started, skipping", "proposal", p.ProposalID, "step", i)
				continue
			}
			s.failStep(ctx, roomID, p.ProposalID, i, fmt.Sprintf("cannot mark step %d started: %v", i, err))
			s.setProposalStatus(ctx, p.ProposalID, "failed")
			return
		}
		action, ok := s.actions.Get(step.Action)
		if !ok {
			s.finishStep(ctx, roomID, p.ProposalID, i, false, "unknown action "+step.Action)
			s.setProposalStatus(ctx, p.ProposalID, "failed")
			return
		}
		if err := action.Validate(step.Params); err != nil {
			s.finishStep(ctx, roomID, p.ProposalID, i, false, "validation failed: "+err.Error())
			s.setProposalStatus(ctx, p.ProposalID, "failed")
			return
		}
		sctx, cancel := context.WithTimeout(ctx, stepTimeout)
		out, err := action.Execute(sctx, step.Params)
		cancel()
		output := out
		if err != nil {
			output = err.Error()
		}
		s.finishStep(ctx, roomID, p.ProposalID, i, err == nil, output)
		if err != nil {
			s.setProposalStatus(ctx, p.ProposalID, "failed")
			return
		}
	}
	s.setProposalStatus(ctx, p.ProposalID, "executed")
}

// finishStep records a step result and emits the action.executed event.
func (s *Service) finishStep(ctx context.Context, roomID, proposalID string, i int, ok bool, output string) {
	status := "done"
	if !ok {
		status = "failed"
	}
	if err := s.store.FinishProposalStep(ctx, proposalID, i, status, output); err != nil {
		s.log.Warn("room: record step result failed", "proposal", proposalID, "step", i, "err", err)
	}
	if err := s.executorEmit(ctx, roomID, events.ActionExecuted, events.ActionExecutedPayload{
		ProposalID: proposalID, StepIndex: i, OK: ok, Output: output,
	}); err != nil {
		s.log.Warn("room: emit action.executed failed", "proposal", proposalID, "step", i, "err", err)
	}
}

// failStep emits a failing action.executed for a step that never ran.
func (s *Service) failStep(ctx context.Context, roomID, proposalID string, i int, output string) {
	if err := s.executorEmit(ctx, roomID, events.ActionExecuted, events.ActionExecutedPayload{
		ProposalID: proposalID, StepIndex: i, OK: false, Output: output,
	}); err != nil {
		s.log.Warn("room: emit action.executed failed", "proposal", proposalID, "step", i, "err", err)
	}
}

func (s *Service) setProposalStatus(ctx context.Context, proposalID, status string) {
	if err := s.store.SetProposalStatus(ctx, proposalID, status); err != nil {
		s.log.Warn("room: set proposal status failed", "proposal", proposalID, "status", status, "err", err)
	}
}

func (s *Service) isRoomOpen(ctx context.Context, roomID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := s.rooms[roomID]
	return st != nil && st.snap.Status == "open"
}

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
		if p.ProposalID != proposalID {
			continue
		}
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
	return false
}
