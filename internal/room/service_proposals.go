package room

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"piperoom/internal/events"
	"piperoom/internal/store"
)

// CreateProposal validates every step against the actions registry and stores
// the proposal (and its proposal.created event). It never executes anything.
func (s *Service) CreateProposal(ctx context.Context, roomID string, p events.ProposalCreatedPayload, actor events.Actor) (string, error) {
	if len(p.RequiredLayers) == 0 {
		return "", validation("proposal requires at least one approving layer")
	}
	if len(p.Steps) == 0 {
		return "", validation("proposal requires at least one step")
	}
	for i, step := range p.Steps {
		a, ok := s.actions.Get(step.Action)
		if !ok {
			return "", validation(fmt.Sprintf("unknown action %q", step.Action))
		}
		if step.Layer != a.Layer() {
			return "", validation(fmt.Sprintf("step %d (%s): layer %q does not match the action's layer %q", i, step.Action, step.Layer, a.Layer()))
		}
		if err := a.Validate(step.Params); err != nil {
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

	p.ProposalID = NewID("p_")
	if _, err := s.emitLocked(ctx, st, roomID, events.ProposalCreated, actor, p); err != nil {
		return "", err
	}
	stepsJSON, err := json.Marshal(p.Steps)
	if err != nil {
		return "", fmt.Errorf("room: marshal proposal steps: %w", err)
	}
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
	return p.ProposalID, nil
}

// ApproveProposal records a layer owner's approval. When every required layer
// is approved the executor runs the steps in order.
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
		s.executeProposal(ctx, prow.RoomID, prow)
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
		return events.Event{}, err
	}
	_ = s.store.SetProposalStatus(ctx, proposalID, "rejected")
	return ev, nil
}

// executeProposal runs the approved steps in order, stopping at the first
// failure. Each step is executed at most once: executed_count is persisted in
// the proposals table so a crash cannot cause a re-run.
func (s *Service) executeProposal(ctx context.Context, roomID string, p store.ProposalRow) {
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

	var steps []events.ProposalStep
	if err := json.Unmarshal(p.Steps, &steps); err != nil {
		_ = s.systemEmit(ctx, roomID, events.ActionExecuted, events.ActionExecutedPayload{ProposalID: p.ProposalID, StepIndex: 0, OK: false, Output: "cannot decode steps: " + err.Error()})
		return
	}

	for i := p.ExecutedCount; i < len(steps); i++ {
		step := steps[i]
		emit := func(ok bool, output string) {
			_ = s.systemEmit(ctx, roomID, events.ActionExecuted, events.ActionExecutedPayload{
				ProposalID: p.ProposalID, StepIndex: i, OK: ok, Output: output,
			})
		}
		if !s.isRoomOpen(ctx, roomID) {
			return
		}
		action, ok := s.actions.Get(step.Action)
		if !ok {
			emit(false, "unknown action "+step.Action)
			_ = s.store.RecordExecution(ctx, p.ProposalID, i+1)
			break
		}
		if err := action.Validate(step.Params); err != nil {
			emit(false, "validation failed: "+err.Error())
			_ = s.store.RecordExecution(ctx, p.ProposalID, i+1)
			break
		}
		out, err := action.Execute(ctx, step.Params)
		if err != nil {
			emit(false, outOrErr(out, err))
			_ = s.store.RecordExecution(ctx, p.ProposalID, i+1)
			break
		}
		emit(true, out)
		_ = s.store.RecordExecution(ctx, p.ProposalID, i+1)
	}
	_ = s.store.SetProposalStatus(ctx, p.ProposalID, "executed")
}

func (s *Service) isRoomOpen(ctx context.Context, roomID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := s.rooms[roomID]
	return st != nil && st.snap.Status == "open"
}

func outOrErr(out string, err error) string {
	if err != nil {
		return err.Error()
	}
	return out
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
