package room

import (
	"context"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"time"

	"piperoom/internal/events"
)

const (
	traceTimeout = 20 * time.Second
	maxOutput    = 1000
)

func (s *Service) roomOfProposal(pid string) (*roomEntry, error) {
	s.idxMu.Lock()
	id, ok := s.proposalRoom[pid]
	s.idxMu.Unlock()
	if !ok {
		return nil, newErr(http.StatusNotFound, "not_found", "proposal not found")
	}
	return s.room(id)
}

// Approve records one layer owner's approval; when every required layer has
// approved, the steps run in the background in order.
func (s *Service) Approve(ctx context.Context, u User, proposalID, layer string) (events.Event, error) {
	return s.decide(ctx, u, proposalID, layer, false, "")
}

// Reject records one layer owner's rejection; the proposal can no longer run.
func (s *Service) Reject(ctx context.Context, u User, proposalID, layer, reason string) (events.Event, error) {
	reason = strings.TrimSpace(reason)
	if len([]rune(reason)) > maxTextRunes {
		return events.Event{}, validation("reason is too long")
	}
	return s.decide(ctx, u, proposalID, layer, true, reason)
}

func (s *Service) decide(ctx context.Context, u User, pid, layer string, reject bool, reason string) (events.Event, error) {
	r, err := s.roomOfProposal(pid)
	if err != nil {
		return events.Event{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := s.writable(r); err != nil {
		return events.Event{}, err
	}
	ps := r.st.proposals[pid]
	if ps == nil {
		return events.Event{}, newErr(http.StatusNotFound, "not_found", "proposal not found")
	}
	if !slices.Contains(events.Layers, layer) {
		return events.Event{}, validation("unknown layer")
	}
	// Ownership comes from the users file, never from the request.
	if !slices.Contains(u.Layers, layer) {
		return events.Event{}, newErr(httpForbidden, "not_layer_owner", "You do not own this layer.")
	}
	if !slices.Contains(ps.payload.RequiredLayers, layer) {
		return events.Event{}, validation("this proposal does not need that layer")
	}
	if ps.finished() || ps.approvals[layer] != approvalPending {
		return events.Event{}, newErr(httpConflict, "already_decided", "This layer already decided, or the proposal is finished.")
	}
	if reject {
		return s.appendLocked(ctx, r, events.ApprovalRejected, u.Actor(),
			events.ApprovalRejectedPayload{ProposalID: pid, Layer: layer, Reason: reason})
	}
	ev, err := s.appendLocked(ctx, r, events.ApprovalGiven, u.Actor(), events.ApprovalGivenPayload{ProposalID: pid, Layer: layer})
	if err != nil {
		return events.Event{}, err
	}
	if ps.allApproved() && !ps.started {
		ps.started = true
		s.wg.Add(1)
		s.exec.Add(1)
		go func() {
			defer s.wg.Done()
			defer s.exec.Done()
			s.execute(r, pid)
		}()
	}
	return ev, nil
}

// execute runs an approved proposal's steps in order and stops at the first
// failure. A step that already has an action.executed event, or that this
// process already started, is never run again.
func (s *Service) execute(r *roomEntry, pid string) {
	ctx := s.base
	r.mu.Lock()
	ps := r.st.proposals[pid]
	steps := slices.Clone(ps.payload.Steps)
	pipeID := r.st.snap.PipelineID
	r.mu.Unlock()
	pipe, pipeOK := s.cfg.Owners.Pipeline(pipeID)

	for i, st := range steps {
		r.mu.Lock()
		done, stop := ps.done[i], ps.failed || r.st.snap.Status != StatusOpen
		r.mu.Unlock()
		if done {
			continue
		}
		if stop {
			return
		}
		ok, out := true, ""
		a, found := s.cfg.Actions.Get(st.Action)
		switch {
		case !found:
			ok, out = false, "action no longer registered"
		case !pipeOK:
			ok, out = false, "the room's pipeline is unknown"
		default:
			// Validate and scope again right before Execute (docs/api.md): the
			// systems may have changed since the proposal was made.
			if err := checkStep(a, pipe, st.Params); err != nil {
				ok, out = false, "refused: "+err.Error()
			} else {
				ectx, cancel := context.WithTimeout(ctx, s.cfg.ExecTimeout)
				res, err := a.Execute(ectx, st.Params)
				cancel()
				if err != nil {
					ok, out = false, err.Error()
				} else {
					out = res
				}
			}
		}
		if len(out) > maxOutput {
			out = out[:maxOutput] + "…"
		}
		r.mu.Lock()
		_, err := s.appendLocked(ctx, r, events.ActionExecuted, systemActor,
			events.ActionExecutedPayload{ProposalID: pid, StepIndex: i, OK: ok, Output: out})
		r.mu.Unlock()
		if err != nil {
			s.log.Error("record action result", "room", r.id, "proposal", pid, "step", i, "err", err)
			return
		}
		if !ok {
			return
		}
	}
}

// CreateHandoff asks the agent for a summary and offers the driver role to
// another user (driver only).
func (s *Service) CreateHandoff(ctx context.Context, u User, roomID string, in HandoffInput) (events.Event, error) {
	to, ok := s.cfg.Users.Get(in.To)
	if !ok {
		return events.Event{}, validation("unknown user in 'to'")
	}
	if to.ID == u.ID {
		return events.Event{}, validation("cannot hand off to yourself")
	}
	r, err := s.room(roomID)
	if err != nil {
		return events.Event{}, err
	}
	r.mu.Lock()
	err = s.driverGuard(ctx, r, u)
	r.mu.Unlock()
	if err != nil {
		return events.Event{}, err
	}
	if s.cfg.Handoff == nil {
		return events.Event{}, newErr(http.StatusServiceUnavailable, "unavailable", "Hand-off summaries are not configured.")
	}
	evs, err := s.allEvents(ctx, roomID)
	if err != nil {
		return events.Event{}, fmt.Errorf("room: read log for hand-off: %w", err)
	}
	hctx, cancel := context.WithTimeout(ctx, s.cfg.HandoffTimeout)
	defer cancel()
	summary, err := s.cfg.Handoff(hctx, evs, s.cfg.AgentOptions)
	if err != nil {
		s.log.Error("hand-off summary failed", "room", roomID, "err", err)
		return events.Event{}, newErr(http.StatusBadGateway, "summary_failed", "The hand-off summary could not be written. Try again.")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := s.writable(r); err != nil {
		return events.Event{}, err
	}
	// The summary took a while: the driver may have changed in the meantime.
	if d := r.st.snap.Driver; d == nil || d.ID != u.ID {
		return events.Event{}, newErr(httpConflict, "stale_handoff", "You are no longer the driver.")
	}
	return s.appendLocked(ctx, r, events.HandoffCreated, u.Actor(), events.HandoffCreatedPayload{
		HandoffID: newID("h_", s.now()), To: to.Actor(), Summary: summary, StayAsWatcher: in.StayAsWatcher,
	})
}

// AcceptHandoff moves the driver role to the hand-off's recipient.
func (s *Service) AcceptHandoff(ctx context.Context, u User, handoffID string) (events.Event, error) {
	s.idxMu.Lock()
	id, ok := s.handoffRoom[handoffID]
	s.idxMu.Unlock()
	if !ok {
		return events.Event{}, newErr(http.StatusNotFound, "not_found", "hand-off not found")
	}
	r, err := s.room(id)
	if err != nil {
		return events.Event{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := s.writable(r); err != nil {
		return events.Event{}, err
	}
	h := r.st.handoffs[handoffID]
	if h == nil {
		return events.Event{}, newErr(http.StatusNotFound, "not_found", "hand-off not found")
	}
	if h.payload.To.ID != u.ID {
		return events.Event{}, newErr(httpForbidden, "not_recipient", "Only the person it was offered to can accept.")
	}
	if h.accepted {
		return events.Event{}, newErr(httpConflict, "already_decided", "This hand-off was already accepted.")
	}
	if d := r.st.snap.Driver; h.voided || d == nil || d.ID != h.creator {
		return events.Event{}, newErr(httpConflict, "stale_handoff", "This hand-off is no longer valid.")
	}
	if _, err := s.appendLocked(ctx, r, events.ControlGranted, u.Actor(),
		events.ControlGrantedPayload{To: u.Actor(), LeaseUntil: s.now().Add(LeaseDuration)}); err != nil {
		return events.Event{}, err
	}
	return s.appendLocked(ctx, r, events.HandoffAccepted, u.Actor(), events.HandoffAcceptedPayload{HandoffID: handoffID})
}
