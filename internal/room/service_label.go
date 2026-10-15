package room

import (
	"context"
	"fmt"
	"strings"

	"piperoom/internal/events"
)

// Label limits (docs/api.md, POST /api/rooms/{id}/label).
const (
	maxLabelCauseChars = 500
	maxLabelFixChars   = 500
)

// LabelInput is the body of POST /api/rooms/{id}/label.
type LabelInput struct {
	ActualLayer   string
	Cause         string
	FixThatWorked string
	FixProposalID string
	AgentHelped   *bool
}

func errLabelNotAllowed() *Error {
	return &Error{Status: 403, Code: "not_allowed", Msg: "labelling a room needs a member of its pipeline"}
}

// Label records the human label of a closed room (POST /api/rooms/{id}/label).
// Any room, shadow or not; re-labelling appends another event and the last
// one wins. A shadow room must have its blind label first, so the blind label
// stays blind (shadow.go).
func (s *Service) Label(ctx context.Context, user User, roomID string, in LabelInput) (events.Event, error) {
	// Shared store: fold what other replicas wrote, reading outside the lock.
	s.syncRoom(ctx, roomID)
	st, err := s.lockRoom(ctx, roomID)
	if err != nil {
		return events.Event{}, err
	}
	defer s.unlockRoom(st)
	if s.roomOpen(st) {
		return events.Event{}, &Error{Status: 409, Code: "room_open", Msg: "the room is still open; label it after it closes"}
	}
	grant, ok := s.canGradeShadow(st.snap.PipelineID, user)
	if !ok {
		return events.Event{}, errLabelNotAllowed()
	}
	if err := validateLabel(in); err != nil {
		return events.Event{}, err
	}
	if st.snap.Shadow && !st.snap.shadowLabelled {
		return events.Event{}, errNotLabelled()
	}
	if in.FixProposalID != "" {
		ran, err := s.proposalRan(ctx, roomID, in.FixProposalID)
		if err != nil {
			return events.Event{}, err
		}
		if !ran {
			return events.Event{}, validation("fix_proposal_id must be a proposal of this room with an executed step")
		}
	}
	ident := s.identityFor(user, grant)
	e, err := s.emitLocked(ctx, st, roomID, events.RoomLabelled, s.humanActor(user), events.RoomLabelledPayload{
		ActualLayer: in.ActualLayer, Cause: in.Cause, FixThatWorked: in.FixThatWorked,
		FixProposalID: in.FixProposalID, AgentHelped: in.AgentHelped, Identity: &ident,
	})
	if err != nil {
		return events.Event{}, mapUnstorable(err)
	}
	s.log.Info("room: room labelled", "room", roomID, "user", user.ID, "grant", grant)
	return e, nil
}

func validateLabel(in LabelInput) error {
	if in.ActualLayer != "" && !isLayer(in.ActualLayer) {
		return validation(fmt.Sprintf("actual_layer must be one of %s", strings.Join(events.Layers, ", ")))
	}
	if strings.TrimSpace(in.Cause) == "" {
		return validation("cause is required")
	}
	if err := checkLen("cause", in.Cause, maxLabelCauseChars); err != nil {
		return err
	}
	if err := checkLen("fix_that_worked", in.FixThatWorked, maxLabelFixChars); err != nil {
		return err
	}
	return rejectUnstorable(in.Cause, in.FixThatWorked, in.FixProposalID)
}

// proposalRan reports whether the room log holds a proposal.created with this
// id and an action.executed of it that succeeded.
func (s *Service) proposalRan(ctx context.Context, roomID, proposalID string) (bool, error) {
	evs, err := s.allEvents(ctx, roomID)
	if err != nil {
		return false, fmt.Errorf("room: read log for label: %w", err)
	}
	created, ran := false, false
	for _, e := range evs {
		switch e.Type {
		case events.ProposalCreated:
			if p, err := events.Decode[events.ProposalCreatedPayload](e); err == nil && p.ProposalID == proposalID {
				created = true
			}
		case events.ActionExecuted:
			if p, err := events.Decode[events.ActionExecutedPayload](e); err == nil && p.ProposalID == proposalID && p.OK {
				ran = true
			}
		}
	}
	return created && ran, nil
}
