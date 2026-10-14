package room

import (
	"context"
	"fmt"
	"slices"
	"time"

	"piperoom/internal/events"
)

// nudgeWindow is the minimum gap between two nudges of the same proposal
// layer.
const nudgeWindow = 10 * time.Minute

// Nudge records that a member asked a pending layer's owners to look
// (approval.nudged). The proposal must be open and the layer still pending on
// it; at most one nudge per proposal and layer per nudgeWindow.
func (s *Service) Nudge(ctx context.Context, user User, roomID, proposalID, layer string) (events.Event, error) {
	if proposalID == "" {
		return events.Event{}, validation("proposal_id is required")
	}
	if !slices.Contains(events.Layers, layer) {
		return events.Event{}, validation("layer is not a known layer")
	}
	s.syncRoom(ctx, roomID)
	s.mu.Lock()
	defer s.mu.Unlock()
	st, err := s.getStateLocked(ctx, roomID)
	if err != nil {
		return events.Event{}, err
	}
	check := func() error {
		if !s.roomOpen(st) {
			return roomClosed("room is closed")
		}
		var prop *Proposal
		for i := range st.snap.OpenProposals {
			if st.snap.OpenProposals[i].ProposalID == proposalID {
				prop = &st.snap.OpenProposals[i]
			}
		}
		// A proposal that is no longer open was decided (or never existed).
		if prop == nil {
			return alreadyDecided("proposal is not open")
		}
		state, required := prop.Approvals[layer]
		if !required {
			return validation("layer is not required by that proposal")
		}
		if state != "pending" {
			return alreadyDecided("layer already decided")
		}
		if at, ok := st.snap.nudges[nudgeKey(proposalID, layer)]; ok {
			if ago := s.now().Sub(at); ago >= 0 && ago < nudgeWindow {
				return alreadyNudged(fmt.Sprintf("nudged %d min ago", int(ago/time.Minute)))
			}
		}
		return nil
	}
	if err := check(); err != nil {
		return events.Event{}, err
	}
	return s.emitCheckedLocked(ctx, st, roomID, check, events.ApprovalNudged, s.humanActor(user),
		events.ApprovalNudgedPayload{ProposalID: proposalID, Layer: layer})
}

func nudgeKey(proposalID, layer string) string { return proposalID + "\x00" + layer }
