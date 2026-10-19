package room

import "context"

// Write mode (PIPEROOM_WRITE_MODE, P0 item 1; docs/product/fit-2026-10.md
// §2 row 1). With it off the room works as usual up to the last approval;
// the executor then runs nothing (enqueueExecutionLocked), so a new install
// can watch the agent propose before anyone grants it a write.

// BlockedWriteModeOff is Proposal.Blocked for a fully approved proposal that
// does not run because write mode is off.
const BlockedWriteModeOff = "write_mode_off"

// Mode is the room view's mode member (docs/api.md, P0 items 1 and 4).
type Mode struct {
	// Write is true when a fully approved proposal runs (write mode on).
	Write bool `json:"write"`
	// Solo is true when the caller may approve every layer alone.
	Solo bool `json:"solo"`
	// AutoApprove lists the pipeline's policy actions; never null.
	AutoApprove []string `json:"auto_approve"`
}

// WriteOn reports whether fully approved proposals run (PIPEROOM_WRITE_MODE=on
// or --demo). GET /healthz reports it.
func (s *Service) WriteOn() bool { return !s.writeOff }

// RoomFor is GetRoom for one caller: the snapshot plus what depends on the
// server's settings and on who asks (mode, open_proposals[].blocked). The
// published snapshot stays free of both: it is folded from the log alone, and
// every client folding the same log must reach it.
func (s *Service) RoomFor(ctx context.Context, u User, roomID string) (RoomSnapshot, error) {
	snap, err := s.GetRoom(ctx, roomID)
	if err != nil {
		return RoomSnapshot{}, err
	}
	snap.Mode = &Mode{Write: !s.writeOff, Solo: s.soloFor(u, snap.PipelineID), AutoApprove: s.autoApproveFor(snap.PipelineID)}
	if s.writeOff {
		// GetRoom returned a clone, so these writes never reach the
		// published snapshot.
		for i := range snap.OpenProposals {
			if fullyApproved(snap.OpenProposals[i]) {
				snap.OpenProposals[i].Blocked = BlockedWriteModeOff
			}
		}
	}
	return snap, nil
}

// soloFor and autoApproveFor, the mode's solo and auto_approve, are in
// autoapprove.go.
