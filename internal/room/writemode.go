package room

import (
	"context"
	"slices"
)

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
	// Staging is true when the driver can try a proposal on the pipeline's
	// staging stack (X8; stagingOn): the same for every caller.
	Staging bool `json:"staging,omitempty"`
}

// WriteOn reports whether fully approved proposals run (PIPEROOM_WRITE_MODE=on
// or --demo). GET /healthz reports it.
func (s *Service) WriteOn() bool { return !s.writeOff }

// RoomFor is GetRoom for one caller: the snapshot plus what depends on the
// server's settings, the clock and who asks (mode, open_proposals[].blocked,
// a stale open_proposals[].scheduled hidden). The published snapshot stays
// free of all of these: it is folded from the log alone, and every client
// folding the same log must reach it.
func (s *Service) RoomFor(ctx context.Context, u User, roomID string) (RoomSnapshot, error) {
	snap, err := s.GetRoom(ctx, roomID)
	if err != nil {
		return RoomSnapshot{}, err
	}
	snap.Mode = &Mode{Write: !s.writeOff, Solo: s.soloForRoom(u, snap), AutoApprove: s.autoApproveFor(snap.PipelineID), Staging: s.stagingOn(snap.PipelineID)}
	// X8: which steps can be staged and whether approval waits for a stage
	// depend on the server's actions and owners.yaml; a stage past its
	// expires_at is lost and no longer shown as running.
	acts := s.actionsFor(snap.PipelineID)
	now := s.now()
	for i := range snap.OpenProposals {
		p := &snap.OpenProposals[i]
		p.Stageable = stageableSteps(acts, p.Steps)
		p.StagingRequired = s.stagingGate(snap.PipelineID, *p) != nil
		if p.StageRunning != nil && !stageRunningAt(*p, now) {
			p.StageRunning = nil
		}
	}
	// The setup member's server-side fields: the configured token budget and
	// whether the maintenance window has ended (docs/api.md).
	s.fillSetupRuntime(&snap)
	// A countdown past run_at plus the grace is not shown (P2b-4b); the
	// published snapshot keeps it, as the log does.
	s.hideStaleCountdowns(&snap)
	// GetRoom returned a clone, so this write never reaches the published snapshot.
	s.markBlocked(snap.OpenProposals)
	return snap, nil
}

// markBlocked sets Blocked on every fully approved proposal when write mode is
// off. It writes into ps, so callers pass a clone (KAN-596: the room view and
// the list rows share this one definition of "blocked").
func (s *Service) markBlocked(ps []Proposal) {
	if !s.writeOff {
		return
	}
	for i := range ps {
		if fullyApproved(ps[i]) {
			ps[i].Blocked = BlockedWriteModeOff
		}
	}
}

// ListRow is snap as a list row: the published snapshot with a setup member
// trimmed the way ListRoomsFor does (KAN-602) and Blocked set the way
// markBlocked does, on a copy. It returns snap itself when there is nothing to
// change and never writes into snap, which every subscriber shares (KAN-596).
func (s *Service) ListRow(snap *RoomSnapshot) *RoomSnapshot {
	row := snap
	if snap.Setup != nil && !snap.Setup.list {
		// Shallow copy: trimSetupForList only replaces the Setup pointer.
		c := *snap
		trimSetupForList(&c)
		row = &c
	}
	if !s.writeOff {
		return row
	}
	need := false
	for _, p := range row.OpenProposals {
		if p.Blocked == "" && fullyApproved(p) {
			need = true
			break
		}
	}
	if !need {
		return row
	}
	if row == snap {
		c := *snap
		row = &c
	}
	// The only write is Proposal.Blocked, a string, on a cloned slice.
	row.OpenProposals = slices.Clone(row.OpenProposals)
	s.markBlocked(row.OpenProposals)
	return row
}

// soloFor and autoApproveFor, the mode's solo and auto_approve, are in
// autoapprove.go.
