package store

import (
	"context"
	"time"

	"piperoom/internal/events"
)

// Store is the full store surface the room service needs: the append-only
// event log (events.Store) plus the aux tables used to enforce rules across
// restarts. Both *PG and memstore.Store implement it.
type Store interface {
	events.Store

	CreateRoom(ctx context.Context, r Room) error
	GetRoom(ctx context.Context, id string) (Room, error)
	ListRooms(ctx context.Context) ([]Room, error)
	UpdateDriver(ctx context.Context, id, driverID, driverName string, leaseUntil time.Time) error
	CloseRoom(ctx context.Context, id string, closedAt time.Time) error
	// DeleteEmptyRoom removes a room row that has no events yet, for the
	// window between CreateRoom and the room.created append. A missing room is
	// not an error; a room that has events is ErrRoomNotEmpty and is kept.
	DeleteEmptyRoom(ctx context.Context, id string) error

	// LastRoomByDedupKey returns the newest room (by opened_at, ties by id)
	// whose room.created event carries dedupKey, open or closed, or an error
	// wrapping ErrNotFound. dedupKey is the full alert index key
	// ("<pipeline>|<key>") exactly as room.created stores it. An empty key
	// never matches.
	LastRoomByDedupKey(ctx context.Context, dedupKey string) (Room, error)

	CreateProposal(ctx context.Context, p ProposalRow) error
	DeleteProposal(ctx context.Context, proposalID string) error
	GetProposal(ctx context.Context, proposalID string) (ProposalRow, error)
	// RecordApproval stores one decision with its params_hash and identity; a
	// second decision on the same layer wraps ErrAlreadyExists.
	RecordApproval(ctx context.Context, a ApprovalRow) error
	// ListApprovals returns a proposal's decisions ordered by layer.
	ListApprovals(ctx context.Context, proposalID string) ([]ApprovalRow, error)
	// SetProposalParamsHash fills the hash of a proposal that has none (a row
	// from before auth PR 4). It never replaces an existing hash: a different
	// one wraps ErrParamsHashConflict, the same one is a no-op.
	SetProposalParamsHash(ctx context.Context, proposalID, hash string) error
	DeleteApproval(ctx context.Context, proposalID, layer string) error
	StartProposalStep(ctx context.Context, proposalID string, stepIndex int) error
	FinishProposalStep(ctx context.Context, proposalID string, stepIndex int, status, output string) error
	SetProposalStatus(ctx context.Context, proposalID, status string) error

	CreateSuggestion(ctx context.Context, r SuggestionRow) error
	DeleteSuggestion(ctx context.Context, suggestionID string) error
	GetSuggestion(ctx context.Context, suggestionID string) (SuggestionRow, error)
	SetSuggestionStatus(ctx context.Context, suggestionID, status string) error

	CreateHandoff(ctx context.Context, h HandoffRow) error
	DeleteHandoff(ctx context.Context, handoffID string) error
	GetHandoff(ctx context.Context, handoffID string) (HandoffRow, error)
	SetHandoffStatus(ctx context.Context, handoffID, status string) error
	VoidHandoffs(ctx context.Context, roomID, exceptHandoffID string) error
}

// PurgeResult counts what one PurgeClosedBefore call deleted. It carries
// counts and room ids only, never content, so it is safe to log.
type PurgeResult struct {
	RoomIDs      []string // rooms removed, so the caller can drop cached state
	Examined     int      // rooms the call looked at (removed, gone meanwhile, or failed)
	Failed       []PurgeFailure
	Events       int64
	Proposals    int64
	Approvals    int64
	Steps        int64
	Suggestions  int64
	Handoffs     int64
	SlackThreads int64
}

// PurgeFailure is one room that could not be purged. Its transaction rolled
// back, so the room is intact.
type PurgeFailure struct {
	RoomID string
	Err    error
}

// Add folds o into r.
func (r *PurgeResult) Add(o PurgeResult) {
	r.RoomIDs = append(r.RoomIDs, o.RoomIDs...)
	r.Examined += o.Examined
	r.Failed = append(r.Failed, o.Failed...)
	r.Events += o.Events
	r.Proposals += o.Proposals
	r.Approvals += o.Approvals
	r.Steps += o.Steps
	r.Suggestions += o.Suggestions
	r.Handoffs += o.Handoffs
	r.SlackThreads += o.SlackThreads
}

// Purger is the retention surface. It is separate from Store so a store (or a
// test fake) that cannot delete simply does not implement it. Both *PG and
// memstore.Store do.
type Purger interface {
	// PurgeClosedBefore deletes up to maxRooms closed rooms whose closed_at is
	// before cutoff, with every event and child row. Open rooms are never
	// touched. Each room goes in its own transaction with its own timeout.
	// Rooms in exclude are skipped (the caller passes rooms that already
	// failed in this run). One room failing does not stop the others: it is
	// reported in Failed and the call carries on. The returned error is only
	// for the call itself (listing failed, ctx done). Examined < maxRooms
	// means no qualifying room is left.
	PurgeClosedBefore(ctx context.Context, cutoff time.Time, maxRooms int, exclude []string) (PurgeResult, error)
}
