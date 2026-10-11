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

	CreateProposal(ctx context.Context, p ProposalRow) error
	GetProposal(ctx context.Context, proposalID string) (ProposalRow, error)
	RecordApproval(ctx context.Context, proposalID, layer, decision, reason string) error
	RecordExecution(ctx context.Context, proposalID string, executedCount int) error
	SetProposalStatus(ctx context.Context, proposalID, status string) error

	CreateSuggestion(ctx context.Context, r SuggestionRow) error
	GetSuggestion(ctx context.Context, suggestionID string) (SuggestionRow, error)
	SetSuggestionStatus(ctx context.Context, suggestionID, status string) error

	CreateHandoff(ctx context.Context, h HandoffRow) error
	GetHandoff(ctx context.Context, handoffID string) (HandoffRow, error)
	SetHandoffStatus(ctx context.Context, handoffID, status string) error
}
