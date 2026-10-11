package httpapi

import (
	"context"
	"net/http"

	"piperoom/internal/alertin"
	"piperoom/internal/events"
	"piperoom/internal/room"
	"piperoom/internal/topology"
)

// Service is what the handlers need from the room service. *room.Service
// implements it; tests use a fake. Every method that can break a rule returns
// a *room.Error (see room.AsError) which the handlers map to docs/api.md §Errors.
type Service interface {
	// Authenticate returns the user with this id, or false.
	Authenticate(id string) (room.User, bool)

	CreateRoom(ctx context.Context, u room.User, in room.CreateRoomInput) (room.Snapshot, error)
	// IngestAlert opens (or returns the already-open) room for a verified alert.
	IngestAlert(ctx context.Context, a alertin.Alert) (room.Snapshot, error)
	ListRooms(ctx context.Context) ([]room.Snapshot, error)
	GetRoom(ctx context.Context, roomID string) (room.Snapshot, error)
	Events(ctx context.Context, roomID string, after int64, limit int) (room.EventsPage, error)

	Comment(ctx context.Context, u room.User, roomID string, in room.CommentInput) (events.Event, error)
	Steer(ctx context.Context, u room.User, roomID, text string) (events.Event, error)
	SendSuggestion(ctx context.Context, u room.User, roomID, suggestionID string) (events.Event, error)
	RequestControl(ctx context.Context, u room.User, roomID string) (events.Event, error)
	GrantControl(ctx context.Context, u room.User, roomID, to string) (events.Event, error)
	Close(ctx context.Context, u room.User, roomID, resolution string) (events.Event, error)

	Approve(ctx context.Context, u room.User, proposalID, layer string) (events.Event, error)
	Reject(ctx context.Context, u room.User, proposalID, layer, reason string) (events.Event, error)

	CreateHandoff(ctx context.Context, u room.User, roomID string, in room.HandoffInput) (events.Event, error)
	AcceptHandoff(ctx context.Context, u room.User, handoffID string) (events.Event, error)

	Topology(ctx context.Context, pipelineID string) (topology.Graph, error)
	Trace(ctx context.Context, u room.User, roomID string, in room.TraceInput) (events.Event, error)
}

// Verifier checks and parses an alert webhook (alertin.Config satisfies it).
type Verifier interface {
	Ingest(src alertin.Source, h http.Header, body []byte) (alertin.Alert, error)
}
