// Package store is the room service's persistence: the append-only event log
// (events.Store) plus the room registry that gives each log its sequence counter.
package store

import (
	"context"
	"errors"

	"piperoom/internal/events"
)

// ErrRoomNotFound is returned when a room ID has no registry row.
var ErrRoomNotFound = errors.New("store: room not found")

// ErrRoomExists is returned by CreateRoom for an ID that already exists.
var ErrRoomExists = errors.New("store: room already exists")

// Store is the event log plus the room registry. The Postgres implementation is
// in this package; memstore holds an in-memory one for unit tests.
type Store interface {
	events.Store
	// CreateRoom registers an empty log for roomID. Append fails with
	// ErrRoomNotFound until this has been called.
	CreateRoom(ctx context.Context, roomID string) error
	// RoomIDs lists every registered room ID, oldest first.
	RoomIDs(ctx context.Context) ([]string, error)
}
