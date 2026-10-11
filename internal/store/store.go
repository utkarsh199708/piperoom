// Package store is the room service's persistence: the append-only event log
// (events.Store) plus the room registry that gives each log its sequence counter.
package store

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"

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

// Limits for Store.Since, shared by every implementation so they cannot drift.
const (
	// DefaultSinceLimit applies when the caller passes limit <= 0.
	DefaultSinceLimit = 500
	// MaxSinceLimit caps any larger limit (docs/api.md: "limit max 1000").
	MaxSinceLimit = 1000
)

// ClampLimit applies the Since limit rules: <= 0 means DefaultSinceLimit and
// anything above MaxSinceLimit is cut down to it.
func ClampLimit(limit int) int {
	switch {
	case limit <= 0:
		return DefaultSinceLimit
	case limit > MaxSinceLimit:
		return MaxSinceLimit
	default:
		return limit
	}
}

// MarshalPayload turns an Append payload into the JSON that is stored. A
// json.RawMessage is kept as it is (after checking it is valid JSON, so a bad
// one fails here with a clear error and not deep inside the database); a nil
// payload becomes JSON null; anything else goes through json.Marshal.
//
// One edit is made to every payload: the NUL escape \u0000 becomes \ufffd.
// Postgres jsonb cannot store NUL, and tool output or record values (untrusted
// text) can contain it, so without this an event would fail to append. Both
// stores apply it so they stay identical.
func MarshalPayload(payload any) (json.RawMessage, error) {
	if raw, ok := payload.(json.RawMessage); ok {
		if len(raw) == 0 {
			return json.RawMessage("null"), nil
		}
		if !json.Valid(raw) {
			return nil, errors.New("store: payload is not valid JSON")
		}
		return replaceNULEscapes(raw), nil
	}
	b, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("store: marshal payload: %w", err)
	}
	return replaceNULEscapes(b), nil
}

var nulEscape = []byte(`\u0000`)

// replaceNULEscapes swaps each JSON NUL escape for the replacement character.
// It walks escape pairs left to right so an escaped backslash followed by the
// text "u0000" (written \\u0000 in JSON) is not mistaken for a NUL escape.
// The input is never modified.
func replaceNULEscapes(b []byte) []byte {
	if !bytes.Contains(b, nulEscape) {
		return b
	}
	out := make([]byte, 0, len(b))
	for i := 0; i < len(b); i++ {
		if b[i] == '\\' && i+1 < len(b) {
			if i+6 <= len(b) && bytes.Equal(b[i:i+6], nulEscape) {
				out = append(out, `\ufffd`...)
				i += 5
				continue
			}
			out = append(out, b[i], b[i+1])
			i++
			continue
		}
		out = append(out, b[i])
	}
	return out
}
