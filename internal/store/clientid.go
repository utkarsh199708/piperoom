package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"piperoom/internal/events"
)

// ErrClientIDTaken is returned (wrapped) by Append when the event's payload
// carries a client_id that the same actor already used in the room
// (docs/api.md, Idempotent posts). Nothing is written and no seq is used. The
// caller looks the first event up with EventByClientID and answers as a repeat.
var ErrClientIDTaken = errors.New("store: client_id already used in this room")

// ClientIDIndex is the unique index of migration 00016 that enforces the key.
const ClientIDIndex = "room_events_client_id_uq"

// ClientIDs finds the event a request's idempotency key was used for. Both
// *PG and memstore.Store implement it.
type ClientIDs interface {
	// EventByClientID returns the event of roomID whose actor id is actorID
	// and whose payload client_id is clientID, or an error wrapping
	// ErrNotFound.
	EventByClientID(ctx context.Context, roomID, actorID, clientID string) (events.Event, error)
}

var _ ClientIDs = (*PG)(nil)

// isClientIDViolation reports the unique violation (SQLSTATE 23505) of the
// client_id index, told apart from any other unique rule by its name.
func isClientIDViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505" && pgErr.ConstraintName == ClientIDIndex
}

// classifyEventInsert is classify for the room_events INSERT: a client_id
// violation also wraps ErrClientIDTaken.
func classifyEventInsert(err error) error {
	if isClientIDViolation(err) {
		return fmt.Errorf("%w: %w", ErrClientIDTaken, err)
	}
	return classify(err)
}

// EventByClientID implements ClientIDs. The WHERE repeats the index predicate
// so Postgres can answer from room_events_client_id_uq.
func (s *PG) EventByClientID(ctx context.Context, roomID, actorID, clientID string) (events.Event, error) {
	if CheckText(roomID, actorID, clientID) != nil {
		return events.Event{}, fmt.Errorf("store: event by client_id: %w", ErrNotFound) // no such key can be stored
	}
	ctx, cancel := dbCtx(ctx)
	defer cancel()
	row := s.pool.QueryRow(ctx,
		`SELECT seq, type, actor, payload, at FROM room_events
		 WHERE room_id = $1 AND actor->>'id' = $2 AND payload->>'client_id' = $3 AND payload ? 'client_id'`,
		roomID, actorID, clientID)
	e, err := scanEvent(roomID, row)
	if errors.Is(err, pgx.ErrNoRows) {
		return events.Event{}, fmt.Errorf("store: event by client_id in %s: %w", roomID, ErrNotFound)
	}
	if err != nil {
		return events.Event{}, fmt.Errorf("store: event by client_id in %s: %w", roomID, err)
	}
	return e, nil
}
