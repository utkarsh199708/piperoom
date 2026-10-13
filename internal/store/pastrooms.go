package store

import (
	"context"
	"encoding/json"
	"fmt"

	"piperoom/internal/events"
)

// ClosedRoomReader is the bounded read surface the similar_incidents tool
// uses to index past rooms. It is separate from Store like Purger and
// ShadowStore: a store (or a test fake) without it is served by a slower
// fallback in the tool, built on ListRooms and Since. Both *PG and
// memstore.Store implement it.
type ClosedRoomReader interface {
	// ListClosedRooms returns up to limit closed rooms, newest close first
	// (ties by id). pipelineID "" means every pipeline.
	ListClosedRooms(ctx context.Context, pipelineID string, limit int) ([]Room, error)
	// EventsOfTypes returns the room's events whose type is in types, in seq
	// order, at most limit. The "data" field of an agent.tool_result payload is
	// dropped: it can be megabytes and the caller reads the summary only.
	EventsOfTypes(ctx context.Context, roomID string, types []events.Type, limit int) ([]events.Event, error)
}

var _ ClosedRoomReader = (*PG)(nil)

// StripToolResultData removes "data" from an agent.tool_result payload and
// returns every other payload unchanged. memstore uses it so both stores give
// EventsOfTypes callers the same payloads.
func StripToolResultData(e events.Event) (events.Event, error) {
	if e.Type != events.AgentToolResult {
		return e, nil
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(e.Payload, &m); err != nil {
		return events.Event{}, fmt.Errorf("store: decode tool_result payload: %w", err)
	}
	delete(m, "data")
	b, err := json.Marshal(m)
	if err != nil {
		return events.Event{}, fmt.Errorf("store: encode tool_result payload: %w", err)
	}
	e.Payload = b
	return e, nil
}

// ListClosedRooms implements ClosedRoomReader.
func (s *PG) ListClosedRooms(ctx context.Context, pipelineID string, limit int) ([]Room, error) {
	ctx, cancel := dbCtx(ctx)
	defer cancel()
	if limit <= 0 {
		return []Room{}, nil
	}
	rows, err := s.pool.Query(ctx,
		`SELECT id, title, severity, pipeline_id, status, opened_at, closed_at, last_seq
		 FROM rooms
		 WHERE status = 'closed' AND closed_at IS NOT NULL AND ($1 = '' OR pipeline_id = $1)
		 ORDER BY closed_at DESC, id ASC LIMIT $2`, pipelineID, limit)
	if err != nil {
		return nil, fmt.Errorf("store: list closed rooms: %w", err)
	}
	defer rows.Close()
	out := []Room{}
	for rows.Next() {
		var r Room
		if err := rows.Scan(&r.ID, &r.Title, &r.Severity, &r.PipelineID, &r.Status, &r.OpenedAt, &r.ClosedAt, &r.LastSeq); err != nil {
			return nil, fmt.Errorf("store: scan closed room: %w", err)
		}
		utcTimes(&r)
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: list closed rooms: %w", err)
	}
	return out, nil
}

// EventsOfTypes implements ClosedRoomReader.
func (s *PG) EventsOfTypes(ctx context.Context, roomID string, types []events.Type, limit int) ([]events.Event, error) {
	ctx, cancel := dbCtx(ctx)
	defer cancel()
	if limit <= 0 || len(types) == 0 {
		return []events.Event{}, nil
	}
	names := make([]string, len(types))
	for i, t := range types {
		names[i] = string(t)
	}
	// "payload - 'data'" drops the key in the database, so a tool result's
	// recorded data never crosses the wire.
	rows, err := s.pool.Query(ctx,
		`SELECT seq, type, actor,
		        CASE WHEN type = 'agent.tool_result' THEN payload - 'data' ELSE payload END, at
		 FROM room_events WHERE room_id = $1 AND type = ANY($2) ORDER BY seq ASC LIMIT $3`,
		roomID, names, limit)
	if err != nil {
		return nil, fmt.Errorf("store: events of types %s: %w", roomID, err)
	}
	defer rows.Close()
	out := []events.Event{}
	for rows.Next() {
		e, err := scanEvent(roomID, rows)
		if err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: events of types %s: %w", roomID, err)
	}
	return out, nil
}
