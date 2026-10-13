package memstore

import (
	"context"
	"fmt"
	"slices"
	"sort"

	"piperoom/internal/events"
	"piperoom/internal/store"
)

var _ store.ClosedRoomReader = (*Store)(nil)

// ListClosedRooms implements store.ClosedRoomReader.
func (s *Store) ListClosedRooms(ctx context.Context, pipelineID string, limit int) ([]store.Room, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	out := []store.Room{}
	for _, log := range s.rooms {
		m := log.meta
		if m.Status != "closed" || m.ClosedAt == nil || (pipelineID != "" && m.PipelineID != pipelineID) {
			continue
		}
		out = append(out, m)
	}
	s.mu.Unlock()
	sort.Slice(out, func(i, j int) bool {
		if !out[i].ClosedAt.Equal(*out[j].ClosedAt) {
			return out[i].ClosedAt.After(*out[j].ClosedAt)
		}
		return out[i].ID < out[j].ID
	})
	if limit < 0 {
		limit = 0
	}
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

// EventsOfTypes implements store.ClosedRoomReader.
func (s *Store) EventsOfTypes(ctx context.Context, roomID string, types []events.Type, limit int) ([]events.Event, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if limit <= 0 || len(types) == 0 {
		return []events.Event{}, nil
	}
	s.mu.Lock()
	log, ok := s.rooms[roomID]
	if !ok {
		// Like Postgres, a room deleted since it was listed has no events.
		s.mu.Unlock()
		return []events.Event{}, nil
	}
	var picked []events.Event
	for _, e := range log.events {
		if len(picked) >= limit {
			break
		}
		if slices.Contains(types, e.Type) {
			picked = append(picked, e)
		}
	}
	s.mu.Unlock()
	out := make([]events.Event, 0, len(picked))
	for _, e := range picked {
		e, err := store.StripToolResultData(e)
		if err != nil {
			return nil, fmt.Errorf("memstore: %w", err)
		}
		out = append(out, e)
	}
	return out, nil
}
