package room

import (
	"context"
	"fmt"
	"time"

	"piperoom/internal/events"
	"piperoom/internal/topology"
)

// Topology returns the live pipeline map.
func (s *Service) Topology(ctx context.Context, pipelineID string) (topology.Graph, error) {
	tctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	g, err := s.topo.Build(tctx, pipelineID)
	if err != nil {
		return topology.Graph{}, fmt.Errorf("room: topology: %w", err)
	}
	return g, nil
}

// Trace follows a record through the pipeline and pins the result to the room.
func (s *Service) Trace(ctx context.Context, user User, roomID, table, key string) (events.Event, error) {
	if table == "" || key == "" {
		return events.Event{}, validation("table and key are required")
	}

	s.mu.Lock()
	st, err := s.getStateLocked(ctx, roomID)
	if err != nil {
		s.mu.Unlock()
		return events.Event{}, err
	}
	if !s.roomOpen(st) {
		s.mu.Unlock()
		return events.Event{}, roomClosed("room is closed")
	}
	pipelineID := st.snap.PipelineID
	s.mu.Unlock()

	tctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	stages, err := s.topo.Trace(tctx, pipelineID, table, key)
	cancel()
	if err != nil {
		return events.Event{}, fmt.Errorf("room: trace: %w", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	st2 := s.rooms[roomID]
	if st2 == nil {
		return events.Event{}, notFound("room not found")
	}
	if !s.roomOpen(st2) {
		return events.Event{}, roomClosed("room is closed")
	}
	return s.emitLocked(ctx, st2, roomID, events.TracePinned, humanActor(user), events.TracePinnedPayload{
		Table: table, Key: key, Stages: stages,
	})
}
