package room

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"time"

	"piperoom/internal/events"
	"piperoom/internal/topology"
)

// tableRe is the accepted shape of a table argument ("schema.table"). The
// value flows into tool prompts and identifier lookups, and a malformed name
// is a client error, not an internal one.
var tableRe = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*$`)

// Topology returns the live pipeline map. An unknown pipeline is a 404
// (docs/api.md), not a 500 from the builder.
func (s *Service) Topology(ctx context.Context, pipelineID string) (topology.Graph, error) {
	if !pipelineIDRe.MatchString(pipelineID) {
		return topology.Graph{}, notFound("pipeline not found")
	}
	if s.owners != nil {
		if _, ok := s.owners.Pipeline(pipelineID); !ok {
			return topology.Graph{}, notFound("pipeline not found")
		}
	}
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
	if !tableRe.MatchString(table) {
		return events.Event{}, validation(fmt.Sprintf("table %q must be schema.name", table))
	}
	if err := rejectUnstorable(key); err != nil {
		return events.Event{}, err
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
		// A well-formed table that is not in the pipeline is bad input, not an
		// internal error (docs/api.md:117): map topology.ErrBadInput to 400.
		if errors.Is(err, topology.ErrBadInput) {
			return events.Event{}, validation(err.Error())
		}
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
