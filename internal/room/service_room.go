package room

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"

	"piperoom/internal/events"
	"piperoom/internal/store"
)

var severities = map[string]bool{"sev1": true, "sev2": true, "sev3": true, "sev4": true}

// CreateRoom opens a room; the creator becomes driver and the agent starts.
func (s *Service) CreateRoom(ctx context.Context, user User, title, severity, pipelineID string) (RoomSnapshot, error) {
	if title == "" || pipelineID == "" {
		return RoomSnapshot{}, validation("title and pipeline_id are required")
	}
	if !severities[severity] {
		return RoomSnapshot{}, validation("severity must be sev1..sev4")
	}
	// With an owners config loaded, the pipeline must be known (docs/api.md:
	// unknown pipeline on POST /api/rooms → 400 validation). Without one
	// (--demo, unit tests) there is nothing to validate against.
	if s.owners != nil {
		if _, ok := s.owners.Pipeline(pipelineID); !ok {
			return RoomSnapshot{}, validation(fmt.Sprintf("unknown pipeline %q", pipelineID))
		}
	}
	roomID := NewID("r_")
	now := s.now().UTC()
	lease := now.Add(DriverLease)

	s.mu.Lock()
	if err := s.store.CreateRoom(ctx, store.Room{
		ID: roomID, Title: title, Severity: severity, PipelineID: pipelineID, Status: "open", OpenedAt: now,
	}); err != nil {
		s.mu.Unlock()
		return RoomSnapshot{}, fmt.Errorf("room: create: %w", err)
	}
	st := &roomState{snap: NewSnapshot(), inbox: newInbox()}
	st.snap.ID = roomID
	s.rooms[roomID] = st

	if _, err := s.emitLocked(ctx, st, roomID, events.RoomCreated, humanActor(user), events.RoomCreatedPayload{
		Title: title, Severity: severity, PipelineID: pipelineID,
	}); err != nil {
		s.mu.Unlock()
		return RoomSnapshot{}, err
	}
	if _, err := s.emitLocked(ctx, st, roomID, events.ControlGranted, humanActor(user), events.ControlGrantedPayload{
		To: humanActor(user), LeaseUntil: lease,
	}); err != nil {
		s.mu.Unlock()
		return RoomSnapshot{}, err
	}
	if err := s.store.UpdateDriver(ctx, roomID, user.ID, user.Name, lease); err != nil {
		s.log.Warn("room: persist creator driver failed", "room", roomID, "err", err)
	}
	snap := st.snap.Clone()
	s.startAgentLocked(roomID, title, pipelineID, nil, st)
	s.mu.Unlock()
	return snap, nil
}

// CreateRoomFromAlert handles the generic alert webhook: it maps title/
// severity/pipeline_id when present and stores the raw body verbatim in
// room.created.alert. Unknown pipelines still open a room (proposals stay
// disabled because actions are scope-checked), with no driver until someone
// takes control.
func (s *Service) CreateRoomFromAlert(ctx context.Context, title, severity, pipelineID string, raw json.RawMessage) (RoomSnapshot, error) {
	if title == "" {
		title = "New alert"
	}
	if !severities[severity] {
		severity = "sev3"
	}
	if pipelineID == "" {
		pipelineID = "unknown"
	}
	roomID := NewID("r_")
	now := s.now().UTC()

	s.mu.Lock()
	if err := s.store.CreateRoom(ctx, store.Room{
		ID: roomID, Title: title, Severity: severity, PipelineID: pipelineID, Status: "open", OpenedAt: now,
	}); err != nil {
		s.mu.Unlock()
		return RoomSnapshot{}, fmt.Errorf("room: create from alert: %w", err)
	}
	st := &roomState{snap: NewSnapshot(), inbox: newInbox()}
	st.snap.ID = roomID
	s.rooms[roomID] = st

	if _, err := s.emitLocked(ctx, st, roomID, events.RoomCreated, systemActor, events.RoomCreatedPayload{
		Title: title, Severity: severity, PipelineID: pipelineID, Alert: raw,
	}); err != nil {
		s.mu.Unlock()
		return RoomSnapshot{}, err
	}
	snap := st.snap.Clone()
	s.startAgentLocked(roomID, title, pipelineID, raw, st)
	s.mu.Unlock()
	return snap, nil
}

// GetRoom returns a room snapshot or a not-found error.
func (s *Service) GetRoom(ctx context.Context, roomID string) (RoomSnapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, err := s.getStateLocked(ctx, roomID)
	if err != nil {
		return RoomSnapshot{}, err
	}
	return st.snap.Clone(), nil
}

// ListRooms returns every room, open rooms first.
func (s *Service) ListRooms(ctx context.Context) ([]RoomSnapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ensureAllLoadedLocked(ctx); err != nil {
		return nil, err
	}
	out := make([]RoomSnapshot, 0, len(s.rooms))
	for _, st := range s.rooms {
		out = append(out, st.snap.Clone())
	}
	sort.SliceStable(out, func(i, j int) bool {
		if (out[i].Status == "open") != (out[j].Status == "open") {
			return out[i].Status == "open"
		}
		return out[i].OpenedAt.After(out[j].OpenedAt)
	})
	return out, nil
}

// Events returns events after afterSeq and the room's current last_seq.
func (s *Service) Events(ctx context.Context, roomID string, afterSeq int64, limit int) ([]events.Event, int64, error) {
	if _, err := s.GetRoom(ctx, roomID); err != nil {
		return nil, 0, err
	}
	if limit <= 0 {
		limit = 500
	}
	if limit > 1000 {
		limit = 1000
	}
	evs, err := s.store.Since(ctx, roomID, afterSeq, limit)
	if err != nil {
		return nil, 0, fmt.Errorf("room: events: %w", err)
	}
	s.mu.Lock()
	st := s.rooms[roomID]
	lastSeq := int64(0)
	if st != nil {
		lastSeq = st.snap.LastSeq
	} else if row, err := s.store.GetRoom(ctx, roomID); err == nil {
		lastSeq = row.LastSeq
	}
	s.mu.Unlock()
	return evs, lastSeq, nil
}

// CloseRoom closes the room (driver only) and stops the agent.
func (s *Service) CloseRoom(ctx context.Context, user User, roomID, resolution string) (events.Event, error) {
	s.mu.Lock()
	st, err := s.getStateLocked(ctx, roomID)
	if err != nil {
		s.mu.Unlock()
		return events.Event{}, err
	}
	if !s.roomOpen(st) {
		s.mu.Unlock()
		return events.Event{}, roomClosed("room already closed")
	}
	if !s.isDriver(st, user.ID) {
		s.mu.Unlock()
		return events.Event{}, ErrNotDriver()
	}
	e, err := s.emitLocked(ctx, st, roomID, events.RoomClosed, humanActor(user), events.RoomClosedPayload{Resolution: resolution})
	if err != nil {
		s.mu.Unlock()
		return events.Event{}, err
	}
	if err := s.store.CloseRoom(ctx, roomID, s.now().UTC()); err != nil {
		s.log.Warn("room: persist close failed", "room", roomID, "err", err)
	}
	cancel := st.agentCancel
	s.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	return e, nil
}
