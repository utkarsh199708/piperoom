package room

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"

	"piperoom/internal/alertin"
	"piperoom/internal/events"
	"piperoom/internal/store"
)

var severities = map[string]bool{"sev1": true, "sev2": true, "sev3": true, "sev4": true}

// pipelineIDRe is the accepted shape of pipeline_id on POST /api/rooms. The
// id flows into logs, prompts and identifier-based lookups, so only a safe
// charset may pass (C13 prompt audit, item (b)).
var pipelineIDRe = regexp.MustCompile(`^[a-zA-Z0-9_.-]+$`)

// CreateRoom opens a room; the creator becomes driver and the agent starts.
func (s *Service) CreateRoom(ctx context.Context, user User, title, severity, pipelineID string) (RoomSnapshot, error) {
	if title == "" || pipelineID == "" {
		return RoomSnapshot{}, validation("title and pipeline_id are required")
	}
	// The shape check applies even without an owners config (demo, unit
	// tests): it does not depend on anything being loaded.
	if !pipelineIDRe.MatchString(pipelineID) {
		return RoomSnapshot{}, validation(fmt.Sprintf("pipeline_id %q must match ^[a-zA-Z0-9_.-]+$", pipelineID))
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
	s.notifyRoomOpenedLocked(roomID)
	s.mu.Unlock()
	return snap, nil
}

// CreateRoomFromAlert handles the generic alert webhook: it maps title/
// severity/pipeline_id when present and stores the raw body verbatim in
// room.created.alert. The pipeline_id takes the same shape check as POST
// /api/rooms, but a missing or unknown pipeline NEVER drops the alert
// (docs/api.md:103, LEAD-ARCH round 1 #10): the room opens anyway with a
// system message and proposals disabled, so the agent cannot propose a fix
// against a pipeline it knows nothing about. The 400 for an unknown pipeline
// stays only on POST /api/rooms (CreateRoom). With no owners config (unit
// tests) there is nothing to validate against, so every pipeline is treated as
// known. No driver until someone takes control.
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
	if !pipelineIDRe.MatchString(pipelineID) {
		return RoomSnapshot{}, validation(fmt.Sprintf("pipeline_id %q must match ^[a-zA-Z0-9_.-]+$", pipelineID))
	}
	pipelineKnown := s.owners == nil || s.knownPipeline(pipelineID)
	dedupKey, resolved := alertDedup(raw)
	indexKey := alertIndexKey(pipelineID, dedupKey)
	roomID := NewID("r_")
	now := s.now().UTC()

	s.mu.Lock()
	// The same alert while its room is still open joins that room: no second
	// room, no second agent, and the hook is not called again (the room was
	// opened before).
	if existing, ok := s.openAlertRoomLocked(indexKey); ok {
		st := s.rooms[existing]
		snap, err := s.joinAlertRoomLocked(ctx, existing, st, title, resolved)
		s.mu.Unlock()
		return snap, err
	}
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
	if indexKey != "" {
		st.dedupKey = indexKey
		s.alertRooms[indexKey] = roomID
	}
	// A missing/unknown pipeline opens with a system message so the room shows
	// why no fix is proposed. events.go has no dedicated system-message type,
	// so the message is an agent.message carried by the system actor (the room
	// renders it with the actor name "System").
	if !pipelineKnown {
		if _, err := s.emitLocked(ctx, st, roomID, events.AgentMessage, systemActor, events.AgentMessagePayload{
			Text: fmt.Sprintf("pipeline %q is unknown; proposals are disabled for this room", pipelineID),
		}); err != nil {
			s.mu.Unlock()
			return RoomSnapshot{}, err
		}
	}
	snap := st.snap.Clone()
	s.startAgentLocked(roomID, title, pipelineID, raw, st)
	s.notifyRoomOpenedLocked(roomID)
	s.mu.Unlock()
	return snap, nil
}

// alertDedup reads a webhook body the way alertin does and returns the dedup
// key that makes a repeat of the same alert join the room already open for it
// (alertin.Alert.DedupKey), and whether the source reports the alert resolved.
// The key is derived from the raw body alone, so a restart rebuilds it. Without
// a dedup_key in the body the key is a hash of the body: only byte-identical
// bodies are deduplicated. A body alertin cannot parse, or no body, gets no key
// and always opens a room.
func alertDedup(raw json.RawMessage) (key string, resolved bool) {
	if len(raw) == 0 {
		return "", false
	}
	a, err := alertin.Config{}.Parse(alertin.Generic, raw)
	if err != nil {
		return "", false
	}
	return a.DedupKey, a.Status == alertin.StatusResolved
}

// alertIndexKey scopes an alert dedup key to its pipeline: the same key sent for
// two pipelines is two incidents. An empty dedup key stays empty (no dedup).
func alertIndexKey(pipelineID, dedupKey string) string {
	if dedupKey == "" {
		return ""
	}
	return pipelineID + "\x00" + dedupKey
}

// openAlertRoomLocked returns the open room that already holds indexKey (see
// alertIndexKey), if any. A key whose room is closed is not a match. Caller must
// hold s.mu.
func (s *Service) openAlertRoomLocked(indexKey string) (string, bool) {
	if indexKey == "" {
		return "", false
	}
	roomID, ok := s.alertRooms[indexKey]
	if !ok {
		return "", false
	}
	st, ok := s.rooms[roomID]
	if !ok || !s.roomOpen(st) {
		return "", false
	}
	return roomID, true
}

// joinAlertRoomLocked records a repeat of an alert in the room that is already
// open for it, as a system message, and returns that room. A repeat that the
// source reports resolved does not close the room: only the driver does that.
// Caller must hold s.mu.
func (s *Service) joinAlertRoomLocked(ctx context.Context, roomID string, st *roomState, title string, resolved bool) (RoomSnapshot, error) {
	text := fmt.Sprintf("alert %q fired again; joined this room instead of opening a new one", title)
	if resolved {
		text = fmt.Sprintf("alert %q reported resolved at its source; the room stays open until the driver closes it", title)
	}
	if _, err := s.emitLocked(ctx, st, roomID, events.AgentMessage, systemActor, events.AgentMessagePayload{Text: text}); err != nil {
		return RoomSnapshot{}, err
	}
	return st.snap.Clone(), nil
}

// indexAlertRoomLocked puts an open webhook room under its alert index key
// (pipeline and dedup key), so a repeat of the alert joins it after a restart
// too. A key shared by several open rooms (rooms opened before dedup existed)
// keeps the first one. Caller must hold s.mu.
func (s *Service) indexAlertRoomLocked(ctx context.Context, roomID string, st *roomState) {
	dedupKey, _ := alertDedup(s.rawAlert(ctx, roomID))
	key := alertIndexKey(st.snap.PipelineID, dedupKey)
	if key == "" {
		return
	}
	st.dedupKey = key
	if _, taken := s.alertRooms[key]; !taken {
		s.alertRooms[key] = roomID
	}
}

// notifyRoomOpenedLocked runs the OnRoomOpened hook off the request path, so
// a slow hook never delays the HTTP response. Caller holds s.mu, which keeps
// wg.Add from racing Close; after Close the hook is not called.
func (s *Service) notifyRoomOpenedLocked(roomID string) {
	if s.onRoomOpened == nil || s.closed {
		return
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.onRoomOpened(s.ctx, roomID)
	}()
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
	if err := s.ensureAllLoadedLocked(ctx, false); err != nil {
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
	// A closed room no longer takes repeats of its alert: the next one opens a
	// new room.
	if st.dedupKey != "" && s.alertRooms[st.dedupKey] == roomID {
		delete(s.alertRooms, st.dedupKey)
	}
	if err := s.store.CloseRoom(ctx, roomID, s.now().UTC()); err != nil {
		s.log.Warn("room: persist close failed", "room", roomID, "err", err)
	}
	cancel := st.agentCancel
	execCancel := st.execCancel
	s.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if execCancel != nil {
		execCancel()
	}
	return e, nil
}
