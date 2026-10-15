package room

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"piperoom/internal/agent"
	"piperoom/internal/events"
	"piperoom/internal/store"
)

// This file holds the room-service surface the built-in watcher (internal/watch)
// uses: the OnRoomClosed hook (see Deps), and the queries and writes below.
// The watcher opens, joins and resolves through OpenAlertRoom with Source
// alertin.Watcher and DedupKey "watcher:<pipeline>". Only PostIncidentJSON (a
// different incident joined the room, QA G1) wakes or starts an agent.

// maxNoteBytes caps a PostRoomNote text. Evidence is a few lines of numbers; a
// runaway caller must not fill the event log.
const maxNoteBytes = 4096

// hookDedupKey turns a room's alert index key ("<pipeline>|<key>") back into the
// key the alert source gave, which is what OnRoomClosed passes and what
// LastRoomForKey takes. A room with no key gives "".
func hookDedupKey(pipelineID, indexKey string) string {
	return strings.TrimPrefix(indexKey, pipelineID+"|")
}

// OpenRoomForPipeline returns an open room for pipelineID, whichever alert
// source or person opened it, so the watcher can post its evidence there
// (PostRoomNote) instead of opening a second room. If several rooms are open
// for the pipeline, it returns the newest one (latest opened_at; the smaller
// id on a tie, the same order as the stores), so evidence lands in the room
// people are most likely watching. It reads the room cache, which Load fills at
// start-up; with a single-process store it does no store I/O, with a shared
// one it refreshes the cache first (one ListRooms read). A pipelineID that is
// not a valid pipeline id (pipelineIDRe) matches nothing.
func (s *Service) OpenRoomForPipeline(pipelineID string) (roomID string, ok bool) {
	if !pipelineIDRe.MatchString(pipelineID) {
		return "", false
	}
	if s.coord != nil {
		// Shared store: rooms other replicas opened or closed since the last
		// fold count too. The reads happen outside s.mu (refreshAll); a
		// failure falls back to the cache (the follower keeps it close).
		ctx, cancel := context.WithTimeout(s.ctx, lastRoomLookupTimeout)
		if err := s.refreshAll(ctx); err != nil && s.ctx.Err() == nil {
			s.log.Warn("room: open room for pipeline: refresh failed; using the cache", "pipeline", pipelineID, "err", err)
		}
		cancel()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var newest *roomState
	for id, st := range s.rooms {
		if st.snap.PipelineID != pipelineID || !s.roomOpen(st) {
			continue
		}
		if newest == nil || st.snap.OpenedAt.After(newest.snap.OpenedAt) ||
			(st.snap.OpenedAt.Equal(newest.snap.OpenedAt) && id < roomID) {
			newest, roomID = st, id
		}
	}
	return roomID, newest != nil
}

// lastRoomLookupTimeout bounds the store query behind LastRoomForKey, whose
// signature carries no context.
const lastRoomLookupTimeout = 5 * time.Second

// LastRoomForKey returns the newest room, open or closed, that an alert with
// dedupKey opened for pipelineID, found from the dedup_key stored in
// room.created, so it works across restarts and for rooms that were closed a
// long time ago. dedupKey is the key as the source gave it (for the watcher,
// "watcher:<pipeline>"). closedAt is nil while the room is open. ok is false
// when no such room exists, when dedupKey is empty (no dedup), or when the
// lookup fails: the failure is logged at warn, and the caller sees "no room", so
// a database outage makes a cooldown check fail open for that tick. That is
// accepted: with the database down, opening a room would fail as well, so no
// duplicate room can result.
func (s *Service) LastRoomForKey(pipelineID, dedupKey string) (roomID string, closedAt *time.Time, ok bool) {
	if !pipelineIDRe.MatchString(pipelineID) {
		return "", nil, false
	}
	indexKey := alertIndexKey(pipelineID, dedupKey)
	if indexKey == "" {
		return "", nil, false
	}
	ctx, cancel := context.WithTimeout(s.ctx, lastRoomLookupTimeout)
	defer cancel()
	row, err := s.store.LastRoomByDedupKey(ctx, indexKey)
	if err != nil {
		// After Close, s.ctx is cancelled and the lookup fails for that reason
		// alone; that is not worth a warning.
		if !errors.Is(err, store.ErrNotFound) && s.ctx.Err() == nil {
			s.log.Warn("room: last room for key failed", "pipeline", pipelineID, "key", dedupKey, "err", err)
		}
		return "", nil, false
	}
	closedAt = row.ClosedAt
	if row.Status != "open" && closedAt == nil {
		t := s.now().UTC()
		closedAt = &t
	}
	// The event log is the source of truth. When the room is cached and its
	// snapshot disagrees with the row (the row write after room.closed failed),
	// the snapshot wins.
	// Shared store: fold a close another replica wrote first, reading the
	// store before s.mu is taken. A room deleted meanwhile is evicted and
	// simply loses the snapshot vote.
	s.syncRoom(ctx, row.ID)
	s.mu.Lock()
	if st, cached := s.rooms[row.ID]; cached {
		if s.roomOpen(st) {
			closedAt = nil
		} else if st.snap.ClosedAt != nil {
			t := *st.snap.ClosedAt
			closedAt = &t
		}
	}
	s.mu.Unlock()
	return row.ID, closedAt, true
}

// PostRoomNote appends text to an open room as a system message, for evidence
// and "still firing" updates from the watcher. It is the same join the service
// makes when OpenAlertRoom finds the room already open for a key
// (joinAlertRoomLocked), with caller-supplied text, and for any open room, not
// only one holding the watcher's key (an Alertmanager room found through
// OpenRoomForPipeline). It never starts or wakes the agent: system messages do
// not reach the agent inbox, and a room restored by Load stays idle. Humans see
// the note; the agent sees evidence only through PostRoomEvidence. A closed
// room returns a room_closed error and an unknown one not_found.
//
// The text is made storable first, so evidence is kept rather than refused:
// NUL bytes and invalid UTF-8 become U+FFFD (Postgres text and jsonb hold
// neither; Connect trace text can carry them). Then it is trimmed and cut so
// that the stored note, ellipsis included, is at most 4096 bytes, on a UTF-8
// boundary.
func (s *Service) PostRoomNote(ctx context.Context, roomID, text string) error {
	return s.PostRoomEvidence(ctx, roomID, text, nil)
}

// PostRoomEvidence is PostRoomNote for a watcher join that carries its
// evidence JSON (F6): after the note, the room's agent gets the evidence as a
// quiet "alert_update" input (agent.AlertEvidenceText; it never wakes an idle
// agent), and an old first look is re-run (alertUpdateLocked). Empty evidence
// is a plain note.
func (s *Service) PostRoomEvidence(ctx context.Context, roomID, text string, evidence json.RawMessage) error {
	return s.postWatcherNote(ctx, roomID, text, evidence, false)
}

// PostIncidentJSON is PostRoomEvidence for a join whose signals are new to the
// room: a different incident (watch.IncidentSink, QA G1). After the note the
// agent gets an "alert_update" with NewIncident set, which wakes an idle agent
// (agent.HumanInput.NewIncident). A room with no agent run (never started
// after a restart, given up, or one whose run ended on its own) gets a new run
// first, the way a steer starts one: through the agent slots and, with a
// shared store, the room lease, so there are never two runs. A shadow room only
// gets the note: its run is one-shot. evidence may be nil.
func (s *Service) PostIncidentJSON(ctx context.Context, roomID, text string, evidence json.RawMessage) error {
	return s.postWatcherNote(ctx, roomID, text, evidence, true)
}

// postWatcherNote is PostRoomEvidence and PostIncidentJSON.
func (s *Service) postWatcherNote(ctx context.Context, roomID, text string, evidence json.RawMessage, newIncident bool) error {
	text = sanitizeAgentText(text)
	text = strings.TrimSpace(text)
	if text == "" {
		return validation("note text is required")
	}
	if len(text) > maxNoteBytes {
		cut := maxNoteBytes - len("…")
		for cut > 0 && !utf8.RuneStart(text[cut]) {
			cut--
		}
		text = text[:cut] + "…"
	}
	// Backstop: the steps above leave nothing for this to refuse.
	if err := rejectUnstorable(text); err != nil {
		return err
	}
	// Shared store: fold what other replicas wrote, reading outside s.mu.
	s.syncRoom(ctx, roomID)
	s.mu.Lock()
	defer s.mu.Unlock()
	st, err := s.getStateLocked(ctx, roomID)
	if err != nil {
		return err
	}
	if !s.roomOpen(st) {
		return roomClosed("room already closed")
	}
	e, err := s.emitLocked(ctx, st, roomID, events.AgentMessage, systemActor, events.AgentMessagePayload{Text: text})
	if err != nil {
		// A full log cannot take more evidence; retrying would not help, so
		// the note is dropped with a log line instead of an error the watcher
		// would retry on every tick.
		if errors.Is(err, store.ErrLogFull) {
			s.log.Warn("room: note not stored, room log is full", "room", roomID)
			return nil
		}
		return fmt.Errorf("room: post note: %w", mapUnstorable(err))
	}
	switch {
	case newIncident:
		s.newIncidentLocked(ctx, roomID, st, e.Seq, text, evidence)
	case len(evidence) > 0:
		s.alertUpdateLocked(roomID, st, e.Seq, evidence)
	}
	return nil
}

// newIncidentLocked gives the room's agent a new incident: it starts a run
// when the room has none and queues a waking "alert_update" with NewIncident
// set. With a shared store only the lease owner's inbox takes it. Caller holds
// s.mu.
func (s *Service) newIncidentLocked(ctx context.Context, roomID string, st *roomState, seq int64, note string, raw json.RawMessage) {
	if st.snap.Shadow || st.inbox == nil || st.deleting || !s.roomOpen(st) {
		return
	}
	if st.agentCancel != nil && st.agentDone == st.agentRun {
		// The run ended on its own and its supervisor is gone: free the room
		// for a new run, as releaseAgent would have.
		st.agentCancel()
		st.agentCancel = nil
	}
	s.ensureAgentLocked(ctx, roomID, st)
	if st.agentCancel == nil || (s.coord != nil && !st.leased) {
		// Not started here (log full, service closing, another replica holds
		// the room): the note is in the room for the people.
		s.log.Info("room: new incident joined; no agent run here to tell", "room", roomID)
		return
	}
	text := note
	if len(raw) > 0 {
		text = agent.AlertEvidenceText(raw)
	}
	// After ensureAgentLocked: a lease taken there resets the inbox.
	st.inbox.push(agent.HumanInput{Seq: seq, Actor: systemActor, Kind: "alert_update", Text: text, NewIncident: true})
	if len(raw) > 0 {
		s.refireTriageLocked(roomID, st, raw)
	}
}

// markAgentDone records that run ended on its own (Run returned nil while the
// room's context was live): its supervisor has returned, so a new incident may
// start another run (newIncidentLocked).
func (s *Service) markAgentDone(st *roomState, run uint64) {
	s.mu.Lock()
	if st.agentRun == run {
		st.agentDone = run
	}
	s.mu.Unlock()
}

// WatchRoomInfo is the open room of a pipeline as the watcher's join rule
// sees it (watch.RoomInfo; cmd/server adapts it).
type WatchRoomInfo struct {
	RoomID string
	// AlertSource is room.created.alert_source: "watcher", a signed source or
	// "webhook"; "" for a room a person opened.
	AlertSource string
	OpenedAt    time.Time
	// LastEventAt is the time of the room's newest activity: every event but
	// the service's own bookkeeping (countsAsActivity), so a server restart or
	// a status change does not make an old room look recent.
	LastEventAt time.Time
}

// OpenRoomInfo describes the open room OpenRoomForPipeline picks for
// pipelineID (the newest), from its room.created event and its newest
// activity, both folded from the log. ok is false when the pipeline has no
// open room.
func (s *Service) OpenRoomInfo(pipelineID string) (WatchRoomInfo, bool) {
	roomID, ok := s.OpenRoomForPipeline(pipelineID)
	if !ok {
		return WatchRoomInfo{}, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	st, cached := s.rooms[roomID]
	if !cached || !s.roomOpen(st) {
		return WatchRoomInfo{}, false
	}
	last := st.snap.facts.lastActivity
	if last.IsZero() {
		last = st.snap.OpenedAt
	}
	return WatchRoomInfo{RoomID: roomID, AlertSource: st.snap.facts.alertSource, OpenedAt: st.snap.OpenedAt, LastEventAt: last}, true
}

// countsAsActivity reports whether e is room activity for LastEventAt:
// room.created and everything people, the agent and the executor write, and
// the system's notes (watcher evidence, a re-fired alert). Not: agent.status
// (the service writes queued, working and the restart's idle), the restart
// notice, and the system's other events (lease expiry, limits, its own pages).
func countsAsActivity(e events.Event) bool {
	switch {
	case e.Type == events.RoomCreated:
		return true
	case e.Type == events.AgentStatus:
		return false
	case e.Actor.Kind == events.ActorSystem && e.Actor.ID == systemActor.ID:
		if e.Type != events.AgentMessage {
			return false
		}
		p, ok := decode[events.AgentMessagePayload](e)
		return ok && p.Text != restartNoticeText
	}
	return true
}
