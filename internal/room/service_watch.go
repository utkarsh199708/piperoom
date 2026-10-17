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
	"piperoom/internal/watch"
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
		// fold count too. The reads happen outside every lock (refreshAll); a
		// failure falls back to the cache (the follower keeps it close).
		ctx, cancel := context.WithTimeout(s.ctx, lastRoomLookupTimeout)
		if err := s.refreshAll(ctx); err != nil && s.ctx.Err() == nil {
			s.log.Warn("room: open room for pipeline: refresh failed; using the cache", "pipeline", pipelineID, "err", err)
		}
		cancel()
	}
	rooms, opening := s.roomsForPipeline(pipelineID)
	// A room being opened for the pipeline right now is waited for, so the
	// watcher does not open a second one beside it: its lock is held until
	// it is published (or dropped, when its room.created failed).
	for _, id := range opening {
		st := s.lockCachedRoom(id)
		if st == nil {
			continue
		}
		if !st.gone {
			snap := st.snap.Clone()
			rooms[id] = &snap
		}
		s.unlockRoom(st)
	}
	var newest *RoomSnapshot
	for id, p := range rooms {
		if p.PipelineID != pipelineID || p.Status != "open" {
			continue
		}
		if newest == nil || p.OpenedAt.After(newest.OpenedAt) ||
			(p.OpenedAt.Equal(newest.OpenedAt) && id < roomID) {
			newest, roomID = p, id
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
	// published snapshot is closed while the row is not (the row write after
	// room.closed failed), the snapshot's close counts. It only ever adds a
	// close: a snapshot still open may just not have published a close in
	// progress, whose row write already landed.
	// Shared store: fold a close another replica wrote first, reading the
	// store outside the room's lock. A room deleted meanwhile is evicted and
	// simply loses the snapshot vote.
	s.syncRoom(ctx, row.ID)
	if p := s.published(row.ID); p != nil && p.Status != "open" && p.ClosedAt != nil {
		t := *p.ClosedAt
		closedAt = &t
	}
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
// neither; Connect trace text can carry them), and bidi controls are dropped
// (stripBidi), not refused as in typed text. Then it is trimmed and cut so
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
	text = stripBidi(sanitizeAgentText(text))
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
	if err := checkStorable(text); err != nil {
		return err
	}
	// Shared store: fold what other replicas wrote, reading outside the lock.
	s.syncRoom(ctx, roomID)
	st, err := s.lockRoom(ctx, roomID)
	if err != nil {
		return err
	}
	defer s.unlockRoom(st)
	if !s.roomOpen(st) {
		return roomClosed("room already closed")
	}
	// The note keeps the evidence's incident keys, so RoomSignalKeys can tell
	// after a restart which signals the room carried (Q3, QA S1).
	keys := watch.EvidenceKeys(evidence)
	state := ""
	switch {
	case len(evidence) > 0:
		state = alertFiring
	case isAllClearNote(text):
		state = alertResolved
	}
	var payload any = events.AgentMessagePayload{Text: text}
	if len(keys) > 0 || state != "" {
		payload = incidentNotePayload{AgentMessagePayload: events.AgentMessagePayload{Text: text}, SignalKeys: keys, AlertState: state}
	}
	if newIncident {
		in := incidentNotePayload{AgentMessagePayload: events.AgentMessagePayload{Text: text}, NewIncident: true, SignalKeys: keys, AlertState: state}
		if len(evidence) > 0 {
			in.AgentText = agent.AlertEvidenceText(evidence)
			in.Signal, in.Writers, in.Layer = firstWatcherSignal(evidence)
		}
		payload = in
	}
	e, err := s.emitLocked(ctx, st, roomID, events.AgentMessage, systemActor, payload)
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
		s.newIncidentLocked(ctx, roomID, st, evidence)
	case len(evidence) > 0:
		s.alertUpdateLocked(roomID, st, e.Seq, evidence)
	}
	return nil
}

// incidentNotePayload is the agent.message a new-incident join writes: the
// note, plus the mark and the text the agent gets (QA F21). The log is the
// store-backed inbox (docs/design/replicas.md section 4): whichever replica
// holds the room's agent lease folds this event and inputFor turns it into
// the waking "alert_update" with NewIncident set, so the watcher's leader
// need not be the lease owner. Only the service writes system messages, so
// the mark cannot come from a person. Readers of AgentMessagePayload ignore
// the extra fields.
type incidentNotePayload struct {
	events.AgentMessagePayload
	NewIncident bool `json:"new_incident,omitempty"`
	// AgentText is the evidence as the agent reads it
	// (agent.AlertEvidenceText); "" means the note text.
	AgentText string `json:"agent_text,omitempty"`
	// SignalKeys are the incident keys of the note's watcher evidence
	// (watch.EvidenceKeys), kept so RoomSignalKeys works from the log.
	SignalKeys []string `json:"signal_keys,omitempty"`
	// AlertState is what the note says about the room's alert: alertFiring
	// (evidence, a firing repeat) or alertResolved (the source's resolved
	// report, the watcher's all-clear); "" says nothing. The post-mortem
	// reads it to tell a close while firing from a fix (Q5 T4).
	AlertState string `json:"alert_state,omitempty"`
	// Signal, Writers and Layer are a new incident's first watcher signal
	// with a known layer (firstWatcherSignal), so the room pages that
	// layer's owners too (Q5 T3). Empty for other notes.
	Signal  string   `json:"signal,omitempty"`
	Writers []string `json:"writers,omitempty"`
	Layer   string   `json:"layer,omitempty"`
}

// Alert states of incidentNotePayload.AlertState.
const (
	alertFiring   = "firing"
	alertResolved = "resolved"
)

// allClearSuffix ends the watcher's all-clear note in a room it only joined
// (internal/watch resolve: "watcher: <pipeline>: all watcher signals are
// clear"). The watcher posts it without evidence, so its text is the only
// mark; only the watcher writes system notes through postWatcherNote.
const allClearSuffix = ": all watcher signals are clear"

func isAllClearNote(text string) bool {
	return strings.HasPrefix(text, "watcher: ") && strings.HasSuffix(text, allClearSuffix)
}

// resolvedJoinSuffix ends the note a resolved repeat writes (joinAlertRoomLocked).
const resolvedJoinSuffix = "reported resolved at its source; the room stays open until the driver closes it"

// noteAlertState is the alert state a system agent.message records (see
// incidentNotePayload.AlertState); a note written before the field existed is
// read from the service's own fixed wording. Caller checks the actor.
func noteAlertState(e events.Event) string {
	p, ok := decode[incidentNotePayload](e)
	if !ok {
		return ""
	}
	switch {
	case p.AlertState != "":
		return p.AlertState
	case isAllClearNote(p.Text), strings.HasPrefix(p.Text, "alert ") && strings.HasSuffix(p.Text, resolvedJoinSuffix):
		return alertResolved
	case len(p.SignalKeys) > 0 || p.NewIncident:
		return alertFiring
	}
	return ""
}

// incidentInput is the agent input of a new-incident note, or nil for any
// other agent.message.
func incidentInput(e events.Event) *agent.HumanInput {
	if e.Type != events.AgentMessage || e.Actor.Kind != events.ActorSystem {
		return nil
	}
	p, ok := decode[incidentNotePayload](e)
	if !ok || !p.NewIncident {
		return nil
	}
	text := p.AgentText
	if text == "" {
		text = p.Text
	}
	return &agent.HumanInput{Seq: e.Seq, Actor: e.Actor, Kind: "alert_update", Text: text, NewIncident: true}
}

// newIncidentLocked gives the room's agent a new incident on the replica
// that wrote the note: the note's fold already queued the waking input
// (inputFor) for the lease owner, so this only starts a run when the room
// has none, and re-runs an old first look. Caller holds st's lock.
func (s *Service) newIncidentLocked(ctx context.Context, roomID string, st *roomState, raw json.RawMessage) {
	if !s.wakeForIncidentLocked(ctx, roomID, st) {
		// Not started here (log full, service closing, another replica holds
		// the room): the lease owner reads the incident from the log.
		s.log.Info("room: new incident joined; the agent runs elsewhere or not at all", "room", roomID)
		return
	}
	if len(raw) > 0 {
		s.refireTriageLocked(roomID, st, raw)
	}
}

// wakeForIncidentLocked makes sure the room has an agent run here to read a
// new incident: a run that ended on its own is freed and a new one started,
// the way a steer starts one (agent slots, and with a shared store the room
// lease). A lease taken here primes the inbox with the pending input, the
// incident included. It reports whether a run is here. Caller holds st's
// lock.
func (s *Service) wakeForIncidentLocked(ctx context.Context, roomID string, st *roomState) bool {
	if st.snap.Shadow || st.inbox == nil || st.deleting || !s.roomOpen(st) {
		return false
	}
	if st.agentCancel != nil && st.agentDone == st.agentRun {
		// The run ended on its own and its supervisor is gone: free the room
		// for a new run, as releaseAgent would have.
		st.agentCancel()
		st.agentCancel = nil
	}
	s.ensureAgentLocked(roomID, st)
	return st.agentCancel != nil && (s.coord == nil || st.leased)
}

// markAgentDone records that run ended on its own (Run returned nil while the
// room's context was live): its supervisor has returned, so a new incident may
// start another run (newIncidentLocked).
func (s *Service) markAgentDone(st *roomState, run uint64) {
	s.lockState(st)
	if !st.gone && st.agentRun == run {
		st.agentDone = run
	}
	s.unlockRoom(st)
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
	// The published snapshot answers without the room's lock; a room still
	// being opened has none yet, so its lock is waited for, as
	// OpenRoomForPipeline does.
	snap := s.published(roomID)
	if snap == nil {
		st := s.lockCachedRoom(roomID)
		if st == nil {
			return WatchRoomInfo{}, false
		}
		c := st.snap.Clone()
		s.unlockRoom(st)
		snap = &c
	}
	if snap.Status != "open" {
		return WatchRoomInfo{}, false
	}
	last := snap.facts.lastActivity
	if last.IsZero() {
		last = snap.OpenedAt
	}
	return WatchRoomInfo{RoomID: roomID, AlertSource: snap.facts.alertSource, OpenedAt: snap.OpenedAt, LastEventAt: last}, true
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

// RoomSignalKeys returns the incident keys a watcher room carried: the union
// of watch.EvidenceKeys over room.created.alert of a watcher room and the
// evidence of every watcher note, sorted and de-duplicated (watch.
// ClosedRoomKeysSink). It reads the folded log, so it works for closed rooms
// and after a restart; the fold is cached like any room's. ok is false when
// the room is unknown or carries no keys.
func (s *Service) RoomSignalKeys(roomID string) ([]string, bool) {
	var keys []string
	if !s.withRoomFacts(roomID, func(snap RoomSnapshot) { keys = append(keys, snap.facts.signalKeys...) }) {
		return nil, false
	}
	return keys, len(keys) > 0
}

// RoomTitleFlags returns the room's title_flags: the injectscan kinds found in
// its title (watch.RoomTitleFlagsSink). ok is false for an unknown room.
func (s *Service) RoomTitleFlags(roomID string) ([]string, bool) {
	flags := []string{}
	if !s.withRoomFacts(roomID, func(snap RoomSnapshot) { flags = append(flags, snap.TitleFlags...) }) {
		return nil, false
	}
	return flags, true
}

// withRoomFacts runs f on the room's folded snapshot (the published one, or
// the room loaded from the log); false when the room is unknown or cannot be
// read. f must not keep or change what the snapshot shares.
func (s *Service) withRoomFacts(roomID string, f func(RoomSnapshot)) bool {
	if roomID == "" {
		return false
	}
	ctx, cancel := context.WithTimeout(s.ctx, lastRoomLookupTimeout)
	defer cancel()
	s.syncRoom(ctx, roomID)
	snap, err := s.readRoom(ctx, roomID)
	if err != nil {
		var re *Error
		if !errors.As(err, &re) && s.ctx.Err() == nil {
			s.log.Warn("room: read room facts failed", "room", roomID, "err", err)
		}
		return false
	}
	f(*snap)
	return true
}
