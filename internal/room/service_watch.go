package room

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"piperoom/internal/events"
	"piperoom/internal/store"
)

// This file holds the room-service surface the built-in watcher (internal/watch)
// uses: the OnRoomClosed hook (see Deps), and the three queries and one write
// below. The watcher opens, joins and resolves through OpenAlertRoom with
// Source alertin.Watcher and DedupKey "watcher:<pipeline>"; nothing here starts
// an agent.

// maxNoteBytes caps a PostRoomNote text. Evidence is a few lines of numbers; a
// runaway caller must not fill the event log.
const maxNoteBytes = 4096

// replacementChar stands in for bytes a note cannot store (U+FFFD).
const replacementChar = string(rune(0xFFFD))

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
// start-up; it does no store I/O. A pipelineID that is not a valid pipeline id
// (pipelineIDRe) matches nothing.
func (s *Service) OpenRoomForPipeline(pipelineID string) (roomID string, ok bool) {
	if !pipelineIDRe.MatchString(pipelineID) {
		return "", false
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
// the note; the agent sees only the alert body it was started with. A closed
// room returns a room_closed error and an unknown one not_found.
//
// The text is made storable first, so evidence is kept rather than refused:
// NUL bytes and invalid UTF-8 become U+FFFD (Postgres text and jsonb hold
// neither; Connect trace text can carry them). Then it is trimmed and cut so
// that the stored note, ellipsis included, is at most 4096 bytes, on a UTF-8
// boundary.
func (s *Service) PostRoomNote(ctx context.Context, roomID, text string) error {
	text = strings.ReplaceAll(strings.ToValidUTF8(text, replacementChar), string(rune(0)), replacementChar)
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
	if err := checkTextStorable("note", text); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	st, err := s.getStateLocked(ctx, roomID)
	if err != nil {
		return err
	}
	if !s.roomOpen(st) {
		return roomClosed("room already closed")
	}
	if _, err := s.emitLocked(ctx, st, roomID, events.AgentMessage, systemActor, events.AgentMessagePayload{Text: text}); err != nil {
		return fmt.Errorf("room: post note: %w", mapUnstorable(err))
	}
	return nil
}
