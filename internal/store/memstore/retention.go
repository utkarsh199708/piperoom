package memstore

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"piperoom/internal/store"
)

var (
	_ store.Purger  = (*Store)(nil)
	_ store.Deleter = (*Store)(nil)
)

// PurgeClosedBefore purges closed rooms of every pipeline, with no archive.
func (s *Store) PurgeClosedBefore(ctx context.Context, cutoff time.Time, maxRooms int, exclude []string) (store.PurgeResult, error) {
	return s.PurgeClosed(ctx, store.PurgeQuery{Cutoff: cutoff, MaxRooms: maxRooms, Exclude: exclude})
}

// PurgeClosed implements store.Purger with the same rules as the Postgres
// store: only closed rooms with closed_at before the cutoff (of one pipeline
// when set), oldest first, at most MaxRooms, every row keyed by the room goes
// with it, and the archive runs first (outside the lock: it reads the log).
func (s *Store) PurgeClosed(ctx context.Context, q store.PurgeQuery) (store.PurgeResult, error) {
	var res store.PurgeResult
	if err := ctx.Err(); err != nil {
		return res, err
	}
	if q.MaxRooms <= 0 {
		return res, nil
	}
	ids := s.expired(q)
	res.Examined = len(ids)
	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			return res, fmt.Errorf("memstore: purge: %w", err)
		}
		wantSeq := int64(-1)
		if q.Archive != nil {
			seq, err := q.Archive(ctx, id)
			if err != nil {
				res.Failed = append(res.Failed, store.PurgeFailure{RoomID: id, Err: fmt.Errorf("memstore: purge %s: archive: %w", id, err)})
				continue
			}
			wantSeq = seq
		}
		s.mu.Lock()
		one, ok, err := s.purgeClosedLocked(id, q, wantSeq)
		s.mu.Unlock()
		if err != nil {
			res.Failed = append(res.Failed, store.PurgeFailure{RoomID: id, Err: err})
			continue
		}
		if ok {
			res.Add(one)
		}
	}
	return res, nil
}

// expired lists the rooms q selects, oldest close first.
func (s *Store) expired(q store.PurgeQuery) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	type cand struct {
		id string
		at time.Time
	}
	var cands []cand
	skip := make(map[string]bool, len(q.Exclude))
	for _, id := range q.Exclude {
		skip[id] = true
	}
	for id, log := range s.rooms {
		if skip[id] {
			continue
		}
		m := log.meta
		if q.PipelineID != "" && m.PipelineID != q.PipelineID {
			continue
		}
		if m.Status == "closed" && m.ClosedAt != nil && m.ClosedAt.Before(q.Cutoff) {
			cands = append(cands, cand{id, *m.ClosedAt})
		}
	}
	sort.Slice(cands, func(i, j int) bool {
		if !cands[i].at.Equal(cands[j].at) {
			return cands[i].at.Before(cands[j].at)
		}
		return cands[i].id < cands[j].id
	})
	if len(cands) > q.MaxRooms {
		cands = cands[:q.MaxRooms]
	}
	ids := make([]string, len(cands))
	for i, c := range cands {
		ids[i] = c.id
	}
	return ids
}

// purgeClosedLocked re-checks that the room still qualifies (and still ends at
// wantSeq when that is >= 0) and deletes it. Caller holds s.mu.
func (s *Store) purgeClosedLocked(id string, q store.PurgeQuery, wantSeq int64) (store.PurgeResult, bool, error) {
	log, ok := s.rooms[id]
	if !ok {
		return store.PurgeResult{}, false, nil
	}
	m := log.meta
	if m.Status != "closed" || m.ClosedAt == nil || !m.ClosedAt.Before(q.Cutoff) {
		return store.PurgeResult{}, false, nil
	}
	if wantSeq >= 0 && log.lastSeq != wantSeq {
		return store.PurgeResult{}, false, fmt.Errorf("memstore: purge %s: archived %d events, room has %d: %w", id, wantSeq, log.lastSeq, store.ErrRoomChanged)
	}
	res := s.deleteLocked(id)
	// The same content-free audit row a Postgres purge writes in its
	// transaction; here the lock is the transaction.
	s.deletions[id] = q.Audit(id, m.PipelineID, res.Events, s.now)
	res.RoomIDs = []string{id}
	return res, true, nil
}

// deleteLocked removes a room and every row keyed by it and counts them.
// Caller holds s.mu and has checked the room exists.
func (s *Store) deleteLocked(id string) store.PurgeResult {
	var res store.PurgeResult
	log := s.rooms[id]
	res.Events += int64(len(log.events))
	delete(s.rooms, id)

	for pid, p := range s.proposals {
		if p.RoomID != id {
			continue
		}
		res.Proposals++
		for key := range s.approvals {
			if strings.HasPrefix(key, pid+"\x00") {
				res.Approvals++
				delete(s.approvals, key)
			}
		}
		for key := range s.steps {
			if strings.HasPrefix(key, pid+"\x00") {
				res.Steps++
				delete(s.steps, key)
			}
		}
		delete(s.proposals, pid)
	}
	for sid, r := range s.suggestions {
		if r.RoomID == id {
			res.Suggestions++
			delete(s.suggestions, sid)
		}
	}
	for hid, h := range s.handoffs {
		if h.RoomID == id {
			res.Handoffs++
			delete(s.handoffs, hid)
		}
	}
	if _, ok := s.shadowRuns[id]; ok {
		res.ShadowEvents += int64(len(s.shadowEvents[id]))
		res.ShadowRuns++
		delete(s.shadowEvents, id)
		delete(s.shadowBytes, id)
		delete(s.shadowRuns, id)
	}
	return res
}

// DeleteRoom implements store.Deleter.
func (s *Store) DeleteRoom(ctx context.Context, d store.RoomDeletion, wantSeq int64) (store.RoomDeletion, store.PurgeResult, error) {
	if err := ctx.Err(); err != nil {
		return store.RoomDeletion{}, store.PurgeResult{}, err
	}
	if err := store.CheckText(d.RoomID, d.DeletedBy, d.Reason); err != nil {
		return store.RoomDeletion{}, store.PurgeResult{}, fmt.Errorf("memstore: delete room: %w", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	log, ok := s.rooms[d.RoomID]
	if !ok {
		return store.RoomDeletion{}, store.PurgeResult{}, fmt.Errorf("memstore: delete room %q: %w", d.RoomID, store.ErrNotFound)
	}
	if wantSeq >= 0 && log.lastSeq != wantSeq {
		return store.RoomDeletion{}, store.PurgeResult{}, fmt.Errorf("memstore: delete room %s: archived %d events, room has %d: %w", d.RoomID, wantSeq, log.lastSeq, store.ErrRoomChanged)
	}
	if _, dup := s.deletions[d.RoomID]; dup {
		return store.RoomDeletion{}, store.PurgeResult{}, fmt.Errorf("memstore: delete room %s: audit row: %w", d.RoomID, store.ErrAlreadyExists)
	}
	d.PipelineID = log.meta.PipelineID
	res := s.deleteLocked(d.RoomID)
	d.EventCount = res.Events
	d.DeletedAt = d.DeletedAt.UTC()
	s.deletions[d.RoomID] = d
	res.RoomIDs = []string{d.RoomID}
	res.Examined = 1
	return d, res, nil
}

// GetRoomDeletion implements store.Deleter.
func (s *Store) GetRoomDeletion(ctx context.Context, roomID string) (store.RoomDeletion, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, ok := s.deletions[roomID]
	if !ok {
		return store.RoomDeletion{}, fmt.Errorf("memstore: room deletion %q: %w", roomID, store.ErrNotFound)
	}
	return d, nil
}
