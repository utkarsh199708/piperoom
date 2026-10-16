package memstore

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"piperoom/internal/events"
	"piperoom/internal/store"
)

var _ store.ShadowStore = (*Store)(nil)

// copyRun returns a deep copy, so callers can never change the stored row.
func copyRun(r store.ShadowRun) store.ShadowRun {
	cp := r
	if r.FinalHypothesis != nil {
		cp.FinalHypothesis = append(json.RawMessage(nil), r.FinalHypothesis...)
	}
	cp.UTC()
	return cp
}

// CreateShadowRun implements store.ShadowStore: idempotent, and the room must
// exist (the Postgres foreign key).
func (s *Store) CreateShadowRun(ctx context.Context, r store.ShadowRun) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := r.Check(); err != nil {
		return fmt.Errorf("memstore: create shadow run %s: %w", r.RoomID, err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.rooms[r.RoomID]; !ok {
		return fmt.Errorf("memstore: create shadow run: room %q not found: %w", r.RoomID, store.ErrNotFound)
	}
	if _, ok := s.shadowRuns[r.RoomID]; ok {
		return nil
	}
	cp := copyRun(r)
	cp.LastSeq, cp.RevealedAt = 0, nil
	s.shadowRuns[r.RoomID] = &cp
	return nil
}

// GetShadowRun implements store.ShadowStore.
func (s *Store) GetShadowRun(ctx context.Context, roomID string) (store.ShadowRun, error) {
	if err := ctx.Err(); err != nil {
		return store.ShadowRun{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.shadowRuns[roomID]
	if !ok {
		return store.ShadowRun{}, fmt.Errorf("memstore: shadow run %q: %w", roomID, store.ErrNotFound)
	}
	return copyRun(*r), nil
}

// UpdateShadowRun implements store.ShadowStore; see the interface for the columns.
func (s *Store) UpdateShadowRun(ctx context.Context, r store.ShadowRun) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := r.Check(); err != nil {
		return fmt.Errorf("memstore: update shadow run %s: %w", r.RoomID, err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.shadowRuns[r.RoomID]
	if !ok {
		return fmt.Errorf("memstore: update shadow run %q: %w", r.RoomID, store.ErrNotFound)
	}
	// Same columns as the Postgres store: the hypothesis columns belong to
	// AppendShadow, revealed_at to MarkRevealed.
	cp := copyRun(r)
	cp.LastSeq, cp.RevealedAt = cur.LastSeq, cur.RevealedAt
	cp.FirstHypothesisAt, cp.FinalHypothesisAt, cp.FinalHypothesis = cur.FirstHypothesisAt, cur.FinalHypothesisAt, cur.FinalHypothesis
	s.shadowRuns[r.RoomID] = &cp
	return nil
}

// AppendShadow implements store.ShadowStore with the same pre-check, seq and
// hypothesis rules as the Postgres store.
func (s *Store) AppendShadow(ctx context.Context, roomID string, typ events.Type, actor events.Actor, payload any) (events.Event, error) {
	if err := ctx.Err(); err != nil {
		return events.Event{}, err
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return events.Event{}, fmt.Errorf("memstore: marshal shadow payload: %w", err)
	}
	actorJSON, err := json.Marshal(actor)
	if err != nil {
		return events.Event{}, fmt.Errorf("memstore: marshal shadow actor: %w", err)
	}
	if err := store.CheckAppend(roomID, typ, raw, actorJSON); err != nil {
		return events.Event{}, fmt.Errorf("memstore: append shadow to %s: %w", roomID, err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	run, ok := s.shadowRuns[roomID]
	if !ok {
		return events.Event{}, fmt.Errorf("memstore: append shadow to %q: no run row: %w", roomID, store.ErrNotFound)
	}
	// The room's log caps hold for its shadow log too, with the same rule as
	// the Postgres store.
	size := int64(len(raw))
	if !s.limits.Fits(typ, actor, run.LastSeq, s.shadowBytes[roomID], size) {
		return events.Event{}, fmt.Errorf("memstore: append shadow %s to %s (%d events, %d bytes): %w", typ, roomID, run.LastSeq, s.shadowBytes[roomID], store.ErrLogFull)
	}
	s.shadowBytes[roomID] += size
	run.LastSeq++
	e := events.Event{RoomID: roomID, Seq: run.LastSeq, Type: typ, Actor: actor, Payload: raw, At: s.now().UTC().Truncate(time.Microsecond)}
	s.shadowEvents[roomID] = append(s.shadowEvents[roomID], e)
	if typ == events.AgentHypothesis {
		at := e.At
		if run.FirstHypothesisAt == nil {
			first := at
			run.FirstHypothesisAt = &first
		}
		run.FinalHypothesisAt = &at
		run.FinalHypothesis = append(json.RawMessage(nil), raw...)
	}
	return e, nil
}

// ShadowSince implements store.ShadowStore.
func (s *Store) ShadowSince(ctx context.Context, roomID string, afterSeq int64, limit int) ([]events.Event, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	out := []events.Event{}
	if limit <= 0 {
		return out, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, e := range s.shadowEvents[roomID] {
		if e.Seq > afterSeq {
			out = append(out, e)
			if len(out) >= limit {
				break
			}
		}
	}
	return out, nil
}

// CountShadowEvents implements store.ShadowStore.
func (s *Store) CountShadowEvents(ctx context.Context, roomID string, typ events.Type) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, e := range s.shadowEvents[roomID] {
		if e.Type == typ {
			n++
		}
	}
	return n, nil
}

// MarkRevealed implements store.ShadowStore.
func (s *Store) MarkRevealed(ctx context.Context, roomID string, at time.Time) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	run, ok := s.shadowRuns[roomID]
	if !ok {
		return false, fmt.Errorf("memstore: mark revealed %q: %w", roomID, store.ErrNotFound)
	}
	if run.RevealedAt != nil {
		return false, nil
	}
	t := at.UTC()
	run.RevealedAt = &t
	return true, nil
}

// ListShadowRuns implements store.ShadowStore.
func (s *Store) ListShadowRuns(ctx context.Context, pipelineID string, since time.Time) ([]store.ShadowRunRoom, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	out := []store.ShadowRunRoom{}
	for id, run := range s.shadowRuns {
		log, ok := s.rooms[id]
		if !ok {
			continue
		}
		m := log.meta
		if m.Status != "closed" || m.ClosedAt == nil || m.ClosedAt.Before(since) {
			continue
		}
		if pipelineID != "" && m.PipelineID != pipelineID {
			continue
		}
		rr := store.ShadowRunRoom{
			ShadowRun:   copyRun(*run),
			PipelineID:  m.PipelineID,
			AlertSource: alertSource(log.events),
			OpenedAt:    m.OpenedAt.UTC(),
			ClosedAt:    m.ClosedAt.UTC(),
		}
		out = append(out, rr)
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].ClosedAt.Equal(out[j].ClosedAt) {
			return out[i].ClosedAt.After(out[j].ClosedAt)
		}
		return out[i].RoomID < out[j].RoomID
	})
	return out, nil
}

// alertSource reads room.created.alert_source from a room's first event.
func alertSource(evs []events.Event) string {
	if len(evs) == 0 || evs[0].Type != events.RoomCreated {
		return ""
	}
	var p events.RoomCreatedPayload
	if err := json.Unmarshal(evs[0].Payload, &p); err != nil {
		return ""
	}
	return p.AlertSource
}

// UnrevealedShadowRuns implements store.ShadowStore.
func (s *Store) UnrevealedShadowRuns(ctx context.Context) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	ids := []string{}
	for id, run := range s.shadowRuns {
		log, ok := s.rooms[id]
		if !ok || log.meta.Status != "closed" {
			continue
		}
		if run.FinishedAt != nil && run.RevealedAt == nil {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	return ids, nil
}
