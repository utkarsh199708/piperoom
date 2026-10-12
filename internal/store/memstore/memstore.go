// Package memstore is an in-memory store.Store used by unit tests across the
// room, httpapi and ws packages. It implements the same semantics as the
// Postgres store (seqs start at 1 with no gaps, subscribers replay then stream
// live, uniqueness rules) but keeps everything in RAM.
package memstore

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"

	"piperoom/internal/events"
	"piperoom/internal/store"
)

// Store is an in-memory implementation of store.Store.
type Store struct {
	mu    sync.Mutex
	rooms map[string]*roomLog
	now   func() time.Time

	proposals   map[string]*store.ProposalRow
	suggestions map[string]*store.SuggestionRow
	handoffs    map[string]*store.HandoffRow
	approvals   map[string]struct{}
	steps       map[string]string // proposalID + "\x00" + stepIndex -> status
}

type roomLog struct {
	meta    store.Room
	lastSeq int64
	events  []events.Event
	subs    map[*subscriber]struct{}
}

type subscriber struct {
	roomID  string
	lastSeq int64
	ch      chan events.Event
	wake    chan struct{}
}

// New returns an empty in-memory store.
func New() *Store {
	return &Store{
		rooms:       map[string]*roomLog{},
		now:         time.Now,
		proposals:   map[string]*store.ProposalRow{},
		suggestions: map[string]*store.SuggestionRow{},
		handoffs:    map[string]*store.HandoffRow{},
		approvals:   map[string]struct{}{},
		steps:       map[string]string{},
	}
}

// NewWithClock lets tests inject a deterministic clock.
func NewWithClock(now func() time.Time) *Store {
	s := New()
	s.now = now
	return s
}

var _ store.Store = (*Store)(nil)

// Append assigns the next seq for the room and stores the event.
func (s *Store) Append(ctx context.Context, roomID string, typ events.Type, actor events.Actor, payload any) (events.Event, error) {
	if err := ctx.Err(); err != nil {
		return events.Event{}, err
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return events.Event{}, fmt.Errorf("memstore: marshal payload: %w", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	log, ok := s.rooms[roomID]
	if !ok {
		return events.Event{}, fmt.Errorf("memstore: room %q not found: %w", roomID, store.ErrNotFound)
	}
	log.lastSeq++
	log.meta.LastSeq = log.lastSeq
	e := events.Event{
		RoomID:  roomID,
		Seq:     log.lastSeq,
		Type:    typ,
		Actor:   actor,
		Payload: raw,
		At:      s.now().UTC(),
	}
	log.events = append(log.events, e)
	for sub := range log.subs {
		// Only signal; the subscriber goroutine reads Since(lastSeq) itself,
		// so no event is lost even if several appends coalesce one wake.
		select {
		case sub.wake <- struct{}{}:
		default:
		}
	}
	return e, nil
}

// Since returns events with Seq > afterSeq in order, at most limit.
func (s *Store) Since(ctx context.Context, roomID string, afterSeq int64, limit int) ([]events.Event, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if limit <= 0 {
		return []events.Event{}, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	log, ok := s.rooms[roomID]
	if !ok {
		return nil, fmt.Errorf("memstore: room %q not found: %w", roomID, store.ErrNotFound)
	}
	out := []events.Event{}
	for _, e := range log.events {
		if e.Seq > afterSeq {
			out = append(out, e)
			if len(out) >= limit {
				break
			}
		}
	}
	return out, nil
}

// Subscribe replays events after afterSeq then streams new ones live.
func (s *Store) Subscribe(ctx context.Context, roomID string, afterSeq int64) (<-chan events.Event, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	sub := &subscriber{roomID: roomID, lastSeq: afterSeq, ch: make(chan events.Event, 256), wake: make(chan struct{}, 1)}
	s.mu.Lock()
	log, ok := s.rooms[roomID]
	if !ok {
		s.mu.Unlock()
		return nil, fmt.Errorf("memstore: room %q not found: %w", roomID, store.ErrNotFound)
	}
	log.subs[sub] = struct{}{}
	s.mu.Unlock()
	go s.runSub(ctx, sub)
	return sub.ch, nil
}

// runSub replays events after lastSeq, then streams live until ctx ends. Only
// this goroutine sends on sub.ch, so closing it in the deferred cleanup is safe.
func (s *Store) runSub(ctx context.Context, sub *subscriber) {
	defer func() {
		s.mu.Lock()
		if log := s.rooms[sub.roomID]; log != nil {
			delete(log.subs, sub)
		}
		s.mu.Unlock()
		close(sub.ch)
	}()
	for {
		drained, err := s.drain(ctx, sub)
		if err != nil {
			return
		}
		if ctx.Err() != nil {
			return
		}
		if drained {
			continue
		}
		select {
		case <-sub.wake:
		case <-ctx.Done():
			return
		}
	}
}

// drain delivers all events after lastSeq; reports whether any were sent.
func (s *Store) drain(ctx context.Context, sub *subscriber) (bool, error) {
	sent := false
	for {
		after := sub.lastSeq
		evs, err := s.Since(ctx, sub.roomID, after, 1000)
		if err != nil {
			return sent, err
		}
		if len(evs) == 0 {
			return sent, nil
		}
		for _, e := range evs {
			if e.Seq <= sub.lastSeq {
				continue
			}
			select {
			case sub.ch <- e:
				sub.lastSeq = e.Seq
				sent = true
			case <-ctx.Done():
				return sent, ctx.Err()
			}
		}
		if len(evs) < 1000 {
			return sent, nil
		}
	}
}

// CreateRoom stores the room row.
func (s *Store) CreateRoom(ctx context.Context, r store.Room) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.rooms[r.ID]; ok {
		return fmt.Errorf("memstore: room %q already exists", r.ID)
	}
	r.LastSeq = 0
	s.rooms[r.ID] = &roomLog{meta: r, subs: map[*subscriber]struct{}{}}
	return nil
}

// GetRoom returns a room row or store.ErrNotFound.
func (s *Store) GetRoom(ctx context.Context, id string) (store.Room, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	log, ok := s.rooms[id]
	if !ok {
		return store.Room{}, fmt.Errorf("memstore: room %q not found: %w", id, store.ErrNotFound)
	}
	return log.meta, nil
}

// ListRooms returns all rooms in the same total order as the Postgres store:
// newest first, ties broken by id.
func (s *Store) ListRooms(ctx context.Context) ([]store.Room, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]store.Room, 0, len(s.rooms))
	for _, log := range s.rooms {
		out = append(out, log.meta)
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].OpenedAt.Equal(out[j].OpenedAt) {
			return out[i].OpenedAt.After(out[j].OpenedAt)
		}
		return out[i].ID < out[j].ID
	})
	return out, nil
}

// DeleteEmptyRoom removes a room that has no events; see store.Store.
func (s *Store) DeleteEmptyRoom(ctx context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	log, ok := s.rooms[id]
	if !ok {
		return nil
	}
	if len(log.events) > 0 {
		return fmt.Errorf("memstore: delete empty room %q: %w", id, store.ErrRoomNotEmpty)
	}
	delete(s.rooms, id)
	return nil
}

// LastRoomByDedupKey returns the newest room whose room.created event carries
// dedupKey, with the same order as the Postgres store (opened_at, then id).
func (s *Store) LastRoomByDedupKey(ctx context.Context, dedupKey string) (store.Room, error) {
	if err := ctx.Err(); err != nil {
		return store.Room{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var best *store.Room
	if dedupKey != "" {
		for _, log := range s.rooms {
			if !createdWithKey(log.events, dedupKey) {
				continue
			}
			m := log.meta
			if best == nil || m.OpenedAt.After(best.OpenedAt) || (m.OpenedAt.Equal(best.OpenedAt) && m.ID < best.ID) {
				best = &m
			}
		}
	}
	if best == nil {
		return store.Room{}, fmt.Errorf("memstore: no room with dedup key %q: %w", dedupKey, store.ErrNotFound)
	}
	return *best, nil
}

// createdWithKey reports whether the room.created event in evs holds dedupKey.
func createdWithKey(evs []events.Event, dedupKey string) bool {
	for _, e := range evs {
		if e.Type != events.RoomCreated {
			continue
		}
		var p events.RoomCreatedPayload
		if err := json.Unmarshal(e.Payload, &p); err == nil && p.DedupKey == dedupKey {
			return true
		}
	}
	return false
}

// UpdateDriver records the current driver on the room row.
func (s *Store) UpdateDriver(ctx context.Context, id, driverID, driverName string, leaseUntil time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	log, ok := s.rooms[id]
	if !ok {
		return fmt.Errorf("memstore: room %q not found: %w", id, store.ErrNotFound)
	}
	log.meta.DriverID = driverID
	log.meta.DriverName = driverName
	log.meta.DriverLeaseUntil = &leaseUntil
	return nil
}

// CloseRoom marks a room closed.
func (s *Store) CloseRoom(ctx context.Context, id string, closedAt time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	log, ok := s.rooms[id]
	if !ok {
		return fmt.Errorf("memstore: room %q not found: %w", id, store.ErrNotFound)
	}
	log.meta.Status = "closed"
	log.meta.ClosedAt = &closedAt
	return nil
}

// ---- proposals ----

// CreateProposal stores a proposal.
func (s *Store) CreateProposal(ctx context.Context, p store.ProposalRow) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.proposals[p.ProposalID]; ok {
		return fmt.Errorf("memstore: proposal %q already exists", p.ProposalID)
	}
	cp := p
	s.proposals[p.ProposalID] = &cp
	return nil
}

// DeleteProposal removes a proposal row.
func (s *Store) DeleteProposal(ctx context.Context, proposalID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.proposals, proposalID)
	return nil
}

// GetProposal returns a proposal row or store.ErrNotFound.
func (s *Store) GetProposal(ctx context.Context, proposalID string) (store.ProposalRow, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, ok := s.proposals[proposalID]
	if !ok {
		return store.ProposalRow{}, fmt.Errorf("memstore: proposal %q not found: %w", proposalID, store.ErrNotFound)
	}
	return *p, nil
}

// RecordApproval enforces one decision per layer.
func (s *Store) RecordApproval(ctx context.Context, proposalID, layer, decision, reason string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := proposalID + "\x00" + layer
	if _, ok := s.approvals[key]; ok {
		return fmt.Errorf("memstore: approval %s/%s: %w", proposalID, layer, store.ErrAlreadyExists)
	}
	s.approvals[key] = struct{}{}
	return nil
}

// DeleteApproval removes one approval.
func (s *Store) DeleteApproval(ctx context.Context, proposalID, layer string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.approvals, proposalID+"\x00"+layer)
	return nil
}

// StartProposalStep marks a step started; a second call returns
// store.ErrStepOrder, exactly like the Postgres store.
func (s *Store) StartProposalStep(ctx context.Context, proposalID string, stepIndex int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := fmt.Sprintf("%s\x00%d", proposalID, stepIndex)
	if _, ok := s.steps[key]; ok {
		return fmt.Errorf("memstore: step %s/%d already started: %w", proposalID, stepIndex, store.ErrStepOrder)
	}
	s.steps[key] = "started"
	return nil
}

// FinishProposalStep records the final state of a step. Only a "started" step
// can be finished; anything else returns store.ErrStepOrder.
func (s *Store) FinishProposalStep(ctx context.Context, proposalID string, stepIndex int, status, output string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := fmt.Sprintf("%s\x00%d", proposalID, stepIndex)
	if st, ok := s.steps[key]; !ok || st != "started" {
		return fmt.Errorf("memstore: step %s/%d is not started: %w", proposalID, stepIndex, store.ErrStepOrder)
	}
	s.steps[key] = status
	return nil
}

// SetProposalStatus updates a proposal's status.
func (s *Store) SetProposalStatus(ctx context.Context, proposalID, status string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, ok := s.proposals[proposalID]
	if !ok {
		return fmt.Errorf("memstore: proposal %q not found: %w", proposalID, store.ErrNotFound)
	}
	p.Status = status
	return nil
}

// ProposalStepStatus returns the recorded status of one proposal step
// ("started" | "done" | "failed"), or "" when the step has no row. It exists
// so unit tests can assert that a cancelled executor finished its step
// (review item 3) without a database.
func (s *Store) ProposalStepStatus(proposalID string, stepIndex int) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.steps[fmt.Sprintf("%s\x00%d", proposalID, stepIndex)]
}

// ---- suggestions ----

// CreateSuggestion stores a pending suggestion.
func (s *Store) CreateSuggestion(ctx context.Context, r store.SuggestionRow) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.suggestions[r.SuggestionID]; ok {
		return fmt.Errorf("memstore: suggestion %q already exists", r.SuggestionID)
	}
	cp := r
	cp.Status = "pending"
	s.suggestions[r.SuggestionID] = &cp
	return nil
}

// DeleteSuggestion removes a suggestion row.
func (s *Store) DeleteSuggestion(ctx context.Context, suggestionID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.suggestions, suggestionID)
	return nil
}

// GetSuggestion returns a suggestion row or store.ErrNotFound.
func (s *Store) GetSuggestion(ctx context.Context, suggestionID string) (store.SuggestionRow, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.suggestions[suggestionID]
	if !ok {
		return store.SuggestionRow{}, fmt.Errorf("memstore: suggestion %q not found: %w", suggestionID, store.ErrNotFound)
	}
	return *r, nil
}

// SetSuggestionStatus marks a suggestion sent.
func (s *Store) SetSuggestionStatus(ctx context.Context, suggestionID, status string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.suggestions[suggestionID]
	if !ok {
		return fmt.Errorf("memstore: suggestion %q not found: %w", suggestionID, store.ErrNotFound)
	}
	r.Status = status
	return nil
}

// ---- handoffs ----

// CreateHandoff stores a pending handoff.
func (s *Store) CreateHandoff(ctx context.Context, h store.HandoffRow) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.handoffs[h.HandoffID]; ok {
		return fmt.Errorf("memstore: handoff %q already exists", h.HandoffID)
	}
	cp := h
	cp.Status = "pending"
	s.handoffs[h.HandoffID] = &cp
	return nil
}

// DeleteHandoff removes a handoff row.
func (s *Store) DeleteHandoff(ctx context.Context, handoffID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.handoffs, handoffID)
	return nil
}

// GetHandoff returns a handoff row or store.ErrNotFound.
func (s *Store) GetHandoff(ctx context.Context, handoffID string) (store.HandoffRow, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	h, ok := s.handoffs[handoffID]
	if !ok {
		return store.HandoffRow{}, fmt.Errorf("memstore: handoff %q not found: %w", handoffID, store.ErrNotFound)
	}
	return *h, nil
}

// SetHandoffStatus marks a handoff accepted or voided.
func (s *Store) SetHandoffStatus(ctx context.Context, handoffID, status string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	h, ok := s.handoffs[handoffID]
	if !ok {
		return fmt.Errorf("memstore: handoff %q not found: %w", handoffID, store.ErrNotFound)
	}
	h.Status = status
	return nil
}

// VoidHandoffs voids every pending handoff in a room except the accepted one.
func (s *Store) VoidHandoffs(ctx context.Context, roomID, exceptHandoffID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, h := range s.handoffs {
		if h.RoomID == roomID && h.Status == "pending" && id != exceptHandoffID {
			h.Status = "voided"
		}
	}
	return nil
}
