// Package memstore is an in-memory store.Store for unit tests. It follows the
// same rules as the Postgres store (gap-free seq per room, same Since limits,
// replay-then-live Subscribe that closes its channel when ctx ends or the
// consumer stalls), so tests written against it hold against the real one.
package memstore

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"time"

	"piperoom/internal/events"
	"piperoom/internal/store"
	"piperoom/internal/store/pump"
)

// Store is an in-memory event log. The zero value is not usable; call New.
type Store struct {
	clock func() time.Time
	log   *slog.Logger

	// Subscriber tuning; tests in this package override them before the first
	// Subscribe.
	slowAfter time.Duration

	mu    sync.Mutex
	order []string         // room ids in creation order
	rooms map[string]*room // guarded by mu
}

type room struct {
	log  []events.Event // log[i].Seq == i+1
	subs map[chan struct{}]struct{}
}

var _ store.Store = (*Store)(nil)

// New returns an empty store that stamps events with time.Now.
func New() *Store { return NewWithClock(time.Now) }

// NewWithClock returns an empty store that stamps events with clock(), so a
// test can make "at" deterministic. clock is called with the store lock held
// and must not call back into the store.
func NewWithClock(clock func() time.Time) *Store {
	if clock == nil {
		clock = time.Now
	}
	return &Store{
		clock:     clock,
		log:       slog.New(slog.DiscardHandler),
		slowAfter: pump.DefaultSlowAfter,
		rooms:     map[string]*room{},
	}
}

// CreateRoom registers an empty log for roomID.
func (s *Store) CreateRoom(_ context.Context, roomID string) error {
	if roomID == "" {
		return errors.New("store: empty room id")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.rooms[roomID]; ok {
		return fmt.Errorf("store: create room %q: %w", roomID, store.ErrRoomExists)
	}
	s.rooms[roomID] = &room{subs: map[chan struct{}]struct{}{}}
	s.order = append(s.order, roomID)
	return nil
}

// RoomIDs lists every registered room ID, oldest first.
func (s *Store) RoomIDs(_ context.Context) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string{}, s.order...), nil // never nil, and not an alias of s.order
}

// Append stores one event under the next seq of the room.
func (s *Store) Append(ctx context.Context, roomID string, typ events.Type, actor events.Actor, payload any) (events.Event, error) {
	if err := ctx.Err(); err != nil {
		return events.Event{}, fmt.Errorf("store: append to room %q: %w", roomID, err)
	}
	raw, err := store.MarshalPayload(payload)
	if err != nil {
		return events.Event{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.rooms[roomID]
	if !ok {
		return events.Event{}, fmt.Errorf("store: append to room %q: %w", roomID, store.ErrRoomNotFound)
	}
	ev := events.Event{
		RoomID:  roomID,
		Seq:     int64(len(r.log)) + 1,
		Type:    typ,
		Actor:   actor,
		Payload: slices.Clone(raw), // the caller may reuse its buffer
		At:      s.clock().UTC(),
	}
	r.log = append(r.log, ev)
	for wake := range r.subs {
		select {
		case wake <- struct{}{}:
		default: // a wake-up is already pending
		}
	}
	ev.Payload = slices.Clone(ev.Payload)
	return ev, nil
}

// Since returns events with Seq > afterSeq in order. limit <= 0 means 500 and
// anything above 1000 is capped. The result is never nil.
func (s *Store) Since(_ context.Context, roomID string, afterSeq int64, limit int) ([]events.Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sinceLocked(roomID, afterSeq, limit)
}

func (s *Store) sinceLocked(roomID string, afterSeq int64, limit int) ([]events.Event, error) {
	r, ok := s.rooms[roomID]
	if !ok {
		return nil, fmt.Errorf("store: events of room %q: %w", roomID, store.ErrRoomNotFound)
	}
	limit = store.ClampLimit(limit)
	afterSeq = max(afterSeq, 0)
	out := []events.Event{}
	// log[i].Seq == i+1, so the first wanted event sits at index afterSeq.
	for i := afterSeq; i < int64(len(r.log)) && len(out) < limit; i++ {
		ev := r.log[i]
		ev.Payload = slices.Clone(ev.Payload) // callers must not be able to edit the log
		out = append(out, ev)
	}
	return out, nil
}

// Subscribe replays events after afterSeq, then streams new ones. The channel
// (capacity 256) closes when ctx ends or when the consumer leaves it full for
// more than 5 s.
func (s *Store) Subscribe(ctx context.Context, roomID string, afterSeq int64) (<-chan events.Event, error) {
	afterSeq = max(afterSeq, 0)
	wake := make(chan struct{}, 1)

	// Registering and taking the first page under one lock hold means no append
	// can fall between them.
	s.mu.Lock()
	first, err := s.sinceLocked(roomID, afterSeq, pump.DefaultPage)
	if err != nil {
		s.mu.Unlock()
		return nil, err
	}
	r := s.rooms[roomID]
	r.subs[wake] = struct{}{}
	slowAfter := s.slowAfter
	s.mu.Unlock()

	return pump.Start(ctx, pump.Config{
		Read: func(_ context.Context, after int64, limit int) ([]events.Event, error) {
			return s.Since(ctx, roomID, after, limit)
		},
		Wake:      wake,
		SlowAfter: slowAfter,
		Log:       s.log,
		OnExit: func() {
			s.mu.Lock()
			delete(r.subs, wake)
			s.mu.Unlock()
		},
	}, afterSeq, first), nil
}
