package room

import (
	"context"
	"errors"
	"fmt"

	"piperoom/internal/store"
)

// Each cached room has its own lock, roomState.mu, which guards the room's
// state (all of roomState but dedupKey and openingFor, which are the
// registry's). Work on a room takes it with lockRoom, lockCachedRoom or
// lockState and gives it back with unlockRoom, which publishes the snapshot
// (roomState.pub) when it changed, so readers can answer from pub without any
// lock.
//
// Lock order: Service.deleteMu, then an alert key lock (lockAlertKey), then
// agentGate.gmu, then ONE room's mu, then s.reg and the leaf locks (listHub,
// agentSlots, inbox, execQueue, follower). Nobody holds two room locks at
// once, and no hook, agent runner or WebSocket call runs under a room lock.
// A slow database call under one room's lock therefore holds up that room
// only.

// lockRoom locks roomID, loading the room into the cache first when it is
// not cached. The caller releases it with unlockRoom.
func (s *Service) lockRoom(ctx context.Context, roomID string) (*roomState, error) {
	for {
		st := s.cached(roomID)
		if st == nil {
			var err error
			if st, _, err = s.loadRoom(ctx, roomID, true); err != nil {
				if ctx.Err() == nil && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
					continue // the caller that ran the load went away; load again
				}
				return nil, err
			}
		}
		st.mu.Lock()
		if !st.gone {
			return st, nil
		}
		// Evicted while we waited: the next look-up finds the current state,
		// or loads the room again (and finds it gone from the store).
		st.mu.Unlock()
	}
}

// lockCachedRoom locks roomID only when it is cached, with no load: nil when
// it is not.
func (s *Service) lockCachedRoom(roomID string) *roomState {
	for {
		st := s.cached(roomID)
		if st == nil {
			return nil
		}
		st.mu.Lock()
		if !st.gone {
			return st
		}
		st.mu.Unlock()
	}
}

// tryLockCachedRoom is lockCachedRoom for the background loops: nil also when
// the room's lock is busy, so one room stuck in a slow call never holds them
// up.
func (s *Service) tryLockCachedRoom(roomID string) (st *roomState, busy bool) {
	st = s.cached(roomID)
	if st == nil {
		return nil, false
	}
	if !st.mu.TryLock() {
		return nil, true
	}
	if st.gone {
		st.mu.Unlock()
		return nil, false
	}
	return st, false
}

// lockState locks st, a room state the caller already holds (an agent
// supervisor, the executor, a fold whose read was made outside the lock). The
// room may have left the cache since: the caller checks st.gone, and a
// background goroutine does nothing for a room that is gone.
func (s *Service) lockState(st *roomState) {
	st.mu.Lock()
}

// unlockRoom publishes st's snapshot when it changed, then tells the list hub,
// then releases st's lock. Published before told: the hub reads pub, so it
// never sends a room older than the change that woke it. A room that left the
// cache is not published; whoever evicted it told the hub.
func (s *Service) unlockRoom(st *roomState) {
	if st.changed && !st.gone {
		st.changed = false
		st.publish()
		s.markListDirty(st.snap.ID)
	}
	st.mu.Unlock()
}

// publish stores a copy of the snapshot as the room's published one. Caller
// holds st.mu, or st is not cached yet.
func (st *roomState) publish() {
	snap := st.snap.Clone()
	st.pub.Store(&snap)
}

// rangeRooms calls fn for every cached room in turn, with that room's lock
// held and never two at once. Rooms that leave the cache meanwhile are
// skipped.
func (s *Service) rangeRooms(fn func(roomID string, st *roomState)) {
	for id, st := range s.cachedRooms() {
		s.lockState(st)
		if !st.gone {
			fn(id, st)
		}
		s.unlockRoom(st)
	}
}

// readRoom returns roomID's snapshot for a read. A cached room answers from
// its published snapshot without waiting for any lock, so a slow command on
// the room, or on another one, does not hold the read up; the result is shared
// and read-only. A room that is not cached is loaded first (loadRoom).
func (s *Service) readRoom(ctx context.Context, roomID string) (*RoomSnapshot, error) {
	if p := s.published(roomID); p != nil {
		return p, nil
	}
	st, err := s.lockRoom(ctx, roomID)
	if err != nil {
		return nil, err
	}
	snap := st.snap.Clone()
	s.unlockRoom(st)
	return &snap, nil
}

// ---- loading ----

// roomLoad is one load of a room into the cache in flight (loadRoom). Every
// caller that wants the room meanwhile waits for it instead of reading the
// log again.
type roomLoad struct {
	done chan struct{}
	st   *roomState // the cached state once done; nil with err
	err  error
	// cancelled is set, under s.reg, when the room is evicted while it
	// loads: the log it read may predate the delete, so the load must not
	// bring the room back.
	cancelled bool
}

// loadRoom returns roomID's cached state, loading it from the store when it
// is not cached: one load per room at a time, made outside every lock.
// fresh is true for the one caller whose load put the room into the cache.
// With wait false a load already in flight is not waited for: st is nil, and
// so is err (passes over every room skip it; the load caches it).
func (s *Service) loadRoom(ctx context.Context, roomID string, wait bool) (st *roomState, fresh bool, err error) {
	st, l, leader := s.startLoad(roomID)
	if st != nil {
		return st, false, nil
	}
	if !leader && !wait {
		return nil, false, nil
	}
	if !leader {
		select {
		case <-l.done:
			return l.st, false, l.err
		case <-ctx.Done():
			return nil, false, ctx.Err()
		}
	}
	built, key, err := s.buildRoom(ctx, roomID)
	st, fresh, err = s.finishLoad(roomID, l, built, key, err)
	if fresh {
		s.markListDirty(roomID)
	}
	return st, fresh, err
}

// buildRoom reads a room from the store: its row (a missing room is the 404
// of mapStoreErr), then its log. key is the alert index key of an open alert
// room, "" otherwise.
func (s *Service) buildRoom(ctx context.Context, roomID string) (*roomState, string, error) {
	if _, err := s.store.GetRoom(ctx, roomID); err != nil {
		return nil, "", mapStoreErr(err)
	}
	l, err := s.rebuild(ctx, roomID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, "", mapStoreErr(err) // deleted while it was read
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, "", err
		}
		return nil, "", fmt.Errorf("room: rebuild %s: %w", roomID, err)
	}
	key := ""
	if l.snap.Status == "open" {
		key = l.dedupKey
	}
	return l.state(), key, nil
}
