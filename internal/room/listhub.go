package room

import (
	"context"
	"sync"
)

// The rooms list is pushed (GET /api/rooms/ws, docs/api.md "Room list
// socket"), not polled. Every fold marks its room dirty (markListDirtyLocked,
// under s.mu); one hub goroutine wakes, copies the dirty rooms' snapshots
// under s.mu, releases it and hands the copies to every subscriber. A burst of
// events on one room between two wakes is one update. Subscribers filter by
// tenancy and by a digest of the list-relevant fields themselves.

// listSubBuffer is the updates one subscriber may have queued. A subscriber
// that falls further behind loses its queue and gets one Resync instead.
const listSubBuffer = 64

// RoomUpdate is one change of the rooms list.
type RoomUpdate struct {
	// Room is the room's snapshot after the change; nil when DeletedID is set.
	// It is shared by every subscriber: read it, never change it.
	Room *RoomSnapshot
	// DeletedID is the id of a room that is gone (deleted, purged, or no
	// longer in a shared store).
	DeletedID string
	// Resync says this subscriber fell behind and updates were dropped: it
	// must re-list (ListRooms) to know the current state.
	Resync bool
}

// listHub fans list changes out to subscribers.
type listHub struct {
	mu    sync.Mutex
	dirty map[string]struct{}
	subs  map[*listSub]struct{}
	wake  chan struct{}
	once  sync.Once
}

type listSub struct {
	ch chan RoomUpdate
}

func newListHub() *listHub {
	return &listHub{dirty: map[string]struct{}{}, subs: map[*listSub]struct{}{}, wake: make(chan struct{}, 1)}
}

// markListDirtyLocked records that roomID's list entry may have changed and
// wakes the hub. Caller holds s.mu. It never blocks, and with no subscriber it
// does nothing.
func (s *Service) markListDirtyLocked(roomID string) {
	h := s.list
	h.mu.Lock()
	if len(h.subs) == 0 {
		h.mu.Unlock()
		return
	}
	h.dirty[roomID] = struct{}{}
	h.mu.Unlock()
	select {
	case h.wake <- struct{}{}:
	default:
	}
}

// SubscribeRooms returns a channel of rooms-list changes until ctx ends; the
// channel is then closed. A new subscriber should list the rooms after it
// subscribed (ListRooms) and apply updates on top: an update for a room it
// already has replaces it. After Close it returns a closed channel.
func (s *Service) SubscribeRooms(ctx context.Context) <-chan RoomUpdate {
	sub := &listSub{ch: make(chan RoomUpdate, listSubBuffer)}
	h := s.list
	s.mu.Lock()
	closed := s.closed
	if !closed {
		h.once.Do(func() {
			s.wg.Add(1)
			go func() {
				defer s.wg.Done()
				s.runListHub()
			}()
		})
	}
	s.mu.Unlock()
	if closed {
		close(sub.ch)
		return sub.ch
	}
	h.mu.Lock()
	h.subs[sub] = struct{}{}
	h.mu.Unlock()
	context.AfterFunc(ctx, func() { s.unsubscribeRooms(sub) })
	return sub.ch
}

// unsubscribeRooms removes sub and closes its channel, once.
func (s *Service) unsubscribeRooms(sub *listSub) {
	h := s.list
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, ok := h.subs[sub]; !ok {
		return
	}
	delete(h.subs, sub)
	close(sub.ch)
}

// runListHub publishes dirty rooms until the service closes, then closes
// every subscriber.
func (s *Service) runListHub() {
	h := s.list
	defer func() {
		h.mu.Lock()
		for sub := range h.subs {
			delete(h.subs, sub)
			close(sub.ch)
		}
		h.mu.Unlock()
	}()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-h.wake:
		}
		h.mu.Lock()
		ids := h.dirty
		h.dirty = map[string]struct{}{}
		h.mu.Unlock()
		if len(ids) == 0 {
			continue
		}
		updates := make([]RoomUpdate, 0, len(ids))
		s.mu.Lock()
		for id := range ids {
			if st, ok := s.rooms[id]; ok {
				snap := st.snap.Clone()
				updates = append(updates, RoomUpdate{Room: &snap})
			} else {
				updates = append(updates, RoomUpdate{DeletedID: id})
			}
		}
		s.mu.Unlock()
		h.publish(updates)
	}
}

// publish hands updates to every subscriber without blocking. A subscriber
// whose buffer is full has its queue dropped and gets a single Resync.
func (h *listHub) publish(updates []RoomUpdate) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for sub := range h.subs {
		for _, u := range updates {
			select {
			case sub.ch <- u:
				continue
			default:
			}
			// Drain what it has not read: it re-lists anyway.
			for drained := false; !drained; {
				select {
				case <-sub.ch:
				default:
					drained = true
				}
			}
			sub.ch <- RoomUpdate{Resync: true}
			break
		}
	}
}

// SharedStore reports whether the store is shared with other replicas: their
// changes reach this replica's list hub only when it folds them, so a list
// subscriber should also re-list now and then.
func (s *Service) SharedStore() bool { return s.coord != nil }
