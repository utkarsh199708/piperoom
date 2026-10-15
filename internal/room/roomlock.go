package room

import "context"

// Each cached room has its own lock, roomState.mu, which guards the room's
// state (all of roomState but dedupKey, which is the registry's). Work on a
// room takes it with lockRoom, lockCachedRoom or lockState and gives it back
// with unlockRoom, which publishes the snapshot (roomState.pub) when it
// changed, so readers can answer from pub without any lock. The lock order is
// s.mu, then one room's mu, then s.reg and the leaf locks; nobody holds two
// room locks at once.
//
// A read of a cached room (readRoom, published) takes no lock at all. For now
// every room lock is still taken under s.mu, so commands on different rooms
// wait for each other as they did with s.mu alone.

// lockRoom locks roomID, loading the room into the cache first when it is
// not cached. The caller releases it with unlockRoom.
func (s *Service) lockRoom(ctx context.Context, roomID string) (*roomState, error) {
	s.mu.Lock()
	st, err := s.getStateLocked(ctx, roomID)
	if err != nil {
		s.mu.Unlock()
		return nil, err
	}
	st.mu.Lock()
	return st, nil
}

// lockCachedRoom locks roomID only when it is cached, with no load: nil when
// it is not.
func (s *Service) lockCachedRoom(roomID string) *roomState {
	s.mu.Lock()
	st := s.cached(roomID)
	if st == nil {
		s.mu.Unlock()
		return nil
	}
	st.mu.Lock()
	return st
}

// lockState locks st, a room state the caller already holds (an agent
// supervisor, the executor, a fold whose read was made outside the lock). The
// room may have left the cache since: st.gone says so.
func (s *Service) lockState(st *roomState) {
	s.mu.Lock()
	st.mu.Lock()
}

// unlockRoom publishes st's snapshot when it changed and releases st's lock
// and s.mu.
func (s *Service) unlockRoom(st *roomState) {
	s.releaseRoomLocked(st)
	s.mu.Unlock()
}

// holdRoomLocked locks st for a pass over many rooms that holds s.mu already
// (Load, the open-room limit, the lease loops). releaseRoomLocked gives it
// back.
func (s *Service) holdRoomLocked(st *roomState) {
	st.mu.Lock()
}

// releaseRoomLocked publishes st's snapshot when it changed, then tells the
// list hub, then releases st's lock. Published before told: the hub reads pub,
// so it never sends a room older than the change that woke it. A room that
// left the cache is not published; whoever evicted it told the hub.
func (s *Service) releaseRoomLocked(st *roomState) {
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

// readRoom returns roomID's snapshot for a read. A cached room answers from
// its published snapshot without waiting for any lock, so a slow command on
// the room, or on another one, does not hold the read up; the result is shared
// and read-only. A room that is not cached is loaded first, under its lock.
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
