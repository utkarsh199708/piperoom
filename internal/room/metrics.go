package room

// AgentSlotStats reports this replica's agent slots in use and the rooms queued
// for one (PIPEROOM_MAX_AGENTS), for the /metrics gauges.
func (s *Service) AgentSlotStats() (inUse, queued int) {
	return s.slots.stats()
}

// OpenRooms counts the open rooms in this replica's room cache, without a
// store call, for the /metrics gauge. Load caches every open room at start
// and rooms are cached as they are opened or reached here, so with several
// replicas each one reports the rooms it has seen.
//
// s.mu is held across store appends, so a slow database would stall a scrape
// exactly when an operator needs it. When the lock is busy the last computed
// count is returned instead: it can be stale by the rooms opened or closed
// since, and is 0 only if no scrape has ever got the lock.
func (s *Service) OpenRooms() int {
	if !s.mu.TryLock() {
		return int(s.openRoomsSeen.Load())
	}
	defer s.mu.Unlock()
	n := 0
	for _, st := range s.rooms {
		if s.roomOpen(st) {
			n++
		}
	}
	s.openRoomsSeen.Store(int64(n))
	return n
}
