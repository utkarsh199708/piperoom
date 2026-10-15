package room

// AgentSlotStats reports this replica's agent slots in use and the rooms queued
// for one (PIPEROOM_MAX_AGENTS), for the /metrics gauges.
func (s *Service) AgentSlotStats() (inUse, queued int) {
	return s.slots.stats()
}

// OpenRooms is how many rooms in this replica's room cache are open, for the
// /metrics gauge. Load caches every open room at start and rooms are cached as
// they are opened or reached here, so with several replicas each one reports
// the rooms it has seen. It reads a counter kept exact under s.mu, so it never
// waits on that lock (held across store appends) and is never a stale zero.
func (s *Service) OpenRooms() int {
	return int(s.openRooms.Load())
}

// cacheRoomLocked puts st in the room cache under id, replacing any room cached
// there, and keeps the open-room count exact. Caller holds s.mu.
func (s *Service) cacheRoomLocked(id string, st *roomState) {
	if old, ok := s.rooms[id]; ok && s.roomOpen(old) {
		s.openRooms.Add(-1)
	}
	s.rooms[id] = st
	if s.roomOpen(st) {
		s.openRooms.Add(1)
	}
}

// uncacheRoomLocked drops id from the room cache and keeps the open-room count
// exact. Caller holds s.mu.
func (s *Service) uncacheRoomLocked(id string) {
	if old, ok := s.rooms[id]; ok && s.roomOpen(old) {
		s.openRooms.Add(-1)
	}
	delete(s.rooms, id)
}

// countOpenChangeLocked adjusts the open-room count after st's snapshot
// changed; wasOpen is whether it was open before. Caller holds s.mu, and st is
// in s.rooms (a room fed events before it is cached is counted by
// cacheRoomLocked instead).
func (s *Service) countOpenChangeLocked(st *roomState, wasOpen bool) {
	switch now := s.roomOpen(st); {
	case now && !wasOpen:
		s.openRooms.Add(1)
	case !now && wasOpen:
		s.openRooms.Add(-1)
	}
}
