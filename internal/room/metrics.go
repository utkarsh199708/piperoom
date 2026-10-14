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
func (s *Service) OpenRooms() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, st := range s.rooms {
		if s.roomOpen(st) {
			n++
		}
	}
	return n
}
