package room

// AgentSlotStats reports this replica's agent slots in use and the rooms queued
// for one (PIPEROOM_MAX_AGENTS), for the /metrics gauges.
func (s *Service) AgentSlotStats() (inUse, queued int) {
	return s.slots.stats()
}

// OpenRooms is how many rooms in this replica's room cache are open, for the
// /metrics gauge. Load caches every open room at start and rooms are cached as
// they are opened or reached here, so with several replicas each one reports
// the rooms it has seen. It counts the rooms' published snapshots under the
// registry lock only, so it never waits on a room whose command holds its lock
// across a store append, and right after Load it is already exact.
func (s *Service) OpenRooms() int {
	n := 0
	for _, p := range s.publishedRooms() {
		if p.Status == "open" {
			n++
		}
	}
	return n
}
