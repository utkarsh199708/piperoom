package store

// HubListening reports whether the hub's LISTEN connection is up, so a test
// can tell a delivery by the safety poll from one by the fallback poll.
func (s *PG) HubListening() bool { return s.hub.isListening() }
