package httpapi

import "time"

// SetFleetTTLForTest sets how long the fleet facts are reused; a negative
// value turns the cache off, so every request reads the store.
func (s *Server) SetFleetTTLForTest(d time.Duration) { s.fleet.ttl = d }
