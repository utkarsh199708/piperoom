package store

import "context"

// HubListening reports whether the hub's LISTEN connection is up, so a test
// can tell a delivery by the safety poll from one by the fallback poll.
func (s *PG) HubListening() bool { return s.hub.isListening() }

// WaitHubListening blocks until the hub's LISTEN connection is up or ctx ends,
// waking on each change rather than polling.
func (s *PG) WaitHubListening(ctx context.Context) error {
	for {
		s.hub.mu.Lock()
		up, changed := s.hub.listening, s.hub.listenChanged
		s.hub.mu.Unlock()
		if up {
			return nil
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
