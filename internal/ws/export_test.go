package ws

import "context"

// RecheckSessions runs the presence tick's session check now, so a test does
// not wait 20 seconds.
func (g *Gateway) RecheckSessions() { g.recheckSessions() }

// ConnCount is how many connections the gateway has registered across all
// rooms.
func (g *Gateway) ConnCount() int { return g.Connections() }

// WaitConns blocks until exactly n connections are registered or ctx ends. A
// client's Dial returns before the server registers the connection, so a test
// that acts on the registry waits here first; it wakes on each registration
// rather than polling.
func (g *Gateway) WaitConns(ctx context.Context, n int) error {
	for {
		g.mu.Lock()
		got, changed := g.connectionsLocked(), g.connsChanged
		g.mu.Unlock()
		if got == n {
			return nil
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
