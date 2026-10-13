package ws

// RecheckSessions runs the presence tick's session check now, so a test does
// not wait 20 seconds.
func (g *Gateway) RecheckSessions() { g.recheckSessions() }
