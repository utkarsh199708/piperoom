package ws

// RecheckSessions runs the presence tick's session check now, so a test does
// not wait 20 seconds.
func (g *Gateway) RecheckSessions() { g.recheckSessions() }

// ConnCount is how many connections the gateway has registered across all
// rooms. A client's Dial returns before the server registers the connection,
// so a test that acts on the registry waits on this first.
func (g *Gateway) ConnCount() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	n := 0
	for _, hub := range g.rooms {
		n += len(hub.conns)
	}
	return n
}

// Slots is how many connection slots roomID and userID hold now.
func (g *Gateway) Slots(roomID, userID string) (perRoom, perUser int) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.roomSlots[roomID], g.userSlots[userID]
}

// TotalSlots is how many connection slots the gateway holds in all.
func (g *Gateway) TotalSlots() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.slots
}
