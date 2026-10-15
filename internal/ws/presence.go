package ws

import (
	"context"
	"encoding/json"
	"sort"
	"sync"
	"time"

	"github.com/coder/websocket"

	"piperoom/internal/events"
	"piperoom/internal/room"
)

// client is one WebSocket connection.
type client struct {
	sessionID string // "" for an X-User (dev, no cookie) connection
	userID    string
	userName  string
	verified  bool
	state     string
	// stateSince is when state last changed (or the connection registered).
	stateSince time.Time
	lastSeen   time.Time
	send       chan []byte
	conn       *websocket.Conn
	cancel     func()
	onClose    func()
	once       sync.Once
	// sessionEnded is set (under Gateway.mu) once the session-end frames are
	// on their way, so a logout and the register-time check never both send
	// them.
	sessionEnded bool
}

// roomHub is the set of connections for one room.
type roomHub struct {
	conns map[*client]struct{}
}

// write sends one message directly (blocking, with a deadline). Used during
// replay so the backlog is delivered in order without buffering.
func (c *client) write(ctx context.Context, b []byte) error {
	wctx, cancel := context.WithTimeout(ctx, writeTimeout)
	defer cancel()
	return c.conn.Write(wctx, websocket.MessageText, b)
}

// trySend enqueues one message without blocking. Live events use it: a client
// that cannot keep up is closed and reconnects with after=.
func (c *client) trySend(b []byte) bool {
	select {
	case c.send <- b:
		return true
	default:
		return false
	}
}

// shutdown ends the connection exactly once. The connection is closed BEFORE
// cancelling the context so the close frame carries the right status code.
func (c *client) shutdown(code websocket.StatusCode, reason string) {
	c.once.Do(func() {
		_ = c.conn.Close(code, reason)
		c.cancel()
		if c.onClose != nil {
			c.onClose()
		}
	})
}

func (g *Gateway) addClient(roomID string, c *client) {
	g.mu.Lock()
	defer g.mu.Unlock()
	hub := g.rooms[roomID]
	if hub == nil {
		hub = &roomHub{conns: map[*client]struct{}{}}
		g.rooms[roomID] = hub
	}
	hub.conns[c] = struct{}{}
	g.broadcastLocked(roomID)
	g.connsChangedLocked()
}

func (g *Gateway) removeClient(roomID string, c *client) {
	g.mu.Lock()
	defer g.mu.Unlock()
	hub := g.rooms[roomID]
	if hub == nil {
		return
	}
	delete(hub.conns, c)
	if len(hub.conns) == 0 {
		delete(g.rooms, roomID)
	}
	g.broadcastLocked(roomID)
	g.connsChangedLocked()
}

// connsChangedLocked wakes everyone waiting on a registry change. Caller holds
// g.mu.
func (g *Gateway) connsChangedLocked() {
	close(g.connsChanged)
	g.connsChanged = make(chan struct{})
}

func (g *Gateway) setPresence(roomID string, c *client, state string) {
	if state != "viewing" && state != "away" {
		state = "viewing"
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if c.state != state {
		c.state = state
		c.stateSince = g.now()
		g.broadcastLocked(roomID)
	}
	c.lastSeen = g.now()
}

// members derives a de-duplicated member list (viewing wins over away).
func (h *roomHub) members() []Member {
	byID := map[string]Member{}
	for c := range h.conns {
		m, ok := byID[c.userID]
		if !ok {
			m = Member{ID: c.userID, Name: c.userName, Verified: c.verified, State: c.state, Since: c.stateSince.UTC()}
		} else if c.state == "viewing" && m.State != "viewing" {
			m.State, m.Since = "viewing", c.stateSince.UTC()
		} else if c.state == m.State && c.stateSince.Before(m.Since) {
			m.Since = c.stateSince.UTC()
		}
		byID[c.userID] = m
	}
	out := make([]Member, 0, len(byID))
	for _, m := range byID {
		out = append(out, m)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// broadcastLocked sends the presence list to every connection in the room.
// Caller holds g.mu.
func (g *Gateway) broadcastLocked(roomID string) {
	hub := g.rooms[roomID]
	if hub == nil {
		return
	}
	b, err := json.Marshal(serverMsg{Type: "presence", Members: hub.members()})
	if err != nil {
		return
	}
	for c := range hub.conns {
		if !c.trySend(b) {
			g.spawnShutdown(c, "send buffer full")
		}
	}
}

// spawnShutdown shuts a client down (policy violation, with reason) on its own
// goroutine so the close frame and the onClose cleanup never block the caller.
// Caller holds g.mu. The goroutine is tracked by g.wg; it is never started
// after Close has begun (g.closed), so g.wg.Add never races with g.wg.Wait.
func (g *Gateway) spawnShutdown(c *client, reason string) {
	if g.closed {
		// Close is already shutting every client down; do not add work after
		// the WaitGroup may have started waiting.
		return
	}
	g.wg.Add(1)
	go func() {
		defer g.wg.Done()
		c.shutdown(websocket.StatusPolicyViolation, reason)
	}()
}

// sessionExpiredReason is the close reason when a session ends under a live
// connection (logout, revoke, expiry, user removed). The web app reads it and
// goes to the login page.
const sessionExpiredReason = "session_expired"

// spawnSessionEnd tells a client its session ended: an error frame with code
// session_expired (docs/api.md), then a close with the same reason text. Like
// spawnShutdown it runs on its own goroutine and the caller holds g.mu.
func (g *Gateway) spawnSessionEnd(c *client) {
	if g.closed || c.sessionEnded {
		return
	}
	c.sessionEnded = true
	g.wg.Add(1)
	go func() {
		defer g.wg.Done()
		if b, err := json.Marshal(serverMsg{Type: "error", Error: &serverError{Code: sessionExpiredReason, Message: "your session ended; sign in again"}}); err == nil {
			// Best effort: a client that cannot take the frame is closed anyway.
			_ = c.write(g.ctx, b)
		}
		c.shutdown(websocket.StatusPolicyViolation, sessionExpiredReason)
	}()
}

// CloseSession ends every connection of one cookie session: an error frame
// {type:"error", error:{code:"session_expired"}}, then a policy-violation close
// with the reason "session_expired". It returns at once; the frames go out on
// their own goroutines. An empty id is ignored.
func (g *Gateway) CloseSession(sessionID string) {
	if sessionID == "" {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	for _, hub := range g.rooms {
		for c := range hub.conns {
			if c.sessionID == sessionID {
				g.spawnSessionEnd(c)
			}
		}
	}
	for c := range g.lists {
		if c.sessionID == sessionID {
			g.spawnSessionEnd(c)
		}
	}
}

// Connections is how many connections the gateway has registered across all
// rooms plus the room-list sockets (the /metrics gauge).
func (g *Gateway) Connections() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.connectionsLocked()
}

func (g *Gateway) connectionsLocked() int {
	n := 0
	for _, hub := range g.rooms {
		n += len(hub.conns)
	}
	return n + len(g.lists)
}

// RoomDeletedReason is the close reason of a deleted room's connections
// (docs/api.md, WebSocket).
const RoomDeletedReason = "room deleted"

// CloseRoom ends every connection to a deleted room: the room.deleted event
// (never stored) as the last frame, then a normal close with the reason "room
// deleted". It returns at once; the frames go out on their own goroutines.
func (g *Gateway) CloseRoom(roomID string, deleted events.Event) {
	b, err := json.Marshal(serverMsg{Type: "event", Event: &deleted})
	if err != nil {
		g.log.Error("ws: encode room.deleted", "room", roomID, "err", err)
		b = nil
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	hub := g.rooms[roomID]
	if hub == nil {
		return
	}
	for c := range hub.conns {
		g.spawnRoomDeleted(c, b)
	}
}

// spawnRoomDeleted writes the last frame and closes, on its own goroutine.
// Caller holds g.mu (see spawnShutdown for why that keeps wg safe).
func (g *Gateway) spawnRoomDeleted(c *client, frame []byte) {
	if g.closed {
		return
	}
	g.wg.Add(1)
	go func() {
		defer g.wg.Done()
		if frame != nil {
			// Best effort: a client that cannot take the frame still gets the
			// close reason.
			_ = c.write(g.ctx, frame)
		}
		c.shutdown(websocket.StatusNormalClosure, RoomDeletedReason)
	}()
}

// recheckSessions asks SessionAlive about each cookie session that has a live
// connection and closes the connections of the ones that are gone. It takes the
// ids under g.mu but calls SessionAlive (a store read) outside it.
func (g *Gateway) recheckSessions() {
	if g.SessionAlive == nil {
		return
	}
	g.mu.Lock()
	seen := map[string]Identity{}
	for _, hub := range g.rooms {
		for c := range hub.conns {
			if c.sessionID != "" {
				seen[c.sessionID] = Identity{User: room.User{ID: c.userID, Name: c.userName}, SessionID: c.sessionID}
			}
		}
	}
	for c := range g.lists {
		if c.sessionID != "" {
			seen[c.sessionID] = Identity{User: room.User{ID: c.userID, Name: c.userName}, SessionID: c.sessionID}
		}
	}
	g.mu.Unlock()
	// One shared deadline for the whole pass, so a hanging database cannot hold
	// the presence loop for N x its statement timeout. A check cut off by the
	// deadline errors, and SessionAlive treats an error as "still alive".
	ctx, cancel := context.WithTimeout(g.ctx, recheckTimeout)
	defer cancel()
	for id, ident := range seen {
		if ctx.Err() != nil {
			return
		}
		if !g.SessionAlive(ctx, ident) {
			g.CloseSession(id)
		}
	}
}

// recheckTimeout bounds one whole session recheck pass on a presence tick.
const recheckTimeout = 5 * time.Second

// awayAfter is how long a connection may go without any client message
// before it is marked away: 2x the 20 s presence heartbeat plus slack
// (docs/api.md).
const awayAfter = 45 * time.Second

// presenceLoop marks idle clients away every 20 seconds, until Close.
func (g *Gateway) presenceLoop() {
	tick := time.NewTicker(20 * time.Second)
	defer tick.Stop()
	for {
		select {
		case <-g.ctx.Done():
			return
		case <-tick.C:
			g.markAway()
			g.recheckSessions()
		}
	}
}

// markAway flips connections silent for longer than awayAfter to away and
// stamps stateSince, broadcasting each room whose presence changed.
func (g *Gateway) markAway() {
	g.mu.Lock()
	defer g.mu.Unlock()
	now := g.now()
	for roomID, hub := range g.rooms {
		changed := false
		for c := range hub.conns {
			if c.state == "viewing" && now.After(c.lastSeen.Add(awayAfter)) {
				c.state = "away"
				c.stateSince = now
				changed = true
			}
		}
		if changed {
			g.broadcastLocked(roomID)
		}
	}
}
