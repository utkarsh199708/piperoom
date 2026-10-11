package ws

import (
	"encoding/json"
	"sort"
	"sync"
	"time"

	"github.com/coder/websocket"
)

// client is one WebSocket connection.
type client struct {
	userID   string
	userName string
	state    string
	lastSeen time.Time
	send     chan []byte
	conn     *websocket.Conn
	cancel   func()
	onClose  func()
	once     sync.Once
}

// roomHub is the set of connections for one room.
type roomHub struct {
	conns map[*client]struct{}
}

func (c *client) trySend(b []byte) bool {
	select {
	case c.send <- b:
		return true
	default:
		return false
	}
}

// shutdown ends the connection exactly once.
func (c *client) shutdown(code websocket.StatusCode, reason string) {
	c.once.Do(func() {
		c.cancel()
		_ = c.conn.Close(code, reason)
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
}

func (g *Gateway) setPresence(roomID string, c *client, state string) {
	if state != "viewing" && state != "away" {
		state = "viewing"
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if c.state != state {
		c.state = state
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
			m = Member{ID: c.userID, Name: c.userName, State: c.state}
		} else if c.state == "viewing" {
			m.State = "viewing"
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
			go c.shutdown(websocket.StatusPolicyViolation, "send buffer full")
		}
	}
}

// presenceLoop marks idle clients away every 20 seconds.
func (g *Gateway) presenceLoop() {
	tick := time.NewTicker(20 * time.Second)
	defer tick.Stop()
	for range tick.C {
		g.mu.Lock()
		now := g.now()
		for roomID, hub := range g.rooms {
			changed := false
			for c := range hub.conns {
				if c.state == "viewing" && now.After(c.lastSeen.Add(20*time.Second)) {
					c.state = "away"
					changed = true
				}
			}
			if changed {
				g.broadcastLocked(roomID)
			}
		}
		g.mu.Unlock()
	}
}
