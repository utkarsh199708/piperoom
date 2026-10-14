package ws

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/coder/websocket"

	"piperoom/internal/room"
)

// The room list socket (GET /api/rooms/ws, docs/api.md "Room list socket")
// pushes the rooms list: a "rooms" frame with exactly what GET /api/rooms
// answers this user, then a "room" frame each time a room's list-relevant
// state changes and a "room_deleted" frame when one goes away. It shares the
// room socket's auth, origin rules, per-user cap, session checks and Close.

const (
	// listSendBuffer is the outbound queue of one list connection. A full
	// queue closes it; the client reconnects and gets a fresh "rooms" frame.
	listSendBuffer = 64
	// listCoalesce is the least time between two "room" frames of one room.
	listCoalesce = 500 * time.Millisecond
	// listRelist is how often a list connection re-lists when the store is
	// shared: another replica's changes reach this one's hub only when it
	// folds them, so a periodic re-list closes that gap.
	listRelist = 10 * time.Second
	// listTimeout bounds one ListRooms read of a list connection.
	listTimeout = 10 * time.Second
)

// listMsg is a server frame of the list socket.
type listMsg struct {
	Type   string              `json:"type"`
	Rooms  []room.RoomSnapshot `json:"rooms,omitempty"`
	Room   *room.RoomSnapshot  `json:"room,omitempty"`
	RoomID string              `json:"room_id,omitempty"`
}

// ServeRoomsHTTP serves GET /api/rooms/ws.
func (g *Gateway) ServeRoomsHTTP(w http.ResponseWriter, r *http.Request) {
	var ident Identity
	ok := false
	if g.Identify != nil {
		ident, ok = g.Identify(r)
	}
	if !ok {
		writeHTTPError(w, http.StatusUnauthorized, "unauthenticated", "not signed in")
		return
	}
	user := ident.User
	release, msg := g.reserve("", user.ID)
	if release == nil {
		writeHTTPError(w, http.StatusTooManyRequests, "too_many_connections", msg)
		return
	}
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{OriginPatterns: g.origins()})
	if err != nil {
		release()
		return
	}
	conn.SetReadLimit(4096)

	c := &client{
		sessionID: ident.SessionID,
		userID:    user.ID,
		userName:  user.Name,
		verified:  user.Verified,
		send:      make(chan []byte, listSendBuffer),
		conn:      conn,
	}
	ctx, cancel := context.WithCancel(g.ctx)
	c.cancel = cancel
	c.onClose = func() {
		g.removeListClient(c)
		release()
	}

	// Subscribe before listing, so nothing that changes in between is lost:
	// an update for a room the first frame already shows is only re-checked
	// against its digest.
	updates := g.svc.SubscribeRooms(ctx)
	l := &listConn{g: g, c: c, user: user, known: map[string][32]byte{}, sent: map[string]time.Time{}, pending: map[string]*room.RoomSnapshot{}}
	rooms, err := l.list(ctx)
	if err != nil {
		g.log.Error("ws: list rooms failed", "err", err)
		c.shutdown(websocket.StatusInternalError, "list failed")
		return
	}
	for i := range rooms {
		l.known[rooms[i].ID] = listDigest(&rooms[i])
	}
	b, err := json.Marshal(listMsg{Type: "rooms", Rooms: rooms})
	if err != nil {
		c.shutdown(websocket.StatusInternalError, "encode failed")
		return
	}
	if err := c.write(ctx, b); err != nil {
		c.shutdown(websocket.StatusInternalError, "write failed")
		return
	}

	if !g.addListClient(c) {
		c.shutdown(websocket.StatusGoingAway, "server shutting down")
		return
	}
	g.recheckNewClient(ctx, c, ident)
	for _, run := range []func(){
		func() { g.writeLoop(ctx, c) },
		func() { g.listReadLoop(ctx, c) },
		func() { l.loop(ctx, updates) },
	} {
		if !g.trackConn() {
			c.shutdown(websocket.StatusGoingAway, "server shutting down")
			return
		}
		go func() {
			defer g.wg.Done()
			run()
		}()
	}
}

// origins are the allowed WebSocket origin hosts (see OriginPatterns).
func (g *Gateway) origins() []string {
	if g.OriginPatterns != nil {
		return g.OriginPatterns
	}
	return []string{"localhost:5173", "127.0.0.1:5173"}
}

// addListClient registers a list connection, so logout, session expiry and
// Close reach it. It reports false once the gateway is closing.
func (g *Gateway) addListClient(c *client) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return false
	}
	g.lists[c] = struct{}{}
	return true
}

func (g *Gateway) removeListClient(c *client) {
	g.mu.Lock()
	defer g.mu.Unlock()
	delete(g.lists, c)
}

// listReadLoop reads the client's pings; their content is ignored. A client
// silent for readTimeout is closed.
func (g *Gateway) listReadLoop(ctx context.Context, c *client) {
	for {
		rctx, cancel := context.WithTimeout(ctx, readTimeout)
		_, _, err := c.conn.Read(rctx)
		cancel()
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				c.shutdown(websocket.StatusPolicyViolation, "read timeout")
			} else {
				c.shutdown(websocket.StatusNormalClosure, "")
			}
			return
		}
	}
}

// listConn is the state of one list connection.
type listConn struct {
	g    *Gateway
	c    *client
	user room.User
	// known is the digest of every room the client has, by id.
	known map[string][32]byte
	// sent is when the last "room" frame of a room went out; pending holds a
	// change that came within listCoalesce of it.
	sent    map[string]time.Time
	pending map[string]*room.RoomSnapshot
}

// list reads the rooms this user may see, as GET /api/rooms answers.
func (l *listConn) list(ctx context.Context) ([]room.RoomSnapshot, error) {
	lctx, cancel := context.WithTimeout(ctx, listTimeout)
	defer cancel()
	return l.g.svc.ListRoomsFor(lctx, l.user)
}

// loop turns hub updates into frames until ctx ends or the service closes.
func (l *listConn) loop(ctx context.Context, updates <-chan room.RoomUpdate) {
	var relist <-chan time.Time
	if l.g.svc.SharedStore() {
		t := time.NewTicker(listRelist)
		defer t.Stop()
		relist = t.C
	}
	flush := time.NewTimer(time.Hour)
	flush.Stop()
	defer flush.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case u, ok := <-updates:
			if !ok {
				l.c.shutdown(websocket.StatusGoingAway, "server shutting down")
				return
			}
			switch {
			case u.Resync:
				if !l.resync(ctx) {
					return
				}
			case u.DeletedID != "":
				if !l.deleted(u.DeletedID) {
					return
				}
			case u.Room != nil:
				if !l.changed(u.Room) {
					return
				}
			}
		case <-relist:
			if !l.resync(ctx) {
				return
			}
		case <-flush.C:
			if !l.flushDue() {
				return
			}
		}
		if d, ok := l.nextDue(); ok {
			flush.Reset(d)
		}
	}
}

// changed sends a room frame for snap when the client may see it and its
// list-relevant state changed; within listCoalesce of the last frame of the
// room it is held and sent when due. It reports false when the connection
// was closed.
func (l *listConn) changed(snap *room.RoomSnapshot) bool {
	if !l.g.svc.CanSeePipeline(l.user, snap.PipelineID) {
		return true
	}
	if d, ok := l.known[snap.ID]; ok && d == listDigest(snap) {
		delete(l.pending, snap.ID)
		return true
	}
	if last, ok := l.sent[snap.ID]; ok && l.g.now().Sub(last) < listCoalesce {
		l.pending[snap.ID] = snap
		return true
	}
	return l.sendRoom(snap)
}

func (l *listConn) sendRoom(snap *room.RoomSnapshot) bool {
	b, err := json.Marshal(listMsg{Type: "room", Room: snap})
	if err != nil {
		return true
	}
	if !l.c.trySend(b) {
		l.c.shutdown(websocket.StatusPolicyViolation, "send buffer full")
		return false
	}
	l.known[snap.ID] = listDigest(snap)
	l.sent[snap.ID] = l.g.now()
	delete(l.pending, snap.ID)
	return true
}

// deleted sends room_deleted for a room the client has.
func (l *listConn) deleted(id string) bool {
	delete(l.pending, id)
	delete(l.sent, id)
	if _, ok := l.known[id]; !ok {
		return true
	}
	delete(l.known, id)
	b, err := json.Marshal(listMsg{Type: "room_deleted", RoomID: id})
	if err != nil {
		return true
	}
	if !l.c.trySend(b) {
		l.c.shutdown(websocket.StatusPolicyViolation, "send buffer full")
		return false
	}
	return true
}

// resync re-lists and sends what differs from what the client has. A failed
// read keeps the connection; the next update or re-list tries again.
func (l *listConn) resync(ctx context.Context) bool {
	rooms, err := l.list(ctx)
	if err != nil {
		if ctx.Err() == nil {
			l.g.log.Warn("ws: re-list rooms failed", "err", err)
		}
		return true
	}
	seen := make(map[string]bool, len(rooms))
	for i := range rooms {
		seen[rooms[i].ID] = true
		if !l.changed(&rooms[i]) {
			return false
		}
	}
	for id := range l.known {
		if !seen[id] && !l.deleted(id) {
			return false
		}
	}
	return true
}

// flushDue sends the held changes whose wait is over.
func (l *listConn) flushDue() bool {
	now := l.g.now()
	for id, snap := range l.pending {
		if now.Sub(l.sent[id]) >= listCoalesce {
			if !l.sendRoom(snap) {
				return false
			}
		}
	}
	return true
}

// nextDue is the wait until the earliest held change may go out.
func (l *listConn) nextDue() (time.Duration, bool) {
	var next time.Duration
	found := false
	now := l.g.now()
	for id := range l.pending {
		d := max(l.sent[id].Add(listCoalesce).Sub(now), time.Millisecond)
		if !found || d < next {
			next, found = d, true
		}
	}
	return next, found
}

// listDigest hashes the fields the list shows (docs/api.md): a change to
// anything else (steps, last_seq, members) sends nothing.
func listDigest(s *room.RoomSnapshot) [32]byte {
	type proposal struct {
		ID        string            `json:"id"`
		Approvals map[string]string `json:"approvals"`
	}
	d := struct {
		Status      string       `json:"status"`
		Title       string       `json:"title"`
		Severity    string       `json:"severity"`
		Driver      string       `json:"driver"`
		LeaseGone   bool         `json:"lease_expired"`
		Agent       string       `json:"agent"`
		Triage      *room.Triage `json:"triage"`
		HypText     string       `json:"hyp_text"`
		HypConf     string       `json:"hyp_conf"`
		Proposals   []proposal   `json:"proposals"`
		Suggestions int          `json:"suggestions"`
		ClosedAt    *time.Time   `json:"closed_at"`
		Resolution  string       `json:"resolution"`
	}{
		Status: s.Status, Title: s.Title, Severity: s.Severity, Agent: s.AgentStatus.State,
		Triage: s.Triage, Suggestions: len(s.PendingSuggestions), ClosedAt: s.ClosedAt, Resolution: s.Resolution,
	}
	if s.Driver != nil {
		d.Driver, d.LeaseGone = s.Driver.ID, s.Driver.LeaseExpired
	}
	if s.Hypothesis != nil {
		d.HypText, d.HypConf = s.Hypothesis.Text, s.Hypothesis.Confidence
	}
	for _, p := range s.OpenProposals {
		d.Proposals = append(d.Proposals, proposal{ID: p.ProposalID, Approvals: p.Approvals})
	}
	b, _ := json.Marshal(d)
	return sha256.Sum256(b)
}
