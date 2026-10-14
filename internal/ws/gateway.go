// Package ws implements the WebSocket gateway: replay events after seq N,
// then stream live, plus an in-memory presence hub.
package ws

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/go-chi/chi/v5"

	"piperoom/internal/events"
	"piperoom/internal/room"
	"piperoom/internal/store"
)

// sendBuffer is the per-connection outbound queue for live events. When it
// fills, the connection is closed with a policy violation so the client
// reconnects with after= (docs/api.md). Replay events are written directly
// (blocking) and never go through this buffer.
const sendBuffer = 256

// readTimeout bounds each read; clients send presence every 20 s, so a peer
// that stops reading (and sending presence) is closed rather than left "away"
// forever.
const readTimeout = 60 * time.Second

// writeTimeout bounds each WebSocket write.
const writeTimeout = 10 * time.Second

// replayBatch is the page size when replaying the backlog.
const replayBatch = 500

// replayTimeout bounds the whole replay phase. A client that neither reads its
// backlog nor goes away is disconnected instead of blocking a goroutine and
// holding an unbounded write path (review item a). Replay pages are read with
// this deadline and every frame is written with writeTimeout.
const replayTimeout = 30 * time.Second

// Default connection caps. A browser tab holds one connection, so 20 per user
// covers anyone with a few rooms open on two machines; a page that reconnects
// in a loop without closing hits the cap instead of exhausting the server.
// 200 per room is far above any incident team and bounds the fan-out cost of
// one room's events.
const (
	DefaultMaxConnsPerRoom = 200
	DefaultMaxConnsPerUser = 20
)

// Gateway serves /api/rooms/{id}/ws.
type Gateway struct {
	svc   *room.Service
	store store.Store
	log   *slog.Logger
	now   func() time.Time

	// ReplayTimeout overrides how long the replay phase may take before the
	// connection is closed with a policy violation; zero means replayTimeout.
	// Set it before the gateway serves traffic.
	ReplayTimeout time.Duration

	// OriginPatterns, when non-nil, replaces the default allowed WebSocket
	// origin hosts (the Vite dev UI on localhost:5173). The request's own host
	// is always accepted. Entries are matched with path.Match, so they must be
	// exact hosts: never build one from unvalidated input. An empty non-nil
	// slice means same-origin only. Set before serving.
	OriginPatterns []string

	// Identify resolves who is upgrading. httpapi sets it to read the user the
	// authenticate middleware already put on the request, so the session
	// cookie (or, in dev mode, the X-User header) is the only credential:
	// there is no ?user= (docs/design/auth.md §1.6). With Identify nil every
	// upgrade is refused with 401. Set before serving.
	Identify func(r *http.Request) (Identity, bool)

	// SessionAlive, when set, is asked about every cookie session that has a
	// live connection on each presence tick (20 s). A false answer closes that
	// session's connections with "session_expired". Set before serving.
	SessionAlive func(ctx context.Context, id Identity) bool

	// MaxConnsPerRoom and MaxConnsPerUser cap live connections (counted from
	// the upgrade until the connection ends, replay included). An upgrade
	// over either cap is refused with 429 too_many_connections before the
	// handshake. Zero means the Default value. Set before serving.
	MaxConnsPerRoom int
	MaxConnsPerUser int

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu        sync.Mutex
	closed    bool
	rooms     map[string]*roomHub
	roomSlots map[string]int // connections per room, held from reserve to release
	userSlots map[string]int // connections per user id
}

// NewGateway builds the gateway and starts its presence loop.
func NewGateway(svc *room.Service, st store.Store, log *slog.Logger) *Gateway {
	if log == nil {
		log = slog.Default()
	}
	ctx, cancel := context.WithCancel(context.Background())
	g := &Gateway{svc: svc, store: st, log: log, now: time.Now, ctx: ctx, cancel: cancel, rooms: map[string]*roomHub{},
		roomSlots: map[string]int{}, userSlots: map[string]int{}}
	g.wg.Add(1)
	go func() {
		defer g.wg.Done()
		g.presenceLoop()
	}()
	return g
}

// Close stops the presence loop, closes every live connection and waits for
// all per-connection goroutines to finish. It is idempotent. Connections are
// shut down in parallel: closing one connection waits for the peer's close
// frame (bounded by the library's timeouts), so a dead peer must not delay
// every other client (review item 12).
func (g *Gateway) Close() {
	g.mu.Lock()
	if g.closed {
		g.mu.Unlock()
		return
	}
	g.closed = true
	var clients []*client
	for _, hub := range g.rooms {
		for c := range hub.conns {
			clients = append(clients, c)
		}
	}
	g.mu.Unlock()
	g.cancel()
	var closing sync.WaitGroup
	for _, c := range clients {
		closing.Add(1)
		go func(c *client) {
			defer closing.Done()
			c.shutdown(websocket.StatusGoingAway, "server shutting down")
		}(c)
	}
	closing.Wait()
	g.wg.Wait()
}

// trackConn registers one per-connection goroutine with g.wg unless the
// gateway is already closing, in which case it returns false and the caller
// must not start the goroutine. Running under g.mu makes the registration
// atomic with Close's closed flag and its subsequent wg.Wait (review item 12:
// no wg.Add after Wait has begun).
func (g *Gateway) trackConn() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return false
	}
	g.wg.Add(1)
	return true
}

type serverMsg struct {
	Type    string        `json:"type"`
	Event   *events.Event `json:"event,omitempty"`
	Members []Member      `json:"members,omitempty"`
	Error   *serverError  `json:"error,omitempty"`
}

type serverError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type clientMsg struct {
	Type  string `json:"type"`
	State string `json:"state"`
}

// Member is one presence entry. Verified is the connection user's
// room.User.Verified (docs/design/auth.md section 2.1).
type Member struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	Verified bool   `json:"verified"`
	State    string `json:"state"` // "viewing" | "away"
}

// Identity is who a connection belongs to. SessionID is the public id of the
// cookie session, or "" for a request authenticated by the dev X-User header
// (curl, wscat), which has no session to expire or revoke.
type Identity struct {
	User      room.User
	SessionID string
}

// ServeHTTP authenticates (Identify), verifies the room, upgrades, replays and
// then streams live events.
func (g *Gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
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
	roomID := chi.URLParam(r, "id")
	if _, err := g.svc.GetRoom(r.Context(), roomID); err != nil {
		var re *room.Error
		if errors.As(err, &re) && re.Code == "not_found" {
			writeHTTPError(w, http.StatusNotFound, "not_found", "room not found")
			return
		}
		g.log.Error("ws: get room failed", "room", roomID, "err", err)
		writeHTTPError(w, http.StatusInternalServerError, "internal_error", "internal error")
		return
	}
	after, err := parseAfterQuery(r)
	if err != nil {
		writeHTTPError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}

	// Counted before the handshake so a refusal is a plain HTTP error the
	// client can read, and held until the connection ends.
	release, msg := g.reserve(roomID, user.ID)
	if release == nil {
		writeHTTPError(w, http.StatusTooManyRequests, "too_many_connections", msg)
		return
	}

	origins := g.OriginPatterns
	if origins == nil {
		origins = []string{"localhost:5173", "127.0.0.1:5173"}
	}
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		OriginPatterns: origins,
	})
	if err != nil {
		// Accept already wrote the response.
		release()
		return
	}
	conn.SetReadLimit(4096)

	c := &client{
		sessionID: ident.SessionID,
		userID:    user.ID,
		userName:  user.Name,
		verified:  user.Verified,
		state:     "viewing",
		lastSeen:  g.now(),
		send:      make(chan []byte, sendBuffer),
		conn:      conn,
		once:      sync.Once{},
	}
	ctx, cancel := context.WithCancel(g.ctx)
	c.cancel = cancel
	c.onClose = func() {
		g.removeClient(roomID, c)
		release()
	}

	// Replay the backlog with blocking writes so a slow client cannot overflow
	// the live send buffer while catching up. The whole replay is bounded by
	// replayTimeout: a client that cannot catch up is closed with a policy
	// violation and reconnects with after= (docs/api.md). Every batch is
	// strictly seq > lastSeq, so events are never skipped or repeated; a full
	// page triggers the next page (item 19).
	lastSeq := after
	rto := g.ReplayTimeout
	if rto <= 0 {
		rto = replayTimeout
	}
	// The deadline hangs off the connection context (not r.Context(), which
	// net/http cancels around the hijacked connection); both a stuck store and
	// a client that never reads release the goroutine within rto.
	rctx, rcancel := context.WithTimeout(ctx, rto)
	for {
		evs, err := g.store.Since(rctx, roomID, lastSeq, replayBatch)
		if err != nil {
			timedOut := rctx.Err() != nil
			rcancel()
			switch {
			case timedOut:
				c.shutdown(websocket.StatusPolicyViolation, "replay timeout")
			case errors.Is(err, store.ErrNotFound):
				// Deleted while this connection was still replaying: it was
				// not registered yet, so CloseRoom could not reach it.
				c.shutdown(websocket.StatusNormalClosure, RoomDeletedReason)
			default:
				c.shutdown(websocket.StatusInternalError, "replay failed")
			}
			return
		}
		if len(evs) == 0 {
			break
		}
		for _, e := range evs {
			b, err := json.Marshal(serverMsg{Type: "event", Event: &e})
			if err != nil {
				continue
			}
			if err := c.write(rctx, b); err != nil {
				timedOut := rctx.Err() != nil
				rcancel()
				if timedOut {
					c.shutdown(websocket.StatusPolicyViolation, "replay timeout")
				} else {
					c.shutdown(websocket.StatusInternalError, "write failed")
				}
				return
			}
			lastSeq = e.Seq
		}
		if len(evs) < replayBatch {
			break
		}
	}
	rcancel()

	// Start the writer before announcing presence so presence messages are
	// drained immediately.
	if !g.trackConn() {
		c.shutdown(websocket.StatusGoingAway, "server shutting down")
		return
	}
	go func() {
		defer g.wg.Done()
		g.writeLoop(ctx, c)
	}()

	g.addClient(roomID, c)
	// A delete that finished after the first lookup found no connection here
	// to close; now that this one is registered, CloseRoom would reach it, so
	// one more lookup closes the gap.
	if _, err := g.svc.GetRoom(ctx, roomID); err != nil {
		var re *room.Error
		if errors.As(err, &re) && re.Code == "not_found" {
			c.shutdown(websocket.StatusNormalClosure, RoomDeletedReason)
		} else {
			c.shutdown(websocket.StatusInternalError, "room lookup failed")
		}
		return
	}
	// A logout (or revoke) that ran between Identify and addClient found no
	// connection to close, and the presence tick would only notice it up to
	// 20 s later. The session is revoked in the store before CloseSession
	// runs, so asking once more now that the client is registered closes
	// that gap: a logout after this point finds the client itself.
	g.recheckNewClient(ctx, c, ident)

	ch, err := g.store.Subscribe(ctx, roomID, lastSeq)
	if err != nil {
		c.shutdown(websocket.StatusInternalError, "subscribe failed")
		return
	}

	if !g.trackConn() {
		c.shutdown(websocket.StatusGoingAway, "server shutting down")
		return
	}
	go func() {
		defer g.wg.Done()
		g.readLoop(ctx, roomID, c)
	}()
	if !g.trackConn() {
		c.shutdown(websocket.StatusGoingAway, "server shutting down")
		return
	}
	go func() {
		defer g.wg.Done()
		g.eventLoop(ctx, c, ch)
	}()
}

// reserve takes one connection slot for roomID and userID. It returns a
// release func that gives the slot back (safe to call more than once), or nil
// and the reason when either cap is reached.
func (g *Gateway) reserve(roomID, userID string) (func(), string) {
	perRoom, perUser := g.MaxConnsPerRoom, g.MaxConnsPerUser
	if perRoom <= 0 {
		perRoom = DefaultMaxConnsPerRoom
	}
	if perUser <= 0 {
		perUser = DefaultMaxConnsPerUser
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.userSlots[userID] >= perUser {
		return nil, fmt.Sprintf("you already have %d live connections; close some Piperoom tabs and retry", perUser)
	}
	if g.roomSlots[roomID] >= perRoom {
		return nil, fmt.Sprintf("this room already has %d live connections; retry later", perRoom)
	}
	g.userSlots[userID]++
	g.roomSlots[roomID]++
	var once sync.Once
	return func() {
		once.Do(func() {
			g.mu.Lock()
			defer g.mu.Unlock()
			if g.userSlots[userID]--; g.userSlots[userID] <= 0 {
				delete(g.userSlots, userID)
			}
			if g.roomSlots[roomID]--; g.roomSlots[roomID] <= 0 {
				delete(g.roomSlots, roomID)
			}
		})
	}, ""
}

// recheckNewClient ends c's session at once when SessionAlive says the session
// is already gone. Connections without a cookie session are not checked.
func (g *Gateway) recheckNewClient(ctx context.Context, c *client, ident Identity) {
	if g.SessionAlive == nil || c.sessionID == "" {
		return
	}
	actx, cancel := context.WithTimeout(ctx, recheckTimeout)
	alive := g.SessionAlive(actx, ident)
	cancel()
	if alive {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.spawnSessionEnd(c)
}

func (g *Gateway) writeLoop(ctx context.Context, c *client) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-c.send:
			if err := c.write(ctx, msg); err != nil {
				c.shutdown(websocket.StatusInternalError, "write failed")
				return
			}
		}
	}
}

func (g *Gateway) readLoop(ctx context.Context, roomID string, c *client) {
	for {
		rctx, cancel := context.WithTimeout(ctx, readTimeout)
		typ, data, err := c.conn.Read(rctx)
		cancel()
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				c.shutdown(websocket.StatusPolicyViolation, "read timeout")
			} else {
				c.shutdown(websocket.StatusNormalClosure, "")
			}
			return
		}
		if typ != websocket.MessageText {
			continue
		}
		var msg clientMsg
		if err := json.Unmarshal(data, &msg); err != nil || msg.Type != "presence" {
			continue
		}
		g.setPresence(roomID, c, msg.State)
	}
}

func (g *Gateway) eventLoop(ctx context.Context, c *client, ch <-chan events.Event) {
	for {
		select {
		case <-ctx.Done():
			return
		case e, ok := <-ch:
			if !ok {
				return
			}
			b, err := json.Marshal(serverMsg{Type: "event", Event: &e})
			if err != nil {
				continue
			}
			if !c.trySend(b) {
				c.shutdown(websocket.StatusPolicyViolation, "send buffer full")
				return
			}
		}
	}
}

func parseAfterQuery(r *http.Request) (int64, error) {
	q := r.URL.Query().Get("after")
	if q == "" {
		return 0, nil
	}
	n, err := strconv.ParseInt(q, 10, 64)
	if err != nil || n < 0 {
		return 0, errors.New("after must be a non-negative integer")
	}
	return n, nil
}

func writeHTTPError(w http.ResponseWriter, status int, code, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]string{"code": code, "message": msg}})
}
