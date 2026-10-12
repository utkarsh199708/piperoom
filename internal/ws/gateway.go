// Package ws implements the WebSocket gateway: replay events after seq N,
// then stream live, plus an in-memory presence hub.
package ws

import (
	"context"
	"encoding/json"
	"errors"
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

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu     sync.Mutex
	closed bool
	rooms  map[string]*roomHub
}

// NewGateway builds the gateway and starts its presence loop.
func NewGateway(svc *room.Service, st store.Store, log *slog.Logger) *Gateway {
	if log == nil {
		log = slog.Default()
	}
	ctx, cancel := context.WithCancel(context.Background())
	g := &Gateway{svc: svc, store: st, log: log, now: time.Now, ctx: ctx, cancel: cancel, rooms: map[string]*roomHub{}}
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

// Member is one presence entry.
type Member struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	State string `json:"state"` // "viewing" | "away"
}

// ServeHTTP authenticates (?user=), verifies the room, upgrades, replays and
// then streams live events.
func (g *Gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	userID := r.URL.Query().Get("user")
	user, ok := g.svc.LookupUser(userID)
	if !ok {
		writeHTTPError(w, http.StatusUnauthorized, "unauthenticated", "unknown user")
		return
	}
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

	origins := g.OriginPatterns
	if origins == nil {
		origins = []string{"localhost:5173", "127.0.0.1:5173"}
	}
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		OriginPatterns: origins,
	})
	if err != nil {
		// Accept already wrote the response.
		return
	}
	conn.SetReadLimit(4096)

	c := &client{
		userID:   user.ID,
		userName: user.Name,
		state:    "viewing",
		lastSeen: g.now(),
		send:     make(chan []byte, sendBuffer),
		conn:     conn,
		once:     sync.Once{},
	}
	ctx, cancel := context.WithCancel(g.ctx)
	c.cancel = cancel
	c.onClose = func() { g.removeClient(roomID, c) }

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
			if timedOut {
				c.shutdown(websocket.StatusPolicyViolation, "replay timeout")
			} else {
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
