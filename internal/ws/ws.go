// Package ws serves GET /api/rooms/{id}/ws (docs/api.md §WebSocket): it
// replays the room's event log after a sequence number, streams new events
// live and keeps an in-memory presence list.
//
// The handler reads the room id with r.PathValue("id"), so mount it on a
// router that sets path values (net/http's ServeMux and chi both do).
package ws

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/coder/websocket"

	"piperoom/internal/events"
	"piperoom/internal/room"
)

// Defaults for Config.
const (
	// DefaultAwayAfter is how long a member may stay silent before they show
	// as away: 2× the 20 s client heartbeat plus slack (docs/api.md).
	DefaultAwayAfter     = 45 * time.Second
	DefaultSweepInterval = 5 * time.Second
	DefaultPingInterval  = 30 * time.Second
	DefaultSendBuffer    = 256
	DefaultSlowGrace     = 5 * time.Second
	DefaultWriteTimeout  = 10 * time.Second

	pingTimeout = 10 * time.Second
	// maxClientMessage bounds a client message; presence messages are tiny.
	maxClientMessage = 4 << 10
)

// Config wires the handler. A nil Authenticate denies everyone, a nil
// RoomExists reports every room missing and a nil Subscribe answers 503, so a
// half-wired handler fails closed.
type Config struct {
	// Subscribe replays events with seq > after, then streams live ones, and
	// closes the channel when ctx ends (events.Store.Subscribe).
	Subscribe func(ctx context.Context, roomID string, after int64) (<-chan events.Event, error)
	// Authenticate resolves ?user=; there is no X-User on a WebSocket.
	Authenticate func(id string) (room.User, bool)
	RoomExists   func(ctx context.Context, roomID string) bool
	Logger       *slog.Logger
	// OriginPatterns are the browser origin hosts allowed besides the
	// request's own host (coder/websocket AcceptOptions.OriginPatterns).
	// Default: localhost:5173 and 127.0.0.1:5173, the Vite dev server.
	OriginPatterns []string
	// Now is the presence clock. Default time.Now.
	Now func() time.Time

	// The fields below tune timing; zero means the default. They exist for
	// tests and operators, not for normal use.
	AwayAfter     time.Duration // DefaultAwayAfter
	SweepInterval time.Duration // DefaultSweepInterval
	// PingInterval is how often the server pings to detect dead peers; a
	// negative value turns pings off.
	PingInterval time.Duration // DefaultPingInterval
	SendBuffer   int           // DefaultSendBuffer
	// SlowGrace is how long a full send buffer may stay full before the
	// client is closed with StatusPolicyViolation. The wait matters because
	// a replay of a long log fills the buffer faster than any network
	// drains it; only a client that makes no room for SlowGrace is slow.
	// A negative value closes the client the moment the buffer is full.
	SlowGrace    time.Duration // DefaultSlowGrace
	WriteTimeout time.Duration // DefaultWriteTimeout
}

// Handler is the WebSocket endpoint.
type Handler struct {
	cfg  Config
	log  *slog.Logger
	hub  *hub
	base context.Context
	stop context.CancelFunc

	mu     sync.Mutex // guards closed and wg.Add
	closed bool
	wg     sync.WaitGroup

	// beforeWrite is a test hook that runs before each frame is written, so
	// tests can make the writer slow without depending on TCP buffer sizes.
	beforeWrite func()
}

// New returns the handler. Call Shutdown when the server stops: hijacked
// connections are not closed by http.Server.Shutdown.
func New(cfg Config) *Handler {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if len(cfg.OriginPatterns) == 0 {
		cfg.OriginPatterns = []string{"localhost:5173", "127.0.0.1:5173"}
	}
	if cfg.AwayAfter <= 0 {
		cfg.AwayAfter = DefaultAwayAfter
	}
	if cfg.SweepInterval <= 0 {
		cfg.SweepInterval = DefaultSweepInterval
	}
	if cfg.PingInterval == 0 {
		cfg.PingInterval = DefaultPingInterval
	}
	if cfg.SendBuffer <= 0 {
		cfg.SendBuffer = DefaultSendBuffer
	}
	if cfg.SlowGrace == 0 {
		cfg.SlowGrace = DefaultSlowGrace
	}
	if cfg.WriteTimeout <= 0 {
		cfg.WriteTimeout = DefaultWriteTimeout
	}
	base, stop := context.WithCancel(context.Background())
	return &Handler{
		cfg:  cfg,
		log:  cfg.Logger,
		hub:  newHub(cfg.Now, cfg.AwayAfter),
		base: base,
		stop: stop,
	}
}

// PresenceMembers returns who is in the room right now (never nil).
func (h *Handler) PresenceMembers(roomID string) []PresenceMember {
	return h.hub.members(roomID)
}

// Shutdown closes every connection with StatusGoingAway and waits until their
// goroutines have ended or ctx is done. New connections are refused (503).
func (h *Handler) Shutdown(ctx context.Context) error {
	h.mu.Lock()
	h.closed = true
	h.mu.Unlock()
	h.stop()

	done := make(chan struct{})
	go func() {
		h.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// enter registers one connection; false after Shutdown.
func (h *Handler) enter() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return false
	}
	h.wg.Add(1)
	return true
}

type errorBody struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

// writeHTTPError answers before the upgrade, in the docs/api.md error shape.
func writeHTTPError(w http.ResponseWriter, status int, code, msg string) {
	var b errorBody
	b.Error.Code, b.Error.Message = code, msg
	buf, _ := json.Marshal(b)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(append(buf, '\n'))
}

// ServeHTTP checks user and room, then upgrades.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	roomID := r.PathValue("id")
	userID := r.URL.Query().Get("user")
	if userID == "" || h.cfg.Authenticate == nil {
		writeHTTPError(w, http.StatusUnauthorized, "unauthenticated", "Send ?user= with a known user id.")
		return
	}
	user, ok := h.cfg.Authenticate(userID)
	if !ok {
		writeHTTPError(w, http.StatusUnauthorized, "unauthenticated", "Unknown user.")
		return
	}
	if roomID == "" || h.cfg.RoomExists == nil || !h.cfg.RoomExists(r.Context(), roomID) {
		writeHTTPError(w, http.StatusNotFound, "not_found", "No such room.")
		return
	}
	var after int64
	if v := r.URL.Query().Get("after"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil || n < 0 {
			writeHTTPError(w, http.StatusBadRequest, "bad_request", "after must be a non-negative integer.")
			return
		}
		after = n
	}
	if h.cfg.Subscribe == nil || !h.enter() {
		writeHTTPError(w, http.StatusServiceUnavailable, "unavailable", "The live feed is not available.")
		return
	}
	defer h.wg.Done()
	h.serve(w, r, roomID, user, after)
}

func (h *Handler) serve(w http.ResponseWriter, r *http.Request, roomID string, user room.User, after int64) {
	// subCtx ends the store subscription. It ends when this function returns,
	// on Shutdown, or when the server cancels the request.
	subCtx, cancelSub := context.WithCancel(r.Context())
	defer cancelSub()
	stopBase := context.AfterFunc(h.base, cancelSub)
	defer stopBase()

	// Subscribe before the upgrade so a store error is still a plain HTTP error.
	sub, err := h.cfg.Subscribe(subCtx, roomID, after)
	if err != nil {
		h.log.Error("ws: subscribe", "room", roomID, "user", user.ID, "err", err)
		writeHTTPError(w, http.StatusServiceUnavailable, "unavailable", "The live feed is not available.")
		return
	}

	wsc, err := websocket.Accept(w, r, &websocket.AcceptOptions{OriginPatterns: h.cfg.OriginPatterns})
	if err != nil {
		// Accept has already answered the client (bad origin, not a WebSocket request, ...).
		h.log.Debug("ws: accept", "room", roomID, "user", user.ID, "err", err)
		return
	}
	wsc.SetReadLimit(maxClientMessage)

	c := newConn(h, wsc, roomID, user)
	h.hub.join(roomID, c)
	defer h.hub.leave(roomID, c)
	h.log.Info("ws: connected", "room", roomID, "user", user.ID, "after", after)

	// connCtx is deliberately not tied to the request: a context error on a
	// websocket call tears the connection down without a close frame, which
	// would lose the status code of a graceful close.
	connCtx, cancelConn := context.WithCancel(context.Background())
	defer cancelConn()

	var wg sync.WaitGroup
	run := func(f func()) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			f()
		}()
	}
	run(func() { c.readLoop(connCtx) })
	run(func() { c.writeLoop(connCtx) })
	run(func() { c.pump(sub, after) })
	run(func() { c.housekeeping(connCtx) })

	select {
	case <-c.done:
	case <-h.base.Done():
		c.fail(websocket.StatusGoingAway, "server shutting down")
	case <-r.Context().Done():
		c.fail(websocket.StatusGoingAway, "request canceled")
	}
	<-c.done
	// Graceful close first, then cancel: Close lets the reader see the peer's
	// reply, and unblocks any goroutine stuck in Read or Write.
	if err := wsc.Close(c.code, c.reason); err != nil {
		h.log.Debug("ws: close", "room", roomID, "user", user.ID, "err", err)
	}
	cancelConn()
	cancelSub()
	wg.Wait()
	h.log.Info("ws: disconnected", "room", roomID, "user", user.ID, "code", int(c.code), "reason", c.reason)
}

// ---- one connection ----

type conn struct {
	h      *Handler
	ws     *websocket.Conn
	roomID string
	user   room.User

	// send carries event frames in order. Its capacity is the per-connection
	// buffer of docs/api.md.
	send chan []byte

	// Presence frames are snapshots, so only the latest matters. They bypass
	// send: a full event buffer must not make presence updates fail, and a
	// presence update must never be able to push a client over the buffer.
	pmu         sync.Mutex
	pending     []byte
	presenceSig chan struct{}

	// declared and lastSeen are owned by hub.mu.
	declared string
	lastSeen time.Time

	closeOnce sync.Once
	done      chan struct{} // closed once code and reason are set
	code      websocket.StatusCode
	reason    string
}

func newConn(h *Handler, ws *websocket.Conn, roomID string, user room.User) *conn {
	return &conn{
		h:           h,
		ws:          ws,
		roomID:      roomID,
		user:        user,
		send:        make(chan []byte, h.cfg.SendBuffer),
		presenceSig: make(chan struct{}, 1),
		done:        make(chan struct{}),
	}
}

// fail ends the connection with a close status. Only the first call counts.
// reason must be short and fixed (the protocol allows 125 bytes).
func (c *conn) fail(code websocket.StatusCode, reason string) {
	c.closeOnce.Do(func() {
		c.code, c.reason = code, reason
		close(c.done)
	})
}

// setPresence stores the latest presence frame and wakes the writer.
func (c *conn) setPresence(frame []byte) {
	c.pmu.Lock()
	c.pending = frame
	c.pmu.Unlock()
	select {
	case c.presenceSig <- struct{}{}:
	default: // a wake-up is already queued
	}
}

func (c *conn) takePresence() []byte {
	c.pmu.Lock()
	defer c.pmu.Unlock()
	f := c.pending
	c.pending = nil
	return f
}

type eventFrame struct {
	Type  string       `json:"type"`
	Event events.Event `json:"event"`
}

type errorFrame struct {
	Type  string `json:"type"`
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

// pump moves events from the store subscription to the send buffer. last is
// the highest seq already sent; anything at or below it is dropped, so a
// store that repeats an event cannot make the client fold it twice.
func (c *conn) pump(sub <-chan events.Event, last int64) {
	for {
		select {
		case <-c.done:
			return
		case ev, ok := <-sub:
			if !ok {
				// The feed ends when its context ends (Shutdown, request
				// canceled) or when the store gives up on this subscriber. In
				// both cases the client reconnects with its highest seq.
				if c.h.base.Err() != nil {
					c.fail(websocket.StatusGoingAway, "server shutting down")
				} else {
					c.fail(websocket.StatusTryAgainLater, "live feed ended")
				}
				return
			}
			if ev.Seq <= last {
				continue
			}
			if ev.Seq > last+1 {
				// The store promises no gaps. Forward anyway: dropping the
				// event would be worse, and the client sees the jump.
				c.h.log.Warn("ws: gap in event sequence", "room", c.roomID, "after_seq", last, "got_seq", ev.Seq)
			}
			last = ev.Seq
			frame, err := json.Marshal(eventFrame{Type: "event", Event: ev})
			if err != nil {
				c.h.log.Error("ws: marshal event", "room", c.roomID, "seq", ev.Seq, "err", err)
				c.fail(websocket.StatusInternalError, "cannot encode event")
				return
			}
			if !c.enqueue(frame) {
				return
			}
		}
	}
}

// enqueue puts an event frame in the send buffer. If the buffer stays full for
// SlowGrace, the client is too slow: it is closed with StatusPolicyViolation
// and reconnects with after=<highest seq it saw>.
func (c *conn) enqueue(frame []byte) bool {
	select {
	case c.send <- frame:
		return true
	default:
	}
	grace := c.h.cfg.SlowGrace
	if grace > 0 {
		t := time.NewTimer(grace)
		defer t.Stop()
		select {
		case c.send <- frame:
			return true
		case <-c.done:
			return false
		case <-t.C:
		}
	}
	c.h.log.Warn("ws: slow client dropped", "room", c.roomID, "user", c.user.ID)
	c.fail(websocket.StatusPolicyViolation, "send buffer full; reconnect with after=")
	return false
}

// trySend queues a small control frame (an error reply) without ever
// blocking or dropping the client; it is dropped when the buffer is full.
func (c *conn) trySend(frame []byte) {
	select {
	case c.send <- frame:
	default:
	}
}

func (c *conn) writeLoop(ctx context.Context) {
	for {
		select {
		case <-c.done:
			return
		case frame := <-c.send:
			if !c.write(ctx, frame) {
				return
			}
		case <-c.presenceSig:
			if frame := c.takePresence(); frame != nil && !c.write(ctx, frame) {
				return
			}
		}
	}
}

func (c *conn) write(ctx context.Context, frame []byte) bool {
	if c.h.beforeWrite != nil {
		c.h.beforeWrite()
	}
	wctx, cancel := context.WithTimeout(ctx, c.h.cfg.WriteTimeout)
	defer cancel()
	if err := c.ws.Write(wctx, websocket.MessageText, frame); err != nil {
		c.fail(websocket.StatusGoingAway, "write failed")
		return false
	}
	return true
}

// readLoop handles client messages. It must always run: the library only
// answers pings and close frames while someone reads.
func (c *conn) readLoop(ctx context.Context) {
	for {
		typ, data, err := c.ws.Read(ctx)
		if err != nil {
			// The peer closed or the connection broke; the library has
			// already closed it, so the status here is only for the log.
			c.fail(websocket.StatusNormalClosure, "")
			return
		}
		c.handle(typ, data)
	}
}

// handle applies one client message. A malformed message gets an error frame
// and does not close the connection: a buggy tab should not lose its feed.
func (c *conn) handle(typ websocket.MessageType, data []byte) {
	if typ != websocket.MessageText {
		c.h.hub.touch(c.roomID, c, "")
		c.replyError("bad_request", "Messages must be JSON text.")
		return
	}
	var m struct {
		Type  string `json:"type"`
		State string `json:"state"`
	}
	if err := json.Unmarshal(data, &m); err != nil {
		c.h.hub.touch(c.roomID, c, "")
		c.replyError("bad_request", "Messages must be JSON objects.")
		return
	}
	switch {
	case m.Type != "presence":
		c.h.hub.touch(c.roomID, c, "")
		c.replyError("bad_request", "Unknown message type.")
	case m.State != StateViewing && m.State != StateAway:
		c.h.hub.touch(c.roomID, c, "")
		c.replyError("bad_request", `state must be "viewing" or "away".`)
	default:
		c.h.hub.touch(c.roomID, c, m.State)
	}
}

func (c *conn) replyError(code, msg string) {
	var f errorFrame
	f.Type = "error"
	f.Error.Code, f.Error.Message = code, msg
	buf, err := json.Marshal(f)
	if err != nil {
		return
	}
	c.trySend(buf)
}

// housekeeping sweeps presence and pings the peer. Both run per connection so
// that no goroutine outlives the connections of a room.
func (c *conn) housekeeping(ctx context.Context) {
	sweep := time.NewTicker(c.h.cfg.SweepInterval)
	defer sweep.Stop()
	var pingC <-chan time.Time
	if c.h.cfg.PingInterval > 0 {
		t := time.NewTicker(c.h.cfg.PingInterval)
		defer t.Stop()
		pingC = t.C
	}
	for {
		select {
		case <-c.done:
			return
		case <-sweep.C:
			c.h.hub.sweep(c.roomID)
		case <-pingC:
			pctx, cancel := context.WithTimeout(ctx, pingTimeout)
			err := c.ws.Ping(pctx)
			cancel()
			if err != nil {
				c.fail(websocket.StatusGoingAway, "ping timeout")
				return
			}
		}
	}
}
