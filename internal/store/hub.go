package store

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// notifyChannel is the LISTEN/NOTIFY channel Append notifies on.
const notifyChannel = "room_events"

const (
	listenConnectTimeout = 10 * time.Second
	listenMinBackoff     = 200 * time.Millisecond
	listenMaxBackoff     = 5 * time.Second
	// listenKeepalive is how long the hub waits for a notification before it
	// checks the connection is still alive. Without it a half-open TCP
	// connection would block WaitForNotification forever and the hub would
	// never reconnect.
	listenKeepalive = 30 * time.Second
)

// hub owns the single LISTEN connection of the process and turns each
// notification into a wake-up for the subscribers of that room. It never
// carries event data: a wake-up only says "read Since(your cursor) again", so
// notifications may be merged, lost or duplicated without affecting delivery.
type hub struct {
	connCfg   *pgx.ConnConfig // template; copied for every (re)connect
	log       *slog.Logger
	keepalive time.Duration

	mu   sync.Mutex
	subs map[string]map[*hubSub]struct{} // room id -> subscribers

	pid atomic.Uint32 // backend pid of the current LISTEN connection (0 = none)
}

type hubSub struct {
	wake chan struct{} // capacity 1: wake-ups coalesce
}

func newHub(connCfg *pgx.ConnConfig, log *slog.Logger) *hub {
	cfg := connCfg.Copy()
	// Names the connection in pg_stat_activity so operators can tell it apart.
	if cfg.RuntimeParams == nil {
		cfg.RuntimeParams = map[string]string{}
	}
	if _, ok := cfg.RuntimeParams["application_name"]; !ok {
		cfg.RuntimeParams["application_name"] = "piperoom-room-listener"
	}
	return &hub{
		connCfg:   cfg,
		log:       log,
		keepalive: listenKeepalive,
		subs:      map[string]map[*hubSub]struct{}{},
	}
}

// add registers a subscriber for roomID. remove must be called exactly once
// when the subscriber ends.
func (h *hub) add(roomID string) (wake <-chan struct{}, remove func()) {
	s := &hubSub{wake: make(chan struct{}, 1)}
	h.mu.Lock()
	set := h.subs[roomID]
	if set == nil {
		set = map[*hubSub]struct{}{}
		h.subs[roomID] = set
	}
	set[s] = struct{}{}
	h.mu.Unlock()
	return s.wake, func() {
		h.mu.Lock()
		delete(h.subs[roomID], s)
		if len(h.subs[roomID]) == 0 {
			delete(h.subs, roomID)
		}
		h.mu.Unlock()
	}
}

func signal(s *hubSub) {
	select {
	case s.wake <- struct{}{}:
	default: // a wake-up is already pending
	}
}

func (h *hub) wakeRoom(roomID string) {
	h.mu.Lock()
	for s := range h.subs[roomID] {
		signal(s)
	}
	h.mu.Unlock()
}

func (h *hub) wakeAll() {
	h.mu.Lock()
	for _, set := range h.subs {
		for s := range set {
			signal(s)
		}
	}
	h.mu.Unlock()
}

// run keeps a LISTEN connection alive until ctx ends, reconnecting with
// backoff. After every (re)connect it wakes all subscribers, because
// notifications sent while the connection was down are gone for good.
func (h *hub) run(ctx context.Context) {
	delay := listenMinBackoff
	for ctx.Err() == nil {
		listening, err := h.listen(ctx)
		if ctx.Err() != nil {
			return
		}
		if listening {
			delay = listenMinBackoff // it worked for a while: start the backoff over
		}
		h.log.Warn("store: LISTEN connection lost, reconnecting", "error", err, "retry_in", delay)
		if !sleep(ctx, delay) {
			return
		}
		if !listening {
			delay = min(delay*2, listenMaxBackoff)
		}
	}
}

// listen opens one connection, subscribes it and dispatches notifications until
// it fails. listening reports whether LISTEN had succeeded.
func (h *hub) listen(ctx context.Context) (listening bool, err error) {
	cctx, cancel := context.WithTimeout(ctx, listenConnectTimeout)
	conn, err := pgx.ConnectConfig(cctx, h.connCfg.Copy())
	cancel()
	if err != nil {
		return false, fmt.Errorf("store: connect LISTEN connection: %w", err)
	}
	defer func() {
		// ctx may already be cancelled; closing still needs a live context.
		closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
		defer cancel()
		_ = conn.Close(closeCtx)
		h.pid.Store(0)
	}()

	lctx, cancel := context.WithTimeout(ctx, listenConnectTimeout)
	_, err = conn.Exec(lctx, "LISTEN "+notifyChannel) // constant identifier, no input
	cancel()
	if err != nil {
		return false, fmt.Errorf("store: LISTEN %s: %w", notifyChannel, err)
	}
	h.pid.Store(conn.PgConn().PID())
	h.log.Debug("store: LISTEN connection established", "channel", notifyChannel)
	h.wakeAll()

	for {
		wctx, cancel := context.WithTimeout(ctx, h.keepalive)
		n, err := conn.WaitForNotification(wctx)
		cancel()
		switch {
		case err == nil:
			h.dispatch(n)
		case ctx.Err() != nil:
			return true, ctx.Err()
		case errors.Is(err, context.DeadlineExceeded):
			// Quiet period: prove the connection is alive. The wait itself
			// timing out does not damage the connection.
			pctx, cancel := context.WithTimeout(ctx, listenConnectTimeout)
			_, perr := conn.Exec(pctx, "SELECT 1")
			cancel()
			if perr != nil {
				return true, fmt.Errorf("store: LISTEN connection health check: %w", perr)
			}
		default:
			return true, fmt.Errorf("store: waiting for notification: %w", err)
		}
	}
}

// dispatch maps a "roomID:seq" payload to a wake-up for that room. An
// unparsable payload wakes everyone: harmless, and it can never lose an event.
func (h *hub) dispatch(n *pgconn.Notification) {
	if n == nil {
		return
	}
	i := strings.LastIndexByte(n.Payload, ':')
	if i < 0 {
		h.wakeAll()
		return
	}
	h.wakeRoom(n.Payload[:i])
}

// sleep waits d or until ctx ends; it reports whether the full wait happened.
func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}
