package store

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"

	"piperoom/internal/events"
)

// subscriber is one Subscribe call. Only its own goroutine (runSub) delivers
// events, so lastSeq needs no locking; the hub only signals it via wake.
type subscriber struct {
	roomID  string
	lastSeq int64
	ch      chan events.Event
	wake    chan struct{}
}

// hub owns the single LISTEN connection per process and fans notifications
// out to subscribers by room.
type hub struct {
	store      *PG
	connString string
	log        *slog.Logger
	mu         sync.Mutex
	subs       map[string]map[*subscriber]struct{}
}

func newHub(store *PG, connString string, log *slog.Logger) *hub {
	return &hub{
		store:      store,
		connString: connString,
		log:        log,
		subs:       map[string]map[*subscriber]struct{}{},
	}
}

// run owns the LISTEN connection, reconnecting on error and re-arming every
// subscriber so it re-reads from its last seq (no events lost on reconnect).
// listenAppName identifies the hub's LISTEN connection so tests can find and
// terminate it to exercise reconnection.
const listenAppName = "piperoom-listen"

func (h *hub) run(ctx context.Context) {
	for ctx.Err() == nil {
		conn, err := h.connect(ctx)
		if err != nil {
			h.log.Warn("store: listen connect failed", "err", err)
			h.signalAll()
			if !sleep(ctx, 2*time.Second) {
				return
			}
			continue
		}
		if _, err := conn.Exec(ctx, "LISTEN room_events"); err != nil {
			h.log.Warn("store: LISTEN failed", "err", err)
			closeConn(conn)
			h.signalAll()
			if !sleep(ctx, 2*time.Second) {
				return
			}
			continue
		}
		h.log.Info("store: listening for room_events notifications")
		// Catch up anything appended while we were disconnected.
		h.signalAll()
		for ctx.Err() == nil {
			n, err := conn.WaitForNotification(ctx)
			if err != nil {
				if ctx.Err() != nil {
					break // shutting down
				}
				h.log.Warn("store: notification wait failed", "err", err)
				break
			}
			if n.Channel != "room_events" {
				continue
			}
			if roomID, ok := parseNotify(n.Payload); ok {
				h.signalRoom(roomID)
			}
		}
		closeConn(conn)
		h.signalAll()
	}
}

// connect opens the LISTEN connection with a deadline and a recognizable
// application_name (for tests and ops).
func (h *hub) connect(ctx context.Context) (*pgx.Conn, error) {
	cc, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	cfg, err := pgx.ParseConfig(h.connString)
	if err != nil {
		return nil, fmt.Errorf("store: parse listen config: %w", err)
	}
	cfg.RuntimeParams["application_name"] = listenAppName
	conn, err := pgx.ConnectConfig(cc, cfg)
	if err != nil {
		return nil, fmt.Errorf("store: listen connect: %w", err)
	}
	return conn, nil
}

func closeConn(conn *pgx.Conn) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = conn.Close(ctx)
}

// parseNotify splits "room_id:seq" into the room id.
func parseNotify(payload string) (string, bool) {
	i := strings.LastIndexByte(payload, ':')
	if i <= 0 || i == len(payload)-1 {
		return "", false
	}
	if _, err := strconv.ParseInt(payload[i+1:], 10, 64); err != nil {
		return "", false
	}
	return payload[:i], true
}

func (h *hub) register(sub *subscriber) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.subs[sub.roomID] == nil {
		h.subs[sub.roomID] = map[*subscriber]struct{}{}
	}
	h.subs[sub.roomID][sub] = struct{}{}
}

func (h *hub) unregister(sub *subscriber) {
	h.mu.Lock()
	defer h.mu.Unlock()
	set := h.subs[sub.roomID]
	if set == nil {
		return
	}
	delete(set, sub)
	if len(set) == 0 {
		delete(h.subs, sub.roomID)
	}
}

func (h *hub) signalRoom(roomID string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for sub := range h.subs[roomID] {
		wake(sub)
	}
}

func (h *hub) signalAll() {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, set := range h.subs {
		for sub := range set {
			wake(sub)
		}
	}
}

func wake(sub *subscriber) {
	select {
	case sub.wake <- struct{}{}:
	default:
	}
}

// runSub replays events after afterSeq, then streams live until ctx ends. It
// drains Since(lastSeq) on every wake (and a slow safety tick) so events are
// never skipped or repeated even if notifications are merged or lost.
func (s *PG) runSub(ctx context.Context, sub *subscriber) {
	defer func() {
		s.hub.unregister(sub)
		close(sub.ch)
	}()
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		drained, err := s.drain(ctx, sub)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			s.log.Warn("store: drain subscriber", "room", sub.roomID, "err", err)
		}
		if ctx.Err() != nil {
			return
		}
		if drained {
			continue
		}
		select {
		case <-sub.wake:
		case <-tick.C:
		case <-ctx.Done():
			return
		}
	}
}

// drain delivers all events after lastSeq; reports whether any were sent.
func (s *PG) drain(ctx context.Context, sub *subscriber) (bool, error) {
	sent := false
	for {
		after := sub.lastSeq
		events, err := s.Since(ctx, sub.roomID, after, 1000)
		if err != nil {
			return sent, err
		}
		if len(events) == 0 {
			return sent, nil
		}
		for _, e := range events {
			if e.Seq <= sub.lastSeq {
				continue
			}
			select {
			case sub.ch <- e:
				sub.lastSeq = e.Seq
				sent = true
			case <-ctx.Done():
				return sent, ctx.Err()
			}
		}
	}
}

func sleep(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}
