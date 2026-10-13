package store

import (
	"context"
	"crypto/rand"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"

	"piperoom/internal/events"
)

// Fan-out across processes (docs/design/replicas.md section 6.1). Append
// notifies room_events with "room_id:seq" in its transaction; every process
// LISTENs on one connection. Per room with local subscribers there is one feed
// that reads the new events once and keeps the last feedBuffer of them, so N
// sockets on one room cost one query per event, not N. While the LISTEN
// connection is down, every feed polls every pollInterval instead.

// feedBuffer is how many recent events a room feed keeps. A subscriber that is
// further behind (it just connected, or it was slow) reads its gap from the
// store itself and then rejoins the buffer.
const feedBuffer = 64

// DefaultPollInterval is how often each room feed and each append watcher
// re-reads while the LISTEN connection is down or reconnecting.
const DefaultPollInterval = 2 * time.Second

// retryAfterError is how long a feed or a subscriber waits before it reads
// again after a failed read when no notify wakes it sooner.
const retryAfterError = 2 * time.Second

// subscriber is one Subscribe call. Only its own goroutine (runSub) delivers
// events, so lastSeq needs no locking; its feed only signals it via wake.
type subscriber struct {
	roomID  string
	lastSeq int64
	ch      chan events.Event
	wake    chan struct{}
	feed    *roomFeed
}

// roomFeed reads one room's new events once for all its local subscribers.
// Only its goroutine (runFeed) writes tail and buf; subscribers read them under
// mu.
type roomFeed struct {
	roomID string
	wake   chan struct{}
	cancel context.CancelFunc

	mu   sync.Mutex
	subs map[*subscriber]struct{}
	// tail is the highest seq read; -1 until the feed has read the room's
	// last_seq once.
	tail int64
	// buf holds the events tail-len(buf)+1 .. tail, in order.
	buf []events.Event
}

// after returns the buffered events with seq > lastSeq. ok is false when the
// buffer cannot answer (the feed has not started, or lastSeq is older than the
// buffer): the subscriber then reads from the store.
func (f *roomFeed) after(lastSeq int64) (evs []events.Event, ok bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.tail < 0 {
		return nil, false
	}
	if lastSeq >= f.tail {
		return nil, true
	}
	first := f.tail - int64(len(f.buf)) // the seq just before buf[0]
	if lastSeq < first {
		return nil, false
	}
	return append([]events.Event(nil), f.buf[lastSeq-first:]...), true
}

// add appends events read after tail and wakes every subscriber.
func (f *roomFeed) add(evs []events.Event) {
	f.mu.Lock()
	for _, e := range evs {
		if e.Seq <= f.tail {
			continue
		}
		if e.Seq != f.tail+1 {
			// Not contiguous (cannot happen on a gap-free log); start over
			// so after() never hands out a hole.
			f.buf = f.buf[:0]
		}
		f.buf = append(f.buf, e)
		f.tail = e.Seq
	}
	if n := len(f.buf); n > feedBuffer {
		f.buf = append(f.buf[:0:0], f.buf[n-feedBuffer:]...)
	}
	subs := make([]*subscriber, 0, len(f.subs))
	for sub := range f.subs {
		subs = append(subs, sub)
	}
	f.mu.Unlock()
	for _, sub := range subs {
		signal(sub.wake)
	}
}

func (f *roomFeed) getTail() int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.tail
}

func (f *roomFeed) setTail(seq int64) {
	f.mu.Lock()
	f.tail = seq
	f.buf = f.buf[:0]
	f.mu.Unlock()
}

// AppendWatch receives the append notifications of every process that writes
// to this database (docs/design/replicas.md section 6.2). Both funcs must
// return at once; the room service only marks the room and signals its own
// goroutine.
type AppendWatch struct {
	// OnAppend is called for each notified append, in arrival order.
	OnAppend func(roomID string, seq int64)
	// OnResync is called when notifications may have been missed: after the
	// LISTEN connection (re)connects, and every poll interval while it is down.
	OnResync func()
}

// hub owns the single LISTEN connection per process and fans notifications
// out to room feeds and append watchers.
type hub struct {
	store      *PG
	connString string
	appName    string
	log        *slog.Logger
	listen     bool          // false: never LISTEN, poll only (tests of the fallback)
	poll       time.Duration // poll interval while not listening

	ctx context.Context // the hub's lifetime; feeds derive from it

	mu        sync.Mutex
	feeds     map[string]*roomFeed
	watchers  map[*AppendWatch]struct{}
	listening bool
	// stopped is set under mu when run returns, so register never adds a
	// feed goroutine to hubWg after Close's Wait could have returned.
	stopped bool
}

func newHub(ctx context.Context, store *PG, connString, appName string, log *slog.Logger, listen bool, poll time.Duration) *hub {
	if poll <= 0 {
		poll = DefaultPollInterval
	}
	return &hub{
		store:      store,
		connString: connString,
		appName:    appName,
		log:        log,
		listen:     listen,
		poll:       poll,
		ctx:        ctx,
		feeds:      map[string]*roomFeed{},
		watchers:   map[*AppendWatch]struct{}{},
	}
}

// listenAppName builds the unique application_name of one hub's LISTEN
// connection: a recognizable prefix plus random bytes, unique per instance
// and process.
func listenAppName() string {
	var b [6]byte
	rand.Read(b[:]) // never returns an error (crypto/rand docs)
	return fmt.Sprintf("%s-%d-%x", listenAppNamePrefix, os.Getpid(), b)
}

// The LISTEN connection's application_name starts with listenAppNamePrefix and
// ends with a per-instance suffix so a test can terminate exactly its own
// hub's backend without killing a running dev server's LISTEN connection.
const listenAppNamePrefix = "piperoom-listen"

// run owns the LISTEN connection, reconnecting on error, and runs the poll
// fallback while it is down. Every (re)connect makes every feed and watcher
// catch up, so nothing appended while disconnected is lost.
func (h *hub) run() {
	ctx := h.ctx
	defer func() {
		h.mu.Lock()
		h.stopped = true
		h.mu.Unlock()
	}()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		h.pollLoop(ctx)
	}()
	defer wg.Wait()
	if !h.listen {
		h.log.Info("store: LISTEN disabled; room feeds poll", "every", h.poll.String())
		<-ctx.Done()
		return
	}
	for ctx.Err() == nil {
		conn, err := h.connect(ctx)
		if err != nil {
			h.log.Warn("store: listen connect failed", "err", err)
			if !sleep(ctx, 2*time.Second) {
				return
			}
			continue
		}
		// The LISTEN statement is a network round-trip on a fresh connection;
		// bound it so a hung backend cannot stall the reconnect loop forever.
		lctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		_, err = conn.Exec(lctx, "LISTEN room_events")
		cancel()
		if err != nil {
			h.log.Warn("store: LISTEN failed", "err", err)
			closeConn(conn)
			if !sleep(ctx, 2*time.Second) {
				return
			}
			continue
		}
		h.log.Info("store: listening for room_events notifications")
		h.setListening(true)
		// Catch up anything appended while we were disconnected.
		h.resyncAll()
		for ctx.Err() == nil {
			n, err := conn.WaitForNotification(ctx)
			if err != nil {
				if ctx.Err() != nil {
					break // shutting down
				}
				h.log.Warn("store: notification wait failed; polling until reconnected", "err", err)
				break
			}
			if n.Channel != "room_events" {
				continue
			}
			if roomID, seq, ok := parseNotify(n.Payload); ok {
				h.notify(roomID, seq)
			}
		}
		h.setListening(false)
		closeConn(conn)
		h.resyncAll()
	}
}

// pollLoop wakes every feed and every watcher each poll interval while the
// LISTEN connection is down (the fallback when a NOTIFY may be missed).
func (h *hub) pollLoop(ctx context.Context) {
	t := time.NewTicker(h.poll)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		if !h.isListening() {
			h.resyncAll()
		}
	}
}

func (h *hub) setListening(v bool) {
	h.mu.Lock()
	h.listening = v
	h.mu.Unlock()
}

func (h *hub) isListening() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.listening
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
	cfg.RuntimeParams["application_name"] = h.appName
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

// parseNotify splits "room_id:seq".
func parseNotify(payload string) (string, int64, bool) {
	i := strings.LastIndexByte(payload, ':')
	if i <= 0 || i == len(payload)-1 {
		return "", 0, false
	}
	seq, err := strconv.ParseInt(payload[i+1:], 10, 64)
	if err != nil {
		return "", 0, false
	}
	return payload[:i], seq, true
}

// notify routes one notification to the room's feed and every watcher.
func (h *hub) notify(roomID string, seq int64) {
	h.mu.Lock()
	f := h.feeds[roomID]
	ws := h.watcherList()
	h.mu.Unlock()
	if f != nil {
		signal(f.wake)
	}
	for _, w := range ws {
		if w.OnAppend != nil {
			w.OnAppend(roomID, seq)
		}
	}
}

// resyncAll wakes every feed and tells every watcher to re-read.
func (h *hub) resyncAll() {
	h.mu.Lock()
	feeds := make([]*roomFeed, 0, len(h.feeds))
	for _, f := range h.feeds {
		feeds = append(feeds, f)
	}
	ws := h.watcherList()
	h.mu.Unlock()
	for _, f := range feeds {
		signal(f.wake)
	}
	for _, w := range ws {
		if w.OnResync != nil {
			w.OnResync()
		}
	}
}

// watcherList copies the watchers. Caller holds h.mu.
func (h *hub) watcherList() []*AppendWatch {
	out := make([]*AppendWatch, 0, len(h.watchers))
	for w := range h.watchers {
		out = append(out, w)
	}
	return out
}

func (h *hub) addWatcher(w *AppendWatch) {
	h.mu.Lock()
	h.watchers[w] = struct{}{}
	h.mu.Unlock()
}

func (h *hub) removeWatcher(w *AppendWatch) {
	h.mu.Lock()
	delete(h.watchers, w)
	h.mu.Unlock()
}

// register adds sub to its room's feed, starting the feed if it is the first
// subscriber. It reports false when the hub has stopped.
func (h *hub) register(sub *subscriber) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.stopped || h.ctx.Err() != nil {
		return false
	}
	f := h.feeds[sub.roomID]
	if f == nil {
		fctx, cancel := context.WithCancel(h.ctx)
		f = &roomFeed{
			roomID: sub.roomID,
			wake:   make(chan struct{}, 1),
			cancel: cancel,
			subs:   map[*subscriber]struct{}{},
			tail:   -1,
		}
		h.feeds[sub.roomID] = f
		h.store.hubWg.Add(1)
		go func() {
			defer h.store.hubWg.Done()
			h.runFeed(fctx, f)
		}()
	}
	f.mu.Lock()
	f.subs[sub] = struct{}{}
	f.mu.Unlock()
	sub.feed = f
	return true
}

// unregister removes sub; the last subscriber of a room stops its feed.
func (h *hub) unregister(sub *subscriber) {
	h.mu.Lock()
	defer h.mu.Unlock()
	f := sub.feed
	if f == nil {
		return
	}
	f.mu.Lock()
	delete(f.subs, sub)
	empty := len(f.subs) == 0
	f.mu.Unlock()
	if empty && h.feeds[sub.roomID] == f {
		delete(h.feeds, sub.roomID)
		f.cancel()
	}
}

// runFeed reads the room's new events on every wake until its context ends.
// The first read only learns the room's last_seq: older events are each
// subscriber's own replay.
func (h *hub) runFeed(ctx context.Context, f *roomFeed) {
	for {
		retry := h.readFeed(ctx, f)
		if ctx.Err() != nil {
			return
		}
		var timer <-chan time.Time
		if retry {
			timer = time.After(retryAfterError)
		}
		select {
		case <-ctx.Done():
			return
		case <-f.wake:
		case <-timer:
		}
	}
}

// readFeed reads everything after the feed's tail. It reports whether a read
// failed (the feed then retries on its own).
func (h *hub) readFeed(ctx context.Context, f *roomFeed) (retry bool) {
	if f.getTail() < 0 {
		row, err := h.store.GetRoom(ctx, f.roomID)
		if err != nil {
			if ctx.Err() == nil {
				h.log.Warn("store: room feed: read last seq", "room", f.roomID, "err", err)
			}
			return true
		}
		f.setTail(row.LastSeq)
		// Wake the subscribers: those at or past the tail now wait on the
		// buffer instead of reading the store.
		f.add(nil)
	}
	for {
		evs, err := h.store.Since(ctx, f.roomID, f.getTail(), 1000)
		if err != nil {
			if ctx.Err() == nil {
				h.log.Warn("store: room feed: read events", "room", f.roomID, "err", err)
			}
			return true
		}
		if len(evs) == 0 {
			return false
		}
		f.add(evs)
		if len(evs) < 1000 {
			return false
		}
	}
}

// runSub replays events after afterSeq, then streams live until ctx ends. It
// takes events from its room feed's buffer when it can and reads the store
// itself when it is behind, so events are never skipped or repeated.
func (s *PG) runSub(ctx context.Context, sub *subscriber) {
	defer func() {
		s.hub.unregister(sub)
		close(sub.ch)
	}()
	for {
		evs, ok := sub.feed.after(sub.lastSeq)
		failed := false
		if !ok {
			var err error
			evs, err = s.Since(ctx, sub.roomID, sub.lastSeq, 1000)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				s.log.Warn("store: subscriber read", "room", sub.roomID, "err", err)
				failed = true
			}
		}
		for _, e := range evs {
			if e.Seq <= sub.lastSeq {
				continue
			}
			select {
			case sub.ch <- e:
				sub.lastSeq = e.Seq
			case <-ctx.Done():
				return
			}
		}
		if len(evs) > 0 {
			continue // there may be more
		}
		var timer <-chan time.Time
		if failed {
			timer = time.After(retryAfterError)
		}
		select {
		case <-sub.wake:
		case <-timer:
		case <-ctx.Done():
			return
		}
	}
}

// signal does a non-blocking send on a 1-slot wake channel.
func signal(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
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
