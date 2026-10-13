// Package watch is the built-in watcher: one goroutine that polls the CDC
// source database and Kafka Connect, keeps a short memory of what it saw, and
// opens (or joins) a room when a pipeline is in trouble. Design:
// docs/design/watcher.md.
//
// v1 signals: connector_down, slot_inactive, slot_wal (with a growth rate and
// an ETA). dlq_growth and config_risk come from docs/design/dlq.md: the first
// counts new records in dead-letter topics from offsets alone, the second
// reads connector configs for settings that drop records silently. Every
// check is a read; the watcher never fixes anything and never
// calls a model. Object names come only from owners.Pipeline.
//
// Wiring: see cmd/server/watch.go. It calls Run in a goroutine after the room
// service has loaded, calls OnRoomClosed from the service's room-closed hook, and
// gives Deps a RoomSink that adapts room.Service (mapping its "room closed" and
// "resolved alert, no room" errors to ErrRoomClosed and ErrNoRoom).
//
// connector_down also covers a connector that Kafka Connect does not know
// (HTTP 404): it fires as "missing" at sev2.
//
// Known gap (v2): a connector that crash-loops faster than `for` (FAILED for a
// few seconds, then RUNNING again, repeatedly) is seen as RUNNING most ticks and
// never fires. v2 needs a restart counter or a flap rule.
package watch

import (
	"context"
	"errors"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	"piperoom/internal/alertin"
	"piperoom/internal/clients"
	"piperoom/internal/config"
	"piperoom/internal/owners"
)

// SourceDB is the part of clients.SourceDB the watcher uses. v1 reads only the
// replication slots, once per tick.
type SourceDB interface {
	ReplicationSlots(ctx context.Context) ([]clients.ReplicationSlot, error)
}

// ConnectClient is the part of clients.ConnectClient the watcher uses. Both
// calls are GETs.
type ConnectClient interface {
	Status(ctx context.Context, name string) (clients.ConnectorStatus, error)
	Config(ctx context.Context, name string) (map[string]string, error)
}

// Errors a RoomSink returns (wrapped is fine; the watcher uses errors.Is). Any
// other error is treated as transient and retried on the next tick.
var (
	// ErrRoomClosed: the room exists but is closed (or is gone). The watcher
	// then acks the pipeline, as OnRoomClosed would, so a missed close event
	// cannot leave it posting to a closed room.
	ErrRoomClosed = errors.New("watch: room is closed")
	// ErrNoRoom: a resolved alert matched no open room (room.ErrResolvedAlertNoRoom).
	// Nothing is left to resolve; the watcher stops retrying.
	ErrNoRoom = errors.New("watch: no open room for the resolved alert")
)

// RoomSink is how the watcher reaches the room service. A small adapter around
// room.Service implements it.
type RoomSink interface {
	// OpenAlertRoom opens a room for a, or joins the open room that holds
	// a.DedupKey for the same pipeline (room.Service.OpenAlertRoom). created is
	// false when it joined. A resolved alert with no open room returns ErrNoRoom.
	OpenAlertRoom(ctx context.Context, a alertin.Alert) (roomID string, created bool, err error)
	// OpenRoomForPipeline reports an open room for the pipeline, whoever
	// opened it (Alertmanager, a person). The watcher then joins it instead of
	// opening a second room.
	OpenRoomForPipeline(pipelineID string) (roomID string, ok bool)
	// LastRoomForKey finds the newest room opened under the alert dedup key for
	// the pipeline. closedAt is nil while it is still open.
	LastRoomForKey(pipelineID, dedupKey string) (roomID string, closedAt *time.Time, ok bool)
	// PostEvidence adds a system message to an open room. It is the join: the
	// watcher uses it for later signals, "still firing" and for a room it did
	// not open. The text is built from our own numbers, no usernames, no queries.
	// It returns ErrRoomClosed when the room is closed.
	PostEvidence(ctx context.Context, roomID, text string) error
}

// Deps are the watcher's dependencies.
type Deps struct {
	// Source and Connect may be nil: the signals that need them then stay
	// "unknown" and a warning names the missing client.
	Source  SourceDB
	Connect ConnectClient
	// Kafka reads the start and end offsets of dead-letter topics. It is nil
	// when the Kafka client cannot (wire it with
	// `deps.Kafka, _ = admin.(clients.DLQReader)`): dlq_growth is then off
	// with one log line, and config_risk cannot tell a missing DLQ topic.
	Kafka  clients.DLQReader
	Owners *owners.Owners
	// Rooms is required in mode "rooms"; modes "log" and "off" never call it.
	Rooms RoomSink
	// Config comes from config.ParseWatch.
	Config config.Watch
	// MaxOpenRooms is max_open_watcher_rooms: the most rooms this watcher keeps
	// open at once. Wire it to PIPEROOM_MAX_AGENTS. Zero means 3.
	MaxOpenRooms int
	// Clock defaults to the wall clock.
	Clock Clock
	// Log defaults to slog.Default().
	Log *slog.Logger
	// Jitter returns a random offset in [-max, +max] added to each interval.
	// Defaults to math/rand/v2; tests set it to a constant.
	Jitter func(max time.Duration) time.Duration
}

const (
	// stillFiringEvery is the least time between two "still firing" joins.
	stillFiringEvery = 15 * time.Minute
	// overrunMargin: a tick that ends less than this before the next one is
	// due has overrun (design §3: interval − 5 s).
	overrunMargin = 5 * time.Second
	// defaultMaxOpenRooms matches PIPEROOM_MAX_AGENTS.
	defaultMaxOpenRooms = 3
	// stormThreshold: this many pipelines on one signal in one tick = one room.
	stormThreshold = 3
)

// signalIDs lists the signals in a fixed order, so titles and logs are stable.
// New signals go at the end so existing titles keep their order.
var signalIDs = []string{owners.SignalConnectorDown, owners.SignalSlotInactive, owners.SignalSlotWAL,
	owners.SignalDLQGrowth, owners.SignalConfigRisk}

// DedupKey is the alert dedup key of a pipeline's watcher room: one open
// watcher room per pipeline.
func DedupKey(pipelineID string) string { return "watcher:" + pipelineID }

// pipeState is everything the watcher remembers about one pipeline.
type pipeState struct {
	sigs    map[string]*sigState
	samples []sample
	// slot is the slot name found for the pipeline (cached across ticks).
	slot string
	// dlqFirst is, per DLQ topic, when the current growth episode was first
	// seen (the first rising sample). It is cleared when the signal quiets.
	dlqFirst map[string]time.Time
	// risks are the current config findings of the pipeline (codes only), added
	// to the evidence of whatever signal opens a room.
	risks []map[string]any
	// roomID is the room the watcher posts to now; owned is true when the
	// watcher opened it (the dedup key is ours), false when it joined a room
	// someone else opened.
	roomID string
	owned  bool
	// follower: the pipeline took part in a storm and posts into the leader's
	// room. Its updates and its clear land there; it does not repeat "still
	// firing" (the leader does).
	follower bool
	// episode: a signal fired and the pipeline has not cleared yet.
	episode bool
	// reopened: a rise to sev1 re-opened an acked incident once in this
	// episode; bypassCooldown lets that one open skip the cooldown.
	reopened, bypassCooldown bool
	// lastPost is when the watcher last wrote to the room; postedSev the
	// severity at that time.
	lastPost  time.Time
	postedSev string
	// lastStill is the last "still firing" log line.
	lastStill time.Time
	// lastClosed is when a room this pipeline posted into was closed (set by
	// ackLocked, in memory). A storm follower never had a room under its own
	// dedup key, so the room service cannot tell its cooldown; this can.
	lastClosed time.Time
	// warned* keep "skipped" lines to one per episode.
	warnedCooldown, warnedMax, warnedOpen, warnedResolve bool
}

func (ps *pipeState) sig(id string) *sigState {
	s := ps.sigs[id]
	if s == nil {
		s = &sigState{}
		ps.sigs[id] = s
	}
	return s
}

// Watcher is the built-in watcher.
type Watcher struct {
	src      SourceDB
	conn     ConnectClient
	kafka    clients.DLQReader
	own      *owners.Owners
	rooms    RoomSink
	cfg      config.Watch
	maxRooms int
	clock    Clock
	log      *slog.Logger
	jitter   func(time.Duration) time.Duration

	mu    sync.Mutex
	pipes map[string]*pipeState
	// closed remembers the rooms OnRoomClosed reported (newest last, capped).
	// recordPost checks it, so a room that closed while a post or an open was
	// still in flight is not set as the pipeline's room again.
	closed      map[string]bool
	closedOrder []string
	// storms carries a storm over to the next tick when its leader could not
	// get a room: signal id -> member pipelines still waiting.
	storms map[string][]string
	// dlqState is the sample ring of each DLQ topic (topics are cluster-wide,
	// so pipelines that name the same topic share one ring). dlqNoReaderLogged
	// keeps the "cannot read DLQ offsets" line to one.
	dlqState          map[string]*dlqTopicState
	dlqNoReaderLogged bool
}

const maxClosedRemembered = 256

// markClosedLocked remembers that roomID was closed.
func (w *Watcher) markClosedLocked(roomID string) {
	if roomID == "" || w.closed[roomID] {
		return
	}
	w.closed[roomID] = true
	w.closedOrder = append(w.closedOrder, roomID)
	if len(w.closedOrder) > maxClosedRemembered {
		delete(w.closed, w.closedOrder[0])
		w.closedOrder = w.closedOrder[1:]
	}
}

// New checks deps and returns a watcher. It starts nothing; call Run.
func New(d Deps) (*Watcher, error) {
	if d.Owners == nil {
		return nil, errors.New("watch: Deps.Owners is required")
	}
	if d.Config.Mode == config.WatchRooms && d.Rooms == nil {
		return nil, errors.New("watch: Deps.Rooms is required in mode rooms")
	}
	cfg := d.Config
	if cfg.Mode == "" {
		cfg.Mode = config.WatchLog
	}
	if cfg.Interval <= 0 {
		cfg.Interval = 60 * time.Second
	}
	if cfg.CheckTimeout <= 0 {
		cfg.CheckTimeout = 10 * time.Second
	}
	if cfg.Cooldown <= 0 {
		cfg.Cooldown = 60 * time.Minute
	}
	w := &Watcher{
		src: d.Source, conn: d.Connect, kafka: d.Kafka, own: d.Owners, rooms: d.Rooms,
		cfg: cfg, maxRooms: d.MaxOpenRooms, clock: d.Clock, log: d.Log, jitter: d.Jitter,
		pipes: map[string]*pipeState{}, closed: map[string]bool{}, storms: map[string][]string{},
		dlqState: map[string]*dlqTopicState{},
	}
	if w.maxRooms <= 0 {
		w.maxRooms = defaultMaxOpenRooms
	}
	if w.clock == nil {
		w.clock = RealClock()
	}
	if w.log == nil {
		w.log = slog.Default()
	}
	if w.jitter == nil {
		w.jitter = randomJitter
	}
	return w, nil
}

// randomJitter returns a value in [-max, +max].
func randomJitter(max time.Duration) time.Duration {
	if max <= 0 {
		return 0
	}
	return rand.N(2*max+1) - max
}

// pipe returns the state of a pipeline. Caller holds w.mu.
func (w *Watcher) pipe(id string) *pipeState {
	ps := w.pipes[id]
	if ps == nil {
		ps = &pipeState{sigs: map[string]*sigState{}}
		w.pipes[id] = ps
	}
	return ps
}

// Run polls until ctx is cancelled and returns nil then, within a moment: every
// call it makes is bound to ctx. It is one goroutine, so ticks never overlap.
// Mode "off" returns at once.
func (w *Watcher) Run(ctx context.Context) error {
	if w.cfg.Mode == config.WatchOff {
		w.log.Info("watch: mode off, watcher not started")
		return nil
	}
	w.log.Info("watch: started", "mode", string(w.cfg.Mode), "interval", w.cfg.Interval.String(),
		"check_timeout", w.cfg.CheckTimeout.String(), "cooldown", w.cfg.Cooldown.String())
	timer := w.clock.NewTimer(0) // the first tick is at once
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-timer.C():
		}
		start := w.clock.Now()
		w.tick(ctx)
		if ctx.Err() != nil {
			return nil
		}
		timer.Reset(w.nextDelay(start, w.clock.Now()))
	}
}

// nextDelay is the wait after a tick that started at start and ended at end:
// the interval ± jitter counted from the start. When the tick overran, so that
// the next one would be due within overrunMargin (or already is), that slot is
// skipped with a warning and the one after it is used.
func (w *Watcher) nextDelay(start, end time.Time) time.Duration {
	iv := w.cfg.Interval
	margin := min(overrunMargin, iv/4)
	next := start.Add(iv + w.jitter(iv/10))
	for next.Sub(end) < margin {
		w.log.Warn("watch: tick overran, skipping the next one",
			"took", end.Sub(start).String(), "interval", iv.String())
		next = next.Add(iv)
	}
	return next.Sub(end)
}

// tick is one pass: observe, update the state machines, then act.
func (w *Watcher) tick(ctx context.Context) {
	ts := w.targets(ctx)
	if len(ts) == 0 {
		w.log.Debug("watch: nothing to watch")
		return
	}
	obs := w.observe(ctx, ts)
	if ctx.Err() != nil {
		return // cancelled: errors from cancelled calls say nothing about the pipeline
	}
	now := w.clock.Now()
	w.mu.Lock()
	fired := w.evaluate(now, ts, obs)
	w.mu.Unlock()
	if w.cfg.Mode == config.WatchRooms {
		w.emit(ctx, now, ts, fired)
	}
	w.log.Debug("watch: tick done", "mode", string(w.cfg.Mode), "pipelines", len(ts),
		"took", w.clock.Now().Sub(now).String())
}

// OnRoomClosed tells the watcher that the driver closed a room. Wire it to the
// room service's room-closed hook. Signals that still fire in the pipeline
// become acked: the same incident does not open another room, and the watcher
// stays quiet until the signal clears. pipelineID and dedupKey are unused: the
// room is found by roomID, so a storm room acks every pipeline that posts into
// it, whatever key it was opened under.
func (w *Watcher) OnRoomClosed(roomID, pipelineID, dedupKey string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if roomID == "" {
		return
	}
	// Remember every close, whatever the key: a post or an open may still be on
	// its way back, and recordPost must not take a closed room as its own.
	w.markClosedLocked(roomID)
	// Every pipeline that posts into this room is acked: the owner, and the
	// followers of a storm room.
	for id, ps := range w.pipes {
		if ps.roomID == roomID {
			w.ackLocked(id, ps, roomID)
		}
	}
}

// ackLocked turns firing signals into acked and forgets the room.
func (w *Watcher) ackLocked(pipelineID string, ps *pipeState, roomID string) {
	for id, ss := range ps.sigs {
		if ss.phase == phaseFiring {
			ss.phase = phaseAcked
			ss.ackSev = ss.last.sev
			w.log.Info("watch: signal acked, room closed while it still fires",
				"mode", string(w.cfg.Mode), "pipeline", pipelineID, "signal", id, "room", roomID)
		}
	}
	ps.roomID, ps.owned, ps.follower = "", false, false
	ps.lastClosed = w.clock.Now()
	ps.warnedCooldown, ps.warnedMax, ps.warnedOpen, ps.warnedResolve = false, false, false, false
}

// Targets returns how many pipelines the watcher looks at: enabled, with at
// least one v1 signal on, and in scope. It is the same list a tick works on.
func (w *Watcher) Targets() int {
	return len(w.targets(context.Background()))
}

// openOwned counts the rooms this watcher opened and believes are open.
func (w *Watcher) openOwned() int {
	n := 0
	for _, ps := range w.pipes {
		if ps.owned && ps.roomID != "" {
			n++
		}
	}
	return n
}
