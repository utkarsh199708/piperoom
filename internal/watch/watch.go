// Package watch is the built-in watcher: one goroutine that polls the CDC
// source database and Kafka Connect, keeps a short memory of what it saw, and
// opens (or joins) a room when a pipeline is in trouble. Design:
// docs/design/watcher.md.
//
// v1 signals: connector_down, slot_inactive, slot_wal (with a growth rate and
// an ETA). Every check is a read; the watcher never fixes anything and never
// calls a model. Object names come only from owners.Pipeline.
//
// Wiring (a later PR) must: call Run in a goroutine after the room service has
// loaded; call OnRoomClosed from the service's room-closed hook; and give Deps a
// RoomSink that adapts room.Service.
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

// RoomSink is how the watcher reaches the room service. A small adapter around
// room.Service implements it.
type RoomSink interface {
	// OpenAlertRoom opens a room for a, or joins the open room that holds
	// a.DedupKey for the same pipeline (room.Service.OpenAlertRoom). created is
	// false when it joined. A resolved alert with no open room returns an
	// error, which the watcher logs and drops.
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
	PostEvidence(ctx context.Context, roomID, text string) error
}

// Deps are the watcher's dependencies.
type Deps struct {
	// Source and Connect may be nil: the signals that need them then stay
	// "unknown" and a warning names the missing client.
	Source  SourceDB
	Connect ConnectClient
	Owners  *owners.Owners
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

// signalIDs lists the v1 signals in a fixed order, so titles and logs are stable.
var signalIDs = []string{owners.SignalConnectorDown, owners.SignalSlotInactive, owners.SignalSlotWAL}

// DedupKey is the alert dedup key of a pipeline's watcher room: one open
// watcher room per pipeline.
func DedupKey(pipelineID string) string { return "watcher:" + pipelineID }

// pipeState is everything the watcher remembers about one pipeline.
type pipeState struct {
	sigs    map[string]*sigState
	samples []sample
	// slot is the slot name found for the pipeline (cached across ticks).
	slot string
	// roomID is the room the watcher posts to now; owned is true when the
	// watcher opened it (the dedup key is ours), false when it joined a room
	// someone else opened.
	roomID string
	owned  bool
	// episode: a signal fired and the pipeline has not cleared yet.
	episode bool
	// lastPost is when the watcher last wrote to the room; postedSev the
	// severity at that time.
	lastPost  time.Time
	postedSev string
	// lastStill is the last "still firing" log line.
	lastStill time.Time
	// warned* keep "skipped" lines to one per episode.
	warnedCooldown, warnedMax bool
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
	own      *owners.Owners
	rooms    RoomSink
	cfg      config.Watch
	maxRooms int
	clock    Clock
	log      *slog.Logger
	jitter   func(time.Duration) time.Duration

	mu    sync.Mutex
	pipes map[string]*pipeState
	// closedEarly holds room ids reported closed before the watcher recorded
	// them (the room closed while OpenAlertRoom was still returning).
	closedEarly map[string]bool
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
		src: d.Source, conn: d.Connect, own: d.Owners, rooms: d.Rooms,
		cfg: cfg, maxRooms: d.MaxOpenRooms, clock: d.Clock, log: d.Log, jitter: d.Jitter,
		pipes: map[string]*pipeState{}, closedEarly: map[string]bool{},
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
// stays quiet until the signal clears. dedupKey is accepted in either shape,
// "watcher:<pipeline>" or the service's "<pipeline>|watcher:<pipeline>".
func (w *Watcher) OnRoomClosed(roomID, pipelineID, dedupKey string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	keyed := dedupKey == DedupKey(pipelineID) || dedupKey == pipelineID+"|"+DedupKey(pipelineID)
	ps := w.pipes[pipelineID]
	switch {
	case ps != nil && roomID != "" && ps.roomID == roomID:
		w.ackLocked(pipelineID, ps, roomID)
	case keyed:
		// Our room may still be on its way back from OpenAlertRoom; remember it.
		if len(w.closedEarly) > 64 {
			clear(w.closedEarly)
		}
		w.closedEarly[roomID] = true
	}
}

// ackLocked turns firing signals into acked and forgets the room.
func (w *Watcher) ackLocked(pipelineID string, ps *pipeState, roomID string) {
	for id, ss := range ps.sigs {
		if ss.phase == phaseFiring {
			ss.phase = phaseAcked
			w.log.Info("watch: signal acked, room closed while it still fires",
				"mode", string(w.cfg.Mode), "pipeline", pipelineID, "signal", id, "room", roomID)
		}
	}
	ps.roomID, ps.owned = "", false
	ps.warnedCooldown, ps.warnedMax = false, false
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
