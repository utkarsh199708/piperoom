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
// connector_down also counts task restarts (FAILED -> RUNNING) and reports a
// task that restarts crashloop_restarts times in crashloop_window as
// crash-looping, even while it reads RUNNING. Loops faster than the interval
// stay invisible (crashloop.go).
//
// The consumer-group signals (F6): consumer_lag, group_unstable and
// offset_below_log_start read GroupLag per group (Deps.Lag) and the log-start
// offsets of the CDC topics (lag_signals.go).
//
// The drift signals (X1b): count_drift, row_drift, dup_keys and order_skew
// compare source and sink rows and scan the CDC topics every
// reconcile_interval through optional readers (drift.go).
package watch

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"math/rand/v2"
	"strings"
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

// EvidenceSink is an optional RoomSink extension (F6): a join that carries
// the watcher's evidence JSON beside the text, so the service can feed the
// numbers to the agent as an alert_update (agent.AlertEvidenceText). When the
// RoomSink implements it, the watcher calls it instead of PostEvidence for
// every post; evidence is nil for a post without numbers (the "all clear" note
// in a room the watcher only joined). Same errors as PostEvidence.
type EvidenceSink interface {
	PostEvidenceJSON(ctx context.Context, roomID, text string, evidence json.RawMessage) error
}

// PipelineClients are the clients of one pipeline's client set. Either may be
// nil, like Deps.Source and Deps.Connect.
type PipelineClients struct {
	// Set names the client set. Pipelines with the same Set must get the same
	// clients: they share one replication-slot query, one read per connector
	// name and one DLQ offsets call in a tick.
	Set     string
	Source  SourceDB
	Connect ConnectClient
	// Kafka reads DLQ offsets on the set's own cluster (nil: dlq_growth is
	// off for the set's pipelines, as with Deps.Kafka).
	Kafka clients.DLQReader
	// Registry reads subject versions and DDL reads the source's DDL audit
	// table for the change feed (nil: that kind of change is not recorded for
	// the set's pipelines, as with Deps.Registry and Deps.DDL).
	Registry clients.SchemaRegistryReader
	DDL      DDLReader
	// Lag reads consumer-group lag on the set's cluster (nil: the
	// consumer-group signals stay unknown, as with Deps.Lag).
	Lag LagReader
	// Warehouse is the set's sink warehouse (nil: count_drift and row_drift
	// are off for the set's pipelines, as with Deps.Warehouse).
	Warehouse clients.WarehouseReader
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
	Kafka clients.DLQReader
	// Changes, when set, turns on the change feed: every tick the watcher
	// records connector config and state changes, schema version changes and
	// DDL audit rows of each pipeline through it. Registry and DDL feed it;
	// both may be nil, and ClientsFor then gives them per client set.
	Changes  clients.ChangeWriter
	Registry clients.SchemaRegistryReader
	DDL      DDLReader
	// Lag reads consumer-group lag (wire it from the same Kafka client). Nil:
	// consumer_lag, group_unstable and offset_below_log_start stay unknown,
	// with one log line. offset_below_log_start also needs Kafka for the
	// log-start offsets.
	Lag LagReader
	// Warehouse is the sink warehouse client. The drift signals (X1b,
	// drift.go) find their optional readers on it and on Source and Kafka by
	// type assertion: clients.Reconciler on Source and Warehouse for
	// count_drift and row_drift, clients.DupScanner on Kafka (or Lag) for
	// dup_keys and order_skew. Nil, or a client without the reader, turns
	// those checks off for the pipeline with one INFO line.
	Warehouse clients.WarehouseReader
	// ClientsFor, when set, gives each pipeline the clients of its own client
	// set (docs/design/tenancy.md section 5) and Source, Connect and Kafka are
	// not used (nor Lag). Pipelines of one set share each read of a tick.
	ClientsFor func(pipelineID string) PipelineClients
	Owners     *owners.Owners
	// Rooms is required in mode "rooms"; modes "log" and "off" never call it.
	Rooms RoomSink
	// Config comes from config.ParseWatch.
	Config config.Watch
	// MaxOpenRooms is the most rooms this watcher keeps open at once. Wire it
	// from PIPEROOM_WATCH_MAX_OPEN_ROOMS (config.ParseWatchMaxOpenRooms), not
	// from PIPEROOM_MAX_AGENTS: a room beyond the agent pool still opens, gets
	// the triage pass and queues for an agent. Zero means 6.
	MaxOpenRooms int
	// Samples, when set, turns on the sample history and forecasts (X3a,
	// forecast.go): the room store's watch_samples. Forecast comes from
	// config.ParseWatchForecast; a zero Horizon means the default 48 h.
	Samples  SampleStore
	Forecast config.WatchForecast
	// Status, when set, gets one clients.WatchStatus per watched pipeline at
	// the end of every tick (X5a, status.go): the room store's watch_status,
	// which the fleet view reads on any replica.
	Status clients.WatchStatusWriter
	// ConfigVersions, when set, keeps every connector config the watcher
	// reads whose hash or keyed fingerprints changed (X5c,
	// config_versions.go): the room store's connector_config_versions.
	ConfigVersions clients.ConfigVersionWriter
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
	// defaultMaxOpenRooms matches config.DefaultWatchMaxOpenRooms.
	defaultMaxOpenRooms = 6
	// stormThreshold: this many pipelines on one signal in one tick = one room.
	stormThreshold = 3
)

// signalIDs lists the signals in a fixed order, so titles and logs are stable.
// New signals go at the end so existing titles keep their order; the
// forecasts (one id per metric, forecast.go) come last. config_drift has one
// id per connector (config_drift.go), after these (pipeState.ids).
var signalIDs = append([]string{owners.SignalConnectorDown, owners.SignalSlotInactive, owners.SignalSlotWAL,
	owners.SignalDLQGrowth, owners.SignalConfigRisk,
	owners.SignalConsumerLag, owners.SignalGroupUnstable, owners.SignalOffsetBelowLogStart, owners.SignalLongTx}, forecastIDs()...)

// DedupKey is the alert dedup key of a pipeline's watcher room: one open
// watcher room per pipeline.
func DedupKey(pipelineID string) string { return "watcher:" + pipelineID }

// pipeState is everything the watcher remembers about one pipeline.
type pipeState struct {
	sigs    map[string]*sigState
	samples []sample
	// slot is the slot name found for the pipeline (cached across ticks).
	slot string
	// ltx is the long_tx memory of the restart_lsn held now (long_tx.go).
	ltx *longTxMem
	// dlqFirst is, per DLQ topic, when the current growth episode was first
	// seen (the first rising sample). It is cleared when the signal quiets.
	dlqFirst map[string]time.Time
	// groups is the memory of each consumer group (lag samples, states).
	groups map[string]*groupMem
	// taskHist is each connector task's recent state changes (crash loops).
	taskHist map[string]*taskHist
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
	// bypassCooldown: a severity rise re-opened an acked incident
	// (reviveWorse); the next open skips the cooldown.
	bypassCooldown bool
	// reopenOf, when set, is when the room whose close acked the signals now
	// firing again was closed (rearmClosed). The next room, or join, says so.
	reopenOf time.Time
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
	// roomKey is the dedup key the watcher opened roomID under (DedupKey or a
	// ForecastDedupKey); "" for a room it only joined.
	roomKey string
	// lastSample is when each metric was last sampled (forecast.go).
	lastSample map[string]time.Time
	// fcClosed is, per forecast metric, when a room carrying its forecast
	// closed: the per-metric cooldown the watcher saw itself.
	fcClosed map[string]time.Time
	// drift is the memory of the drift checks (drift.go); driftClosed is,
	// per drift id, when a room carrying it closed: the (pipeline, table,
	// kind) cooldown.
	drift       *driftMem
	driftClosed map[string]time.Time
	// cfgBase is each connector's approved config (config_drift.go).
	cfgBase map[string]*driftBase
	// ret is the retention memory (retention.go); nil while config_risk is off.
	ret *retentionMem
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
	src        SourceDB
	conn       ConnectClient
	kafka      clients.DLQReader
	lag        LagReader
	wh         clients.WarehouseReader
	clientsFor func(pipelineID string) PipelineClients
	own        *owners.Owners
	rooms      RoomSink
	cfg        config.Watch
	maxRooms   int
	clock      Clock
	log        *slog.Logger
	jitter     func(time.Duration) time.Duration

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
	// dlqState is the sample ring of each DLQ topic, keyed by dlqStateKey (a
	// topic is cluster-wide, so pipelines of one client set that name the same
	// topic share one ring; another set's topic of that name is another ring).
	// dlqNoReaderLogged
	// keeps the "cannot read DLQ offsets" line to one.
	dlqState map[string]*dlqTopicState
	// dlqSeen is, per pipeline/connector, the DLQ setting last read from its
	// config; a connector that now answers 404 keeps its DLQ topic in the plan
	// for the ring window (see planDLQ).
	dlqSeen           map[string]dlqSeen
	dlqNoReaderLogged bool

	// Change feed (changes.go); chg is touched by the tick goroutine only.
	changes    clients.ChangeWriter
	registry   clients.SchemaRegistryReader
	ddl        DDLReader
	chg        map[string]*changeBase // pipeline -> baseline
	ddlOK      map[string]time.Time   // client set -> last tick whose DDL rows were all recorded
	ddlWarned  map[string]bool
	chgFailing bool
	// lagNoReaderLogged keeps the "no lag reader" line to one.
	lagNoReaderLogged bool
	// retNoReaderLogged keeps the "cannot read topic configs" line to one.
	retNoReaderLogged bool

	// Sample history and forecasts (forecast.go); nil samples = off.
	samples SampleStore
	horizon time.Duration

	// status receives the tick's status rows (status.go); nil = off.
	status clients.WatchStatusWriter
	// Connector config versions (config_versions.go); nil versions = off.
	// versionJobs are planned under mu by evaluate and written by the tick.
	versions    clients.ConfigVersionWriter
	versionJobs []versionJob

	// slotOff keeps the "slot signals off" INFO line of a MySQL pipeline to
	// one per pipeline (source_kind.go). Its own lock: targets runs outside mu.
	slotOffMu sync.Mutex
	slotOff   map[string]bool
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
		src: d.Source, conn: d.Connect, kafka: d.Kafka, lag: d.Lag, wh: d.Warehouse, clientsFor: d.ClientsFor, own: d.Owners, rooms: d.Rooms,
		cfg: cfg, maxRooms: d.MaxOpenRooms, clock: d.Clock, log: d.Log, jitter: d.Jitter,
		pipes: map[string]*pipeState{}, closed: map[string]bool{}, storms: map[string][]string{},
		dlqState: map[string]*dlqTopicState{}, dlqSeen: map[string]dlqSeen{},
		changes: d.Changes, registry: d.Registry, ddl: d.DDL,
		chg: map[string]*changeBase{}, ddlOK: map[string]time.Time{}, ddlWarned: map[string]bool{},
		samples: d.Samples, horizon: d.Forecast.Horizon, status: d.Status,
		versions: d.ConfigVersions,
		slotOff:  map[string]bool{},
	}
	if w.horizon <= 0 {
		w.horizon = 48 * time.Hour // config.DefaultWatchForecastHorizon
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
	w.recordChanges(ctx, now, ts, obs)
	if ctx.Err() != nil {
		return
	}
	// Before evaluate, which judges config_risk with these reads.
	w.readRetention(ctx, now, ts, obs)
	if ctx.Err() != nil {
		return
	}
	w.mu.Lock()
	fired := w.evaluate(now, ts, obs)
	jobs := w.planSamples(now, ts, obs)
	vjobs := w.versionJobs
	w.versionJobs = nil
	w.mu.Unlock()
	w.writeVersions(ctx, vjobs)
	if ctx.Err() != nil {
		return
	}
	w.writeSamples(ctx, now, jobs)
	if ctx.Err() != nil {
		return
	}
	w.checkDrift(ctx, now, ts, fired)
	if ctx.Err() != nil {
		return
	}
	if w.cfg.Mode == config.WatchRooms {
		w.emit(ctx, now, ts, fired)
	}
	if w.status != nil && ctx.Err() == nil {
		w.mu.Lock()
		rows := w.buildStatusesLocked(now, ts)
		w.mu.Unlock()
		w.writeStatuses(ctx, rows)
	}
	w.log.Debug("watch: tick done", "mode", string(w.cfg.Mode), "pipelines", len(ts),
		"took", w.clock.Now().Sub(now).String())
}

// OnRoomClosed tells the watcher that the driver closed a room. Wire it to the
// room service's room-closed hook. Signals that still fire in the pipeline
// become acked: the same incident does not open another room at once. The
// watcher stays quiet for one cooldown; a signal that still fires after it
// opens a new room that says it reopens after a close (rearmClosed). A signal
// that clears first is simply ok again. pipelineID and dedupKey are unused: the
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
//
// A room the watcher opened for forecasts alone (ForecastDedupKey) starts the
// cooldown of those forecasts only: a real signal later on the pipeline must
// still open a room (X3).
func (w *Watcher) ackLocked(pipelineID string, ps *pipeState, roomID string) {
	now := w.clock.Now()
	real := !strings.HasPrefix(ps.roomKey, ForecastDedupKey(pipelineID, ""))
	for id, ss := range ps.sigs {
		m := forecastMetric(id)
		if m == "" && ss.phase != phaseOK && ss.notified {
			real = true
		}
		if ss.phase == phaseFiring && id != retentionID {
			// A retention finding never had a room of its own to close.
			ss.ack(now)
			if driftKind(id) != "" {
				if ps.driftClosed == nil {
					ps.driftClosed = map[string]time.Time{}
				}
				ps.driftClosed[id] = now
			}
			if m != "" {
				if ps.fcClosed == nil {
					ps.fcClosed = map[string]time.Time{}
				}
				ps.fcClosed[m] = now
			}
			w.log.Info("watch: signal acked, room closed while it still fires",
				"mode", string(w.cfg.Mode), "pipeline", pipelineID, "signal", id, "room", roomID)
		}
	}
	ps.roomID, ps.owned, ps.follower, ps.roomKey = "", false, false, ""
	if real {
		ps.lastClosed = now
	}
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
