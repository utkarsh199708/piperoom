package evals

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sort"
	"sync"
	"time"

	"gopkg.in/yaml.v3"

	"piperoom/internal/alertin"
	"piperoom/internal/clients"
	"piperoom/internal/config"
	"piperoom/internal/owners"
	"piperoom/internal/room"
	"piperoom/internal/watch"
)

// How a scenario's room opens (Start.By).
const (
	// StartAlert: an alert webhook opens the room with alert.title (default).
	StartAlert = "alert"
	// StartWatcher: the built-in watcher (internal/watch) sees the pipeline
	// through Start.Watcher's reads and opens the room itself, with its own
	// title and evidence, exactly as in cmd/server with
	// PIPEROOM_WATCH_MODE=rooms.
	StartWatcher = "watcher"
)

// Start says what opens the room.
type Start struct {
	By      string        `yaml:"by"`
	Watcher *WatcherStart `yaml:"watcher"`
}

// WatcherStart is what the watcher's own reads return (they are not the
// agent's tools: the watcher calls the clients directly), and which signals
// the room must open on.
type WatcherStart struct {
	// Connectors are Kafka Connect status answers, one per pipeline connector.
	Connectors []WatchConnector `yaml:"connectors"`
	// Configs are connector configs (Connect's GET /connectors/{name}/config).
	// The watcher reads slot.name and the heartbeat setting from them.
	Configs map[string]map[string]string `yaml:"configs"`
	// Slots are the pg_replication_slots rows.
	Slots []WatchSlot `yaml:"slots"`
	// Signals are the watcher signals the opening evidence must hold, e.g.
	// [connector_down]. A run whose room opens on other signals is a run
	// error: the scenario would be measuring another incident.
	Signals []string `yaml:"signals"`
}

// WatchConnector is a clients.ConnectorStatus in YAML.
type WatchConnector struct {
	Name  string      `yaml:"name"`
	Type  string      `yaml:"type"`
	State string      `yaml:"state"`
	Tasks []WatchTask `yaml:"tasks"`
}

// WatchTask is a clients.TaskStatus in YAML.
type WatchTask struct {
	ID    int    `yaml:"id"`
	State string `yaml:"state"`
	Trace string `yaml:"trace"`
}

// WatchSlot is a clients.ReplicationSlot in YAML.
type WatchSlot struct {
	Name             string `yaml:"name"`
	Plugin           string `yaml:"plugin"`
	Active           bool   `yaml:"active"`
	RetainedWALBytes int64  `yaml:"retained_wal_bytes"`
	WALStatus        string `yaml:"wal_status"`
	SafeWALBytes     *int64 `yaml:"safe_wal_bytes"`
}

// entry is how the scenario's room opens.
func (s Scenario) entry() string {
	if s.Start.By == StartWatcher {
		return StartWatcher
	}
	return StartAlert
}

// pipelineNode is the scenario's pipeline as YAML, for the run's owners
// file. A parsed scenario keeps the node it was written as, so a field the
// owners loader knows and Pipeline round-trips badly (watch:, owners:) stays
// as written.
func (s Scenario) pipelineNode() (*yaml.Node, error) {
	if s.pipelineYAML != nil {
		return s.pipelineYAML, nil
	}
	var n yaml.Node
	if err := n.Encode(s.Pipeline); err != nil {
		return nil, fmt.Errorf("evals: scenario %q: encode pipeline: %w", s.Name, err)
	}
	// Zero fields encode as empty values ("connections: {}"), which the
	// owners loader refuses; a hand-written pipeline would leave them out.
	if n.Kind == yaml.MappingNode {
		kept := n.Content[:0]
		for i := 0; i+1 < len(n.Content); i += 2 {
			if !emptyNode(n.Content[i+1]) {
				kept = append(kept, n.Content[i], n.Content[i+1])
			}
		}
		n.Content = kept
	}
	return &n, nil
}

func emptyNode(v *yaml.Node) bool {
	switch v.Kind {
	case yaml.MappingNode, yaml.SequenceNode:
		return len(v.Content) == 0
	case yaml.ScalarNode:
		return v.Tag == "!!null" || v.Value == "" || (v.Tag == "!!bool" && v.Value == "false")
	}
	return false
}

// validateStart checks the start block. Every scenario also runs through
// owners.Load on the room path; that check is in Validate.
func (s Scenario) validateStart() []error {
	var errs []error
	switch s.Start.By {
	case "", StartAlert:
		if s.Start.Watcher != nil {
			errs = append(errs, errors.New("start.watcher: only with start.by: watcher"))
		}
		return errs
	case StartWatcher:
	default:
		return []error{fmt.Errorf("start.by: unknown %q (want %s or %s)", s.Start.By, StartAlert, StartWatcher)}
	}
	if s.Alert.Title != "" {
		errs = append(errs, errors.New("alert.title: the watcher writes the title; leave it out with start.by: watcher"))
	}
	w := s.Start.Watcher
	if w == nil {
		return append(errs, errors.New("start.watcher: required with start.by: watcher"))
	}
	if len(w.Signals) == 0 {
		errs = append(errs, errors.New("start.watcher.signals: name the signals the room must open on"))
	}
	known := []string{owners.SignalConnectorDown, owners.SignalSlotInactive, owners.SignalSlotWAL}
	for _, sig := range w.Signals {
		if !slices.Contains(known, sig) {
			errs = append(errs, fmt.Errorf("start.watcher.signals: %q is not a signal the eval watcher can see (want one of %v; it has no Kafka client)", sig, known))
		}
	}
	byName := map[string]bool{}
	for i, c := range w.Connectors {
		if c.Name == "" || c.State == "" {
			errs = append(errs, fmt.Errorf("start.watcher.connectors[%d]: name and state are required", i))
		}
		byName[c.Name] = true
	}
	for _, name := range s.Pipeline.Connectors {
		if !byName[name] {
			errs = append(errs, fmt.Errorf("start.watcher.connectors: no status for pipeline connector %q (the watcher reads every one)", name))
		}
	}
	return errs
}

// watcherClients answer the watcher's reads from the scenario. They do no
// I/O and never write.
type watcherClients struct{ w *WatcherStart }

func (c watcherClients) ReplicationSlots(ctx context.Context) ([]clients.ReplicationSlot, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	out := make([]clients.ReplicationSlot, 0, len(c.w.Slots))
	for _, s := range c.w.Slots {
		out = append(out, clients.ReplicationSlot{
			Name: s.Name, Plugin: s.Plugin, Active: s.Active, RetainedWALBytes: s.RetainedWALBytes,
			WALStatus: s.WALStatus, SafeWALBytes: s.SafeWALBytes,
		})
	}
	return out, nil
}

func (c watcherClients) Status(ctx context.Context, name string) (clients.ConnectorStatus, error) {
	if err := ctx.Err(); err != nil {
		return clients.ConnectorStatus{}, err
	}
	for _, wc := range c.w.Connectors {
		if wc.Name != name {
			continue
		}
		st := clients.ConnectorStatus{Name: wc.Name, Type: wc.Type, State: wc.State}
		for _, t := range wc.Tasks {
			st.Tasks = append(st.Tasks, clients.TaskStatus{ID: t.ID, State: t.State, Trace: t.Trace})
		}
		return st, nil
	}
	return clients.ConnectorStatus{}, fmt.Errorf("evals: connector %q: %w", name, clients.ErrNotFound)
}

func (c watcherClients) Config(ctx context.Context, name string) (map[string]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if cfg, ok := c.w.Configs[name]; ok {
		return cfg, nil
	}
	return map[string]string{}, nil
}

// watchRooms is the watcher's RoomSink over the room service, the adapter
// cmd/server/watch.go has, plus a note of the room it opened.
type watchRooms struct {
	svc *room.Service

	mu     sync.Mutex
	opened chan struct{}
	roomID string
	alert  alertin.Alert
}

func (r *watchRooms) OpenAlertRoom(ctx context.Context, a alertin.Alert) (string, bool, error) {
	snap, created, err := r.svc.OpenAlertRoom(ctx, a)
	if err != nil {
		if errors.Is(err, room.ErrResolvedAlertNoRoom) {
			return "", false, fmt.Errorf("%w: %w", watch.ErrNoRoom, err)
		}
		return "", false, err
	}
	r.mu.Lock()
	if created && r.roomID == "" {
		r.roomID, r.alert = snap.ID, a
		close(r.opened)
	}
	r.mu.Unlock()
	return snap.ID, created, nil
}

func (r *watchRooms) OpenRoomForPipeline(pipelineID string) (string, bool) {
	return r.svc.OpenRoomForPipeline(pipelineID)
}

func (r *watchRooms) LastRoomForKey(pipelineID, dedupKey string) (string, *time.Time, bool) {
	return r.svc.LastRoomForKey(pipelineID, dedupKey)
}

func (r *watchRooms) PostEvidence(ctx context.Context, roomID, text string) error {
	return r.svc.PostRoomNote(ctx, roomID, text)
}

// maxWatcherTicks bounds the ticks the eval waits for the watcher to open a
// room: far more than the longest default "for" (10 min) at the 15 s
// minimum interval.
const maxWatcherTicks = 200

// watcherInterval is the watch interval of an eval run: the shortest the
// server allows.
const watcherInterval = 15 * time.Second

// openByWatcher runs the real watcher, in mode rooms, on a fake clock over
// the scenario's reads, until it opens a room; then stops it. Time runs only
// as fast as the watcher's ticks, so a "for: 3m" costs no wall time.
func openByWatcher(ctx context.Context, sc Scenario, svc *room.Service, own *owners.Owners, log *slog.Logger) (string, error) {
	clk := newEvalClock(time.Date(2026, 10, 11, 9, 41, 0, 0, time.UTC))
	sink := &watchRooms{svc: svc, opened: make(chan struct{})}
	cl := watcherClients{w: sc.Start.Watcher}
	w, err := watch.New(watch.Deps{
		Source: cl, Connect: cl, Owners: own, Rooms: sink,
		Config: config.Watch{Mode: config.WatchRooms, Interval: watcherInterval, CheckTimeout: 10 * time.Second},
		Clock:  clk, Log: log, Jitter: func(time.Duration) time.Duration { return 0 },
	})
	if err != nil {
		return "", fmt.Errorf("evals: watcher: %w", err)
	}
	wctx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- w.Run(wctx) }()
	defer func() {
		stop()
		<-done
	}()
	for tick := 0; tick < maxWatcherTicks; tick++ {
		// Wait for the tick to finish: the watcher re-arms its timer, opens
		// the room, or stops.
		deadline, err := clk.waitArmed(ctx, sink.opened, done)
		if err != nil {
			return "", err
		}
		if deadline.IsZero() {
			break
		}
		clk.advanceTo(deadline)
	}
	sink.mu.Lock()
	roomID, alert := sink.roomID, sink.alert
	sink.mu.Unlock()
	if roomID == "" {
		return "", fmt.Errorf("evals: the watcher did not open a room within %d ticks (%s of watcher time)", maxWatcherTicks, maxWatcherTicks*watcherInterval)
	}
	got, err := evidenceSignals(alert.Raw)
	if err != nil {
		return "", err
	}
	want := slices.Clone(sc.Start.Watcher.Signals)
	sort.Strings(want)
	if !slices.Equal(got, want) {
		return roomID, fmt.Errorf("evals: the watcher opened the room on signals %v, the scenario expects %v", got, want)
	}
	return roomID, nil
}

// evidenceSignals lists the signal ids of the watcher's evidence, sorted.
func evidenceSignals(raw json.RawMessage) ([]string, error) {
	var ev struct {
		Signals []struct {
			Signal string `json:"signal"`
		} `json:"signals"`
	}
	if err := json.Unmarshal(raw, &ev); err != nil {
		return nil, fmt.Errorf("evals: read the watcher's evidence: %w", err)
	}
	var out []string
	for _, s := range ev.Signals {
		out = append(out, s.Signal)
	}
	sort.Strings(out)
	return out, nil
}

// evalClock is a watch.Clock that moves only when the harness advances it.
type evalClock struct {
	mu     sync.Mutex
	now    time.Time
	timers []*evalTimer
}

type evalTimer struct {
	c     *evalClock
	ch    chan time.Time
	at    time.Time
	armed bool
}

func newEvalClock(t time.Time) *evalClock { return &evalClock{now: t} }

func (c *evalClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *evalClock) NewTimer(d time.Duration) watch.Timer {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := &evalTimer{c: c, ch: make(chan time.Time, 1), at: c.now.Add(d), armed: true}
	c.timers = append(c.timers, t)
	c.fireLocked()
	return t
}

func (c *evalClock) fireLocked() {
	for _, t := range c.timers {
		if t.armed && !t.at.After(c.now) {
			t.armed = false
			select {
			case t.ch <- c.now:
			default:
			}
		}
	}
}

func (c *evalClock) advanceTo(at time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if at.After(c.now) {
		c.now = at
	}
	c.fireLocked()
}

// armedAt is the deadline of an armed timer that has not fired.
func (c *evalClock) armedAt() (time.Time, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, t := range c.timers {
		if t.armed {
			return t.at, true
		}
	}
	return time.Time{}, false
}

// waitArmed waits until a timer is armed (the tick is over) and returns its
// deadline; a zero time when the room opened or the watcher stopped.
func (c *evalClock) waitArmed(ctx context.Context, opened <-chan struct{}, done <-chan error) (time.Time, error) {
	for {
		select {
		case <-opened:
			return time.Time{}, nil
		case err := <-done:
			if err != nil {
				return time.Time{}, fmt.Errorf("evals: watcher: %w", err)
			}
			return time.Time{}, errors.New("evals: the watcher stopped before it opened a room")
		case <-ctx.Done():
			return time.Time{}, ctx.Err()
		default:
		}
		if at, ok := c.armedAt(); ok {
			return at, nil
		}
		select {
		case <-ctx.Done():
			return time.Time{}, ctx.Err()
		case <-time.After(time.Millisecond):
		}
	}
}

func (t *evalTimer) C() <-chan time.Time { return t.ch }

func (t *evalTimer) Reset(d time.Duration) {
	t.c.mu.Lock()
	defer t.c.mu.Unlock()
	t.at, t.armed = t.c.now.Add(d), true
	t.c.fireLocked()
}

func (t *evalTimer) Stop() {
	t.c.mu.Lock()
	t.armed = false
	t.c.mu.Unlock()
}
