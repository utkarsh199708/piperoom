package watch

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"piperoom/internal/alertin"
	"piperoom/internal/clients"
	"piperoom/internal/config"
	"piperoom/internal/owners"
)

var t0 = time.Date(2026, 10, 13, 10, 0, 0, 0, time.UTC)

// ---- fake clock ----

type fakeClock struct {
	mu     sync.Mutex
	now    time.Time
	timers []*fakeTimer
}

func newFakeClock() *fakeClock { return &fakeClock{now: t0} }

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// Advance moves time forward and fires the timers that are due.
func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
	c.fireLocked()
}

func (c *fakeClock) fireLocked() {
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

func (c *fakeClock) NewTimer(d time.Duration) Timer {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := &fakeTimer{c: c, ch: make(chan time.Time, 1), at: c.now.Add(d), armed: true}
	c.timers = append(c.timers, t)
	c.fireLocked()
	return t
}

// nextDeadline is the deadline of the armed timer; ok is false when none is armed.
func (c *fakeClock) nextDeadline() (time.Time, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, t := range c.timers {
		if t.armed {
			return t.at, true
		}
	}
	return time.Time{}, false
}

type fakeTimer struct {
	c     *fakeClock
	ch    chan time.Time
	at    time.Time
	armed bool
}

func (t *fakeTimer) C() <-chan time.Time { return t.ch }

func (t *fakeTimer) Reset(d time.Duration) {
	t.c.mu.Lock()
	defer t.c.mu.Unlock()
	t.at, t.armed = t.c.now.Add(d), true
	t.c.fireLocked()
}

func (t *fakeTimer) Stop() {
	t.c.mu.Lock()
	defer t.c.mu.Unlock()
	t.armed = false
}

// ---- fake source ----

type fakeSource struct {
	mu    sync.Mutex
	rows  []clients.ReplicationSlot
	err   error
	calls int
	// hook runs inside ReplicationSlots before it answers; it may block.
	hook func(ctx context.Context)
}

func (s *fakeSource) ReplicationSlots(ctx context.Context) ([]clients.ReplicationSlot, error) {
	s.mu.Lock()
	s.calls++
	hook, err := s.hook, s.err
	rows := slices.Clone(s.rows)
	s.mu.Unlock()
	if hook != nil {
		hook(ctx)
	}
	if err != nil {
		return nil, err
	}
	return rows, nil
}

func (s *fakeSource) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

// update changes the row of one slot.
func (s *fakeSource) update(slot string, f func(*clients.ReplicationSlot)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.rows {
		if s.rows[i].Name == slot {
			f(&s.rows[i])
			return
		}
	}
	r := clients.ReplicationSlot{Name: slot}
	f(&r)
	s.rows = append(s.rows, r)
}

func (s *fakeSource) setRetained(slot string, n int64) {
	s.update(slot, func(r *clients.ReplicationSlot) { r.RetainedWALBytes = n })
}

// ---- fake connect ----

type fakeConnect struct {
	mu        sync.Mutex
	status    map[string]clients.ConnectorStatus
	statusErr map[string]error
	cfg       map[string]map[string]string
	cfgErr    map[string]error
	// hang makes Status block on the channel and ignore its context.
	hang map[string]chan struct{}
}

func newFakeConnect() *fakeConnect {
	return &fakeConnect{
		status: map[string]clients.ConnectorStatus{}, statusErr: map[string]error{},
		cfg: map[string]map[string]string{}, cfgErr: map[string]error{},
		hang: map[string]chan struct{}{},
	}
}

func (f *fakeConnect) Status(ctx context.Context, name string) (clients.ConnectorStatus, error) {
	f.mu.Lock()
	h := f.hang[name]
	st, err := f.status[name], f.statusErr[name]
	f.mu.Unlock()
	if h != nil {
		<-h
	}
	return st, err
}

func (f *fakeConnect) Config(ctx context.Context, name string) (map[string]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return maps(f.cfg[name]), f.cfgErr[name]
}

func maps(m map[string]string) map[string]string {
	if m == nil {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func (f *fakeConnect) setStatus(name string, st clients.ConnectorStatus) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.status[name] = st
	delete(f.statusErr, name)
}

func (f *fakeConnect) setStatusErr(name string, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.statusErr[name] = err
}

func running(name string) clients.ConnectorStatus {
	return clients.ConnectorStatus{Name: name, State: "RUNNING", Tasks: []clients.TaskStatus{{ID: 0, State: "RUNNING"}}}
}

func failed(name, trace string) clients.ConnectorStatus {
	return clients.ConnectorStatus{Name: name, State: "FAILED", Tasks: []clients.TaskStatus{{ID: 0, State: "FAILED", Trace: trace}}}
}

// ---- fake kafka (clients.DLQReader) ----

type fakeKafka struct {
	mu     sync.Mutex
	topics map[string][]clients.PartitionOffsets
	err    error
	calls  [][]string // the topic list of every DLQOffsets call
	// hook runs inside DLQOffsets before it answers; it may block.
	hook func(ctx context.Context)
}

func newFakeKafka() *fakeKafka { return &fakeKafka{topics: map[string][]clients.PartitionOffsets{}} }

func (k *fakeKafka) DLQOffsets(ctx context.Context, topics ...string) (map[string][]clients.PartitionOffsets, error) {
	k.mu.Lock()
	k.calls = append(k.calls, slices.Clone(topics))
	hook, err := k.hook, k.err
	out := map[string][]clients.PartitionOffsets{}
	for _, tp := range topics {
		if ps, ok := k.topics[tp]; ok {
			out[tp] = slices.Clone(ps)
		}
	}
	k.mu.Unlock()
	if hook != nil {
		hook(ctx)
	}
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (k *fakeKafka) PeekDLQ(ctx context.Context, topic string, limit int) ([]clients.DLQRecord, error) {
	return nil, fmt.Errorf("fake: PeekDLQ must not be called by the watcher")
}

// set makes the topic exist with one partition from start to end.
func (k *fakeKafka) set(topic string, start, end int64) {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.topics[topic] = []clients.PartitionOffsets{{Partition: 0, Start: start, End: end}}
}

// add appends n records to partition 0 (the topic is created when missing).
func (k *fakeKafka) add(topic string, n int64) {
	k.mu.Lock()
	defer k.mu.Unlock()
	ps := k.topics[topic]
	if len(ps) == 0 {
		ps = []clients.PartitionOffsets{{Partition: 0}}
	}
	ps[0].End += n
	k.topics[topic] = ps
}

// setStart moves the start offset (retention).
func (k *fakeKafka) setStart(topic string, start int64) {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.topics[topic][0].Start = start
}

func (k *fakeKafka) remove(topic string) {
	k.mu.Lock()
	defer k.mu.Unlock()
	delete(k.topics, topic)
}

func (k *fakeKafka) setErr(err error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.err = err
}

func (k *fakeKafka) callLists() [][]string {
	k.mu.Lock()
	defer k.mu.Unlock()
	return slices.Clone(k.calls)
}

// ---- fake rooms ----

type openRec struct {
	alert   alertin.Alert
	roomID  string
	created bool
}

type postRec struct{ roomID, text string }

type lastRoom struct {
	roomID   string
	closedAt *time.Time
}

type fakeRooms struct {
	mu      sync.Mutex
	opens   []openRec
	posts   []postRec
	foreign map[string]string // pipeline -> open room not opened by the watcher
	last    map[string]lastRoom
	byKey   map[string]string // pipeline|dedup -> open room
	n       int
	calls   int // every call of any method
	closed  map[string]bool

	// Fault injection. The hooks run without the fake's lock.
	openErr  func(a alertin.Alert) error // non-nil result fails OpenAlertRoom
	postErr  func(roomID string) error   // non-nil result fails PostEvidence
	onOpened func(roomID string)         // after a room was created
	onPosted func(roomID string)         // after a post was stored
}

func newFakeRooms() *fakeRooms {
	return &fakeRooms{foreign: map[string]string{}, last: map[string]lastRoom{}, byKey: map[string]string{}, closed: map[string]bool{}}
}

func (r *fakeRooms) OpenAlertRoom(ctx context.Context, a alertin.Alert) (string, bool, error) {
	r.mu.Lock()
	r.calls++
	if r.openErr != nil {
		if err := r.openErr(a); err != nil {
			r.mu.Unlock()
			return "", false, err
		}
	}
	idx := a.PipelineID + "|" + a.DedupKey
	if id, ok := r.byKey[idx]; ok {
		r.opens = append(r.opens, openRec{a, id, false})
		r.mu.Unlock()
		return id, false, nil
	}
	if a.Status == alertin.StatusResolved {
		r.mu.Unlock()
		return "", false, fmt.Errorf("fake: %w", ErrNoRoom)
	}
	r.n++
	id := fmt.Sprintf("r%d", r.n)
	r.byKey[idx] = id
	r.last[idx] = lastRoom{roomID: id}
	r.opens = append(r.opens, openRec{a, id, true})
	hook := r.onOpened
	r.mu.Unlock()
	if hook != nil {
		hook(id)
	}
	return id, true, nil
}

func (r *fakeRooms) OpenRoomForPipeline(p string) (string, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls++
	if id, ok := r.foreign[p]; ok {
		return id, true
	}
	for idx, id := range r.byKey {
		if strings.HasPrefix(idx, p+"|") {
			return id, true
		}
	}
	return "", false
}

func (r *fakeRooms) LastRoomForKey(p, key string) (string, *time.Time, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls++
	l, ok := r.last[p+"|"+key]
	return l.roomID, l.closedAt, ok
}

func (r *fakeRooms) PostEvidence(ctx context.Context, roomID, text string) error {
	r.mu.Lock()
	r.calls++
	if r.postErr != nil {
		if err := r.postErr(roomID); err != nil {
			r.mu.Unlock()
			return err
		}
	}
	if r.closed[roomID] {
		r.mu.Unlock()
		return fmt.Errorf("fake: room %s: %w", roomID, ErrRoomClosed)
	}
	r.posts = append(r.posts, postRec{roomID, text})
	hook := r.onPosted
	r.mu.Unlock()
	if hook != nil {
		hook(roomID)
	}
	return nil
}

// closeRoom closes a room the way the service does.
func (r *fakeRooms) closeRoom(roomID string, at time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closed[roomID] = true
	for idx, id := range r.byKey {
		if id == roomID {
			delete(r.byKey, idx)
			r.last[idx] = lastRoom{roomID: id, closedAt: &at}
		}
	}
}

// created returns the firing alerts that opened a new room.
func (r *fakeRooms) created() []openRec {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []openRec
	for _, o := range r.opens {
		if o.created {
			out = append(out, o)
		}
	}
	return out
}

func (r *fakeRooms) resolved() []openRec {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []openRec
	for _, o := range r.opens {
		if o.alert.Status == alertin.StatusResolved {
			out = append(out, o)
		}
	}
	return out
}

func (r *fakeRooms) postsContaining(s string) []postRec {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []postRec
	for _, p := range r.posts {
		if strings.Contains(p.text, s) {
			out = append(out, p)
		}
	}
	return out
}

func (r *fakeRooms) callCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls
}

// ---- logs ----

type logBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *logBuf) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *logBuf) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

func (l *logBuf) count(s string) int {
	return strings.Count(l.String(), s)
}

// ---- owners ----

type pipeSpec struct {
	id    string
	watch string // already indented under the pipeline (4 spaces)
}

func ownersYAML(ps ...pipeSpec) string {
	var b strings.Builder
	b.WriteString("pipelines:\n")
	for _, p := range ps {
		fmt.Fprintf(&b, "  - id: %s\n    title: %s\n    topic_prefix: %s\n    source_tables: [public.t]\n    connectors: [pg-%s]\n    consumer_groups: [g-%s]\n",
			p.id, p.id, p.id, p.id, p.id)
		b.WriteString(p.watch)
	}
	return b.String()
}

func loadOwners(t *testing.T, ps ...pipeSpec) *owners.Owners {
	t.Helper()
	path := filepath.Join(t.TempDir(), "owners.yaml")
	if err := os.WriteFile(path, []byte(ownersYAML(ps...)), 0o600); err != nil {
		t.Fatal(err)
	}
	o, err := owners.Load(path)
	if err != nil {
		t.Fatalf("owners.Load: %v", err)
	}
	return o
}

// ---- harness ----

type opts struct {
	pipes        []pipeSpec
	mode         config.WatchMode
	maxRooms     int
	cooldown     time.Duration
	checkTimeout time.Duration
	noHeartbeat  bool
	noKafka      bool // Deps.Kafka stays nil: the client cannot read DLQ offsets
}

type harness struct {
	t     *testing.T
	w     *Watcher
	clk   *fakeClock
	src   *fakeSource
	conn  *fakeConnect
	rooms *fakeRooms
	kafka *fakeKafka
	logs  *logBuf
	deps  Deps
}

func newHarness(t *testing.T, o opts) *harness {
	t.Helper()
	if len(o.pipes) == 0 {
		o.pipes = []pipeSpec{{id: "orders"}}
	}
	if o.mode == "" {
		o.mode = config.WatchRooms
	}
	if o.cooldown == 0 {
		o.cooldown = time.Hour
	}
	if o.checkTimeout == 0 {
		o.checkTimeout = 200 * time.Millisecond
	}
	h := &harness{t: t, clk: newFakeClock(), src: &fakeSource{}, conn: newFakeConnect(), rooms: newFakeRooms(), kafka: newFakeKafka(), logs: &logBuf{}}
	for _, p := range o.pipes {
		cfg := map[string]string{
			"connector.class": "io.debezium.connector.postgresql.PostgresConnector",
			"slot.name":       "debezium_" + p.id,
		}
		if !o.noHeartbeat {
			cfg["heartbeat.interval.ms"] = "10000"
		}
		h.conn.cfg["pg-"+p.id] = cfg
		h.conn.setStatus("pg-"+p.id, running("pg-"+p.id))
		h.src.update("debezium_"+p.id, func(r *clients.ReplicationSlot) {
			r.Active, r.WALStatus, r.RetainedWALBytes = true, "reserved", 1<<20
		})
	}
	d := Deps{
		Source: h.src, Connect: h.conn, Owners: loadOwners(t, o.pipes...), Rooms: h.rooms,
		Config:       config.Watch{Mode: o.mode, Interval: time.Minute, CheckTimeout: o.checkTimeout, Cooldown: o.cooldown},
		MaxOpenRooms: o.maxRooms, Clock: h.clk,
		Log:    slog.New(slog.NewTextHandler(h.logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
		Jitter: func(time.Duration) time.Duration { return 0 },
	}
	if !o.noKafka {
		d.Kafka = h.kafka
	}
	h.deps = d
	w, err := New(d)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	h.w = w
	return h
}

// step moves the clock by d and runs one tick.
func (h *harness) step(d time.Duration) {
	h.t.Helper()
	h.clk.Advance(d)
	h.w.tick(context.Background())
}

// steps runs n ticks, d apart.
func (h *harness) steps(n int, d time.Duration) {
	h.t.Helper()
	for range n {
		h.step(d)
	}
}

func (h *harness) phase(pipe, sig string) phase {
	h.w.mu.Lock()
	defer h.w.mu.Unlock()
	ps := h.w.pipes[pipe]
	if ps == nil || ps.sigs[sig] == nil {
		return phaseOK
	}
	return ps.sigs[sig].phase
}

func (h *harness) breakConnector(pipe, trace string) {
	h.conn.setStatus("pg-"+pipe, failed("pg-"+pipe, trace))
}

func (h *harness) fixConnector(pipe string) {
	h.conn.setStatus("pg-"+pipe, running("pg-"+pipe))
}

// evidenceOf decodes the Raw evidence of an alert.
func evidenceOf(t *testing.T, raw []byte) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("evidence is not JSON: %v\n%s", err, raw)
	}
	return m
}

// signalEv finds one signal's evidence in the decoded evidence.
func signalEv(t *testing.T, ev map[string]any, signal string) map[string]any {
	t.Helper()
	sigs, _ := ev["signals"].([]any)
	for _, s := range sigs {
		if m, ok := s.(map[string]any); ok && m["signal"] == signal {
			return m
		}
	}
	t.Fatalf("no %s in evidence %v", signal, ev)
	return nil
}
