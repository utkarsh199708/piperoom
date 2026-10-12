package main

// End-to-end test of the watcher wiring: a real watch.Watcher and a real
// room.Service on a memstore, joined by the adapter and the room-closed hook,
// with fake Connect/source clients and a fake clock.

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"piperoom/internal/agent"
	"piperoom/internal/alertin"
	"piperoom/internal/clients"
	"piperoom/internal/config"
	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/room"
	"piperoom/internal/store/memstore"
	"piperoom/internal/watch"
)

var wt0 = time.Date(2026, 10, 13, 10, 0, 0, 0, time.UTC)

// ---- fake clock ----

type fakeClock struct {
	mu     sync.Mutex
	now    time.Time
	timers []*fakeTimer
}

func newFakeClock() *fakeClock { return &fakeClock{now: wt0} }

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

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

func (c *fakeClock) NewTimer(d time.Duration) watch.Timer {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := &fakeTimer{c: c, ch: make(chan time.Time, 1), at: c.now.Add(d), armed: true}
	c.timers = append(c.timers, t)
	c.fireLocked()
	return t
}

// idle reports whether every timer is armed, which for the one-goroutine
// watcher means it finished its tick and is waiting for the next.
func (c *fakeClock) idle() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, t := range c.timers {
		if !t.armed {
			return false
		}
	}
	return len(c.timers) > 0
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

// ---- fake clients ----

type fakeSource struct{}

func (fakeSource) ReplicationSlots(context.Context) ([]clients.ReplicationSlot, error) {
	return []clients.ReplicationSlot{{Name: "debezium_orders", Active: true, WALStatus: "reserved", RetainedWALBytes: 1 << 20}}, nil
}

type fakeConnect struct {
	mu     sync.Mutex
	state  string
	status int // Status calls so far
}

func (f *fakeConnect) setState(s string) {
	f.mu.Lock()
	f.state = s
	f.mu.Unlock()
}

func (f *fakeConnect) statusCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.status
}

func (f *fakeConnect) Status(_ context.Context, name string) (clients.ConnectorStatus, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.status++
	st := clients.ConnectorStatus{Name: name, State: f.state, Tasks: []clients.TaskStatus{{ID: 0, State: f.state}}}
	if f.state == "FAILED" {
		st.Tasks[0].Trace = "org.apache.kafka.connect.errors.ConnectException: boom"
	}
	return st, nil
}

func (f *fakeConnect) Config(context.Context, string) (map[string]string, error) {
	return map[string]string{"slot.name": "debezium_orders", "heartbeat.interval.ms": "10000"}, nil
}

// loadTestOwners loads one pipeline, "orders", with the default watch settings
// (connector_down fires after 3 minutes).
func loadTestOwners(t *testing.T) *owners.Owners {
	t.Helper()
	const y = `pipelines:
  - id: orders
    title: orders
    topic_prefix: orders
    source_tables: [public.orders]
    connectors: [pg-orders]
    consumer_groups: [g-orders]
`
	path := filepath.Join(t.TempDir(), "owners.yaml")
	if err := os.WriteFile(path, []byte(y), 0o600); err != nil {
		t.Fatal(err)
	}
	o, err := owners.Load(path)
	if err != nil {
		t.Fatalf("owners.Load: %v", err)
	}
	return o
}

// ---- harness ----

type watchHarness struct {
	t      *testing.T
	clk    *fakeClock
	conn   *fakeConnect
	svc    *room.Service
	forget *recForget
	// spy sits between the hook and the real watcher and records what the
	// service told it.
	spy *recWatcher
}

func newWatchHarness(t *testing.T) *watchHarness {
	t.Helper()
	o := loadTestOwners(t)
	hook := &roomClosedHook{}
	forget := &recForget{}
	hook.setForget(forget)
	// One fake clock for the watcher, the service and the store. If the service
	// kept the real clock, "now - closedAt" would depend on the date the test
	// runs on, and a cooldown could hide a broken ack.
	clk := newFakeClock()
	svc := room.New(room.Deps{
		Store: memstore.NewWithClock(clk.Now), Now: clk.Now, Users: room.DefaultUsers(), Owners: o,
		Run: func(ctx context.Context, _ agent.Alert, _ agent.Deps, _ agent.Options) error {
			<-ctx.Done()
			return nil
		},
		WriteHandoff: func(context.Context, []events.Event, agent.Options) (events.HandoffSummary, error) {
			return events.HandoffSummary{}, nil
		},
		OnRoomClosed: hook.onClosed,
	})
	// Registered first, so it runs last: the watcher stops before the service closes.
	t.Cleanup(svc.Close)

	h := &watchHarness{t: t, clk: clk, conn: &fakeConnect{state: "RUNNING"}, svc: svc, forget: forget}
	stop, err := startWatcher(context.Background(), watch.Deps{
		Source: fakeSource{}, Connect: h.conn, Owners: o, Rooms: watchRooms{svc: svc},
		Config:       config.Watch{Mode: config.WatchRooms, Interval: time.Minute, CheckTimeout: 5 * time.Second, Cooldown: time.Hour},
		MaxOpenRooms: 3, Clock: h.clk, Jitter: func(time.Duration) time.Duration { return 0 },
	}, hook, discardLog())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(stop)
	h.spy = &recWatcher{inner: hook.watcher}
	hook.setWatcher(h.spy)
	h.settle()
	return h
}

func (h *watchHarness) settle() {
	h.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !h.clk.idle() {
		if time.Now().After(deadline) {
			h.t.Fatal("the watcher did not finish its tick")
		}
		time.Sleep(time.Millisecond)
	}
}

// tick moves the clock one minute and waits for the watcher's next tick to end.
func (h *watchHarness) tick() {
	h.t.Helper()
	before := h.conn.statusCalls()
	h.clk.Advance(time.Minute)
	deadline := time.Now().Add(5 * time.Second)
	for h.conn.statusCalls() == before || !h.clk.idle() {
		if time.Now().After(deadline) {
			h.t.Fatal("the watcher did not run a tick")
		}
		time.Sleep(time.Millisecond)
	}
}

// watcherRoom returns the pipeline's watcher room and whether it is open.
func (h *watchHarness) watcherRoom() (id string, open, ok bool) {
	id, closedAt, ok := h.svc.LastRoomForKey("orders", watch.DedupKey("orders"))
	return id, closedAt == nil, ok
}

func (h *watchHarness) systemTexts(roomID string) []string {
	h.t.Helper()
	evs, _, err := h.svc.Events(context.Background(), roomID, 0, 1000)
	if err != nil {
		h.t.Fatal(err)
	}
	var out []string
	for _, e := range evs {
		if e.Type != events.AgentMessage || e.Actor.Kind != events.ActorSystem {
			continue
		}
		var p events.AgentMessagePayload
		if err := json.Unmarshal(e.Payload, &p); err != nil {
			h.t.Fatal(err)
		}
		out = append(out, p.Text)
	}
	return out
}

func (h *watchHarness) closeRoom(roomID string) {
	h.t.Helper()
	ctx := context.Background()
	arjun, ok := h.svc.LookupUser("arjun")
	if !ok {
		h.t.Fatal("no user arjun")
	}
	if _, err := h.svc.GrantControl(ctx, arjun, roomID, "arjun"); err != nil {
		h.t.Fatalf("take control: %v", err)
	}
	if _, err := h.svc.CloseRoom(ctx, arjun, roomID, "fixed"); err != nil {
		h.t.Fatalf("close: %v", err)
	}
}

// tickUntil ticks until cond holds, at most n times.
func (h *watchHarness) tickUntil(n int, what string, cond func() bool) {
	h.t.Helper()
	for range n {
		if cond() {
			return
		}
		h.tick()
	}
	if !cond() {
		h.t.Fatalf("after %d ticks: %s", n, what)
	}
}

func countRooms(t *testing.T, svc *room.Service) int {
	t.Helper()
	rooms, err := svc.ListRooms(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return len(rooms)
}

// ---- tests ----

// A connector FAILED for longer than `for` opens a watcher room; when it
// recovers the service posts the resolved note and leaves the room open.
func TestWatcherOpensARoomThenResolvesIt(t *testing.T) {
	h := newWatchHarness(t)

	// Healthy: nothing opens.
	h.tick()
	h.tick()
	if _, _, ok := h.watcherRoom(); ok {
		t.Fatal("a room opened for a healthy pipeline")
	}

	h.conn.setState("FAILED")
	h.tick() // breach starts
	h.tick()
	if _, _, ok := h.watcherRoom(); ok {
		t.Fatal("a room opened before `for` (3m) passed")
	}
	h.tickUntil(4, "no watcher room opened", func() bool { _, _, ok := h.watcherRoom(); return ok })
	id, open, _ := h.watcherRoom()
	if !open {
		t.Fatal("the watcher room is not open")
	}
	snap, err := h.svc.GetRoom(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if snap.PipelineID != "orders" || !strings.Contains(snap.Title, "pg-orders") {
		t.Fatalf("room = pipeline %q title %q", snap.PipelineID, snap.Title)
	}
	if got, ok := h.svc.OpenRoomForPipeline("orders"); !ok || got != id {
		t.Fatalf("OpenRoomForPipeline = %q, %v", got, ok)
	}

	// Recovery: after `for` of clear ticks the resolved alert posts its note.
	h.conn.setState("RUNNING")
	resolved := func() bool {
		for _, s := range h.systemTexts(id) {
			if strings.Contains(s, "reported resolved at its source") {
				return true
			}
		}
		return false
	}
	h.tickUntil(8, "no resolved note in the room", resolved)
	if _, open, _ := h.watcherRoom(); !open {
		t.Fatal("the resolved note must leave the room open")
	}
	if n := countRooms(t, h.svc); n != 1 {
		t.Fatalf("%d rooms, want 1", n)
	}
	if len(h.forget.rooms) != 0 {
		t.Fatalf("Forget ran for %v before any room closed", h.forget.rooms)
	}
}

// The driver closes the watcher room while the connector is still failing: the
// watcher is acked and does not open the room again, however long it fails.
func TestWatcherIsAckedWhenTheDriverClosesTheRoom(t *testing.T) {
	h := newWatchHarness(t)
	h.conn.setState("FAILED")
	h.tickUntil(8, "no watcher room opened", func() bool { _, _, ok := h.watcherRoom(); return ok })
	id, _, _ := h.watcherRoom()
	notes := len(h.systemTexts(id))

	h.closeRoom(id)
	// The hook ran before CloseRoom returned: the watcher and Forget both heard.
	if len(h.spy.calls) != 1 || h.spy.calls[0] != [3]string{id, "orders", "watcher:orders"} {
		t.Fatalf("watcher OnRoomClosed calls = %v, want one for %s", h.spy.calls, id)
	}
	if len(h.forget.rooms) != 1 || h.forget.rooms[0] != id {
		t.Fatalf("Forget calls = %v, want [%s]", h.forget.rooms, id)
	}

	// 75 more minutes of the same failure: past "still firing" (15m) and past
	// the cooldown (60m). The room stays closed because the signal is acked (and
	// because the watcher already marked it as notified); a cooldown alone would
	// not hold for this long.
	for range 75 {
		h.tick()
	}
	if n := countRooms(t, h.svc); n != 1 {
		t.Fatalf("%d rooms after the close, want 1: the incident was re-opened", n)
	}
	if got, _, _ := h.watcherRoom(); got != id {
		t.Fatalf("newest watcher room = %q, want the closed %q", got, id)
	}
	if _, open := h.svc.OpenRoomForPipeline("orders"); open {
		t.Fatal("a room is open for the pipeline after the close")
	}
	if got := len(h.systemTexts(id)); got != notes {
		t.Fatalf("the watcher wrote %d notes into the closed room", got-notes)
	}

	// Real service errors reach the watcher through the adapter as ErrRoomClosed
	// (the unit tests use hand-made errors; this one is the room's own).
	err := watchRooms{svc: h.svc}.PostEvidence(context.Background(), id, "late evidence")
	if !errors.Is(err, watch.ErrRoomClosed) {
		t.Fatalf("PostEvidence into a closed room = %v, want watch.ErrRoomClosed", err)
	}
	err = watchRooms{svc: h.svc}.PostEvidence(context.Background(), "r_does_not_exist", "x")
	if !errors.Is(err, watch.ErrNoRoom) || !errors.Is(err, watch.ErrRoomClosed) {
		t.Fatalf("PostEvidence into an unknown room = %v, want watch.ErrNoRoom and watch.ErrRoomClosed", err)
	}
}

// A resolved alert with no open room comes back as watch.ErrNoRoom from the
// real service.
func TestAdapterMapsResolvedWithNoRoomFromTheRealService(t *testing.T) {
	h := newWatchHarness(t)
	_, _, err := watchRooms{svc: h.svc}.OpenAlertRoom(context.Background(), alertin.Alert{
		Source: alertin.Watcher, Status: alertin.StatusResolved, Severity: "sev2",
		PipelineID: "orders", Title: "orders: clear", DedupKey: watch.DedupKey("orders"),
	})
	if !errors.Is(err, watch.ErrNoRoom) || !errors.Is(err, room.ErrResolvedAlertNoRoom) {
		t.Fatalf("err = %v, want watch.ErrNoRoom wrapping room.ErrResolvedAlertNoRoom", err)
	}
}
