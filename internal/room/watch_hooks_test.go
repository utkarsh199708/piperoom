package room_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"piperoom/internal/agent"
	"piperoom/internal/alertin"
	"piperoom/internal/events"
	"piperoom/internal/room"
	"piperoom/internal/store/memstore"
)

// watchHarness is a room service on a memstore with a fake clock, counting agent
// starts and recording OnRoomClosed calls. build makes another service on the
// same store, which is how the tests model a restart.
type watchHarness struct {
	t      *testing.T
	clock  *clock
	store  *memstore.Store
	runs   *atomic.Int32
	mu     sync.Mutex
	closed []closedCall
}

type closedCall struct{ roomID, pipelineID, dedupKey string }

func newWatchHarness(t *testing.T) *watchHarness {
	t.Helper()
	c := &clock{t: time.Date(2026, 10, 13, 9, 0, 0, 0, time.UTC)}
	return &watchHarness{t: t, clock: c, store: memstore.NewWithClock(c.Now), runs: new(atomic.Int32)}
}

func (h *watchHarness) build() *room.Service {
	h.t.Helper()
	svc := room.New(room.Deps{
		Store: h.store, Users: room.DefaultUsers(), Owners: testOwners(), Now: h.clock.Now,
		Run: func(ctx context.Context, _ agent.Alert, _ agent.Deps, _ agent.Options) error {
			h.runs.Add(1)
			<-ctx.Done()
			return nil
		},
		WriteHandoff: fakeWrite,
		OnRoomClosed: func(_ context.Context, roomID, pipelineID, dedupKey string) {
			h.mu.Lock()
			defer h.mu.Unlock()
			h.closed = append(h.closed, closedCall{roomID, pipelineID, dedupKey})
		},
	})
	h.t.Cleanup(svc.Close)
	return svc
}

func (h *watchHarness) closedCalls() []closedCall {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]closedCall(nil), h.closed...)
}

// closeRoom takes control as arjun and closes the room.
func (h *watchHarness) closeRoom(svc *room.Service, roomID string) {
	h.t.Helper()
	ctx := context.Background()
	arjun := mustUser(h.t, svc, "arjun")
	if _, err := svc.GrantControl(ctx, arjun, roomID, "arjun"); err != nil {
		h.t.Fatalf("take control: %v", err)
	}
	if _, err := svc.CloseRoom(ctx, arjun, roomID, "fixed"); err != nil {
		h.t.Fatalf("close: %v", err)
	}
}

func watcherAlert(pipeline string, status string) alertin.Alert {
	return alertin.Alert{
		Source: alertin.Watcher, Status: status, Severity: "sev2", PipelineID: pipeline,
		Title:    pipeline + ": connector pg-orders FAILED 4m",
		DedupKey: "watcher:" + pipeline,
		Raw:      json.RawMessage(`{"signal":"connector_down"}`),
	}
}

func roomEvents(t *testing.T, svc *room.Service, roomID string) []events.Event {
	t.Helper()
	evs, _, err := svc.Events(context.Background(), roomID, 0, 1000)
	if err != nil {
		t.Fatalf("events: %v", err)
	}
	return evs
}

func systemTexts(t *testing.T, svc *room.Service, roomID string) []string {
	t.Helper()
	var out []string
	for _, e := range roomEvents(t, svc, roomID) {
		if e.Type != events.AgentMessage || e.Actor.Kind != events.ActorSystem {
			continue
		}
		var p events.AgentMessagePayload
		if err := json.Unmarshal(e.Payload, &p); err != nil {
			t.Fatalf("decode: %v", err)
		}
		out = append(out, p.Text)
	}
	return out
}

func TestOnRoomClosedFiresWithIDs(t *testing.T) {
	h := newWatchHarness(t)
	svc := h.build()
	ctx := context.Background()

	// A watcher room reports its source key without the pipeline prefix.
	w, _, err := svc.OpenAlertRoom(ctx, watcherAlert("orders", alertin.StatusFiring))
	if err != nil {
		t.Fatalf("open watcher room: %v", err)
	}
	// A room made by hand has no key.
	arjun := mustUser(t, svc, "arjun")
	manual, err := svc.CreateRoom(ctx, arjun, "by hand", "sev3", "orders")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if got := h.closedCalls(); len(got) != 0 {
		t.Fatalf("hook fired before any close: %v", got)
	}

	h.closeRoom(svc, w.ID)
	got := h.closedCalls()
	want := closedCall{w.ID, "orders", "watcher:orders"}
	if len(got) != 1 || got[0] != want {
		t.Fatalf("after close: calls = %v, want [%v]", got, want)
	}

	// A failed close (not the driver) does not fire the hook.
	if _, err := svc.CloseRoom(ctx, mustUser(t, svc, "priya"), manual.ID, "no"); err == nil {
		t.Fatal("close by a non-driver: want error")
	}
	if n := len(h.closedCalls()); n != 1 {
		t.Fatalf("hook fired %d times after a refused close, want 1", n)
	}
	if _, err := svc.CloseRoom(ctx, arjun, manual.ID, "done"); err != nil {
		t.Fatalf("close manual room: %v", err)
	}
	got = h.closedCalls()
	if len(got) != 2 || got[1] != (closedCall{manual.ID, "orders", ""}) {
		t.Fatalf("calls = %v, want a second call for %s with an empty key", got, manual.ID)
	}
	// Closing twice is an error and does not fire again.
	if _, err := svc.CloseRoom(ctx, arjun, manual.ID, "again"); err == nil {
		t.Fatal("second close: want error")
	}
	if n := len(h.closedCalls()); n != 2 {
		t.Fatalf("hook fired %d times, want 2", n)
	}
}

// A hook that calls back into the service must not deadlock: it runs outside s.mu.
func TestOnRoomClosedMayCallService(t *testing.T) {
	var svc *room.Service
	done := make(chan bool, 1)
	svc = room.New(room.Deps{
		Store: memstore.New(), Users: room.DefaultUsers(), Owners: testOwners(), Run: fakeRun, WriteHandoff: fakeWrite,
		OnRoomClosed: func(_ context.Context, _, pipelineID, _ string) {
			_, ok := svc.OpenRoomForPipeline(pipelineID)
			done <- ok
		},
	})
	t.Cleanup(svc.Close)
	r := createRoom(t, svc, "arjun")
	if _, err := svc.CloseRoom(context.Background(), mustUser(t, svc, "arjun"), r.ID, "done"); err != nil {
		t.Fatalf("close: %v", err)
	}
	select {
	case ok := <-done:
		if ok {
			t.Fatal("OpenRoomForPipeline in the close hook found the room that just closed")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("hook never ran, or deadlocked")
	}
}

func TestOpenRoomForPipeline(t *testing.T) {
	h := newWatchHarness(t)
	svc := h.build()
	ctx := context.Background()
	arjun := mustUser(t, svc, "arjun")

	if id, ok := svc.OpenRoomForPipeline("orders"); ok || id != "" {
		t.Fatalf("no rooms: got %q, %v", id, ok)
	}

	first, err := svc.CreateRoom(ctx, arjun, "first", "sev3", "orders")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if id, ok := svc.OpenRoomForPipeline("orders"); !ok || id != first.ID {
		t.Fatalf("one open room: got %q, %v, want %q", id, ok, first.ID)
	}
	if id, ok := svc.OpenRoomForPipeline("billing"); ok {
		t.Fatalf("other pipeline: got %q, want none", id)
	}

	// An Alertmanager-style room counts too, and the newest open room wins.
	h.clock.Advance(time.Minute)
	second, _, err := svc.OpenAlertRoom(ctx, alertin.Alert{
		Source: alertin.Alertmanager, Status: alertin.StatusFiring, Severity: "sev1", PipelineID: "orders",
		Title: "Lag", DedupKey: "alertmanager:lag",
	})
	if err != nil {
		t.Fatalf("open alert room: %v", err)
	}
	h.clock.Advance(time.Minute)
	third, err := svc.CreateRoom(ctx, arjun, "third", "sev3", "orders")
	if err != nil {
		t.Fatalf("create third: %v", err)
	}
	if id, ok := svc.OpenRoomForPipeline("orders"); !ok || id != third.ID {
		t.Fatalf("three open rooms: got %q, %v, want newest %q", id, ok, third.ID)
	}

	// Closing the newest hands the answer to the next newest open room.
	h.closeRoom(svc, third.ID)
	if id, ok := svc.OpenRoomForPipeline("orders"); !ok || id != second.ID {
		t.Fatalf("after closing newest: got %q, %v, want %q", id, ok, second.ID)
	}
	h.closeRoom(svc, second.ID)
	h.closeRoom(svc, first.ID)
	if id, ok := svc.OpenRoomForPipeline("orders"); ok {
		t.Fatalf("all closed: got %q, want none", id)
	}
}

func TestLastRoomForKey(t *testing.T) {
	h := newWatchHarness(t)
	svc := h.build()
	ctx := context.Background()

	if id, at, ok := svc.LastRoomForKey("orders", "watcher:orders"); ok || id != "" || at != nil {
		t.Fatalf("before any room: %q %v %v", id, at, ok)
	}
	if _, _, ok := svc.LastRoomForKey("orders", ""); ok {
		t.Fatal("empty key matched a room")
	}

	first, _, err := svc.OpenAlertRoom(ctx, watcherAlert("orders", alertin.StatusFiring))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	// While the room is open: found, with no close time.
	if id, at, ok := svc.LastRoomForKey("orders", "watcher:orders"); !ok || id != first.ID || at != nil {
		t.Fatalf("open room: %q %v %v, want %q nil true", id, at, ok, first.ID)
	}
	// Another pipeline, or another key, is not a match.
	if _, _, ok := svc.LastRoomForKey("billing", "watcher:orders"); ok {
		t.Fatal("same key on another pipeline matched")
	}
	if _, _, ok := svc.LastRoomForKey("orders", "watcher:billing"); ok {
		t.Fatal("another key matched")
	}

	h.clock.Advance(10 * time.Minute)
	closeAt := h.clock.Now()
	h.closeRoom(svc, first.ID)
	id, at, ok := svc.LastRoomForKey("orders", "watcher:orders")
	if !ok || id != first.ID || at == nil || !at.Equal(closeAt) {
		t.Fatalf("closed room: %q %v %v, want %q %v true", id, at, ok, first.ID, closeAt)
	}

	// A second incident for the key: the newest room wins.
	h.clock.Advance(time.Hour)
	second, created, err := svc.OpenAlertRoom(ctx, watcherAlert("orders", alertin.StatusFiring))
	if err != nil || !created || second.ID == first.ID {
		t.Fatalf("reopen: %q created=%v err=%v", second.ID, created, err)
	}
	if id, at, ok := svc.LastRoomForKey("orders", "watcher:orders"); !ok || id != second.ID || at != nil {
		t.Fatalf("second room open: %q %v %v, want %q nil true", id, at, ok, second.ID)
	}
	h.clock.Advance(5 * time.Minute)
	closeAt = h.clock.Now()
	h.closeRoom(svc, second.ID)

	// A new service on the same store, as after a restart.
	svc2 := h.build()
	if err := svc2.Load(ctx); err != nil {
		t.Fatalf("load: %v", err)
	}
	id, at, ok = svc2.LastRoomForKey("orders", "watcher:orders")
	if !ok || id != second.ID || at == nil || !at.Equal(closeAt) {
		t.Fatalf("after reload: %q %v %v, want %q %v true", id, at, ok, second.ID, closeAt)
	}
	// And a service that has not loaded anything yet finds it too: the lookup
	// goes to the store, not the cache.
	svc3 := h.build()
	if id, _, ok := svc3.LastRoomForKey("orders", "watcher:orders"); !ok || id != second.ID {
		t.Fatalf("before Load: %q %v, want %q true", id, ok, second.ID)
	}
}

// A watcher-source alert opens a room under the key "watcher:<pipeline>", a
// repeat joins it without a second agent, and a resolved one posts the note and
// leaves the room open. After a restart the key is found again and a join still
// does not start an agent.
func TestWatcherAlertOpensJoinsResolves(t *testing.T) {
	h := newWatchHarness(t)
	svc := h.build()
	ctx := context.Background()

	// Resolved with no room: nothing opens.
	if _, created, err := svc.OpenAlertRoom(ctx, watcherAlert("orders", alertin.StatusResolved)); !errors.Is(err, room.ErrResolvedAlertNoRoom) || created {
		t.Fatalf("resolved with no room: created=%v err=%v, want ErrResolvedAlertNoRoom", created, err)
	}

	first, created, err := svc.OpenAlertRoom(ctx, watcherAlert("orders", alertin.StatusFiring))
	if err != nil || !created {
		t.Fatalf("open: created=%v err=%v", created, err)
	}
	waitFor(t, 2*time.Second, func() bool { return h.runs.Load() == 1 })

	// The source and key are stored as the design says.
	created0 := roomEvents(t, svc, first.ID)[0]
	var p events.RoomCreatedPayload
	if err := json.Unmarshal(created0.Payload, &p); err != nil {
		t.Fatalf("decode room.created: %v", err)
	}
	if p.AlertSource != "watcher" || p.DedupKey != "orders|watcher:orders" {
		t.Fatalf("room.created source %q key %q, want watcher / orders|watcher:orders", p.AlertSource, p.DedupKey)
	}

	again, created, err := svc.OpenAlertRoom(ctx, watcherAlert("orders", alertin.StatusFiring))
	if err != nil || created || again.ID != first.ID {
		t.Fatalf("repeat: %q created=%v err=%v, want join of %q", again.ID, created, err, first.ID)
	}
	resolved, created, err := svc.OpenAlertRoom(ctx, watcherAlert("orders", alertin.StatusResolved))
	if err != nil || created || resolved.ID != first.ID || resolved.Status != "open" {
		t.Fatalf("resolved: %q %s created=%v err=%v, want open join of %q", resolved.ID, resolved.Status, created, err, first.ID)
	}
	notes := systemTexts(t, svc, first.ID)
	if len(notes) != 2 || !strings.Contains(notes[0], "fired again") || !strings.Contains(notes[1], "resolved") {
		t.Fatalf("system notes = %q, want a fired-again and a resolved note", notes)
	}
	time.Sleep(50 * time.Millisecond)
	if n := h.runs.Load(); n != 1 {
		t.Fatalf("joins started an agent: %d starts, want 1", n)
	}

	// Restart: a new service, Load, then the watcher fires again. It joins and
	// does not wake the agent.
	svc2 := h.build()
	if err := svc2.Load(ctx); err != nil {
		t.Fatalf("load: %v", err)
	}
	h.runs.Store(0)
	joined, created, err := svc2.OpenAlertRoom(ctx, watcherAlert("orders", alertin.StatusFiring))
	if err != nil || created || joined.ID != first.ID {
		t.Fatalf("after restart: %q created=%v err=%v, want join of %q", joined.ID, created, err, first.ID)
	}
	if id, ok := svc2.OpenRoomForPipeline("orders"); !ok || id != first.ID {
		t.Fatalf("OpenRoomForPipeline after restart: %q %v", id, ok)
	}
	time.Sleep(50 * time.Millisecond)
	if n := h.runs.Load(); n != 0 {
		t.Fatalf("join after restart started an agent: %d starts, want 0", n)
	}

	// Once the room is closed, the next firing opens a new room (and a new agent).
	h.closeRoom(svc2, first.ID)
	next, created, err := svc2.OpenAlertRoom(ctx, watcherAlert("orders", alertin.StatusFiring))
	if err != nil || !created || next.ID == first.ID {
		t.Fatalf("after close: %q created=%v err=%v, want a new room", next.ID, created, err)
	}
	waitFor(t, 2*time.Second, func() bool { return h.runs.Load() == 1 })
}

func TestPostRoomNote(t *testing.T) {
	h := newWatchHarness(t)
	svc := h.build()
	ctx := context.Background()

	// An Alertmanager room takes the watcher's evidence; the agent is not woken.
	am, _, err := svc.OpenAlertRoom(ctx, alertin.Alert{
		Source: alertin.Alertmanager, Status: alertin.StatusFiring, Severity: "sev2", PipelineID: "orders",
		Title: "Lag", DedupKey: "alertmanager:lag",
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	waitFor(t, 2*time.Second, func() bool { return h.runs.Load() == 1 })

	if err := svc.PostRoomNote(ctx, am.ID, "  watcher: slot debezium_orders inactive 6m, 1.3 GB retained  "); err != nil {
		t.Fatalf("post note: %v", err)
	}
	notes := systemTexts(t, svc, am.ID)
	if len(notes) != 1 || notes[0] != "watcher: slot debezium_orders inactive 6m, 1.3 GB retained" {
		t.Fatalf("notes = %q", notes)
	}
	time.Sleep(50 * time.Millisecond)
	if n := h.runs.Load(); n != 1 {
		t.Fatalf("note started an agent: %d starts, want 1", n)
	}

	// After a restart, a note still starts nothing.
	svc2 := h.build()
	if err := svc2.Load(ctx); err != nil {
		t.Fatalf("load: %v", err)
	}
	h.runs.Store(0)
	if err := svc2.PostRoomNote(ctx, am.ID, "still firing"); err != nil {
		t.Fatalf("post note after restart: %v", err)
	}
	time.Sleep(50 * time.Millisecond)
	if n := h.runs.Load(); n != 0 {
		t.Fatalf("note after restart started an agent: %d starts, want 0", n)
	}

	// Bad input and bad rooms.
	var re *room.Error
	if err := svc2.PostRoomNote(ctx, am.ID, "   "); !errors.As(err, &re) || re.Status != 400 {
		t.Fatalf("empty note: %v, want a 400", err)
	}
	if err := svc2.PostRoomNote(ctx, "r_nope", "x"); !errors.As(err, &re) || re.Status != 404 {
		t.Fatalf("unknown room: %v, want a 404", err)
	}
	h.closeRoom(svc2, am.ID)
	if err := svc2.PostRoomNote(ctx, am.ID, "late"); !errors.As(err, &re) || re.Status != 409 {
		t.Fatalf("closed room: %v, want a 409", err)
	}

	// Long text is cut, on a rune boundary.
	other, err := svc2.CreateRoom(ctx, mustUser(t, svc2, "arjun"), "x", "sev3", "orders")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := svc2.PostRoomNote(ctx, other.ID, strings.Repeat("é", 5000)); err != nil {
		t.Fatalf("long note: %v", err)
	}
	notes = systemTexts(t, svc2, other.ID)
	if len(notes) != 1 || len(notes[0]) > 4096+len("…") || !strings.HasSuffix(notes[0], "…") {
		t.Fatalf("long note stored %d bytes, want <= 4099 ending in …", len(notes[0]))
	}
}
