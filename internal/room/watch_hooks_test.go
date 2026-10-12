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
	"unicode/utf8"

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
	// inboxes holds the Inbox each agent run was given, in start order.
	inboxes []agent.Inbox
}

func (h *watchHarness) inbox(i int) agent.Inbox {
	h.mu.Lock()
	defer h.mu.Unlock()
	if i >= len(h.inboxes) {
		h.t.Fatalf("agent run %d never started", i)
	}
	return h.inboxes[i]
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
		Run: func(ctx context.Context, _ agent.Alert, d agent.Deps, _ agent.Options) error {
			h.mu.Lock()
			h.inboxes = append(h.inboxes, d.Inbox)
			h.mu.Unlock()
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

// The close hook runs outside s.mu: it may re-enter the service through
// OpenRoomForPipeline, LastRoomForKey and OpenAlertRoom without deadlocking
// CloseRoom, and it already sees the room as closed.
func TestOnRoomClosedMayReenterService(t *testing.T) {
	type seen struct {
		openFound  bool
		lastID     string
		lastClosed bool
		lastOK     bool
		reopened   bool
		reopenErr  error
	}
	var svc *room.Service
	got := make(chan seen, 1)
	svc = room.New(room.Deps{
		Store: memstore.New(), Users: room.DefaultUsers(), Owners: testOwners(), Run: fakeRun, WriteHandoff: fakeWrite,
		OnRoomClosed: func(ctx context.Context, _, pipelineID, dedupKey string) {
			var r seen
			_, r.openFound = svc.OpenRoomForPipeline(pipelineID)
			var at *time.Time
			r.lastID, at, r.lastOK = svc.LastRoomForKey(pipelineID, dedupKey)
			r.lastClosed = at != nil
			// The watcher fires again at once: a new room, not a join of the closed one.
			var snap room.RoomSnapshot
			snap, r.reopened, r.reopenErr = svc.OpenAlertRoom(ctx, watcherAlert(pipelineID, alertin.StatusFiring))
			if snap.ID == r.lastID {
				r.reopened = false
			}
			got <- r
		},
	})
	t.Cleanup(svc.Close)

	ctx := context.Background()
	w, _, err := svc.OpenAlertRoom(ctx, watcherAlert("orders", alertin.StatusFiring))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	arjun := mustUser(t, svc, "arjun")
	if _, err := svc.GrantControl(ctx, arjun, w.ID, "arjun"); err != nil {
		t.Fatalf("take control: %v", err)
	}
	closeDone := make(chan error, 1)
	go func() {
		_, err := svc.CloseRoom(ctx, arjun, w.ID, "done")
		closeDone <- err
	}()
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("close: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("CloseRoom did not return: the hook deadlocked")
	}
	select {
	case r := <-got:
		if r.openFound {
			t.Error("OpenRoomForPipeline in the hook found the room that just closed")
		}
		if !r.lastOK || r.lastID != w.ID || !r.lastClosed {
			t.Errorf("LastRoomForKey in the hook = %q ok=%v closed=%v, want %q closed", r.lastID, r.lastOK, r.lastClosed, w.ID)
		}
		if r.reopenErr != nil || !r.reopened {
			t.Errorf("OpenAlertRoom in the hook: opened a new room = %v, err %v", r.reopened, r.reopenErr)
		}
	case <-time.After(time.Second):
		t.Fatal("hook never ran")
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
	// The running agent's inbox stays empty: a note is for humans.
	in := h.inbox(0)
	if items, err := in.Drain(ctx); err != nil || len(items) != 0 {
		t.Fatalf("agent inbox after a note: %d items, err %v; want empty", len(items), err)
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
	if len(notes) != 1 || len(notes[0]) > 4096 || len(notes[0]) < 4096-3 || !strings.HasSuffix(notes[0], "…") || !utf8.ValidString(notes[0]) {
		t.Fatalf("long note stored %d bytes, want 4093..4096 bytes of valid UTF-8 ending in …", len(notes[0]))
	}
	// ASCII text over the cap is cut to exactly 4096 bytes with the ellipsis.
	if err := svc2.PostRoomNote(ctx, other.ID, strings.Repeat("a", 6000)); err != nil {
		t.Fatalf("long ascii note: %v", err)
	}
	notes = systemTexts(t, svc2, other.ID)
	if got := notes[len(notes)-1]; len(got) != 4096 || !strings.HasSuffix(got, "a…") {
		t.Fatalf("long ascii note stored %d bytes, want exactly 4096 ending in a…", len(got))
	}
}

// Evidence with NUL bytes or invalid UTF-8 (Connect trace text) is kept with
// U+FFFD in their place, never refused and never lost: Postgres text and jsonb
// reject both. The replacement happens before the length cut.
func TestPostRoomNoteMakesTextStorable(t *testing.T) {
	rep := string(rune(0xFFFD))
	h := newWatchHarness(t)
	svc := h.build()
	ctx := context.Background()
	w, _, err := svc.OpenAlertRoom(ctx, watcherAlert("orders", alertin.StatusFiring))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := svc.PostRoomNote(ctx, w.ID, "task 0 failed: \x00org.apache.kafka\x00 \xff\xfe trace"); err != nil {
		t.Fatalf("note with NUL and bad UTF-8: %v", err)
	}
	if err := svc.PostRoomNote(ctx, w.ID, "\x00\x00\x00"); err != nil {
		t.Fatalf("note of only NULs: %v", err)
	}
	// NULs right at the cut: the text is replaced first, so the cut sees the
	// 3-byte replacement characters and still lands on a boundary.
	if err := svc.PostRoomNote(ctx, w.ID, strings.Repeat("\x00", 3000)); err != nil {
		t.Fatalf("long note of NULs: %v", err)
	}
	notes := systemTexts(t, svc, w.ID)
	if len(notes) != 3 {
		t.Fatalf("notes = %q, want 3", notes)
	}
	if want := "task 0 failed: " + rep + "org.apache.kafka" + rep + " " + rep + " trace"; notes[0] != want {
		t.Fatalf("note 0 = %q, want %q", notes[0], want)
	}
	if notes[1] != rep+rep+rep {
		t.Fatalf("note 1 = %q, want three U+FFFD", notes[1])
	}
	for i, n := range notes {
		if strings.ContainsRune(n, 0) || !utf8.ValidString(n) || len(n) > 4096 {
			t.Fatalf("note %d is not storable: %d bytes", i, len(n))
		}
	}
}

func TestOpenRoomForPipelineTieAndBadID(t *testing.T) {
	h := newWatchHarness(t)
	svc := h.build()
	ctx := context.Background()
	arjun := mustUser(t, svc, "arjun")

	// Two rooms opened at the same instant: the smaller id wins, as in the stores.
	a, err := svc.CreateRoom(ctx, arjun, "a", "sev3", "orders")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	b, err := svc.CreateRoom(ctx, arjun, "b", "sev3", "orders")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	want := a.ID
	if b.ID < a.ID {
		want = b.ID
	}
	if id, ok := svc.OpenRoomForPipeline("orders"); !ok || id != want {
		t.Fatalf("tie: got %q %v, want smaller id %q", id, ok, want)
	}

	// A pipeline id outside the accepted charset matches nothing.
	for _, bad := range []string{"", "or ders", "orders|x", "ord\x00ers", "a/b"} {
		if id, ok := svc.OpenRoomForPipeline(bad); ok {
			t.Errorf("OpenRoomForPipeline(%q) = %q, want none", bad, id)
		}
		if id, _, ok := svc.LastRoomForKey(bad, "watcher:orders"); ok {
			t.Errorf("LastRoomForKey(%q) = %q, want none", bad, id)
		}
	}
}
