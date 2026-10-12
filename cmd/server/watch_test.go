package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"piperoom/internal/alertin"
	"piperoom/internal/config"
	"piperoom/internal/room"
	"piperoom/internal/watch"
)

func discardLog() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// ---- config ----

func TestWatchConfigBadValuesNameTheVariable(t *testing.T) {
	for _, tc := range []struct {
		name string
		set  func(*config.Config)
		want string
	}{
		{"mode", func(c *config.Config) { c.WatchMode = "loud" }, "PIPEROOM_WATCH_MODE"},
		{"interval text", func(c *config.Config) { c.WatchInterval = "often" }, "PIPEROOM_WATCH_INTERVAL"},
		{"interval too small", func(c *config.Config) { c.WatchInterval = "1s" }, "PIPEROOM_WATCH_INTERVAL"},
		{"check timeout text", func(c *config.Config) { c.WatchCheckTimeout = "soon" }, "PIPEROOM_WATCH_CHECK_TIMEOUT"},
		{"check timeout too big", func(c *config.Config) { c.WatchCheckTimeout = "5m" }, "PIPEROOM_WATCH_CHECK_TIMEOUT"},
		{"cooldown zero", func(c *config.Config) { c.WatchCooldown = "0s" }, "PIPEROOM_WATCH_COOLDOWN"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, demo := range []bool{false, true} {
				cfg := config.Load()
				tc.set(&cfg)
				_, err := watchConfig(cfg, demo, discardLog())
				if err == nil || !strings.Contains(err.Error(), tc.want) {
					t.Fatalf("demo=%v: err = %v, want it to name %s", demo, err, tc.want)
				}
			}
		})
	}
}

func TestWatchConfigDefaultsAndDemo(t *testing.T) {
	cfg := config.Load()
	cfg.WatchMode, cfg.WatchInterval, cfg.WatchCheckTimeout, cfg.WatchCooldown = "", "", "", ""
	wc, err := watchConfig(cfg, false, discardLog())
	if err != nil {
		t.Fatal(err)
	}
	if wc.Mode != config.WatchLog || wc.Interval != time.Minute {
		t.Fatalf("defaults = %+v, want mode log and 60s", wc)
	}

	cfg.WatchMode = "rooms"
	if wc, err = watchConfig(cfg, false, discardLog()); err != nil || wc.Mode != config.WatchRooms {
		t.Fatalf("rooms: %+v, %v", wc, err)
	}
	// --demo forces off whatever was asked for.
	for _, m := range []string{"rooms", "log", "off", ""} {
		cfg.WatchMode = m
		if wc, err = watchConfig(cfg, true, discardLog()); err != nil || wc.Mode != config.WatchOff {
			t.Fatalf("demo with mode %q: %+v, %v; want off", m, wc, err)
		}
	}
}

// ---- mode off ----

func TestStartWatcherModeOffBuildsNoWatcher(t *testing.T) {
	// Owners is nil: watch.New would refuse these deps, so the nil error shows
	// that mode off never reaches it.
	hook := &roomClosedHook{}
	stop, err := startWatcher(context.Background(), watch.Deps{Config: config.Watch{Mode: config.WatchOff}}, hook, discardLog())
	if err != nil {
		t.Fatalf("mode off: %v", err)
	}
	stop()
	stop() // a no-op, twice is fine
	if hook.watcher != nil {
		t.Fatal("mode off installed a watcher")
	}
	// Mode log or rooms with broken deps does fail, so the check above is real.
	if _, err := startWatcher(context.Background(), watch.Deps{Config: config.Watch{Mode: config.WatchLog}}, hook, discardLog()); err == nil {
		t.Fatal("mode log without owners: want an error")
	}
}

// ---- adapter ----

type fakeRoomSvc struct {
	snap     room.RoomSnapshot
	created  bool
	openErr  error
	postErr  error
	pipeline string
	gotAlert alertin.Alert
	gotPost  [2]string
}

func (f *fakeRoomSvc) OpenAlertRoom(_ context.Context, a alertin.Alert) (room.RoomSnapshot, bool, error) {
	f.gotAlert = a
	return f.snap, f.created, f.openErr
}

func (f *fakeRoomSvc) OpenRoomForPipeline(p string) (string, bool) {
	f.pipeline = p
	return "r_open", true
}

func (f *fakeRoomSvc) LastRoomForKey(p, k string) (string, *time.Time, bool) {
	at := time.Date(2026, 10, 13, 1, 2, 3, 0, time.UTC)
	return "r_" + p + "_" + k, &at, true
}

func (f *fakeRoomSvc) PostRoomNote(_ context.Context, roomID, text string) error {
	f.gotPost = [2]string{roomID, text}
	return f.postErr
}

func TestAdapterOpenAlertRoom(t *testing.T) {
	boom := errors.New("store down")
	for _, tc := range []struct {
		name    string
		svc     *fakeRoomSvc
		wantID  string
		wantNew bool
		is      []error // errors.Is must hold for each
		isNot   []error
	}{
		{name: "created", svc: &fakeRoomSvc{snap: room.RoomSnapshot{ID: "r_1"}, created: true}, wantID: "r_1", wantNew: true},
		{name: "joined", svc: &fakeRoomSvc{snap: room.RoomSnapshot{ID: "r_2"}}, wantID: "r_2"},
		{
			name: "resolved with no room",
			svc:  &fakeRoomSvc{openErr: fmt.Errorf("wrapped: %w", room.ErrResolvedAlertNoRoom)},
			is:   []error{watch.ErrNoRoom, room.ErrResolvedAlertNoRoom}, isNot: []error{watch.ErrRoomClosed},
		},
		{name: "other error passes through", svc: &fakeRoomSvc{openErr: boom}, is: []error{boom}, isNot: []error{watch.ErrNoRoom, watch.ErrRoomClosed}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			id, created, err := watchRooms{tc.svc}.OpenAlertRoom(context.Background(), alertin.Alert{PipelineID: "orders", DedupKey: "watcher:orders"})
			if len(tc.is)+len(tc.isNot) == 0 {
				if err != nil || id != tc.wantID || created != tc.wantNew {
					t.Fatalf("got (%q, %v, %v), want (%q, %v, nil)", id, created, err, tc.wantID, tc.wantNew)
				}
				return
			}
			if err == nil || id != "" || created {
				t.Fatalf("got (%q, %v, %v), want an error", id, created, err)
			}
			for _, e := range tc.is {
				if !errors.Is(err, e) {
					t.Errorf("err %v: want errors.Is %v", err, e)
				}
			}
			for _, e := range tc.isNot {
				if errors.Is(err, e) {
					t.Errorf("err %v: must not be %v", err, e)
				}
			}
			if tc.svc.gotAlert.PipelineID != "orders" {
				t.Error("the alert was not passed to the service")
			}
		})
	}
}

func TestAdapterPassThroughs(t *testing.T) {
	svc := &fakeRoomSvc{}
	a := watchRooms{svc}
	if id, ok := a.OpenRoomForPipeline("orders"); id != "r_open" || !ok || svc.pipeline != "orders" {
		t.Fatalf("OpenRoomForPipeline = %q, %v", id, ok)
	}
	id, at, ok := a.LastRoomForKey("orders", "watcher:orders")
	if id != "r_orders_watcher:orders" || at == nil || !ok {
		t.Fatalf("LastRoomForKey = %q, %v, %v", id, at, ok)
	}
}

func TestAdapterPostEvidence(t *testing.T) {
	boom := errors.New("db down")
	closed := &room.Error{Status: 409, Code: "room_closed", Msg: "room already closed"}
	missing := &room.Error{Status: 404, Code: "not_found", Msg: "room not found"}
	invalid := &room.Error{Status: 400, Code: "validation", Msg: "note text is required"}
	for _, tc := range []struct {
		name  string
		err   error
		is    []error
		isNot []error
	}{
		{name: "ok"},
		{name: "closed", err: closed, is: []error{watch.ErrRoomClosed, closed}, isNot: []error{watch.ErrNoRoom}},
		{name: "closed, wrapped", err: fmt.Errorf("room: post: %w", closed), is: []error{watch.ErrRoomClosed}},
		// A gone room is both: ErrNoRoom as asked, and ErrRoomClosed so the
		// watcher stops retrying (it only handles ErrRoomClosed from a post).
		{name: "not found", err: missing, is: []error{watch.ErrNoRoom, watch.ErrRoomClosed, missing}},
		{name: "other room error", err: invalid, is: []error{invalid}, isNot: []error{watch.ErrRoomClosed, watch.ErrNoRoom}},
		{name: "plain error", err: boom, is: []error{boom}, isNot: []error{watch.ErrRoomClosed, watch.ErrNoRoom}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			svc := &fakeRoomSvc{postErr: tc.err}
			err := watchRooms{svc}.PostEvidence(context.Background(), "r_1", "evidence")
			if svc.gotPost != [2]string{"r_1", "evidence"} {
				t.Fatalf("service got %v", svc.gotPost)
			}
			if tc.err == nil {
				if err != nil {
					t.Fatalf("err = %v", err)
				}
				return
			}
			for _, e := range tc.is {
				if !errors.Is(err, e) {
					t.Errorf("err %v: want errors.Is %v", err, e)
				}
			}
			for _, e := range tc.isNot {
				if errors.Is(err, e) {
					t.Errorf("err %v: must not be %v", err, e)
				}
			}
		})
	}
}

// ---- hook ----

type recWatcher struct {
	mu    sync.Mutex
	calls [][3]string
	// inner, when set, gets every call after it is recorded.
	inner roomClosedWatcher
}

func (r *recWatcher) OnRoomClosed(roomID, pipelineID, dedupKey string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, [3]string{roomID, pipelineID, dedupKey})
	if r.inner != nil {
		r.inner.OnRoomClosed(roomID, pipelineID, dedupKey)
	}
}

type recForget struct {
	mu    sync.Mutex
	rooms []string
}

func (r *recForget) Forget(roomID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.rooms = append(r.rooms, roomID)
}

func TestHookCallsWatcherAndForget(t *testing.T) {
	h := &roomClosedHook{}
	w, f := &recWatcher{}, &recForget{}
	h.setWatcher(w)
	h.setForget(f)
	h.onClosed(context.Background(), "r_1", "orders", "watcher:orders")
	h.onClosed(context.Background(), "r_2", "billing", "")
	if len(w.calls) != 2 || w.calls[0] != [3]string{"r_1", "orders", "watcher:orders"} || w.calls[1] != [3]string{"r_2", "billing", ""} {
		t.Fatalf("watcher calls = %v", w.calls)
	}
	if len(f.rooms) != 2 || f.rooms[0] != "r_1" || f.rooms[1] != "r_2" {
		t.Fatalf("forget calls = %v", f.rooms)
	}
}

func TestHookWithoutWatcherStillForgets(t *testing.T) {
	// The service is built before the watcher: a close in between, or mode off,
	// has no watcher. It must not panic, and Forget (the #49 follow-up) still runs.
	h := &roomClosedHook{}
	f := &recForget{}
	h.setForget(f)
	h.onClosed(context.Background(), "r_1", "orders", "watcher:orders")
	if len(f.rooms) != 1 || f.rooms[0] != "r_1" {
		t.Fatalf("forget calls = %v", f.rooms)
	}
	// Nothing at all set (demo has no endpoint): still safe.
	(&roomClosedHook{}).onClosed(context.Background(), "r_1", "orders", "")
}

func TestHookIsSafeWhenWatcherIsSetLate(t *testing.T) {
	h := &roomClosedHook{}
	f := &recForget{}
	h.setForget(f)
	var wg sync.WaitGroup
	w := &recWatcher{}
	for i := range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if i == 25 {
				h.setWatcher(w)
			}
			h.onClosed(context.Background(), fmt.Sprintf("r_%d", i), "orders", "")
		}()
	}
	wg.Wait()
	if len(f.rooms) != 50 {
		t.Fatalf("forget ran %d times, want 50", len(f.rooms))
	}
}

// ---- start and stop ----

// recRunner records the order of events around Run.
type recRunner struct {
	mu     sync.Mutex
	events *[]string
	// ignoreCtx keeps Run alive until release is closed, like a client that
	// ignores cancellation.
	ignoreCtx bool
	release   chan struct{}
	started   chan struct{}
}

func (r *recRunner) rec(s string) {
	r.mu.Lock()
	*r.events = append(*r.events, s)
	r.mu.Unlock()
}

func (r *recRunner) Run(ctx context.Context) error {
	r.rec("watch run")
	close(r.started)
	if r.ignoreCtx {
		<-r.release
	} else {
		<-ctx.Done()
	}
	r.rec("watch returned")
	return nil
}

func TestStartWatchStopWaitsForRun(t *testing.T) {
	var evs []string
	r := &recRunner{events: &evs, started: make(chan struct{})}
	stop := startWatch(context.Background(), r, time.Second, discardLog())
	<-r.started
	stop()
	r.mu.Lock()
	got := strings.Join(evs, ",")
	r.mu.Unlock()
	if got != "watch run,watch returned" {
		t.Fatalf("events = %s; stop must not return before Run did", got)
	}
	stop() // idempotent
}

func TestStartWatchStopGivesUpOnAStuckRun(t *testing.T) {
	var evs []string
	r := &recRunner{events: &evs, started: make(chan struct{}), ignoreCtx: true, release: make(chan struct{})}
	defer close(r.release)
	stop := startWatch(context.Background(), r, 50*time.Millisecond, discardLog())
	<-r.started
	begin := time.Now()
	stop()
	if d := time.Since(begin); d < 40*time.Millisecond || d > 2*time.Second {
		t.Fatalf("stop took %v, want about the 50ms timeout", d)
	}
}

type recServer struct{ events *[]string }

func (s recServer) Shutdown(context.Context) error {
	*s.events = append(*s.events, "http shutdown")
	return nil
}
func (s recServer) Close() { *s.events = append(*s.events, "service close") }

// The whole sequence the server follows: load, then the watcher starts; on
// shutdown the watcher is stopped (and has returned) before the HTTP server
// shuts down and before the service closes.
func TestStartStopOrder(t *testing.T) {
	var evs []string
	load := func() { evs = append(evs, "load") }

	load()
	r := &recRunner{events: &evs, started: make(chan struct{})}
	stop := startWatch(context.Background(), r, time.Second, discardLog())
	<-r.started

	srv := recServer{&evs}
	shutdownServer(stop, srv, srv)

	want := "load,watch run,watch returned,http shutdown,service close"
	if got := strings.Join(evs, ","); got != want {
		t.Fatalf("order = %s, want %s", got, want)
	}
}

// startWatcher must not start anything but a watcher that was asked for, and it
// hands the watcher to the hook before Run starts.
func TestStartWatcherInstallsTheWatcherInTheHook(t *testing.T) {
	hook := &roomClosedHook{}
	o := loadTestOwners(t)
	stop, err := startWatcher(context.Background(), watch.Deps{
		Owners: o, Config: config.Watch{Mode: config.WatchLog, Interval: time.Minute},
		Clock: newFakeClock(), Jitter: func(time.Duration) time.Duration { return 0 },
	}, hook, discardLog())
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	if hook.watcher == nil {
		t.Fatal("the hook has no watcher")
	}
}

func TestWatchedPipelines(t *testing.T) {
	if watchedPipelines(nil) != 0 {
		t.Fatal("nil owners")
	}
	if n := watchedPipelines(loadTestOwners(t)); n != 1 {
		t.Fatalf("watched = %d, want 1", n)
	}
}
