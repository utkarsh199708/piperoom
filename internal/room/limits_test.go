package room_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	"piperoom/internal/actions"
	"piperoom/internal/agent"
	"piperoom/internal/config"
	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/room"
	"piperoom/internal/store"
	"piperoom/internal/store/memstore"
)

// runRecorder is an agent runner that hands each run's sink to the test and
// counts starts and stops.
type runRecorder struct {
	mu     sync.Mutex
	starts int
	stops  int
	sinks  chan agent.Sink
}

func newRunRecorder() *runRecorder { return &runRecorder{sinks: make(chan agent.Sink, 8)} }

func (r *runRecorder) run(ctx context.Context, _ agent.Alert, deps agent.Deps, _ agent.Options) error {
	r.mu.Lock()
	r.starts++
	r.mu.Unlock()
	r.sinks <- deps.Sink
	<-ctx.Done()
	r.mu.Lock()
	r.stops++
	r.mu.Unlock()
	return nil
}

func (r *runRecorder) counts() (starts, stops int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.starts, r.stops
}

func (r *runRecorder) sink(t *testing.T) agent.Sink {
	t.Helper()
	select {
	case s := <-r.sinks:
		return s
	case <-time.After(5 * time.Second):
		t.Fatal("the agent did not start")
		return nil
	}
}

// adminUsers is the demo directory plus ada, an admin with no layers.
func adminUsers(t *testing.T) *room.Users {
	t.Helper()
	u, err := room.ParseUsers([]byte(`
users:
  - id: arjun
    name: Arjun
    layers: [kafka-connect, kafka]
  - id: priya
    name: Priya
    layers: [postgres, schema-registry]
  - id: meera
    name: Meera
    layers: [warehouse]
  - id: ada
    name: Ada
    admin: true
`))
	if err != nil {
		t.Fatalf("parse users: %v", err)
	}
	return u
}

type limitsEnv struct {
	svc    *room.Service
	st     *memstore.Store
	clock  *clock
	runs   *runRecorder
	closed []string // room ids the room-closed hook saw
	mu     sync.Mutex
}

func (e *limitsEnv) hookCalls() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]string(nil), e.closed...)
}

// newLimitsEnv builds a service on a memstore with the given limits; the
// store gets the same log caps the server would give it.
func newLimitsEnv(t *testing.T, l config.Limits, act ...actions.Action) *limitsEnv {
	t.Helper()
	e := &limitsEnv{clock: &clock{t: time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)}, runs: newRunRecorder()}
	e.st = memstore.NewWithClock(e.clock.Now)
	e.st.SetLimits(store.Limits{MaxEvents: l.RoomMaxEvents, MaxBytes: l.RoomMaxBytes})
	reg := actions.NewRegistry()
	for _, a := range act {
		if err := reg.Register(a); err != nil {
			t.Fatal(err)
		}
	}
	e.svc = room.New(room.Deps{
		Store: e.st, Actions: reg, Users: adminUsers(t), Owners: testOwners(),
		Run: e.runs.run, WriteHandoff: fakeWrite, Now: e.clock.Now, Limits: l,
		OnRoomClosed: func(_ context.Context, roomID, _, _ string) {
			e.mu.Lock()
			e.closed = append(e.closed, roomID)
			e.mu.Unlock()
		},
	})
	t.Cleanup(e.svc.Close)
	return e
}

func typedEvents(t *testing.T, st *memstore.Store, roomID string, typ events.Type) []events.Event {
	t.Helper()
	var out []events.Event
	for after := int64(0); ; {
		evs, err := st.Since(context.Background(), roomID, after, 1000)
		if err != nil {
			t.Fatalf("events: %v", err)
		}
		for _, e := range evs {
			if typ == "" || e.Type == typ { // "" = every event
				out = append(out, e)
			}
			after = e.Seq
		}
		if len(evs) < 1000 {
			return out
		}
	}
}

// ---- open-room limit ----

// TestOpenLimitWarnsThenCloses: no warning before deadline - 24 h, one warning
// at it (not repeated), no close a moment before the deadline, a system close
// with the retention resolution at it, and the room-closed hook runs.
func TestOpenLimitWarnsThenCloses(t *testing.T) {
	e := newLimitsEnv(t, config.Limits{MaxOpenDays: 14})
	ctx := context.Background()
	snap := createRoom(t, e.svc, "arjun")
	opened := e.clock.Now()
	deadline := opened.AddDate(0, 0, 14)
	if snap.AutoCloseAt == nil || !snap.AutoCloseAt.Equal(deadline) {
		t.Fatalf("auto_close_at = %v, want %v", snap.AutoCloseAt, deadline)
	}

	e.clock.Advance(13*24*time.Hour - time.Minute)
	if res, err := e.svc.EnforceOpenLimit(ctx); err != nil || res != (room.OpenLimitResult{}) {
		t.Fatalf("a minute before the warning: %+v %v", res, err)
	}
	e.clock.Advance(time.Minute)
	if res, err := e.svc.EnforceOpenLimit(ctx); err != nil || res.Warned != 1 {
		t.Fatalf("at deadline - 24h: %+v %v, want one warning", res, err)
	}
	if res, _ := e.svc.EnforceOpenLimit(ctx); res.Warned != 0 {
		t.Fatalf("the warning repeated: %+v", res)
	}
	warns := typedEvents(t, e.st, snap.ID, events.RoomRetentionWarning)
	if len(warns) != 1 || warns[0].Actor.Kind != events.ActorSystem {
		t.Fatalf("warnings = %+v", warns)
	}
	w, _ := events.Decode[events.RoomRetentionWarningPayload](warns[0])
	if !w.CloseAt.Equal(deadline) || w.OpenDays != 14 {
		t.Fatalf("warning payload = %+v", w)
	}

	e.clock.Advance(24*time.Hour - time.Nanosecond)
	if res, _ := e.svc.EnforceOpenLimit(ctx); res.Closed != 0 {
		t.Fatalf("closed before the deadline: %+v", res)
	}
	e.clock.Advance(time.Nanosecond)
	if res, err := e.svc.EnforceOpenLimit(ctx); err != nil || res.Closed != 1 {
		t.Fatalf("at the deadline: %+v %v, want one close", res, err)
	}
	got, err := e.svc.GetRoom(ctx, snap.ID)
	if err != nil || got.Status != "closed" || got.Resolution != "retention: open longer than 14 days" || got.AutoCloseAt != nil {
		t.Fatalf("after the deadline: %+v %v", got, err)
	}
	closes := typedEvents(t, e.st, snap.ID, events.RoomClosed)
	if len(closes) != 1 || closes[0].Actor.Kind != events.ActorSystem {
		t.Fatalf("room.closed = %+v", closes)
	}
	if hooks := e.hookCalls(); len(hooks) != 1 || hooks[0] != snap.ID {
		t.Fatalf("room-closed hook calls = %v", hooks)
	}
	// The closed room is now under closed-room retention, which a later pass
	// does not touch.
	e.clock.Advance(30 * 24 * time.Hour)
	if res, _ := e.svc.EnforceOpenLimit(ctx); res != (room.OpenLimitResult{}) {
		t.Fatalf("closed room touched again: %+v", res)
	}
}

// TestExtendOnce: only the driver, only once, only while open; the deadline
// moves by N days, gets its own warning, and the close names the full span.
func TestExtendOnce(t *testing.T) {
	e := newLimitsEnv(t, config.Limits{MaxOpenDays: 14})
	ctx := context.Background()
	snap := createRoom(t, e.svc, "arjun")
	opened := e.clock.Now()

	_, err := e.svc.ExtendRoom(ctx, mustUser(t, e.svc, "priya"), snap.ID)
	wantCode(t, err, "not_driver")
	if _, err := e.svc.ExtendRoom(ctx, mustUser(t, e.svc, "arjun"), "r_missing"); err == nil {
		t.Fatal("extend of an unknown room succeeded")
	}

	// Warned for the first deadline, then extended.
	e.clock.Advance(13 * 24 * time.Hour)
	if res, _ := e.svc.EnforceOpenLimit(ctx); res.Warned != 1 {
		t.Fatalf("first warning: %+v", res)
	}
	ev, err := e.svc.ExtendRoom(ctx, mustUser(t, e.svc, "arjun"), snap.ID)
	if err != nil {
		t.Fatalf("extend: %v", err)
	}
	p, _ := events.Decode[events.RoomExtendedPayload](ev)
	newDeadline := opened.AddDate(0, 0, 28)
	if ev.Type != events.RoomExtended || p.Days != 14 || !p.CloseAt.Equal(newDeadline) {
		t.Fatalf("extend event = %s %+v", ev.Type, p)
	}
	got, _ := e.svc.GetRoom(ctx, snap.ID)
	if !got.Extended || got.AutoCloseAt == nil || !got.AutoCloseAt.Equal(newDeadline) {
		t.Fatalf("snapshot after extend: extended=%v auto_close_at=%v", got.Extended, got.AutoCloseAt)
	}
	_, err = e.svc.ExtendRoom(ctx, mustUser(t, e.svc, "arjun"), snap.ID)
	wantCode(t, err, "already_extended")

	// The old deadline passes: not closed.
	e.clock.Advance(2 * 24 * time.Hour)
	if res, _ := e.svc.EnforceOpenLimit(ctx); res.Closed != 0 || res.Warned != 0 {
		t.Fatalf("after the old deadline: %+v", res)
	}
	// A warning for the new deadline, then the close.
	e.clock.Advance(12 * 24 * time.Hour) // day 27
	if res, _ := e.svc.EnforceOpenLimit(ctx); res.Warned != 1 {
		t.Fatalf("second warning: %+v", res)
	}
	if n := len(typedEvents(t, e.st, snap.ID, events.RoomRetentionWarning)); n != 2 {
		t.Fatalf("warnings = %d, want 2 (one per deadline)", n)
	}
	e.clock.Advance(24 * time.Hour) // day 28
	if res, _ := e.svc.EnforceOpenLimit(ctx); res.Closed != 1 {
		t.Fatalf("close at the new deadline: %+v", res)
	}
	got, _ = e.svc.GetRoom(ctx, snap.ID)
	if got.Resolution != "retention: open longer than 28 days" {
		t.Fatalf("resolution = %q", got.Resolution)
	}
	_, err = e.svc.ExtendRoom(ctx, mustUser(t, e.svc, "arjun"), snap.ID)
	wantCode(t, err, "room_closed")
}

// TestOpenLimitSurvivesRestart: the extension and the warning come from the
// log, so a new service neither forgets the extension nor warns twice.
func TestOpenLimitSurvivesRestart(t *testing.T) {
	e := newLimitsEnv(t, config.Limits{MaxOpenDays: 14})
	ctx := context.Background()
	snap := createRoom(t, e.svc, "arjun")
	if _, err := e.svc.ExtendRoom(ctx, mustUser(t, e.svc, "arjun"), snap.ID); err != nil {
		t.Fatal(err)
	}
	e.clock.Advance(27 * 24 * time.Hour)
	if res, _ := e.svc.EnforceOpenLimit(ctx); res.Warned != 1 {
		t.Fatalf("warning: %+v", res)
	}
	svc2 := room.New(room.Deps{Store: e.st, Users: adminUsers(t), Owners: testOwners(), Run: fakeRun, Now: e.clock.Now, Limits: config.Limits{MaxOpenDays: 14}})
	t.Cleanup(svc2.Close)
	if err := svc2.Load(ctx); err != nil {
		t.Fatal(err)
	}
	got, _ := svc2.GetRoom(ctx, snap.ID)
	if !got.Extended || got.AutoCloseAt == nil || !got.AutoCloseAt.Equal(snap.OpenedAt.AddDate(0, 0, 28)) {
		t.Fatalf("after restart: extended=%v auto_close_at=%v", got.Extended, got.AutoCloseAt)
	}
	if res, _ := svc2.EnforceOpenLimit(ctx); res.Warned != 0 || res.Closed != 0 {
		t.Fatalf("restart re-warned or closed early: %+v", res)
	}
	_, err := svc2.ExtendRoom(ctx, mustUser(t, svc2, "arjun"), snap.ID)
	wantCode(t, err, "already_extended")
}

// TestOpenLimitAlwaysWarnsADayAhead: a room already past its deadline when
// the limit first looks (the limit was just turned on) is warned with a close
// time 24 h out and closed only then, never at once.
func TestOpenLimitAlwaysWarnsADayAhead(t *testing.T) {
	e := newLimitsEnv(t, config.Limits{MaxOpenDays: 14})
	ctx := context.Background()
	snap := createRoom(t, e.svc, "arjun")
	e.clock.Advance(30 * 24 * time.Hour) // long past the 14-day deadline
	warnedAt := e.clock.Now()
	if res, _ := e.svc.EnforceOpenLimit(ctx); res.Warned != 1 || res.Closed != 0 {
		t.Fatalf("first pass past the deadline: %+v, want a warning and no close", res)
	}
	w, _ := events.Decode[events.RoomRetentionWarningPayload](typedEvents(t, e.st, snap.ID, events.RoomRetentionWarning)[0])
	if !w.CloseAt.Equal(warnedAt.Add(24 * time.Hour)) {
		t.Fatalf("warning close_at = %v, want %v", w.CloseAt, warnedAt.Add(24*time.Hour))
	}
	if got, _ := e.svc.GetRoom(ctx, snap.ID); got.AutoCloseAt == nil || !got.AutoCloseAt.Equal(w.CloseAt) {
		t.Fatalf("auto_close_at = %v, want the warned close %v", got.AutoCloseAt, w.CloseAt)
	}
	e.clock.Advance(24*time.Hour - time.Second)
	if res, _ := e.svc.EnforceOpenLimit(ctx); res != (room.OpenLimitResult{}) {
		t.Fatalf("before the warned close: %+v", res)
	}
	e.clock.Advance(time.Second)
	if res, _ := e.svc.EnforceOpenLimit(ctx); res.Closed != 1 {
		t.Fatalf("at the warned close: %+v", res)
	}
	if got, _ := e.svc.GetRoom(ctx, snap.ID); got.Resolution != "retention: open longer than 31 days" {
		t.Fatalf("resolution = %q", got.Resolution)
	}
}

// TestExtendAfterLateWarning: the extension counts from the close time the
// room shows, not from the old nominal deadline.
func TestExtendAfterLateWarning(t *testing.T) {
	e := newLimitsEnv(t, config.Limits{MaxOpenDays: 14})
	ctx := context.Background()
	snap := createRoom(t, e.svc, "arjun")
	e.clock.Advance(30 * 24 * time.Hour)
	if res, _ := e.svc.EnforceOpenLimit(ctx); res.Warned != 1 {
		t.Fatalf("warning: %+v", res)
	}
	shown, _ := e.svc.GetRoom(ctx, snap.ID)
	ev, err := e.svc.ExtendRoom(ctx, mustUser(t, e.svc, "arjun"), snap.ID)
	if err != nil {
		t.Fatal(err)
	}
	p, _ := events.Decode[events.RoomExtendedPayload](ev)
	if want := shown.AutoCloseAt.AddDate(0, 0, 14); !p.CloseAt.Equal(want) {
		t.Fatalf("extended close_at = %v, want %v", p.CloseAt, want)
	}
	e.clock.Advance(2 * 24 * time.Hour)
	if res, _ := e.svc.EnforceOpenLimit(ctx); res.Closed != 0 {
		t.Fatalf("closed after the extension: %+v", res)
	}
}

func TestOpenLimitOff(t *testing.T) {
	e := newLimitsEnv(t, config.Limits{})
	ctx := context.Background()
	snap := createRoom(t, e.svc, "arjun")
	if snap.AutoCloseAt != nil {
		t.Fatalf("auto_close_at = %v with the limit off", snap.AutoCloseAt)
	}
	e.clock.Advance(1000 * 24 * time.Hour)
	if res, err := e.svc.EnforceOpenLimit(ctx); err != nil || res != (room.OpenLimitResult{}) {
		t.Fatalf("limit off: %+v %v", res, err)
	}
	if got, _ := e.svc.GetRoom(ctx, snap.ID); got.Status != "open" {
		t.Fatal("room closed with the limit off")
	}
	_, err := e.svc.ExtendRoom(ctx, mustUser(t, e.svc, "arjun"), snap.ID)
	wantCode(t, err, "validation")
}

// ---- log caps ----

// TestLogCapWarningAndFull: at 90% one warning; at 100% the agent's run is
// cancelled, agent.status log_full is written once by the system, agent events
// are refused, a steer does not start the agent again (not even after a
// restart), people keep the 2% reserve, and the room can still be closed.
func TestLogCapWarningAndFull(t *testing.T) {
	l := config.Limits{RoomMaxEvents: 1000, RoomMaxBytes: 1 << 30}
	e := newLimitsEnv(t, l)
	ctx := context.Background()
	snap := createRoom(t, e.svc, "arjun") // room.created + control.granted
	sink := e.runs.sink(t)

	var refused error
	for i := 0; i < 2000 && refused == nil; i++ {
		refused = sink.Emit(ctx, events.AgentMessage, events.AgentMessagePayload{Text: "step"})
	}
	if !errors.Is(refused, store.ErrLogFull) {
		t.Fatalf("agent emit at the cap: %v, want ErrLogFull", refused)
	}
	warns := typedEvents(t, e.st, snap.ID, events.RoomLogCapWarning)
	if len(warns) != 1 || warns[0].Seq != 901 || warns[0].Actor.Kind != events.ActorSystem {
		t.Fatalf("cap warnings = %+v, want one at seq 901 (after the 900th event)", warns)
	}
	wp, _ := events.Decode[events.RoomLogCapWarningPayload](warns[0])
	if wp.Events != 900 || wp.MaxEvents != 1000 {
		t.Fatalf("warning payload = %+v", wp)
	}
	var full []events.Event
	agentEvents := 0
	for _, ev := range typedEvents(t, e.st, snap.ID, "") {
		if ev.Actor.Kind == events.ActorAgent {
			agentEvents++
		}
		if ev.Type == events.AgentStatus {
			if p, _ := events.Decode[events.AgentStatusPayload](ev); p.State == room.AgentLogFull {
				full = append(full, ev)
			}
		}
	}
	if len(full) != 1 || full[0].Actor.Kind != events.ActorSystem {
		t.Fatalf("log_full statuses = %+v, want one by the system", full)
	}
	if agentEvents > 1000 {
		t.Fatalf("agent events = %d, over the cap", agentEvents)
	}
	waitFor(t, 5*time.Second, func() bool { _, stops := e.runs.counts(); return stops == 1 })
	got, _ := e.svc.GetRoom(ctx, snap.ID)
	if got.LogCap != room.LogCapFull || got.AgentStatus.State != room.AgentLogFull {
		t.Fatalf("snapshot log_cap=%q agent=%q", got.LogCap, got.AgentStatus.State)
	}
	if err := sink.Emit(ctx, events.AgentMessage, events.AgentMessagePayload{Text: "more"}); !errors.Is(err, store.ErrLogFull) {
		t.Fatalf("agent emit after full: %v", err)
	}

	// The driver may still steer (reserve), but no agent starts.
	if _, err := e.svc.Steer(ctx, mustUser(t, e.svc, "arjun"), snap.ID, "keep going"); err != nil {
		t.Fatalf("steer in the reserve: %v", err)
	}
	time.Sleep(50 * time.Millisecond)
	if starts, _ := e.runs.counts(); starts != 1 {
		t.Fatalf("agent started %d times, want 1 (a full room never restarts it)", starts)
	}

	// A restart keeps the room stopped.
	runs2 := newRunRecorder()
	svc2 := room.New(room.Deps{Store: e.st, Users: adminUsers(t), Owners: testOwners(), Run: runs2.run, Now: e.clock.Now, Limits: l})
	t.Cleanup(svc2.Close)
	_ = svc2.Load(ctx) // the restart notice may not fit; that is logged, not fatal
	if g, _ := svc2.GetRoom(ctx, snap.ID); g.LogCap != room.LogCapFull {
		t.Fatalf("after restart log_cap = %q", g.LogCap)
	}
	if _, err := svc2.AddComment(ctx, mustUser(t, svc2, "priya"), snap.ID, "", "@agent are you there"); err != nil { // inside the reserve, so it is stored and would wake an agent
		t.Fatalf("comment: %v", err)
	}
	time.Sleep(50 * time.Millisecond)
	if starts, _ := runs2.counts(); starts != 0 {
		t.Fatalf("restarted service started the agent %d times", starts)
	}

	// People use up the reserve, then are refused with ErrLogFull.
	var err error
	for i := 0; i < 40 && err == nil; i++ {
		_, err = svc2.AddComment(ctx, mustUser(t, svc2, "priya"), snap.ID, "", "comment")
	}
	if !errors.Is(err, store.ErrLogFull) {
		t.Fatalf("comment past the reserve: %v, want ErrLogFull", err)
	}
	if n := len(typedEvents(t, e.st, snap.ID, "")); n != 1020 {
		t.Fatalf("events = %d, want cap + reserve = 1020", n)
	}
	// Close always works.
	if _, err := svc2.CloseRoom(ctx, mustUser(t, svc2, "arjun"), snap.ID, "done"); err != nil {
		t.Fatalf("close a full room: %v", err)
	}
}

// TestLogCapByBytesStopsOnRefusal: a byte cap the next agent event would pass
// (the count is far from its cap) stops the agent through the refusal.
func TestLogCapByBytesStopsOnRefusal(t *testing.T) {
	e := newLimitsEnv(t, config.Limits{RoomMaxEvents: 1000, RoomMaxBytes: 4000})
	ctx := context.Background()
	snap := createRoom(t, e.svc, "arjun")
	sink := e.runs.sink(t)
	big := strings.Repeat("x", 600)
	var err error
	for i := 0; i < 20 && err == nil; i++ {
		err = sink.Emit(ctx, events.AgentMessage, events.AgentMessagePayload{Text: big})
	}
	if !errors.Is(err, store.ErrLogFull) {
		t.Fatalf("agent emit past the byte cap: %v", err)
	}
	got, _ := e.svc.GetRoom(ctx, snap.ID)
	if got.LogCap != room.LogCapFull {
		t.Fatalf("log_cap = %q, want full", got.LogCap)
	}
	if n := len(typedEvents(t, e.st, snap.ID, events.RoomLogCapWarning)); n != 1 {
		t.Fatalf("warnings = %d, want 1", n)
	}
	waitFor(t, 5*time.Second, func() bool { _, stops := e.runs.counts(); return stops == 1 })
	u, _ := e.st.RoomUsage(ctx, snap.ID)
	if u.Bytes > 4000+80 {
		t.Fatalf("bytes = %d, past cap + reserve", u.Bytes)
	}
}

// ---- data cap ----

func TestToolDataTruncated(t *testing.T) {
	e := newLimitsEnv(t, config.Limits{EventMaxDataBytes: 1024})
	ctx := context.Background()
	snap := createRoom(t, e.svc, "arjun")
	sink := e.runs.sink(t)

	small := json.RawMessage(`{"rows":[1,2,3]}`)
	if err := sink.Emit(ctx, events.AgentToolResult, events.AgentToolResultPayload{StepID: "s_1", Tool: "t", Summary: "ok", Data: small}); err != nil {
		t.Fatal(err)
	}
	// Multi-byte text and characters that JSON escapes, so the preview must
	// be cut on a rune and shrunk to fit once escaped.
	bigText := strings.Repeat(`é"<`, 3000)
	big, _ := json.Marshal(map[string]string{"text": bigText})
	if err := sink.Emit(ctx, events.AgentToolResult, events.AgentToolResultPayload{StepID: "s_2", Tool: "t", Summary: "big", Data: big}); err != nil {
		t.Fatal(err)
	}
	res := typedEvents(t, e.st, snap.ID, events.AgentToolResult)
	if len(res) != 2 {
		t.Fatalf("tool results = %d", len(res))
	}
	p0, _ := events.Decode[events.AgentToolResultPayload](res[0])
	if string(p0.Data) != string(small) || p0.Truncated {
		t.Fatalf("small data changed: %s truncated=%v", p0.Data, p0.Truncated)
	}
	p1, _ := events.Decode[events.AgentToolResultPayload](res[1])
	if !p1.Truncated || len(p1.Data) > 1024 {
		t.Fatalf("big data: truncated=%v len=%d", p1.Truncated, len(p1.Data))
	}
	var td events.TruncatedData
	if err := json.Unmarshal(p1.Data, &td); err != nil {
		t.Fatalf("truncated data is not the documented object: %v (%s)", err, p1.Data)
	}
	if !td.Truncated || td.OriginalBytes != int64(len(big)) || td.Preview == "" || !utf8.ValidString(td.Preview) || !strings.HasPrefix(string(big), td.Preview) {
		t.Fatalf("truncated data = %+v (original %d bytes)", td, len(big))
	}
	if p1.Summary != "big" {
		t.Fatalf("summary changed: %q", p1.Summary)
	}
}

// longAction succeeds with an output far over the data cap.
type longAction struct{ fakeAction }

func (a *longAction) Execute(ctx context.Context, params json.RawMessage) (string, error) {
	return strings.Repeat("é", 2000), nil // 4000 bytes
}

func (a *longAction) CheckScope(p owners.Pipeline, params json.RawMessage) error { return nil }

func TestActionOutputTruncated(t *testing.T) {
	act := &longAction{fakeAction{name: "long_action", layer: "postgres"}}
	e := newLimitsEnv(t, config.Limits{EventMaxDataBytes: 1024}, act)
	ctx := context.Background()
	snap := createRoom(t, e.svc, "arjun")
	pid, err := e.svc.CreateProposal(ctx, snap.ID, events.ProposalCreatedPayload{
		Title: "fix", Risk: "low", Rollback: "re-run the previous config",
		Steps:          []events.ProposalStep{{Action: "long_action", Layer: "postgres", Params: json.RawMessage(`{}`)}},
		RequiredLayers: []string{"postgres"},
	}, events.AgentActor)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.svc.ApproveProposal(ctx, mustUser(t, e.svc, "priya"), pid, "postgres", proposalHash(t, e.svc, pid)); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 5*time.Second, func() bool { return len(typedEvents(t, e.st, snap.ID, events.ActionExecuted)) == 1 })
	p, _ := events.Decode[events.ActionExecutedPayload](typedEvents(t, e.st, snap.ID, events.ActionExecuted)[0])
	if !p.OK || len(p.Output) > 1024 || !utf8.ValidString(p.Output) || !strings.HasSuffix(p.Output, "… [truncated: 4000 bytes]") || !strings.HasPrefix(p.Output, "éé") {
		t.Fatalf("output (%d bytes) = %q", len(p.Output), p.Output)
	}
}

// ---- deletion ----

// releaseAction runs until release is closed.
type releaseAction struct {
	fakeAction
	started chan struct{}
	release chan struct{}
}

func (a *releaseAction) Execute(ctx context.Context, params json.RawMessage) (string, error) {
	close(a.started)
	select {
	case <-a.release:
		return "done", nil
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

func (a *releaseAction) CheckScope(p owners.Pipeline, params json.RawMessage) error { return nil }

// TestDeleteRefusedWhileFixRuns: a delete while an approved fix is executing
// answers 409 conflict and keeps the room; once the run ends it erases.
func TestDeleteRefusedWhileFixRuns(t *testing.T) {
	act := &releaseAction{fakeAction: fakeAction{name: "slow_fix", layer: "postgres"}, started: make(chan struct{}), release: make(chan struct{})}
	e := newLimitsEnv(t, config.Limits{}, act)
	ctx := context.Background()
	snap := createRoom(t, e.svc, "arjun")
	pid, err := e.svc.CreateProposal(ctx, snap.ID, events.ProposalCreatedPayload{
		Title: "fix", Risk: "low", Rollback: "re-run the previous config",
		Steps:          []events.ProposalStep{{Action: "slow_fix", Layer: "postgres", Params: json.RawMessage(`{}`)}},
		RequiredLayers: []string{"postgres"},
	}, events.AgentActor)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.svc.ApproveProposal(ctx, mustUser(t, e.svc, "priya"), pid, "postgres", proposalHash(t, e.svc, pid)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-act.started:
	case <-time.After(5 * time.Second):
		t.Fatal("the fix never started")
	}
	ada := mustUser(t, e.svc, "ada")
	_, err = e.svc.DeleteRoom(ctx, ada, snap.ID, "cleanup", false)
	wantCode(t, err, "conflict")
	var re *room.Error
	if !errors.As(err, &re) || re.Status != 409 {
		t.Fatalf("err = %v, want a 409", err)
	}
	if _, err := e.st.GetRoom(ctx, snap.ID); err != nil {
		t.Fatalf("room gone after a refused delete: %v", err)
	}
	// Not left in the deleting state: the executor can still record its step.
	close(act.release)
	waitFor(t, 5*time.Second, func() bool { return len(typedEvents(t, e.st, snap.ID, events.ActionExecuted)) == 1 })
	var d room.Deletion
	waitFor(t, 5*time.Second, func() bool {
		d, err = e.svc.DeleteRoom(ctx, ada, snap.ID, "cleanup", false)
		return err == nil
	})
	if d.RoomID != snap.ID {
		t.Fatalf("deletion = %+v", d)
	}
}

func TestDeleteRoomAccess(t *testing.T) {
	e := newLimitsEnv(t, config.Limits{})
	ctx := context.Background()
	snap := createRoom(t, e.svc, "arjun")

	// Not an admin: 403 whether or not the room exists, so ids do not leak.
	_, err := e.svc.DeleteRoom(ctx, mustUser(t, e.svc, "arjun"), snap.ID, "cleanup", false)
	wantCode(t, err, "not_allowed")
	_, err = e.svc.DeleteRoom(ctx, mustUser(t, e.svc, "arjun"), "r_missing", "cleanup", false)
	wantCode(t, err, "not_allowed")
	// A User value claiming admin does not count: only users.yaml does.
	_, err = e.svc.DeleteRoom(ctx, room.User{ID: "mallory", Admin: true}, snap.ID, "cleanup", false)
	wantCode(t, err, "not_allowed")

	ada := mustUser(t, e.svc, "ada")
	_, err = e.svc.DeleteRoom(ctx, ada, "r_missing", "cleanup", false)
	wantCode(t, err, "not_found")
	for _, reason := range []string{"", "   ", strings.Repeat("x", 501), "bad\x00byte"} {
		_, err = e.svc.DeleteRoom(ctx, ada, snap.ID, reason, false)
		wantCode(t, err, "validation")
	}
	if _, err := e.svc.GetRoom(ctx, snap.ID); err != nil {
		t.Fatalf("a refused delete removed the room: %v", err)
	}
	if !e.svc.IsAdmin(ada) || e.svc.IsAdmin(mustUser(t, e.svc, "arjun")) {
		t.Fatal("IsAdmin wrong")
	}
}

// TestDeleteOpenRoom: the room, its log and its rows go; the audit row stays;
// the agent stops; the hook runs; the alert key is free; a repeat answers the
// same row and changes nothing.
func TestDeleteOpenRoom(t *testing.T) {
	e := newLimitsEnv(t, config.Limits{})
	ctx := context.Background()
	snap := createRoom(t, e.svc, "arjun")
	e.runs.sink(t) // the agent is running
	if _, err := e.svc.Steer(ctx, mustUser(t, e.svc, "priya"), snap.ID, "look here"); err != nil {
		t.Fatal(err)
	}
	ada := mustUser(t, e.svc, "ada")
	e.clock.Advance(time.Hour)
	d, err := e.svc.DeleteRoom(ctx, ada, snap.ID, "pasted a password", false)
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if d.RoomID != snap.ID || d.PipelineID != "orders" || d.DeletedBy != "ada" || d.Reason != "pasted a password" ||
		d.EventCount != 3 || !d.DeletedAt.Equal(e.clock.Now()) || d.Archived {
		t.Fatalf("deletion = %+v", d)
	}
	if _, err := e.st.GetRoom(ctx, snap.ID); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("room row left: %v", err)
	}
	if _, err := e.st.Since(ctx, snap.ID, 0, 10); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("events left: %v", err)
	}
	if row, err := e.st.GetRoomDeletion(ctx, snap.ID); err != nil || row != d.RoomDeletion {
		t.Fatalf("audit row = %+v %v", row, err)
	}
	_, err = e.svc.GetRoom(ctx, snap.ID)
	wantCode(t, err, "not_found")
	if list, _ := e.svc.ListRooms(ctx); len(list) != 0 {
		t.Fatalf("room still listed: %+v", list)
	}
	waitFor(t, 5*time.Second, func() bool { _, stops := e.runs.counts(); return stops == 1 })
	if hooks := e.hookCalls(); len(hooks) != 1 || hooks[0] != snap.ID {
		t.Fatalf("hook calls = %v", hooks)
	}

	again, err := e.svc.DeleteRoom(ctx, ada, snap.ID, "second try", false)
	if err != nil || again != d {
		t.Fatalf("repeat delete = %+v %v, want the first row %+v", again, err, d)
	}
	if hooks := e.hookCalls(); len(hooks) != 1 {
		t.Fatalf("hook ran again: %v", hooks)
	}
}

// TestDeleteFreesAlertKey: a repeat of the alert after its room was deleted
// opens a new room instead of joining the deleted one.
func TestDeleteFreesAlertKey(t *testing.T) {
	e := newLimitsEnv(t, config.Limits{})
	ctx := context.Background()
	raw := json.RawMessage(`{"title":"lag","pipeline_id":"orders","dedup_key":"k1"}`)
	first, created, err := e.svc.CreateRoomFromAlert(ctx, "lag", "sev2", "orders", raw)
	if err != nil || !created {
		t.Fatalf("first alert: %v %v", created, err)
	}
	if _, err := e.svc.DeleteRoom(ctx, mustUser(t, e.svc, "ada"), first.ID, "test data", false); err != nil {
		t.Fatal(err)
	}
	second, created, err := e.svc.CreateRoomFromAlert(ctx, "lag", "sev2", "orders", raw)
	if err != nil || !created || second.ID == first.ID {
		t.Fatalf("repeat alert after delete: created=%v id=%s err=%v", created, second.ID, err)
	}
}

// TestDeleteArchivesFirst: with an archive dir the export is written there
// (0600, verifiable, identical to the export before the delete), then the
// room is deleted.
func TestDeleteArchivesFirst(t *testing.T) {
	dir := t.TempDir()
	e := newLimitsEnv(t, config.Limits{ArchiveDir: dir})
	ctx := context.Background()
	snap := createRoom(t, e.svc, "arjun")
	if _, err := e.svc.AddComment(ctx, mustUser(t, e.svc, "priya"), snap.ID, "", "evidence"); err != nil {
		t.Fatal(err)
	}
	sum, want := exportRoomFor(t, e.svc, snap.ID)
	d, err := e.svc.DeleteRoom(ctx, mustUser(t, e.svc, "ada"), snap.ID, "erasure request", true)
	if err != nil {
		t.Fatal(err)
	}
	if !d.Archived {
		t.Fatal("archived = false")
	}
	path := filepath.Join(dir, "orders", snap.ID+".json")
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatalf("archive missing: %v", err)
	}
	// NTFS has no POSIX modes (a file reads as 0666, a directory as 0777),
	// so the modes are checked on POSIX only.
	if runtime.GOOS != "windows" && fi.Mode().Perm() != 0o600 {
		t.Fatalf("archive mode = %v, want 0600", fi.Mode().Perm())
	}
	if di, _ := os.Stat(filepath.Dir(path)); runtime.GOOS != "windows" && di.Mode().Perm() != 0o700 {
		t.Fatalf("archive dir mode = %v, want 0700", di.Mode().Perm())
	}
	got, _ := os.ReadFile(path)
	if string(got) != string(want) {
		t.Fatal("archive differs from the export taken before the delete")
	}
	doc := verifyExport(t, sum, got)
	if r, _ := doc["room"].(map[string]any); r["id"] != snap.ID {
		t.Fatalf("archive room = %v", doc["room"])
	}
	entries, _ := os.ReadDir(filepath.Dir(path))
	if len(entries) != 1 {
		t.Fatalf("left-over files in the archive dir: %v", entries)
	}
	// A repeat reports the archive that exists.
	again, err := e.svc.DeleteRoom(ctx, mustUser(t, e.svc, "ada"), snap.ID, "again", true)
	if err != nil || !again.Archived {
		t.Fatalf("repeat: %+v %v", again, err)
	}
}

// TestDeleteArchiveFailureKeepsRoom: an archive that cannot be written stops
// the delete (500 archive_failed); the room keeps working.
func TestDeleteArchiveFailureKeepsRoom(t *testing.T) {
	e := newLimitsEnv(t, config.Limits{ArchiveDir: filepath.Join(t.TempDir(), "missing")})
	ctx := context.Background()
	snap := createRoom(t, e.svc, "arjun")
	_, err := e.svc.DeleteRoom(ctx, mustUser(t, e.svc, "ada"), snap.ID, "cleanup", true)
	wantCode(t, err, "archive_failed")
	if _, err := e.st.GetRoom(ctx, snap.ID); err != nil {
		t.Fatalf("room deleted although the archive failed: %v", err)
	}
	if _, err := e.st.GetRoomDeletion(ctx, snap.ID); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("audit row written although nothing was deleted: %v", err)
	}
	// The deleting mark is gone: the room takes events again.
	if _, err := e.svc.AddComment(ctx, mustUser(t, e.svc, "priya"), snap.ID, "", "still here"); err != nil {
		t.Fatalf("room refuses writes after a failed delete: %v", err)
	}
}

// TestDeleteErasesByDefault: a delete keeps no copy unless asked, even with an
// archive directory configured; asking without one is 400.
func TestDeleteErasesByDefault(t *testing.T) {
	dir := t.TempDir()
	e := newLimitsEnv(t, config.Limits{ArchiveDir: dir})
	ctx := context.Background()
	snap := createRoom(t, e.svc, "arjun")
	d, err := e.svc.DeleteRoom(ctx, mustUser(t, e.svc, "ada"), snap.ID, "erase", false)
	if err != nil {
		t.Fatal(err)
	}
	if d.Archived {
		t.Fatal("archived = true for a plain delete")
	}
	if entries, _ := os.ReadDir(dir); len(entries) != 0 {
		t.Fatalf("a plain delete wrote to the archive dir: %v", entries)
	}

	noDir := newLimitsEnv(t, config.Limits{})
	other := createRoom(t, noDir.svc, "arjun")
	_, err = noDir.svc.DeleteRoom(ctx, mustUser(t, noDir.svc, "ada"), other.ID, "keep a copy", true)
	wantCode(t, err, "validation")
	if _, err := noDir.st.GetRoom(ctx, other.ID); err != nil {
		t.Fatalf("room deleted although archive=true could not be honoured: %v", err)
	}
}

// TestShadowLogCapStopsTheRun: the room's caps hold for its shadow log; the
// agent's events stop at the cap, the run is cancelled and ends as budget,
// and nothing reaches the room log.
func TestShadowLogCapStopsTheRun(t *testing.T) {
	st := &shadowStore{Store: memstore.New()}
	st.SetLimits(store.Limits{MaxEvents: 100})
	var refused atomic.Bool
	e := newShadowEnv(t, shadowCfg{st: st, fn: func(ctx context.Context, _ int, d agent.Deps) error {
		for i := 0; i < 300; i++ {
			if err := d.Sink.Emit(ctx, events.AgentMessage, events.AgentMessagePayload{Text: "step"}); errors.Is(err, store.ErrLogFull) {
				refused.Store(true)
				break
			}
		}
		<-ctx.Done() // the service cancels the run
		return nil
	}})
	snap := e.open(t, "cap")
	run := e.waitFinished(t, snap.ID)
	if !refused.Load() {
		t.Fatal("no agent event was refused")
	}
	if run.Status != store.ShadowBudget {
		t.Fatalf("run status = %q, want %q", run.Status, store.ShadowBudget)
	}
	if n := len(e.shadowLog(t, snap.ID)); n != 100 {
		t.Fatalf("shadow log = %d events, want the cap 100", n)
	}
	assertNoAgentLeak(t, e.roomLog(t, snap.ID))
}

// gatedStore blocks Since while armed, so a test can act while DeleteRoom is
// inside its archive (the export reads the log through Since).
type gatedStore struct {
	*memstore.Store
	armed   chan struct{} // closed to arm
	entered chan struct{} // closed when an armed Since started
	release chan struct{}
	once    sync.Once
}

func (g *gatedStore) Since(ctx context.Context, roomID string, after int64, limit int) ([]events.Event, error) {
	select {
	case <-g.armed:
		g.once.Do(func() { close(g.entered) })
		<-g.release
	default:
	}
	return g.Store.Since(ctx, roomID, after, limit)
}

// TestDeleteRefusesWritesWhileArchiving: between the archive and the delete
// nothing may be appended, or the archive would miss it.
func TestDeleteRefusesWritesWhileArchiving(t *testing.T) {
	c := &clock{t: time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)}
	g := &gatedStore{Store: memstore.NewWithClock(c.Now), armed: make(chan struct{}), entered: make(chan struct{}), release: make(chan struct{})}
	svc := room.New(room.Deps{Store: g, Users: adminUsers(t), Owners: testOwners(), Run: fakeRun, Now: c.Now, Limits: config.Limits{ArchiveDir: t.TempDir()}})
	t.Cleanup(svc.Close)
	ctx := context.Background()
	snap := createRoom(t, svc, "arjun")

	close(g.armed)
	done := make(chan error, 1)
	go func() {
		_, err := svc.DeleteRoom(ctx, mustUser(t, svc, "ada"), snap.ID, "cleanup", true)
		done <- err
	}()
	select {
	case <-g.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the archive never read the log")
	}
	_, err := svc.AddComment(ctx, mustUser(t, svc, "priya"), snap.ID, "", "late")
	wantCode(t, err, "not_found")
	close(g.release)
	if err := <-done; err != nil {
		t.Fatalf("delete: %v", err)
	}
	if row, err := g.GetRoomDeletion(ctx, snap.ID); err != nil || row.EventCount != 2 {
		t.Fatalf("audit row = %+v %v, want the 2 events archived", row, err)
	}
}

// ---- retention: archive and per-pipeline policy ----

// twoPipelineOwners has orders (retention_days 10) and billing (none).
func twoPipelineOwners() *owners.Owners {
	ten := owners.WholeDays(10)
	return &owners.Owners{Pipelines: []owners.Pipeline{
		{ID: "orders", SourceTables: []string{"public.orders"}, RetentionDays: &ten},
		{ID: "billing", SourceTables: []string{"public.invoices"}},
	}}
}

func TestNewRetentionPolicy(t *testing.T) {
	own := twoPipelineOwners()
	for _, tc := range []struct {
		global     int
		wantOrders int
		off        bool
	}{
		{0, 10, false},  // the pipeline's own value
		{30, 10, false}, // shorter than the global
		{5, 5, false},   // the global is shorter: it wins
	} {
		p := room.NewRetentionPolicy(tc.global, own)
		if p.GlobalDays != tc.global || p.PipelineDays["orders"] != tc.wantOrders || len(p.PipelineDays) != 1 || p.Off() != tc.off {
			t.Fatalf("global %d: %+v", tc.global, p)
		}
	}
	if !room.NewRetentionPolicy(0, testOwners()).Off() || !room.NewRetentionPolicy(0, nil).Off() {
		t.Fatal("no days anywhere must be off")
	}
}

// TestPurgeByPolicy: orders keeps 10 days, billing the global 30 (or forever
// with no global), each room by its own pipeline.
func TestPurgeByPolicy(t *testing.T) {
	ctx := context.Background()
	c := &clock{t: time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)}
	st := memstore.NewWithClock(c.Now)
	svc := room.New(room.Deps{Store: st, Users: room.DefaultUsers(), Owners: twoPipelineOwners(), Run: fakeRun, Now: c.Now})
	t.Cleanup(svc.Close)
	open := func(pipeline string) string {
		snap, err := svc.CreateRoom(ctx, mustUser(t, svc, "arjun"), "t", "sev2", pipeline)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := svc.CloseRoom(ctx, mustUser(t, svc, "arjun"), snap.ID, "done"); err != nil {
			t.Fatal(err)
		}
		waitPM(t, st, snap.ID) // the closing draft event is part of the log
		return snap.ID
	}
	orders, billing := open("orders"), open("billing")
	c.Advance(20 * 24 * time.Hour)

	// No global: only orders goes.
	res, err := svc.PurgeByPolicy(ctx, c.Now(), room.NewRetentionPolicy(0, twoPipelineOwners()))
	if err != nil || len(res.RoomIDs) != 1 || res.RoomIDs[0] != orders {
		t.Fatalf("no global: %+v %v", res.RoomIDs, err)
	}
	if _, err := st.GetRoom(ctx, billing); err != nil {
		t.Fatalf("billing purged without a policy: %v", err)
	}
	// Global 30 at day 20: billing stays; at day 31 it goes.
	if res, _ := svc.PurgeByPolicy(ctx, c.Now(), room.NewRetentionPolicy(30, twoPipelineOwners())); len(res.RoomIDs) != 0 {
		t.Fatalf("billing purged before 30 days: %v", res.RoomIDs)
	}
	c.Advance(11 * 24 * time.Hour)
	if res, _ := svc.PurgeByPolicy(ctx, c.Now(), room.NewRetentionPolicy(30, twoPipelineOwners())); len(res.RoomIDs) != 1 || res.RoomIDs[0] != billing {
		t.Fatalf("billing at 31 days: %v", res.RoomIDs)
	}
	// Each purge left a content-free audit row naming the days that applied.
	for id, want := range map[string]store.RoomDeletion{
		orders:  {RoomID: orders, PipelineID: "orders", DeletedBy: "system", Reason: "retention: closed 10 days", EventCount: 4},
		billing: {RoomID: billing, PipelineID: "billing", DeletedBy: "system", Reason: "retention: closed 30 days", EventCount: 4},
	} {
		got, err := st.GetRoomDeletion(ctx, id)
		got.DeletedAt = time.Time{}
		if err != nil || got != want {
			t.Fatalf("audit row of %s = %+v %v, want %+v", id, got, err, want)
		}
	}
}

// TestRetentionArchivesBeforePurge: with an archive dir the closed room's
// export lands there before it is purged; with a broken dir the room is kept,
// the run reports it, and a later run with a working dir purges it.
func TestRetentionArchivesBeforePurge(t *testing.T) {
	ctx := context.Background()
	c := &clock{t: time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)}
	st := memstore.NewWithClock(c.Now)
	good := t.TempDir()
	newSvc := func(dir string) *room.Service {
		svc := room.New(room.Deps{Store: st, Users: room.DefaultUsers(), Owners: testOwners(), Run: fakeRun, Now: c.Now, Limits: config.Limits{ArchiveDir: dir}})
		t.Cleanup(svc.Close)
		return svc
	}
	broken := newSvc(filepath.Join(t.TempDir(), "missing"))
	snap := createRoom(t, broken, "arjun")
	if _, err := broken.CloseRoom(ctx, mustUser(t, broken, "arjun"), snap.ID, "done"); err != nil {
		t.Fatal(err)
	}
	waitPM(t, st, snap.ID) // the closing draft event must not land mid-archive
	c.Advance(40 * 24 * time.Hour)

	res, err := broken.PurgeExpired(ctx, c.Now().AddDate(0, 0, -30))
	if err == nil || len(res.RoomIDs) != 0 || len(res.Failed) != 1 {
		t.Fatalf("broken archive: %+v %v, want the room failed and kept", res, err)
	}
	if _, err := st.GetRoom(ctx, snap.ID); err != nil {
		t.Fatalf("room purged although its archive failed: %v", err)
	}

	working := newSvc(good)
	res, err = working.PurgeExpired(ctx, c.Now().AddDate(0, 0, -30))
	if err != nil || len(res.RoomIDs) != 1 {
		t.Fatalf("working archive: %+v %v", res, err)
	}
	data, err := os.ReadFile(filepath.Join(good, "orders", snap.ID+".json"))
	if err != nil {
		t.Fatalf("archive missing: %v", err)
	}
	var doc struct {
		Room struct {
			ID     string `json:"id"`
			Status string `json:"status"`
		} `json:"room"`
		EventCount int `json:"event_count"`
	}
	if err := json.Unmarshal(data, &doc); err != nil || doc.Room.ID != snap.ID || doc.Room.Status != "closed" || doc.EventCount == 0 {
		t.Fatalf("archive content: %+v %v", doc, err)
	}
	if _, err := st.GetRoom(ctx, snap.ID); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("room kept after its archive: %v", err)
	}
	// The purge wrote its audit row in the same step; the failed run did not.
	row, err := st.GetRoomDeletion(ctx, snap.ID)
	if err != nil || row.DeletedBy != "system" || row.Reason != "retention: closed 30 days" || row.EventCount == 0 ||
		row.PipelineID != "orders" || !row.DeletedAt.Equal(c.Now()) {
		t.Fatalf("retention audit row = %+v %v", row, err)
	}
	// An admin delete of a room retention removed answers that row.
	admins := room.New(room.Deps{Store: st, Users: adminUsers(t), Owners: testOwners(), Run: fakeRun, Now: c.Now, Limits: config.Limits{ArchiveDir: good}})
	t.Cleanup(admins.Close)
	d, err := admins.DeleteRoom(ctx, mustUser(t, admins, "ada"), snap.ID, "erase", false)
	if err != nil || d.RoomDeletion != row || !d.Archived {
		t.Fatalf("delete of a purged room = %+v %v, want the retention row (archived)", d, err)
	}
}
