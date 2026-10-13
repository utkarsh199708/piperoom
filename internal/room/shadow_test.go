package room_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"piperoom/internal/agent"
	"piperoom/internal/alertin"
	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/room"
	"piperoom/internal/store"
	"piperoom/internal/store/memstore"
)

// Shadow mode tests (docs/design/shadow.md section 6).

const shadowOwnersYAML = `pipelines:
  - id: orders
    topic_prefix: pg
    source_tables: [public.orders]
    connectors: [c1]
    shadow: {enabled: true}
    owners:
      warehouse: {approvers: [user:meera], backup: [user:zoe]}
  - id: live
    topic_prefix: lv
    source_tables: [public.live]
    connectors: [c2]
  - id: payments
    topic_prefix: pay
    source_tables: [public.pay]
    connectors: [c3]
    owners:
      kafka: {approvers: [group:payments-oncall]}
layers:
  postgres: priya
  kafka-connect: arjun
  schema-registry: priya
  kafka: arjun
  consumer: kabir
  warehouse: meera
`

const shadowUsersYAML = `users:
  - {id: arjun, name: Arjun, layers: [kafka-connect, kafka]}
  - {id: priya, name: Priya, layers: [postgres, schema-registry]}
  - {id: meera, name: Meera, layers: [warehouse]}
  - {id: kabir, name: Kabir, layers: [consumer]}
  - {id: nolayers, name: No Layers}
`

// Text the fake agent writes; none of it may reach the room log before the
// reveal, and the hypothesis never reaches it at all.
const (
	secretMessage    = "SECRET-AGENT-MESSAGE"
	secretHypothesis = "SECRET-HYPOTHESIS"
	secretData       = "SECRET-ROW-VALUE"
)

// scriptRunner is a fake agent.Runner whose behaviour the test scripts per
// call. Calls for the "live" pipeline block until their run ends.
type scriptRunner struct {
	mu    sync.Mutex
	calls int
	opts  []agent.Options
	deps  []agent.Deps
	fn    func(ctx context.Context, call int, d agent.Deps) error
	live  atomic.Int64 // live-pipeline runs started
}

func (r *scriptRunner) Run(ctx context.Context, a agent.Alert, d agent.Deps, o agent.Options) error {
	if a.PipelineID != "orders" {
		r.live.Add(1)
		<-ctx.Done()
		return nil
	}
	r.mu.Lock()
	r.calls++
	n := r.calls
	r.opts = append(r.opts, o)
	r.deps = append(r.deps, d)
	r.mu.Unlock()
	return r.fn(ctx, n, d)
}

func (r *scriptRunner) Calls() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls
}

// investigate emits one of each agent event, with a hypothesis when hyp.
func investigate(ctx context.Context, d agent.Deps, hyp bool) {
	_ = d.Sink.Emit(ctx, events.AgentStatus, events.AgentStatusPayload{State: "working", Detail: "investigating"})
	_ = d.Sink.Emit(ctx, events.AgentStepStart, events.AgentStepStartPayload{StepID: "s_1", Title: "check"})
	_ = d.Sink.Emit(ctx, events.AgentToolCall, events.AgentToolCallPayload{StepID: "s_1", ToolUseID: "tu_1", Tool: "connect_status", Input: json.RawMessage(`{}`)})
	_ = d.Sink.Emit(ctx, events.AgentToolResult, events.AgentToolResultPayload{StepID: "s_1", ToolUseID: "tu_1", Tool: "connect_status",
		Summary: "task failed", Data: json.RawMessage(`{"value":"` + secretData + `"}`)})
	_ = d.Sink.Emit(ctx, events.AgentMessage, events.AgentMessagePayload{Text: secretMessage})
	if hyp {
		_ = d.Sink.Emit(ctx, events.AgentHypothesis, events.AgentHypothesisPayload{Text: secretHypothesis, Confidence: "high"})
	}
}

// endTurn ends a run the way agent.Run does at end_turn: idle, then Wait on the
// inbox, which a shadow room has closed.
func endTurn(ctx context.Context, d agent.Deps) error {
	_ = d.Sink.Emit(ctx, events.AgentStatus, events.AgentStatusPayload{State: "idle"})
	if err := d.Inbox.Wait(ctx); errors.Is(err, agent.ErrInboxClosed) {
		return nil
	} else if err != nil {
		return err
	}
	return errors.New("test: a shadow inbox must be closed")
}

// shadowStore wraps memstore so a test can fail CreateShadowRun.
type shadowStore struct {
	*memstore.Store
	failCreate atomic.Int64 // calls to fail; -1 = always
	creates    atomic.Int64
}

func (s *shadowStore) CreateShadowRun(ctx context.Context, r store.ShadowRun) error {
	s.creates.Add(1)
	if n := s.failCreate.Load(); n != 0 {
		if n > 0 {
			s.failCreate.Add(-1)
		}
		return errors.New("test: create shadow run refused")
	}
	return s.Store.CreateShadowRun(ctx, r)
}

type shadowEnv struct {
	svc   *room.Service
	st    *shadowStore
	clk   *clock
	run   *scriptRunner
	deps  room.Deps
	users *room.Users
}

type shadowCfg struct {
	fn        func(ctx context.Context, call int, d agent.Deps) error
	grace     time.Duration
	maxAgents int
	solo      string
	st        *shadowStore
}

func loadShadowFixtures(t *testing.T) (*owners.Owners, *room.Users) {
	t.Helper()
	dir := t.TempDir()
	op, up := filepath.Join(dir, "owners.yaml"), filepath.Join(dir, "users.yaml")
	if err := os.WriteFile(op, []byte(shadowOwnersYAML), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(up, []byte(shadowUsersYAML), 0o600); err != nil {
		t.Fatal(err)
	}
	own, err := owners.Load(op)
	if err != nil {
		t.Fatal(err)
	}
	users, err := room.LoadUsers(up)
	if err != nil {
		t.Fatal(err)
	}
	return own, users
}

func newShadowEnv(t *testing.T, cfg shadowCfg) *shadowEnv {
	t.Helper()
	own, users := loadShadowFixtures(t)
	c := &clock{t: time.Date(2026, 10, 13, 9, 0, 0, 0, time.UTC)}
	st := cfg.st
	if st == nil {
		st = &shadowStore{Store: memstore.NewWithClock(c.Now)}
	}
	fn := cfg.fn
	if fn == nil {
		fn = func(ctx context.Context, _ int, d agent.Deps) error {
			investigate(ctx, d, true)
			return endTurn(ctx, d)
		}
	}
	r := &scriptRunner{fn: fn}
	deps := room.Deps{
		Store: st, Users: users, Owners: own, Run: r.Run, WriteHandoff: fakeWrite, Now: c.Now,
		Options:       agent.Options{Model: "live-model", MaxTurns: 40},
		ShadowOptions: agent.Options{Model: "shadow-model", MaxTurns: 20, TokenBudget: 500000},
		ShadowGrace:   cfg.grace, MaxAgents: cfg.maxAgents, SoloUser: cfg.solo,
	}
	svc := room.New(deps)
	t.Cleanup(svc.Close)
	return &shadowEnv{svc: svc, st: st, clk: c, run: r, deps: deps, users: users}
}

// restart builds a second service on the same store and loads it.
func (e *shadowEnv) restart(t *testing.T) *room.Service {
	t.Helper()
	e.svc.Close()
	svc := room.New(e.deps)
	t.Cleanup(svc.Close)
	if err := svc.Load(context.Background()); err != nil {
		t.Fatalf("Load: %v", err)
	}
	e.svc = svc
	return svc
}

func (e *shadowEnv) open(t *testing.T, key string) room.RoomSnapshot {
	t.Helper()
	snap, created, err := e.svc.OpenAlertRoom(context.Background(), alertin.Alert{
		Source: alertin.Generic, Status: alertin.StatusFiring, Title: "orders stale", Severity: "sev2",
		PipelineID: "orders", DedupKey: "generic:" + key,
	})
	if err != nil || !created {
		t.Fatalf("OpenAlertRoom: created=%v err=%v", created, err)
	}
	return snap
}

func (e *shadowEnv) user(t *testing.T, id string) room.User {
	t.Helper()
	u, ok := e.users.Get(id)
	if !ok {
		t.Fatalf("user %q", id)
	}
	return u
}

// close takes control as arjun and closes with a resolution.
func (e *shadowEnv) close(t *testing.T, roomID, resolution string) {
	t.Helper()
	ctx := context.Background()
	arjun := e.user(t, "arjun")
	if snap, _ := e.svc.GetRoom(ctx, roomID); snap.Driver == nil || snap.Driver.ID != "arjun" {
		if _, err := e.svc.GrantControl(ctx, arjun, roomID, "arjun"); err != nil {
			t.Fatalf("take control: %v", err)
		}
	}
	if _, err := e.svc.CloseRoom(ctx, arjun, roomID, resolution); err != nil {
		t.Fatalf("CloseRoom: %v", err)
	}
}

func (e *shadowEnv) runRow(t *testing.T, roomID string) store.ShadowRun {
	t.Helper()
	r, err := e.st.GetShadowRun(context.Background(), roomID)
	if err != nil {
		t.Fatalf("GetShadowRun: %v", err)
	}
	return r
}

func (e *shadowEnv) waitFinished(t *testing.T, roomID string) store.ShadowRun {
	t.Helper()
	var r store.ShadowRun
	waitFor(t, 15*time.Second, func() bool {
		got, err := e.st.GetShadowRun(context.Background(), roomID)
		r = got
		return err == nil && got.Finished()
	})
	return r
}

func (e *shadowEnv) shadowLog(t *testing.T, roomID string) []events.Event {
	t.Helper()
	evs, err := e.st.ShadowSince(context.Background(), roomID, 0, 10000)
	if err != nil {
		t.Fatal(err)
	}
	return evs
}

func (e *shadowEnv) roomLog(t *testing.T, roomID string) []events.Event {
	t.Helper()
	evs, err := e.st.Since(context.Background(), roomID, 0, 10000)
	if err != nil {
		t.Fatal(err)
	}
	return evs
}

// assertNoAgentLeak fails if the room log holds any agent-actor event or any
// text the fake agent wrote.
func assertNoAgentLeak(t *testing.T, evs []events.Event) {
	t.Helper()
	for _, e := range evs {
		if e.Actor == events.AgentActor {
			t.Fatalf("agent-actor event in the room log: seq %d %s %s", e.Seq, e.Type, e.Payload)
		}
		for _, secret := range []string{secretMessage, secretHypothesis, secretData} {
			if strings.Contains(string(e.Payload), secret) {
				t.Fatalf("agent text %q in room event %d %s", secret, e.Seq, e.Type)
			}
		}
	}
}

func countReveals(evs []events.Event) int {
	n := 0
	for _, e := range evs {
		if e.Type == events.ShadowRevealed {
			n++
		}
	}
	return n
}

func statuses(evs []events.Event) []string {
	var out []string
	for _, e := range evs {
		if e.Type == events.AgentStatus {
			var p events.AgentStatusPayload
			_ = json.Unmarshal(e.Payload, &p)
			out = append(out, p.State)
		}
	}
	return out
}

func revealOf(t *testing.T, evs []events.Event) events.ShadowRevealedPayload {
	t.Helper()
	for _, e := range evs {
		if e.Type == events.ShadowRevealed {
			var p events.ShadowRevealedPayload
			if err := json.Unmarshal(e.Payload, &p); err != nil {
				t.Fatal(err)
			}
			if e.Actor.Kind != events.ActorSystem {
				t.Fatalf("reveal actor = %+v, want system", e.Actor)
			}
			return p
		}
	}
	t.Fatal("no shadow.revealed")
	return events.ShadowRevealedPayload{}
}

// The leak test (design section 6): a shadow room's agent output never reaches
// the room log — not the steps, tools, message, hypothesis, not the queued and
// working statuses of a full slot pool, not the retry path's error statuses,
// and not across a restart. Everything agent-authored is in the shadow log.
func TestShadowLeak(t *testing.T) {
	env := newShadowEnv(t, shadowCfg{maxAgents: 1, fn: func(ctx context.Context, call int, d agent.Deps) error {
		investigate(ctx, d, call == 3)
		if call < 3 {
			return fmt.Errorf("test: transient failure %d", call)
		}
		return endTurn(ctx, d)
	}})
	ctx := context.Background()
	arjun := env.user(t, "arjun")

	// A live room holds the only slot, so the shadow run must queue.
	live, err := env.svc.CreateRoom(ctx, arjun, "live incident", "sev2", "live")
	if err != nil {
		t.Fatal(err)
	}
	if live.Shadow {
		t.Fatal("POST /api/rooms must never make a shadow room")
	}
	waitFor(t, 5*time.Second, func() bool { return env.run.live.Load() == 1 })
	snap := env.open(t, "leak")
	if !snap.Shadow {
		t.Fatal("alert room of a shadow pipeline is not shadow")
	}
	waitFor(t, 5*time.Second, func() bool {
		s := statuses(env.shadowLog(t, snap.ID))
		return len(s) > 0 && s[0] == "queued"
	})
	if _, err := env.svc.CloseRoom(ctx, arjun, live.ID, "done"); err != nil {
		t.Fatal(err)
	}
	run := env.waitFinished(t, snap.ID)
	if run.Status != store.ShadowHypothesised || env.run.Calls() != 3 {
		t.Fatalf("status %q after %d calls, want hypothesised after 3", run.Status, env.run.Calls())
	}

	got := statuses(env.shadowLog(t, snap.ID))
	want := map[string]int{"queued": 1, "error": 2}
	for _, s := range got {
		if _, ok := want[s]; ok {
			want[s]--
		}
	}
	if want["queued"] != 0 || want["error"] != 0 || !strings.Contains(strings.Join(got, ","), "queued,working") {
		t.Fatalf("shadow statuses = %v; want queued, working (slot acquired) and two error statuses", got)
	}
	for _, e := range env.shadowLog(t, snap.ID) {
		if e.Actor != events.AgentActor {
			t.Fatalf("non-agent event in the shadow log: %+v", e)
		}
	}
	roomEvs := env.roomLog(t, snap.ID)
	assertNoAgentLeak(t, roomEvs)
	if s, _ := env.svc.GetRoom(ctx, snap.ID); s.AgentStatus.State != "idle" || s.Hypothesis != nil {
		t.Fatalf("snapshot shows agent state: %+v %+v", s.AgentStatus, s.Hypothesis)
	}

	// Restart: Load writes no agent status and no "steer to resume" note.
	svc2 := env.restart(t)
	assertNoAgentLeak(t, env.roomLog(t, snap.ID))
	if n := len(env.roomLog(t, snap.ID)); n != len(roomEvs) {
		t.Fatalf("restart added %d room events to a shadow room", n-len(roomEvs))
	}
	if s, err := svc2.GetRoom(ctx, snap.ID); err != nil || !s.Shadow {
		t.Fatalf("shadow flag lost on restart: %+v %v", s, err)
	}
	env.close(t, snap.ID, "connector restarted")
	after := env.roomLog(t, snap.ID)
	assertNoAgentLeak(t, after)
	if countReveals(after) != 1 {
		t.Fatalf("reveals = %d, want 1", countReveals(after))
	}
}

// Steers, @agent comments and suggestion sends are logged but never start or
// wake a shadow run, before or after a restart.
func TestShadowInputStartsNothing(t *testing.T) {
	env := newShadowEnv(t, shadowCfg{})
	ctx := context.Background()
	snap := env.open(t, "input")
	env.waitFinished(t, snap.ID)
	if env.run.Calls() != 1 {
		t.Fatalf("calls = %d", env.run.Calls())
	}
	poke := func(svc *room.Service) {
		arjun, kabir := env.user(t, "arjun"), env.user(t, "kabir")
		if s, _ := svc.GetRoom(ctx, snap.ID); s.Driver == nil {
			if _, err := svc.GrantControl(ctx, arjun, snap.ID, "arjun"); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := svc.Steer(ctx, arjun, snap.ID, "look at the connector"); err != nil {
			t.Fatalf("steer: %v", err)
		}
		if _, err := svc.AddComment(ctx, arjun, snap.ID, "", "@agent what now?"); err != nil {
			t.Fatalf("comment: %v", err)
		}
		if _, err := svc.Steer(ctx, kabir, snap.ID, "a suggestion"); err != nil {
			t.Fatalf("suggestion: %v", err)
		}
		s, _ := svc.GetRoom(ctx, snap.ID)
		for _, sg := range s.PendingSuggestions {
			if _, err := svc.SendSuggestion(ctx, arjun, snap.ID, sg.SuggestionID); err != nil {
				t.Fatalf("send suggestion: %v", err)
			}
		}
	}
	poke(env.svc)
	time.Sleep(50 * time.Millisecond)
	if env.run.Calls() != 1 {
		t.Fatalf("input started a run: calls = %d", env.run.Calls())
	}
	svc2 := env.restart(t)
	poke(svc2)
	time.Sleep(50 * time.Millisecond)
	if env.run.Calls() != 1 {
		t.Fatalf("input after restart started a run: calls = %d", env.run.Calls())
	}
	evs := env.roomLog(t, snap.ID)
	assertNoAgentLeak(t, evs)
	steers := 0
	for _, e := range evs {
		if e.Type == events.SteerSent {
			steers++
		}
	}
	if steers < 2 {
		t.Fatalf("steers were not logged: %d", steers)
	}
}

// The run gets the shadow options, no actions, no proposals and a Usage
// callback; end_turn on the closed inbox ends it hypothesised or
// no_hypothesis, and a max-turns stop ends it the same way.
func TestShadowRunEndsAtEndTurn(t *testing.T) {
	env := newShadowEnv(t, shadowCfg{fn: func(ctx context.Context, call int, d agent.Deps) error {
		switch call {
		case 1:
			investigate(ctx, d, true)
			d.Usage(4242)
			return endTurn(ctx, d)
		case 2:
			investigate(ctx, d, false)
			return endTurn(ctx, d)
		default: // the runner's max-turns stop: a status, then a clean return
			_ = d.Sink.Emit(ctx, events.AgentStatus, events.AgentStatusPayload{State: "idle", Detail: "stopped after max turns (20)"})
			return nil
		}
	}})
	a := env.open(t, "a")
	ra := env.waitFinished(t, a.ID)
	b := env.open(t, "b")
	rb := env.waitFinished(t, b.ID)
	c := env.open(t, "c")
	rc := env.waitFinished(t, c.ID)
	if ra.Status != store.ShadowHypothesised || rb.Status != store.ShadowNoHypothesis || rc.Status != store.ShadowNoHypothesis {
		t.Fatalf("statuses %q %q %q", ra.Status, rb.Status, rc.Status)
	}
	if ra.Tokens != 4242 || ra.ToolCalls != 1 || ra.Model != "shadow-model" || ra.StartedAt == nil {
		t.Fatalf("run a: %+v", ra)
	}
	if !strings.Contains(strings.Join(statuses(env.shadowLog(t, c.ID)), ","), "idle") {
		t.Fatal("max-turns status not in the shadow log")
	}
	env.run.mu.Lock()
	defer env.run.mu.Unlock()
	for i, d := range env.run.deps {
		if d.Actions != nil || d.Proposals != nil || d.Usage == nil {
			t.Fatalf("call %d deps: actions=%v proposals=%v usage=%v", i, d.Actions, d.Proposals, d.Usage != nil)
		}
		if o := env.run.opts[i]; o.Model != "shadow-model" || o.MaxTurns != 20 || o.TokenBudget != 500000 {
			t.Fatalf("call %d opts = %+v", i, o)
		}
	}
}

// A *BudgetError stops a shadow run as "budget" with no retry, and stops a live
// run with no retry and no "error" status.
func TestBudgetStopIsNotRetried(t *testing.T) {
	budget := func(ctx context.Context, _ int, d agent.Deps) error {
		investigate(ctx, d, false)
		return fmt.Errorf("agent: model turn: %w", &agent.BudgetError{Used: 500001, Limit: 500000})
	}
	env := newShadowEnv(t, shadowCfg{fn: budget})
	snap := env.open(t, "budget")
	if r := env.waitFinished(t, snap.ID); r.Status != store.ShadowBudget {
		t.Fatalf("status %q, want budget", r.Status)
	}
	time.Sleep(1500 * time.Millisecond) // longer than the first retry backoff
	if env.run.Calls() != 1 {
		t.Fatalf("shadow calls = %d, want 1", env.run.Calls())
	}
	for _, s := range statuses(env.shadowLog(t, snap.ID)) {
		if s == "error" {
			t.Fatal("budget stop wrote an error status")
		}
	}

	// Live room: the same error, no retry and no error status in the log.
	var liveCalls atomic.Int64
	c := &clock{t: time.Date(2026, 10, 13, 9, 0, 0, 0, time.UTC)}
	st := memstore.NewWithClock(c.Now)
	svc := room.New(room.Deps{Store: st, Users: room.DefaultUsers(), Owners: testOwners(), Now: c.Now, WriteHandoff: fakeWrite,
		Run: func(ctx context.Context, _ agent.Alert, d agent.Deps, _ agent.Options) error {
			liveCalls.Add(1)
			_ = d.Sink.Emit(ctx, events.AgentStatus, events.AgentStatusPayload{State: "idle", Detail: "token budget reached"})
			return fmt.Errorf("wrapped: %w", &agent.BudgetError{Used: 10, Limit: 10})
		}})
	t.Cleanup(svc.Close)
	live := createRoom(t, svc, "arjun")
	time.Sleep(1500 * time.Millisecond)
	if liveCalls.Load() != 1 {
		t.Fatalf("live calls = %d, want 1", liveCalls.Load())
	}
	for _, s := range statuses(allEvents(t, st, live.ID)) {
		if s == "error" {
			t.Fatal("live budget stop wrote an error status")
		}
	}
}

// midTool is a runner that starts a tool call, signals, and waits for release
// or the end of its context.
type midTool struct {
	started chan struct{}
	release chan struct{}
	fail    bool // after release: fail instead of finishing
}

func newMidTool(fail bool) *midTool {
	return &midTool{started: make(chan struct{}, 8), release: make(chan struct{}), fail: fail}
}

func (m *midTool) fn(ctx context.Context, _ int, d agent.Deps) error {
	_ = d.Sink.Emit(ctx, events.AgentToolCall, events.AgentToolCallPayload{StepID: "s_1", Tool: "slow"})
	m.started <- struct{}{}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-m.release:
	}
	if m.fail {
		return errors.New("test: tool exploded")
	}
	investigate(ctx, d, true)
	return endTurn(ctx, d)
}

// Grace 0: a close while the run is mid-tool cancels it; status interrupted,
// one reveal.
func TestShadowCloseGraceZeroInterrupts(t *testing.T) {
	m := newMidTool(false)
	env := newShadowEnv(t, shadowCfg{fn: m.fn})
	snap := env.open(t, "g0")
	<-m.started
	env.clk.Advance(5 * time.Minute)
	env.close(t, snap.ID, "fixed by hand")
	r := env.waitFinished(t, snap.ID)
	if r.Status != store.ShadowInterrupted {
		t.Fatalf("status %q, want interrupted", r.Status)
	}
	waitFor(t, 5*time.Second, func() bool { return countReveals(env.roomLog(t, snap.ID)) == 1 })
	time.Sleep(50 * time.Millisecond)
	evs := env.roomLog(t, snap.ID)
	if countReveals(evs) != 1 {
		t.Fatalf("reveals = %d", countReveals(evs))
	}
	p := revealOf(t, evs)
	if p.Status != store.ShadowInterrupted || p.HumanMTTRS != 300 || p.TimeToFirstHypothesisS != -1 || p.ToolCalls != 1 {
		t.Fatalf("reveal = %+v", p)
	}
	if env.runRow(t, snap.ID).RevealedAt == nil {
		t.Fatal("revealed_at not set")
	}
}

// Grace 10s: the run keeps going after the close and finishes; hypothesised,
// finished_after_close, one reveal written by the supervisor.
func TestShadowCloseWithGraceFinishes(t *testing.T) {
	m := newMidTool(false)
	env := newShadowEnv(t, shadowCfg{fn: m.fn, grace: 10 * time.Second})
	snap := env.open(t, "g10")
	<-m.started
	env.clk.Advance(time.Minute)
	env.close(t, snap.ID, "fixed by hand")
	if n := countReveals(env.roomLog(t, snap.ID)); n != 0 {
		t.Fatalf("revealed before the run ended: %d", n)
	}
	env.clk.Advance(time.Minute)
	close(m.release)
	r := env.waitFinished(t, snap.ID)
	if r.Status != store.ShadowHypothesised || !r.FinishedAfterClose {
		t.Fatalf("run = %+v", r)
	}
	waitFor(t, 5*time.Second, func() bool { return countReveals(env.roomLog(t, snap.ID)) == 1 })
	p := revealOf(t, env.roomLog(t, snap.ID))
	if !p.FinishedAfterClose || p.Status != store.ShadowHypothesised || p.TimeToFinalHypothesisS != 120 {
		t.Fatalf("reveal = %+v", p)
	}
	assertNoAgentLeak(t, env.roomLog(t, snap.ID))
}

// Grace 10s and a failure after the close: interrupted, one attempt, one
// reveal (no fresh budget for a retry).
func TestShadowFailureAfterCloseNotRetried(t *testing.T) {
	m := newMidTool(true)
	env := newShadowEnv(t, shadowCfg{fn: m.fn, grace: 10 * time.Second})
	snap := env.open(t, "fail-after")
	<-m.started
	env.close(t, snap.ID, "fixed")
	close(m.release)
	r := env.waitFinished(t, snap.ID)
	time.Sleep(1500 * time.Millisecond)
	if r.Status != store.ShadowInterrupted || env.run.Calls() != 1 {
		t.Fatalf("status %q after %d calls, want interrupted after 1", r.Status, env.run.Calls())
	}
	if n := countReveals(env.roomLog(t, snap.ID)); n != 1 {
		t.Fatalf("reveals = %d", n)
	}
}

// The same failure before the close: three attempts, then error.
func TestShadowFailureBeforeCloseRetriesThenErrors(t *testing.T) {
	env := newShadowEnv(t, shadowCfg{fn: func(ctx context.Context, _ int, d agent.Deps) error {
		return errors.New("test: model down")
	}})
	snap := env.open(t, "fail-before")
	r := env.waitFinished(t, snap.ID)
	if r.Status != store.ShadowError || env.run.Calls() != 3 {
		t.Fatalf("status %q after %d calls, want error after 3", r.Status, env.run.Calls())
	}
	n := 0
	for _, s := range statuses(env.shadowLog(t, snap.ID)) {
		if s == "error" {
			n++
		}
	}
	if n != 3 {
		t.Fatalf("error statuses in the shadow log = %d, want 3", n)
	}
	env.close(t, snap.ID, "gave up")
	if p := revealOf(t, env.roomLog(t, snap.ID)); p.Status != store.ShadowError {
		t.Fatalf("reveal status %q", p.Status)
	}
	assertNoAgentLeak(t, env.roomLog(t, snap.ID))
}

// CreateShadowRun failing once at open: the supervisor's second call makes the
// row and the run goes on.
func TestShadowCreateRunRetriedBySupervisor(t *testing.T) {
	st := &shadowStore{Store: memstore.New()}
	st.failCreate.Store(1)
	env := newShadowEnv(t, shadowCfg{st: st})
	snap := env.open(t, "create-once")
	if r := env.waitFinished(t, snap.ID); r.Status != store.ShadowHypothesised {
		t.Fatalf("status %q", r.Status)
	}
	if st.creates.Load() < 2 {
		t.Fatalf("creates = %d", st.creates.Load())
	}
}

// CreateShadowRun failing always: no run, and the close upserts an error row
// and reveals, so the room still counts.
func TestShadowCreateRunAlwaysFails(t *testing.T) {
	st := &shadowStore{Store: memstore.New()}
	st.failCreate.Store(-1)
	env := newShadowEnv(t, shadowCfg{st: st})
	snap := env.open(t, "create-never")
	waitFor(t, 5*time.Second, func() bool { return st.creates.Load() >= 2 })
	time.Sleep(50 * time.Millisecond)
	if env.run.Calls() != 0 {
		t.Fatalf("a run started without a run row: %d calls", env.run.Calls())
	}
	if len(env.roomLog(t, snap.ID)) == 0 {
		t.Fatal("the room was lost")
	}
	st.failCreate.Store(0) // the store is back at close time
	env.close(t, snap.ID, "handled")
	r := env.runRow(t, snap.ID)
	if r.Status != store.ShadowError || r.FinishedAt == nil || r.RevealedAt == nil {
		t.Fatalf("row after close: %+v", r)
	}
	if p := revealOf(t, env.roomLog(t, snap.ID)); p.Status != store.ShadowError {
		t.Fatalf("reveal = %+v", p)
	}
}

// The run ends first and the close reveals; a later MarkRevealed is not first.
func TestShadowRunEndsFirstCloseReveals(t *testing.T) {
	env := newShadowEnv(t, shadowCfg{})
	snap := env.open(t, "end-first")
	env.waitFinished(t, snap.ID)
	if n := countReveals(env.roomLog(t, snap.ID)); n != 0 {
		t.Fatal("revealed while open")
	}
	env.close(t, snap.ID, "resolved")
	if n := countReveals(env.roomLog(t, snap.ID)); n != 1 {
		t.Fatalf("reveals = %d", n)
	}
	first, err := env.st.MarkRevealed(context.Background(), snap.ID, time.Now())
	if err != nil || first {
		t.Fatalf("late MarkRevealed: first=%v err=%v", first, err)
	}
}

// shadow.revealed carries numbers only: its payload type has no text field,
// and the event written has no hypothesis text.
func TestShadowRevealHasNoHypothesisText(t *testing.T) {
	typ := reflect.TypeOf(events.ShadowRevealedPayload{})
	for i := 0; i < typ.NumField(); i++ {
		f := typ.Field(i)
		tag := strings.Split(f.Tag.Get("json"), ",")[0]
		if f.Type.Kind() == reflect.String && tag != "status" {
			t.Fatalf("reveal payload field %s (%s) can carry text", f.Name, tag)
		}
		if strings.Contains(tag, "hypothesis") && !strings.HasPrefix(tag, "time_to_") {
			t.Fatalf("reveal payload field %s names the hypothesis", tag)
		}
	}
	env := newShadowEnv(t, shadowCfg{})
	snap := env.open(t, "reveal-keys")
	env.waitFinished(t, snap.ID)
	env.close(t, snap.ID, "resolved")
	for _, e := range env.roomLog(t, snap.ID) {
		if e.Type != events.ShadowRevealed {
			continue
		}
		var keys map[string]json.RawMessage
		if err := json.Unmarshal(e.Payload, &keys); err != nil {
			t.Fatal(err)
		}
		for k := range keys {
			switch k {
			case "status", "time_to_first_hypothesis_s", "time_to_final_hypothesis_s", "human_mttr_s",
				"finished_after_close", "tool_calls", "tokens":
			default:
				t.Fatalf("unexpected reveal key %q", k)
			}
		}
		if strings.Contains(string(e.Payload), secretHypothesis) {
			t.Fatal("reveal holds the hypothesis")
		}
	}
}

// Close, label and grade rules: resolution required, order enforced, one
// label, member of the pipeline only, identity and grant recorded.
func TestShadowLabelAndGradeRules(t *testing.T) {
	env := newShadowEnv(t, shadowCfg{})
	ctx := context.Background()
	snap := env.open(t, "rules")
	env.waitFinished(t, snap.ID)
	arjun, priya := env.user(t, "arjun"), env.user(t, "priya")

	if _, err := env.svc.ShadowLabel(ctx, priya, snap.ID, "kafka-connect", []string{"schema"}); err == nil {
		t.Fatal("label on an open room")
	} else {
		wantCode(t, err, "room_open")
	}
	if _, err := env.svc.Shadow(ctx, snap.ID, room.ShadowReadOptions{}); err == nil {
		t.Fatal("read on an open room")
	} else {
		wantCode(t, err, "room_open")
	}
	if _, err := env.svc.GrantControl(ctx, arjun, snap.ID, "arjun"); err != nil {
		t.Fatal(err)
	}
	for _, res := range []string{"", "   \n"} {
		if _, err := env.svc.CloseRoom(ctx, arjun, snap.ID, res); err == nil {
			t.Fatalf("close with resolution %q accepted", res)
		} else {
			wantCode(t, err, "validation")
		}
	}
	env.close(t, snap.ID, "schema change; set compatibility NONE")

	if _, err := env.svc.Shadow(ctx, snap.ID, room.ShadowReadOptions{}); err == nil {
		t.Fatal("read before the label")
	} else {
		wantCode(t, err, "not_labelled")
	}
	if _, err := env.svc.ShadowGrade(ctx, priya, snap.ID, true, "yes", ""); err == nil {
		t.Fatal("grade before the label")
	} else {
		wantCode(t, err, "not_labelled")
	}
	for _, tc := range []struct {
		layer string
		terms []string
	}{
		{"database", []string{"x"}},
		{"kafka-connect", nil},
		{"kafka-connect", []string{""}},
		{"kafka-connect", []string{strings.Repeat("x", 201)}},
		{"kafka-connect", make([]string, 21)},
		{"kafka-connect", []string{"a\x00b"}},
	} {
		if _, err := env.svc.ShadowLabel(ctx, priya, snap.ID, tc.layer, tc.terms); err == nil {
			t.Fatalf("label %q %d terms accepted", tc.layer, len(tc.terms))
		} else {
			wantCode(t, err, "validation")
		}
	}
	nolayers := env.user(t, "nolayers")
	if _, err := env.svc.ShadowLabel(ctx, nolayers, snap.ID, "kafka-connect", []string{"schema"}); err == nil {
		t.Fatal("a users.yaml user with no layers labelled")
	} else {
		wantCode(t, err, "not_allowed")
	}
	e, err := env.svc.ShadowLabel(ctx, priya, snap.ID, "kafka-connect", []string{"schema", "compatibility"})
	if err != nil {
		t.Fatalf("label: %v", err)
	}
	var lp events.ShadowLabelledPayload
	_ = json.Unmarshal(e.Payload, &lp)
	if lp.Identity == nil || lp.Identity.Grant != "user:priya" || lp.Identity.Method != "dev" || e.Actor.ID != "priya" {
		t.Fatalf("label identity = %+v actor %+v", lp.Identity, e.Actor)
	}
	if _, err := env.svc.ShadowLabel(ctx, arjun, snap.ID, "kafka", []string{"x"}); err == nil {
		t.Fatal("second label accepted")
	} else {
		wantCode(t, err, "already_decided")
	}

	v, err := env.svc.Shadow(ctx, snap.ID, room.ShadowReadOptions{})
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var h events.AgentHypothesisPayload
	if err := json.Unmarshal(v.Run.FinalHypothesis, &h); err != nil || h.Text != secretHypothesis {
		t.Fatalf("run hypothesis = %s", v.Run.FinalHypothesis)
	}
	for _, se := range v.Events {
		if !se.Shadow {
			t.Fatal("shadow event without shadow: true")
		}
		if se.Type == events.AgentToolResult && strings.Contains(string(se.Payload), secretData) {
			t.Fatal("tool_result.data returned without include=data")
		}
	}
	if v.Room != nil {
		t.Fatal("room block without include=room")
	}
	full, err := env.svc.Shadow(ctx, snap.ID, room.ShadowReadOptions{Data: true, Room: true})
	if err != nil {
		t.Fatal(err)
	}
	withData := false
	for _, se := range full.Events {
		withData = withData || (se.Type == events.AgentToolResult && strings.Contains(string(se.Payload), secretData))
	}
	if !withData || full.Room == nil || full.Room.Created == nil || full.Room.Labelled == nil || full.Room.Graded != nil {
		t.Fatalf("include=data,room: data=%v room=%+v", withData, full.Room)
	}

	for _, rc := range []string{"", "maybe"} {
		if _, err := env.svc.ShadowGrade(ctx, priya, snap.ID, true, rc, ""); err == nil {
			t.Fatalf("root_cause %q accepted", rc)
		} else {
			wantCode(t, err, "validation")
		}
	}
	if _, err := env.svc.ShadowGrade(ctx, priya, snap.ID, true, "yes", strings.Repeat("n", 2001)); err == nil {
		t.Fatal("long notes accepted")
	} else {
		wantCode(t, err, "validation")
	}
	if _, err := env.svc.ShadowGrade(ctx, nolayers, snap.ID, true, "yes", ""); err == nil {
		t.Fatal("non-member graded")
	} else {
		wantCode(t, err, "not_allowed")
	}
	if _, err := env.svc.ShadowGrade(ctx, priya, snap.ID, false, "partial", "first look"); err != nil {
		t.Fatal(err)
	}
	ge, err := env.svc.ShadowGrade(ctx, arjun, snap.ID, true, "yes", "re-graded")
	if err != nil {
		t.Fatal(err)
	}
	var gp events.ShadowGradedPayload
	_ = json.Unmarshal(ge.Payload, &gp)
	if gp.Identity == nil || gp.Identity.Grant != "user:arjun" {
		t.Fatalf("grade identity = %+v", gp.Identity)
	}
	m, err := env.svc.ShadowMetrics(ctx, "", time.Time{})
	if err != nil || m.Graded != 1 || m.LayerCorrect != 1 || m.RootCause.Yes != 1 || m.RootCause.Partial != 0 {
		t.Fatalf("last grade must win: %+v %v", m, err)
	}
	full, _ = env.svc.Shadow(ctx, snap.ID, room.ShadowReadOptions{Room: true})
	if full.Room.Graded == nil || full.Room.Graded.Seq != ge.Seq {
		t.Fatal("include=room graded is not the last grade")
	}
	assertNoAgentLeak(t, env.roomLog(t, snap.ID))
}

// Shadow routes on a live room are 404.
func TestShadowRoutesOnLiveRoom(t *testing.T) {
	env := newShadowEnv(t, shadowCfg{})
	ctx := context.Background()
	arjun := env.user(t, "arjun")
	live, err := env.svc.CreateRoom(ctx, arjun, "live", "sev2", "live")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := env.svc.CloseRoom(ctx, arjun, live.ID, ""); err != nil {
		t.Fatalf("a live room keeps today's close rule: %v", err)
	}
	if _, err := env.svc.ShadowLabel(ctx, arjun, live.ID, "kafka", []string{"x"}); err == nil {
		t.Fatal("label on a live room")
	} else {
		wantCode(t, err, "not_found")
	}
	if _, err := env.svc.Shadow(ctx, live.ID, room.ShadowReadOptions{}); err == nil {
		t.Fatal("read on a live room")
	} else {
		wantCode(t, err, "not_found")
	}
}

// canGradeShadow: the solo user, an owners.yaml approver and a backup pass with
// their grants; a users.yaml user with no layers and a group member of another
// pipeline do not.
func TestShadowGraderRule(t *testing.T) {
	cases := []struct {
		name  string
		solo  string
		user  room.User
		grant string // "" = refused with not_allowed
	}{
		{name: "solo", solo: "nolayers", user: room.User{ID: "nolayers", Name: "No Layers"}, grant: "solo"},
		{name: "approver of one layer", user: room.User{ID: "kabir", Name: "Kabir", Layers: []string{"consumer"}}, grant: "user:kabir"},
		{name: "backup of one layer", user: room.User{ID: "zoe", Name: "Zoe"}, grant: "backup:user:zoe"},
		{name: "pipeline owners approver", user: room.User{ID: "meera", Name: "Meera"}, grant: "user:meera"},
		{name: "users.yaml user with no layers", user: room.User{ID: "nolayers", Name: "No Layers"}},
		{name: "group member of another pipeline", user: room.User{ID: "g1", Name: "G", Groups: []string{"payments-oncall"}}},
		{name: "stranger", user: room.User{ID: "x", Name: "X"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			env := newShadowEnv(t, shadowCfg{solo: tc.solo})
			snap := env.open(t, "grader")
			env.waitFinished(t, snap.ID)
			env.close(t, snap.ID, "resolved")
			e, err := env.svc.ShadowLabel(context.Background(), tc.user, snap.ID, "kafka", []string{"x"})
			if tc.grant == "" {
				if err == nil {
					t.Fatal("refused person labelled")
				}
				wantCode(t, err, "not_allowed")
				return
			}
			if err != nil {
				t.Fatalf("label: %v", err)
			}
			var p events.ShadowLabelledPayload
			_ = json.Unmarshal(e.Payload, &p)
			if p.Identity == nil || p.Identity.Grant != tc.grant {
				t.Fatalf("grant = %+v, want %q", p.Identity, tc.grant)
			}
		})
	}
}

// Load: a running row becomes interrupted, an open room without a row gets
// one, and a closed room whose run ended but was not revealed is revealed.
func TestShadowLoadSweep(t *testing.T) {
	c := &clock{t: time.Date(2026, 10, 13, 9, 0, 0, 0, time.UTC)}
	mem := memstore.NewWithClock(c.Now)
	ctx := context.Background()
	mk := func(id string, closed bool) {
		if err := mem.CreateRoom(ctx, store.Room{ID: id, Title: "t", Severity: "sev2", PipelineID: "orders", Status: "open", OpenedAt: c.Now()}); err != nil {
			t.Fatal(err)
		}
		if _, err := mem.Append(ctx, id, events.RoomCreated, events.Actor{Kind: events.ActorSystem, ID: "system", Name: "System"},
			events.RoomCreatedPayload{Title: "t", Severity: "sev2", PipelineID: "orders", AlertSource: "generic", Shadow: true}); err != nil {
			t.Fatal(err)
		}
		if closed {
			if _, err := mem.Append(ctx, id, events.RoomClosed, events.Actor{Kind: events.ActorHuman, ID: "arjun", Name: "Arjun"},
				events.RoomClosedPayload{Resolution: "fixed"}); err != nil {
				t.Fatal(err)
			}
			if err := mem.CloseRoom(ctx, id, c.Now()); err != nil {
				t.Fatal(err)
			}
		}
	}
	mk("r_running", false)
	mk("r_norow", false)
	mk("r_closed", true)
	mk("r_closed_norow", true)
	start := c.Now()
	_ = mem.CreateShadowRun(ctx, store.ShadowRun{RoomID: "r_running", Status: store.ShadowRunning, StartedAt: &start})
	_ = mem.CreateShadowRun(ctx, store.ShadowRun{RoomID: "r_closed", Status: store.ShadowHypothesised, FinishedAt: &start, ToolCalls: 2})
	c.Advance(time.Minute)

	own, users := loadShadowFixtures(t)
	svc := room.New(room.Deps{Store: mem, Users: users, Owners: own, Now: c.Now, WriteHandoff: fakeWrite,
		Run: func(context.Context, agent.Alert, agent.Deps, agent.Options) error {
			t.Error("Load must not start a run")
			return nil
		}})
	t.Cleanup(svc.Close)
	if err := svc.Load(ctx); err != nil {
		t.Fatal(err)
	}
	if r, _ := mem.GetShadowRun(ctx, "r_running"); r.Status != store.ShadowInterrupted || r.FinishedAt == nil {
		t.Fatalf("running row after restart: %+v", r)
	}
	if r, err := mem.GetShadowRun(ctx, "r_norow"); err != nil || r.Status != store.ShadowInterrupted {
		t.Fatalf("open room without a row: %+v %v", r, err)
	}
	for _, id := range []string{"r_closed", "r_closed_norow"} {
		evs, _ := mem.Since(ctx, id, 0, 100)
		if countReveals(evs) != 1 {
			t.Fatalf("%s: reveals = %d", id, countReveals(evs))
		}
		if r, _ := mem.GetShadowRun(ctx, id); r.RevealedAt == nil {
			t.Fatalf("%s: revealed_at not set", id)
		}
		assertNoAgentLeak(t, evs)
	}
	if p := revealOf(t, mustSince(t, mem, "r_closed_norow")); p.Status != store.ShadowError {
		t.Fatalf("closed room without a row: reveal %+v", p)
	}
	for _, id := range []string{"r_running", "r_norow"} {
		assertNoAgentLeak(t, mustSince(t, mem, id))
		if n := len(mustSince(t, mem, id)); n != 1 {
			t.Fatalf("%s: Load wrote %d room events, want none", id, n-1)
		}
	}
	// A second Load reveals nothing twice.
	svc.Close()
	svc2 := room.New(room.Deps{Store: mem, Users: users, Owners: own, Now: c.Now, WriteHandoff: fakeWrite})
	t.Cleanup(svc2.Close)
	if err := svc2.Load(ctx); err != nil {
		t.Fatal(err)
	}
	if n := countReveals(mustSince(t, mem, "r_closed")); n != 1 {
		t.Fatalf("second Load: reveals = %d", n)
	}
}

func mustSince(t *testing.T, st *memstore.Store, id string) []events.Event {
	t.Helper()
	evs, err := st.Since(context.Background(), id, 0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	return evs
}

// A store without ShadowStore cannot load a config with a shadow pipeline.
type plainStore struct{ store.Store }

func TestShadowLoadRefusesStoreWithoutShadow(t *testing.T) {
	own, users := loadShadowFixtures(t)
	svc := room.New(room.Deps{Store: plainStore{memstore.New()}, Users: users, Owners: own})
	t.Cleanup(svc.Close)
	if err := svc.Load(context.Background()); err == nil || !strings.Contains(err.Error(), "shadow") {
		t.Fatalf("Load = %v, want a shadow store error", err)
	}
}

// Metrics: every closed shadow room counts in rooms; medians over 0, 1, 2 and
// an odd number of values; ungraded rooms count only in rooms; tokens.
func TestShadowMetrics(t *testing.T) {
	ctx := context.Background()
	env := newShadowEnv(t, shadowCfg{fn: func(ctx context.Context, call int, d agent.Deps) error {
		investigate(ctx, d, call != 3) // the third run never hypothesises
		return endTurn(ctx, d)
	}})
	m, err := env.svc.ShadowMetrics(ctx, "", time.Time{})
	if err != nil || m.Rooms != 0 || m.MedianTimeToFirstS != -1 || m.MedianHumanMTTRS != -1 || m.PerRoom == nil || m.TokensAvailable {
		t.Fatalf("empty metrics: %+v %v", m, err)
	}
	var ids []string
	// Room i: hypothesis at +i min (clock moves before the run), closed at +10*i min.
	for i := 1; i <= 3; i++ {
		openAt := env.clk.Now()
		snap := env.open(t, fmt.Sprint("m", i))
		_ = openAt
		env.waitFinished(t, snap.ID)
		ids = append(ids, snap.ID)
		env.clk.Advance(time.Duration(10*i) * time.Minute)
		env.close(t, snap.ID, "resolved")
		if i == 1 {
			m, _ = env.svc.ShadowMetrics(ctx, "", time.Time{})
			if m.Rooms != 1 || m.MedianHumanMTTRS != 600 || m.MedianTimeToFirstS != 0 {
				t.Fatalf("one room: %+v", m)
			}
		}
		if i == 2 {
			m, _ = env.svc.ShadowMetrics(ctx, "", time.Time{})
			if m.Rooms != 2 || m.MedianHumanMTTRS != 900 { // (600 + 1200) / 2
				t.Fatalf("two rooms: %+v", m)
			}
		}
	}
	priya := env.user(t, "priya")
	if _, err := env.svc.ShadowLabel(ctx, priya, ids[0], "kafka-connect", []string{"x"}); err != nil {
		t.Fatal(err)
	}
	if _, err := env.svc.ShadowGrade(ctx, priya, ids[0], true, "partial", ""); err != nil {
		t.Fatal(err)
	}
	if _, err := env.svc.ShadowLabel(ctx, priya, ids[1], "kafka", []string{"x"}); err != nil {
		t.Fatal(err)
	}
	m, err = env.svc.ShadowMetrics(ctx, "orders", time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if m.Rooms != 3 || m.Labelled != 2 || m.Graded != 1 || m.LayerCorrect != 1 || m.RootCause.Partial != 1 {
		t.Fatalf("counts: %+v", m)
	}
	if m.MedianHumanMTTRS != 1200 || m.ByStatus[store.ShadowHypothesised] != 2 || m.ByStatus[store.ShadowNoHypothesis] != 1 {
		t.Fatalf("median/status: %+v", m)
	}
	if m.MedianTimeToFirstS != 0 || m.TokensAvailable || m.TotalTokens != 0 || len(m.PerRoom) != 3 {
		t.Fatalf("hypothesis/tokens: %+v", m)
	}
	for _, r := range m.PerRoom {
		if r.RoomID == ids[2] && (r.TimeToFirstS != -1 || r.Status != store.ShadowNoHypothesis || r.Graded || r.LayerCorrect != nil) {
			t.Fatalf("ungraded no-hypothesis row: %+v", r)
		}
		if r.RoomID == ids[0] && (r.LayerCorrect == nil || !*r.LayerCorrect || r.RootCause != "partial" || r.AlertSource != "generic") {
			t.Fatalf("graded row: %+v", r)
		}
	}
	if m, _ := env.svc.ShadowMetrics(ctx, "payments", time.Time{}); m.Rooms != 0 {
		t.Fatal("pipeline filter")
	}
	if m, _ := env.svc.ShadowMetrics(ctx, "", env.clk.Now().Add(time.Hour)); m.Rooms != 0 {
		t.Fatal("since filter")
	}
}

// Tokens reported through Usage show up in the metrics.
func TestShadowMetricsTokens(t *testing.T) {
	env := newShadowEnv(t, shadowCfg{fn: func(ctx context.Context, _ int, d agent.Deps) error {
		investigate(ctx, d, true)
		d.Usage(1000)
		return endTurn(ctx, d)
	}})
	snap := env.open(t, "tok")
	env.waitFinished(t, snap.ID)
	env.close(t, snap.ID, "ok")
	m, err := env.svc.ShadowMetrics(context.Background(), "", time.Time{})
	if err != nil || !m.TokensAvailable || m.TotalTokens != 1000 {
		t.Fatalf("tokens: %+v %v", m, err)
	}
	if p := revealOf(t, env.roomLog(t, snap.ID)); p.Tokens != 1000 || p.ToolCalls != 1 {
		t.Fatalf("reveal = %+v", p)
	}
}
