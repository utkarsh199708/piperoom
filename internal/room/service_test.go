package room_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"piperoom/internal/actions"
	"piperoom/internal/agent"
	"piperoom/internal/alertin"
	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/room"
	"piperoom/internal/store"
	"piperoom/internal/store/memstore"
)

// ---- fakes ----

type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) Now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *clock) Add(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

type fakeAction struct {
	name, layer string
	runs        atomic.Int32
	fail        bool
	unscoped    bool
	scopeErr    error
	order       *[]string
	mu          *sync.Mutex
}

func (a *fakeAction) Name() string                 { return a.name }
func (a *fakeAction) Layer() string                { return a.layer }
func (a *fakeAction) Describe() string             { return a.name }
func (a *fakeAction) ParamsSchema() map[string]any { return map[string]any{"type": "object"} }
func (a *fakeAction) Validate(p json.RawMessage) error {
	if !json.Valid(p) {
		return errors.New("params are not JSON")
	}
	return nil
}
func (a *fakeAction) Execute(context.Context, json.RawMessage) (string, error) {
	a.runs.Add(1)
	a.mu.Lock()
	*a.order = append(*a.order, a.name)
	a.mu.Unlock()
	if a.fail {
		return "", errors.New("boom")
	}
	return a.name + " ok", nil
}

type scopedAction struct{ *fakeAction }

func (a scopedAction) CheckScope(p owners.Pipeline, _ json.RawMessage) error {
	if p.ID != "orders" {
		return errors.New("wrong pipeline")
	}
	return a.scopeErr
}

type env struct {
	t     *testing.T
	svc   *room.Service
	st    *memstore.Store
	clk   *clock
	users *room.Users
	acts  map[string]*fakeAction
	order *[]string

	runnerStarted chan agent.Deps
	alerts        chan agent.Alert
}

var (
	kabir, arjun, meera, priya room.User
)

func newEnv(t *testing.T, withRunner bool) *env {
	t.Helper()
	clk := &clock{t: time.Date(2026, 10, 11, 10, 0, 0, 0, time.UTC)}
	st := memstore.NewWithClock(clk.Now)
	users := room.DemoUsers()
	for id, p := range map[string]*room.User{"kabir": &kabir, "arjun": &arjun, "meera": &meera, "priya": &priya} {
		*p, _ = users.Get(id)
	}
	own := &owners.Owners{
		Pipelines: []owners.Pipeline{{ID: "orders", TopicPrefix: "pg", SourceTables: []string{"public.orders"}}},
		Layers: map[string]string{"postgres": "priya", "schema-registry": "priya", "kafka-connect": "arjun", "kafka": "arjun",
			"consumer": "kabir", "warehouse": "meera"},
	}
	order := &[]string{}
	mu := &sync.Mutex{}
	e := &env{t: t, st: st, clk: clk, users: users, acts: map[string]*fakeAction{}, order: order,
		runnerStarted: make(chan agent.Deps, 4), alerts: make(chan agent.Alert, 4)}
	reg := actions.NewRegistry()
	add := func(a *fakeAction, scoped bool) {
		a.order, a.mu = order, mu
		e.acts[a.name] = a
		var act actions.Action = a
		if scoped {
			act = scopedAction{a}
		}
		if err := reg.Register(act); err != nil {
			t.Fatal(err)
		}
	}
	add(&fakeAction{name: "restart", layer: "kafka-connect"}, true)
	add(&fakeAction{name: "compat", layer: "schema-registry"}, true)
	add(&fakeAction{name: "backfill", layer: "warehouse"}, true)
	add(&fakeAction{name: "failing", layer: "kafka", fail: true}, true)
	add(&fakeAction{name: "unscoped", layer: "kafka"}, false)
	add(&fakeAction{name: "outofscope", layer: "kafka", scopeErr: errors.New("targets another pipeline")}, true)

	cfg := room.Config{
		Store: st, Owners: own, Users: users, Actions: reg, Now: clk.Now,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		Handoff: func(_ context.Context, evs []events.Event, _ agent.Options) (events.HandoffSummary, error) {
			return events.HandoffSummary{WhatBroke: fmt.Sprintf("%d events", len(evs))}, nil
		},
	}
	if withRunner {
		cfg.Runner = func(ctx context.Context, a agent.Alert, d agent.Deps, _ agent.Options) error {
			e.alerts <- a
			e.runnerStarted <- d
			<-ctx.Done()
			return nil
		}
	}
	svc, err := room.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(svc.Shutdown)
	e.svc = svc
	return e
}

func (e *env) open(u room.User) room.Snapshot {
	e.t.Helper()
	s, err := e.svc.CreateRoom(context.Background(), u, room.CreateRoomInput{Title: "Orders stale", Severity: "sev2", PipelineID: "orders"})
	if err != nil {
		e.t.Fatal(err)
	}
	return s
}

func wantErr(t *testing.T, err error, status int, code string) {
	t.Helper()
	re, ok := room.AsError(err)
	if !ok {
		t.Fatalf("err = %v, want *room.Error %d %s", err, status, code)
	}
	if re.Status != status || re.Code != code {
		t.Fatalf("err = %d %s (%s), want %d %s", re.Status, re.Code, re.Message, status, code)
	}
}

func (e *env) types(roomID string) []events.Type {
	e.t.Helper()
	evs, err := e.st.Since(context.Background(), roomID, 0, 1000)
	if err != nil {
		e.t.Fatal(err)
	}
	out := make([]events.Type, len(evs))
	for i, ev := range evs {
		out[i] = ev.Type
	}
	return out
}

func (e *env) propose(roomID string, d agent.Deps, steps ...events.ProposalStep) (string, error) {
	return d.Proposals.Create(context.Background(), events.ProposalCreatedPayload{Title: "fix", Steps: steps, Risk: "r", Rollback: "b"})
}

func step(action string) events.ProposalStep {
	return events.ProposalStep{Action: action, Params: json.RawMessage(`{"x":1}`), Description: action}
}

var ctx = context.Background()

// ---- rooms ----

func TestCreateRoomValidation(t *testing.T) {
	e := newEnv(t, false)
	for _, in := range []room.CreateRoomInput{
		{Title: "", Severity: "sev2", PipelineID: "orders"},
		{Title: "x", Severity: "sev9", PipelineID: "orders"},
		{Title: "x", Severity: "sev2", PipelineID: "nope"},
	} {
		_, err := e.svc.CreateRoom(ctx, kabir, in)
		wantErr(t, err, 400, "validation")
	}
}

func TestCreateRoomMakesCreatorDriverAndStartsAgent(t *testing.T) {
	e := newEnv(t, true)
	s := e.open(kabir)
	if s.Driver == nil || s.Driver.ID != "kabir" || s.Status != room.StatusOpen || s.LastSeq != 1 {
		t.Fatalf("snapshot = %+v", s)
	}
	select {
	case a := <-e.alerts:
		if a.RoomID != s.ID || a.PipelineID != "orders" || a.Title != "Orders stale" {
			t.Fatalf("alert = %+v", a)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("agent was not started")
	}
}

func TestUnknownRoomIs404(t *testing.T) {
	e := newEnv(t, false)
	_, err := e.svc.GetRoom(ctx, "r_nope")
	wantErr(t, err, 404, "not_found")
	_, err = e.svc.Comment(ctx, kabir, "r_nope", room.CommentInput{Text: "hi"})
	wantErr(t, err, 404, "not_found")
}

func TestListRoomsOpenFirst(t *testing.T) {
	e := newEnv(t, false)
	a := e.open(kabir)
	e.clk.Add(time.Minute)
	b := e.open(kabir)
	if _, err := e.svc.Close(ctx, kabir, a.ID, "done"); err != nil {
		t.Fatal(err)
	}
	got, err := e.svc.ListRooms(ctx)
	if err != nil || len(got) != 2 || got[0].ID != b.ID || got[1].ID != a.ID {
		t.Fatalf("list = %+v, err %v", got, err)
	}
}

func TestEventsPageIsNeverNil(t *testing.T) {
	e := newEnv(t, false)
	s := e.open(kabir)
	p, err := e.svc.Events(ctx, s.ID, 99, 0)
	if err != nil || p.Events == nil || len(p.Events) != 0 || p.LastSeq != 1 {
		t.Fatalf("page = %+v err %v", p, err)
	}
}

// ---- comments and steers ----

func TestCommentMentionsAgent(t *testing.T) {
	e := newEnv(t, false)
	s := e.open(kabir)
	for text, want := range map[string]bool{"@agent check lag": true, "hey @Agent": true, "mail me@agentfoo": false, "no mention": false} {
		ev, err := e.svc.Comment(ctx, arjun, s.ID, room.CommentInput{Text: text})
		if err != nil {
			t.Fatal(err)
		}
		p, _ := events.Decode[events.CommentAddedPayload](ev)
		if p.MentionsAgent != want || p.CommentID == "" {
			t.Errorf("%q: mentions_agent = %v, want %v", text, p.MentionsAgent, want)
		}
	}
	_, err := e.svc.Comment(ctx, arjun, s.ID, room.CommentInput{Text: "   "})
	wantErr(t, err, 400, "validation")
}

func TestSteerDriverSendsOthersSuggest(t *testing.T) {
	e := newEnv(t, true)
	s := e.open(kabir)
	deps := <-e.runnerStarted

	ev, err := e.svc.Steer(ctx, arjun, s.ID, "look at the slot")
	if err != nil || ev.Type != events.SteerSuggested {
		t.Fatalf("non-driver steer = %v, %v", ev.Type, err)
	}
	sp, _ := events.Decode[events.SteerSuggestedPayload](ev)
	snap, _ := e.svc.GetRoom(ctx, s.ID)
	if len(snap.PendingSuggestions) != 1 || snap.PendingSuggestions[0].From.ID != "arjun" {
		t.Fatalf("pending = %+v", snap.PendingSuggestions)
	}
	if ins, _ := deps.Inbox.Drain(ctx); len(ins) != 0 {
		t.Fatalf("a suggestion must not reach the agent: %+v", ins)
	}

	ev, err = e.svc.Steer(ctx, kabir, s.ID, "check lag first")
	if err != nil || ev.Type != events.SteerSent {
		t.Fatalf("driver steer = %v, %v", ev.Type, err)
	}
	ins, _ := deps.Inbox.Drain(ctx)
	if len(ins) != 1 || ins[0].Kind != "steer" || ins[0].Text != "check lag first" || ins[0].Actor.ID != "kabir" {
		t.Fatalf("agent inbox = %+v", ins)
	}

	// Only the driver may forward; a second send is a conflict.
	_, err = e.svc.SendSuggestion(ctx, arjun, s.ID, sp.SuggestionID)
	wantErr(t, err, 403, "not_driver")
	ev, err = e.svc.SendSuggestion(ctx, kabir, s.ID, sp.SuggestionID)
	if err != nil {
		t.Fatal(err)
	}
	if p, _ := events.Decode[events.SteerSentPayload](ev); p.SuggestionID != sp.SuggestionID || p.Text != "look at the slot" {
		t.Fatalf("payload = %+v", p)
	}
	_, err = e.svc.SendSuggestion(ctx, kabir, s.ID, sp.SuggestionID)
	wantErr(t, err, 409, "already_decided")
	_, err = e.svc.SendSuggestion(ctx, kabir, s.ID, "g_missing")
	wantErr(t, err, 404, "not_found")
}

func TestMentionReachesAgentOthersDoNot(t *testing.T) {
	e := newEnv(t, true)
	s := e.open(kabir)
	deps := <-e.runnerStarted
	_, _ = e.svc.Comment(ctx, arjun, s.ID, room.CommentInput{Text: "plain comment", StepID: "s_1"})
	_, _ = e.svc.Comment(ctx, arjun, s.ID, room.CommentInput{Text: "@agent why?", StepID: "s_2"})
	ins, _ := deps.Inbox.Drain(ctx)
	if len(ins) != 1 || ins[0].Kind != "comment" || ins[0].StepID != "s_2" || ins[0].CommentID == "" {
		t.Fatalf("inbox = %+v", ins)
	}
}

// ---- driver lease ----

func TestControlRules(t *testing.T) {
	e := newEnv(t, false)
	s := e.open(kabir)

	// A live lease cannot be taken.
	_, err := e.svc.GrantControl(ctx, arjun, s.ID, "arjun")
	wantErr(t, err, 409, "lease_held")
	// Requesting is allowed; the driver cannot request from themselves.
	if _, err := e.svc.RequestControl(ctx, arjun, s.ID); err != nil {
		t.Fatal(err)
	}
	_, err = e.svc.RequestControl(ctx, kabir, s.ID)
	wantErr(t, err, 400, "validation")
	// Only the driver steers/closes.
	_, err = e.svc.Close(ctx, arjun, s.ID, "x")
	wantErr(t, err, 403, "not_driver")

	// Expired lease: anyone can take over.
	e.clk.Add(31 * time.Minute)
	ev, err := e.svc.GrantControl(ctx, arjun, s.ID, "arjun")
	if err != nil {
		t.Fatal(err)
	}
	if p, _ := events.Decode[events.ControlGrantedPayload](ev); p.To.ID != "arjun" || !p.LeaseUntil.Equal(e.clk.Now().Add(30*time.Minute)) {
		t.Fatalf("grant = %+v", p)
	}
	snap, _ := e.svc.GetRoom(ctx, s.ID)
	if snap.Driver.ID != "arjun" {
		t.Fatalf("driver = %+v", snap.Driver)
	}
	// The driver may hand over directly.
	if _, err := e.svc.GrantControl(ctx, arjun, s.ID, "meera"); err != nil {
		t.Fatal(err)
	}
	_, err = e.svc.GrantControl(ctx, arjun, s.ID, "nobody")
	wantErr(t, err, 400, "validation")
}

func TestLeaseRenewalIsLoggedAndRateLimited(t *testing.T) {
	e := newEnv(t, false)
	s := e.open(kabir)
	count := func() (n int) {
		for _, ty := range e.types(s.ID) {
			if ty == events.ControlGranted {
				n++
			}
		}
		return
	}
	// 10 minutes in: 20 min left (< 25) but the room is only 10 min old, so a renewal is allowed (>= 5 min since grant).
	e.clk.Add(10 * time.Minute)
	if _, err := e.svc.Steer(ctx, kabir, s.ID, "one"); err != nil {
		t.Fatal(err)
	}
	if count() != 1 {
		t.Fatalf("expected one renewal event, got %d", count())
	}
	snap, _ := e.svc.GetRoom(ctx, s.ID)
	if !snap.Driver.LeaseUntil.Equal(e.clk.Now().Add(30 * time.Minute)) {
		t.Fatalf("lease = %v", snap.Driver.LeaseUntil)
	}
	// Right after: lease has 30 min left, nothing to renew.
	if _, err := e.svc.Steer(ctx, kabir, s.ID, "two"); err != nil {
		t.Fatal(err)
	}
	// 6 minutes later 24 min remain (< 25) and >= 5 min since the grant: renew once.
	e.clk.Add(6 * time.Minute)
	_, _ = e.svc.Steer(ctx, kabir, s.ID, "three")
	// 2 minutes later: under the 5 minute gap, no further renewal.
	e.clk.Add(2 * time.Minute)
	_, _ = e.svc.Steer(ctx, kabir, s.ID, "four")
	if count() != 2 {
		t.Fatalf("renewals = %d, want 2", count())
	}
}

func TestExpiredDriverStillActsAndRenews(t *testing.T) {
	e := newEnv(t, false)
	s := e.open(kabir)
	e.clk.Add(2 * time.Hour)
	if _, err := e.svc.Steer(ctx, kabir, s.ID, "back"); err != nil {
		t.Fatalf("an expired driver who was not replaced keeps the role: %v", err)
	}
	snap, _ := e.svc.GetRoom(ctx, s.ID)
	if snap.Driver.ID != "kabir" || !snap.Driver.LeaseUntil.After(e.clk.Now()) {
		t.Fatalf("driver = %+v", snap.Driver)
	}
}

func TestClosedRoomRejectsWrites(t *testing.T) {
	e := newEnv(t, false)
	s := e.open(kabir)
	if _, err := e.svc.Close(ctx, kabir, s.ID, "fixed"); err != nil {
		t.Fatal(err)
	}
	snap, _ := e.svc.GetRoom(ctx, s.ID)
	if snap.Status != room.StatusClosed || snap.Resolution != "fixed" || snap.ClosedAt == nil {
		t.Fatalf("snapshot = %+v", snap)
	}
	_, err := e.svc.Comment(ctx, kabir, s.ID, room.CommentInput{Text: "x"})
	wantErr(t, err, 409, "room_closed")
	_, err = e.svc.Steer(ctx, kabir, s.ID, "x")
	wantErr(t, err, 409, "room_closed")
	_, err = e.svc.Close(ctx, kabir, s.ID, "again")
	wantErr(t, err, 409, "room_closed")
	_, err = e.svc.RequestControl(ctx, arjun, s.ID)
	wantErr(t, err, 409, "room_closed")
}

func TestCloseStopsAgent(t *testing.T) {
	e := newEnv(t, true)
	s := e.open(kabir)
	<-e.runnerStarted
	if _, err := e.svc.Close(ctx, kabir, s.ID, "done"); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { e.svc.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("agent goroutine did not stop after the room closed")
	}
}

// ---- proposals, approvals, executor ----

func openWithAgent(t *testing.T) (*env, room.Snapshot, agent.Deps) {
	e := newEnv(t, true)
	s := e.open(kabir)
	return e, s, <-e.runnerStarted
}

func TestProposalValidation(t *testing.T) {
	e, s, d := openWithAgent(t)
	cases := map[string][]events.ProposalStep{
		"no steps":       nil,
		"unknown action": {step("nope")},
		"not scoped":     {step("unscoped")},
		"out of scope":   {step("outofscope")},
		"bad params":     {{Action: "restart", Params: json.RawMessage(`not json`)}},
		"wrong layer":    {{Action: "restart", Layer: "warehouse", Params: json.RawMessage(`{}`)}},
		"too many steps": make([]events.ProposalStep, 11),
	}
	for name, steps := range cases {
		if _, err := e.propose(s.ID, d, steps...); err == nil {
			t.Errorf("%s: want an error", name)
		}
	}
	if got := e.types(s.ID); slices(got, events.ProposalCreated) {
		t.Fatalf("rejected proposals must not reach the log: %v", got)
	}
}

func slices(ts []events.Type, want events.Type) bool {
	for _, t := range ts {
		if t == want {
			return true
		}
	}
	return false
}

func TestProposalDerivesRequiredLayers(t *testing.T) {
	e, s, d := openWithAgent(t)
	// The proposer states an own layer list in the payload; the server must ignore it.
	pid, err := d.Proposals.Create(ctx, events.ProposalCreatedPayload{
		Title: "fix", RequiredLayers: []string{"postgres"},
		Steps: []events.ProposalStep{step("backfill"), step("compat"), step("restart")},
	})
	if err != nil {
		t.Fatal(err)
	}
	snap, _ := e.svc.GetRoom(ctx, s.ID)
	if len(snap.OpenProposals) != 1 || snap.OpenProposals[0].ProposalID != pid {
		t.Fatalf("open = %+v", snap.OpenProposals)
	}
	got := snap.OpenProposals[0].RequiredLayers
	want := []string{"kafka-connect", "schema-registry", "warehouse"} // events.Layers order
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("required layers = %v, want %v", got, want)
	}
	if snap.OpenProposals[0].Steps[0].Layer != "warehouse" {
		t.Fatalf("step layer must come from the action: %+v", snap.OpenProposals[0].Steps[0])
	}
}

func waitExecuted(t *testing.T, e *env, roomID string, n int) []events.ActionExecutedPayload {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		evs, _ := e.st.Since(ctx, roomID, 0, 1000)
		var out []events.ActionExecutedPayload
		for _, ev := range evs {
			if ev.Type == events.ActionExecuted {
				p, _ := events.Decode[events.ActionExecutedPayload](ev)
				out = append(out, p)
			}
		}
		if len(out) >= n {
			return out
		}
		if time.Now().After(deadline) {
			t.Fatalf("saw %d action.executed events, want %d", len(out), n)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestApprovalFlowRunsStepsInOrderOnce(t *testing.T) {
	e, s, d := openWithAgent(t)
	pid, err := e.propose(s.ID, d, step("compat"), step("backfill"))
	if err != nil {
		t.Fatal(err)
	}
	// Not the owner of the layer.
	_, err = e.svc.Approve(ctx, kabir, pid, "warehouse")
	wantErr(t, err, 403, "not_layer_owner")
	// A layer the proposal does not need.
	_, err = e.svc.Approve(ctx, arjun, pid, "kafka-connect")
	wantErr(t, err, 400, "validation")
	_, err = e.svc.Approve(ctx, arjun, "p_missing", "kafka-connect")
	wantErr(t, err, 404, "not_found")

	if _, err := e.svc.Approve(ctx, priya, pid, "schema-registry"); err != nil {
		t.Fatal(err)
	}
	if n := e.acts["compat"].runs.Load(); n != 0 {
		t.Fatalf("nothing may run before every layer approved, compat ran %d times", n)
	}
	_, err = e.svc.Approve(ctx, priya, pid, "schema-registry")
	wantErr(t, err, 409, "already_decided")

	if _, err := e.svc.Approve(ctx, meera, pid, "warehouse"); err != nil {
		t.Fatal(err)
	}
	res := waitExecuted(t, e, s.ID, 2)
	e.svc.Wait()
	if !res[0].OK || !res[1].OK || res[0].StepIndex != 0 || res[1].StepIndex != 1 || res[0].Output != "compat ok" {
		t.Fatalf("results = %+v", res)
	}
	if fmt.Sprint(*e.order) != "[compat backfill]" {
		t.Fatalf("order = %v", *e.order)
	}
	for _, n := range []string{"compat", "backfill"} {
		if e.acts[n].runs.Load() != 1 {
			t.Fatalf("%s ran %d times, want exactly 1", n, e.acts[n].runs.Load())
		}
	}
	// A finished proposal cannot be approved again.
	_, err = e.svc.Approve(ctx, meera, pid, "warehouse")
	wantErr(t, err, 409, "already_decided")
	if snap, _ := e.svc.GetRoom(ctx, s.ID); len(snap.OpenProposals) != 0 {
		t.Fatalf("finished proposal still open: %+v", snap.OpenProposals)
	}
	// The agent hears about approvals and results.
	ins, _ := d.Inbox.Drain(ctx)
	kinds := map[string]int{}
	for _, in := range ins {
		kinds[in.Kind]++
	}
	if kinds["approval_result"] != 2 || kinds["action_result"] != 2 {
		t.Fatalf("agent inputs = %v", kinds)
	}
}

func TestConcurrentApprovalsRunEachStepOnce(t *testing.T) {
	e, s, d := openWithAgent(t)
	pid, _ := e.propose(s.ID, d, step("compat"), step("backfill"), step("restart"))
	var wg sync.WaitGroup
	for _, c := range []struct {
		u room.User
		l string
	}{{priya, "schema-registry"}, {meera, "warehouse"}, {arjun, "kafka-connect"}} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := e.svc.Approve(ctx, c.u, pid, c.l); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	waitExecuted(t, e, s.ID, 3)
	e.svc.Wait()
	for n, a := range e.acts {
		if r := a.runs.Load(); (n == "compat" || n == "backfill" || n == "restart") && r != 1 {
			t.Fatalf("%s ran %d times", n, r)
		}
	}
}

func TestExecutorStopsAtFirstFailure(t *testing.T) {
	e, s, d := openWithAgent(t)
	pid, err := e.propose(s.ID, d, step("compat"), step("failing"), step("backfill"))
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		u room.User
		l string
	}{{priya, "schema-registry"}, {arjun, "kafka"}, {meera, "warehouse"}} {
		if _, err := e.svc.Approve(ctx, c.u, pid, c.l); err != nil {
			t.Fatal(err)
		}
	}
	res := waitExecuted(t, e, s.ID, 2)
	e.svc.Wait()
	if len(res) != 2 || !res[0].OK || res[1].OK || res[1].StepIndex != 1 || res[1].Output != "boom" {
		t.Fatalf("results = %+v", res)
	}
	if e.acts["backfill"].runs.Load() != 0 {
		t.Fatal("a step after a failed step must not run")
	}
	if snap, _ := e.svc.GetRoom(ctx, s.ID); len(snap.OpenProposals) != 0 {
		t.Fatalf("a failed proposal is finished: %+v", snap.OpenProposals)
	}
}

func TestRejectStopsProposal(t *testing.T) {
	e, s, d := openWithAgent(t)
	pid, _ := e.propose(s.ID, d, step("compat"), step("backfill"))
	_, err := e.svc.Reject(ctx, kabir, pid, "warehouse", "no")
	wantErr(t, err, 403, "not_layer_owner")
	ev, err := e.svc.Reject(ctx, meera, pid, "warehouse", "too risky")
	if err != nil {
		t.Fatal(err)
	}
	if p, _ := events.Decode[events.ApprovalRejectedPayload](ev); p.Reason != "too risky" || p.Layer != "warehouse" {
		t.Fatalf("payload = %+v", p)
	}
	// The other owner can no longer approve it, so nothing can ever run.
	_, err = e.svc.Approve(ctx, priya, pid, "schema-registry")
	wantErr(t, err, 409, "already_decided")
	e.svc.Wait()
	if e.acts["compat"].runs.Load() != 0 {
		t.Fatal("a rejected proposal must not run")
	}
}

func TestExecutorRechecksScopeBeforeExecute(t *testing.T) {
	e, s, d := openWithAgent(t)
	pid, err := e.propose(s.ID, d, step("compat"))
	if err != nil {
		t.Fatal(err)
	}
	// The action's view of the world changes after approval but before it runs.
	e.acts["compat"].scopeErr = errors.New("moved out of scope")
	// scopedAction copies the pointer, so the shared fakeAction sees the change.
	if _, err := e.svc.Approve(ctx, priya, pid, "schema-registry"); err != nil {
		t.Fatal(err)
	}
	res := waitExecuted(t, e, s.ID, 1)
	e.svc.Wait()
	if res[0].OK || e.acts["compat"].runs.Load() != 0 {
		t.Fatalf("a step that fails the scope check must be refused, got %+v (ran %d)", res[0], e.acts["compat"].runs.Load())
	}
}

// ---- hand-off ----

func TestHandoffFlow(t *testing.T) {
	e := newEnv(t, false)
	s := e.open(kabir)

	_, err := e.svc.CreateHandoff(ctx, arjun, s.ID, room.HandoffInput{To: "meera"})
	wantErr(t, err, 403, "not_driver")
	_, err = e.svc.CreateHandoff(ctx, kabir, s.ID, room.HandoffInput{To: "kabir"})
	wantErr(t, err, 400, "validation")
	_, err = e.svc.CreateHandoff(ctx, kabir, s.ID, room.HandoffInput{To: "ghost"})
	wantErr(t, err, 400, "validation")

	ev, err := e.svc.CreateHandoff(ctx, kabir, s.ID, room.HandoffInput{To: "arjun", StayAsWatcher: true})
	if err != nil {
		t.Fatal(err)
	}
	hp, _ := events.Decode[events.HandoffCreatedPayload](ev)
	if hp.To.ID != "arjun" || hp.HandoffID == "" || hp.Summary.WhatBroke == "" {
		t.Fatalf("handoff = %+v", hp)
	}
	_, err = e.svc.AcceptHandoff(ctx, meera, hp.HandoffID)
	wantErr(t, err, 403, "not_recipient")
	_, err = e.svc.AcceptHandoff(ctx, arjun, "h_missing")
	wantErr(t, err, 404, "not_found")
	if _, err := e.svc.AcceptHandoff(ctx, arjun, hp.HandoffID); err != nil {
		t.Fatal(err)
	}
	snap, _ := e.svc.GetRoom(ctx, s.ID)
	if snap.Driver.ID != "arjun" {
		t.Fatalf("driver = %+v", snap.Driver)
	}
	_, err = e.svc.AcceptHandoff(ctx, arjun, hp.HandoffID)
	wantErr(t, err, 409, "already_decided")
}

func TestHandoffStaleAndVoided(t *testing.T) {
	e := newEnv(t, false)
	s := e.open(kabir)
	first, err := e.svc.CreateHandoff(ctx, kabir, s.ID, room.HandoffInput{To: "arjun"})
	if err != nil {
		t.Fatal(err)
	}
	second, _ := e.svc.CreateHandoff(ctx, kabir, s.ID, room.HandoffInput{To: "meera"})
	h1, _ := events.Decode[events.HandoffCreatedPayload](first)
	h2, _ := events.Decode[events.HandoffCreatedPayload](second)
	_, err = e.svc.AcceptHandoff(ctx, arjun, h1.HandoffID)
	wantErr(t, err, 409, "stale_handoff") // voided by the newer one

	// The creator stops being the driver before the recipient accepts.
	e.clk.Add(time.Minute)
	if _, err := e.svc.GrantControl(ctx, kabir, s.ID, "priya"); err != nil {
		t.Fatal(err)
	}
	_, err = e.svc.AcceptHandoff(ctx, meera, h2.HandoffID)
	wantErr(t, err, 409, "stale_handoff")
}

func TestHandoffSummaryFailureIs502(t *testing.T) {
	e := newEnv(t, false)
	_ = e
	// Build a service whose writer fails.
	clk := &clock{t: time.Now()}
	st := memstore.NewWithClock(clk.Now)
	svc, err := room.New(room.Config{
		Store: st, Users: room.DemoUsers(), Actions: actions.NewRegistry(), Now: clk.Now,
		Owners: &owners.Owners{Pipelines: []owners.Pipeline{{ID: "orders"}}},
		Handoff: func(context.Context, []events.Event, agent.Options) (events.HandoffSummary, error) {
			return events.HandoffSummary{}, errors.New("api down: secret-detail")
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer svc.Shutdown()
	s, _ := svc.CreateRoom(ctx, kabir, room.CreateRoomInput{Title: "t", Severity: "sev3", PipelineID: "orders"})
	_, err = svc.CreateHandoff(ctx, kabir, s.ID, room.HandoffInput{To: "arjun"})
	wantErr(t, err, 502, "summary_failed")
	if re, _ := room.AsError(err); re != nil && (len(re.Message) == 0 || containsStr(re.Message, "secret-detail")) {
		t.Fatalf("internal error text leaked: %q", re.Message)
	}
}

func containsStr(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

// ---- alerts ----

func TestIngestAlertDedupAndUnknownPipeline(t *testing.T) {
	e := newEnv(t, true)
	a := alertin.Alert{Source: alertin.Generic, Title: "Orders stale", Severity: "sev2", PipelineID: "orders",
		DedupKey: "generic:1", Raw: json.RawMessage(`{"x":1}`)}
	s1, err := e.svc.IngestAlert(ctx, a)
	if err != nil {
		t.Fatal(err)
	}
	if s1.Driver != nil || s1.Severity != "sev2" {
		t.Fatalf("alert room has no driver: %+v", s1)
	}
	s2, _ := e.svc.IngestAlert(ctx, a)
	if s2.ID != s1.ID {
		t.Fatalf("a repeated alert must reuse the open room: %s vs %s", s2.ID, s1.ID)
	}
	evs, _ := e.st.Since(ctx, s1.ID, 0, 10)
	var rc events.RoomCreatedPayload
	_ = json.Unmarshal(evs[0].Payload, &rc)
	if string(rc.Alert) != `{"x":1}` {
		t.Fatalf("raw alert must be stored verbatim, got %s", rc.Alert)
	}
	// After the room closes, the same incident opens a new room.
	_, _ = e.svc.GrantControl(ctx, kabir, s1.ID, "kabir")
	if _, err := e.svc.Close(ctx, kabir, s1.ID, "x"); err != nil {
		t.Fatal(err)
	}
	if s3, _ := e.svc.IngestAlert(ctx, a); s3.ID == s1.ID {
		t.Fatal("closed room must not be reused")
	}

	unknown := alertin.Alert{Title: "mystery", Severity: "sev3", PipelineID: "ghost", DedupKey: "generic:2"}
	su, err := e.svc.IngestAlert(ctx, unknown)
	if err != nil {
		t.Fatal(err)
	}
	if su.PipelineID != "" {
		t.Fatalf("unknown pipeline must not be stored as if valid: %q", su.PipelineID)
	}
	evs, _ = e.st.Since(ctx, su.ID, 0, 10)
	if len(evs) != 2 || evs[1].Type != events.CommentAdded || evs[1].Actor.Kind != events.ActorSystem {
		t.Fatalf("expected a system message, got %+v", evs)
	}
	select {
	case <-e.runnerStarted: // the first two rooms started agents
	default:
	}
}

func TestUnknownPipelineRoomDisablesProposalsAndAgent(t *testing.T) {
	e := newEnv(t, true)
	if _, err := e.svc.IngestAlert(ctx, alertin.Alert{Title: "x", Severity: "sev3", PipelineID: "ghost"}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-e.alerts:
		t.Fatal("no agent may start without a known pipeline")
	case <-time.After(100 * time.Millisecond):
	}
}

// ---- recovery and wiring ----

func TestStartRebuildsFromLog(t *testing.T) {
	e := newEnv(t, true)
	s := e.open(kabir)
	d := <-e.runnerStarted
	pid, _ := e.propose(s.ID, d, step("compat"))
	_, _ = e.svc.Steer(ctx, arjun, s.ID, "suggestion")

	// A second service over the same log sees the same room.
	svc2, err := room.New(room.Config{
		Store: e.st, Users: e.users, Actions: actions.NewRegistry(), Now: e.clk.Now,
		Owners: &owners.Owners{
			Pipelines: []owners.Pipeline{{ID: "orders"}},
			Layers:    map[string]string{"postgres": "priya", "schema-registry": "priya", "kafka-connect": "arjun", "kafka": "arjun", "consumer": "kabir", "warehouse": "meera"},
		},
		Runner: func(ctx context.Context, _ agent.Alert, _ agent.Deps, _ agent.Options) error {
			<-ctx.Done()
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer svc2.Shutdown()
	if err := svc2.Start(ctx); err != nil {
		t.Fatal(err)
	}
	got, err := svc2.GetRoom(ctx, s.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.OpenProposals) != 1 || got.OpenProposals[0].ProposalID != pid || len(got.PendingSuggestions) != 1 || got.Driver.ID != "kabir" {
		t.Fatalf("recovered snapshot = %+v", got)
	}
	if got.AgentStatus.State != "error" {
		t.Fatalf("an agent that was not resumed must be reported: %+v", got.AgentStatus)
	}
	// Proposals can still be approved after a restart.
	if _, err := svc2.Approve(ctx, priya, pid, "schema-registry"); err != nil {
		t.Fatal(err)
	}
}

func TestNewRejectsUsersOwnersMismatch(t *testing.T) {
	_, err := room.New(room.Config{
		Store: memstore.New(), Users: room.DemoUsers(), Actions: actions.NewRegistry(),
		Owners: &owners.Owners{Layers: map[string]string{"warehouse": "kabir"}},
	})
	if err == nil {
		t.Fatal("users.yaml and owners.yaml disagree: the service must refuse to start")
	}
}

func TestDevFilesAgree(t *testing.T) {
	o, err := owners.Load("../../dev/owners.yaml")
	if err != nil {
		t.Fatal(err)
	}
	u, err := room.LoadUsers("../../dev/users.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if err := u.CheckOwners(o); err != nil {
		t.Fatal(err)
	}
	if err := room.DemoUsers().CheckOwners(o); err != nil {
		t.Fatalf("the built-in demo users must match dev/owners.yaml: %v", err)
	}
}

var _ store.Store = (*memstore.Store)(nil)
