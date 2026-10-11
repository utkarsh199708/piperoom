package room_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"piperoom/internal/actions"
	"piperoom/internal/agent"
	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/room"
	"piperoom/internal/store/memstore"
)

type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *clock) Advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

func fakeRun(ctx context.Context, alert agent.Alert, deps agent.Deps, opts agent.Options) error {
	<-ctx.Done()
	return nil
}

func fakeWrite(ctx context.Context, evs []events.Event, opts agent.Options) (events.HandoffSummary, error) {
	return events.HandoffSummary{WhatBroke: "schema change", Why: "int->bigint"}, nil
}

type fakeAction struct {
	name, layer string
	fail        bool
	scopeErr    error
	mu          sync.Mutex
	calls       int
}

func (a *fakeAction) Name() string     { return a.name }
func (a *fakeAction) Layer() string    { return a.layer }
func (a *fakeAction) Describe() string { return a.name }
func (a *fakeAction) ParamsSchema() map[string]any {
	return map[string]any{"type": "object", "additionalProperties": false, "properties": map[string]any{}}
}
func (a *fakeAction) Validate(json.RawMessage) error { return nil }

func (a *fakeAction) CheckScope(p owners.Pipeline, params json.RawMessage) error {
	return a.scopeErr
}
func (a *fakeAction) Execute(ctx context.Context, params json.RawMessage) (string, error) {
	a.mu.Lock()
	a.calls++
	a.mu.Unlock()
	if a.fail {
		return "", errors.New("boom")
	}
	return "done", nil
}
func (a *fakeAction) Calls() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.calls
}

func newTestService(t *testing.T, act ...actions.Action) (*room.Service, *memstore.Store, *clock) {
	t.Helper()
	c := &clock{t: time.Date(2026, 10, 11, 10, 0, 0, 0, time.UTC)}
	st := memstore.NewWithClock(c.Now)
	reg := actions.NewRegistry()
	for _, a := range act {
		if err := reg.Register(a); err != nil {
			t.Fatalf("register action: %v", err)
		}
	}
	svc := room.New(room.Deps{
		Store:        st,
		Actions:      reg,
		Users:        room.DefaultUsers(),
		Run:          fakeRun,
		WriteHandoff: fakeWrite,
		Now:          c.Now,
	})
	t.Cleanup(svc.Close)
	return svc, st, c
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("condition not met within %s", timeout)
}

func mustUser(t *testing.T, svc *room.Service, id string) room.User {
	t.Helper()
	u, ok := svc.LookupUser(id)
	if !ok {
		t.Fatalf("user %q not found", id)
	}
	return u
}

func createRoom(t *testing.T, svc *room.Service, userID string) room.RoomSnapshot {
	t.Helper()
	snap, err := svc.CreateRoom(context.Background(), mustUser(t, svc, userID), "Incident", "sev2", "orders")
	if err != nil {
		t.Fatalf("create room: %v", err)
	}
	return snap
}

func wantCode(t *testing.T, err error, code string) {
	t.Helper()
	var re *room.Error
	if !errors.As(err, &re) {
		t.Fatalf("want room error %q, got %v", code, err)
	}
	if re.Code != code {
		t.Fatalf("want code %q, got %q (%s)", code, re.Code, re.Msg)
	}
}

func allEvents(t *testing.T, st *memstore.Store, roomID string) []events.Event {
	t.Helper()
	evs, err := st.Since(context.Background(), roomID, 0, 1000)
	if err != nil {
		t.Fatalf("events: %v", err)
	}
	return evs
}

func TestSteerNonDriverBecomesSuggestion(t *testing.T) {
	svc, _, _ := newTestService(t)
	roomID := createRoom(t, svc, "arjun").ID

	ev, err := svc.Steer(context.Background(), mustUser(t, svc, "priya"), roomID, "look at the connector")
	if err != nil {
		t.Fatalf("steer: %v", err)
	}
	if ev.Type != events.SteerSuggested {
		t.Fatalf("want steer.suggested, got %s", ev.Type)
	}
	snap, _ := svc.GetRoom(context.Background(), roomID)
	if len(snap.PendingSuggestions) != 1 {
		t.Fatalf("want 1 pending suggestion, got %d", len(snap.PendingSuggestions))
	}

	ev, err = svc.Steer(context.Background(), mustUser(t, svc, "arjun"), roomID, "check the topic")
	if err != nil {
		t.Fatalf("driver steer: %v", err)
	}
	if ev.Type != events.SteerSent {
		t.Fatalf("want steer.sent, got %s", ev.Type)
	}
}

// TestSteerInputOnlyAfterDriverCheck pins C13 audit item (a): the agent inbox
// only ever sees Kind "steer" for a steer.sent produced by the server's own
// driver check. A non-driver's steer stays a suggestion and must not reach the
// agent; no client-supplied field can set the inbox kind (HTTP /steer accepts
// only {text}, suggestion send accepts no body, WS accepts only presence).
func TestSteerInputOnlyAfterDriverCheck(t *testing.T) {
	c := &clock{t: time.Date(2026, 10, 11, 10, 0, 0, 0, time.UTC)}
	st := memstore.NewWithClock(c.Now)
	inboxCh := make(chan agent.Inbox, 1)
	svc := room.New(room.Deps{
		Store: st,
		Users: room.DefaultUsers(),
		Now:   c.Now,
		Run: func(ctx context.Context, _ agent.Alert, deps agent.Deps, _ agent.Options) error {
			inboxCh <- deps.Inbox
			<-ctx.Done()
			return nil
		},
	})
	t.Cleanup(svc.Close)
	roomID := createRoom(t, svc, "arjun").ID

	var inbox agent.Inbox
	select {
	case inbox = <-inboxCh:
	case <-time.After(5 * time.Second):
		t.Fatal("agent run did not start")
	}

	// Non-driver steer: steer.suggested, nothing in the agent inbox.
	ev, err := svc.Steer(context.Background(), mustUser(t, svc, "priya"), roomID, "look at the connector")
	if err != nil {
		t.Fatalf("non-driver steer: %v", err)
	}
	if ev.Type != events.SteerSuggested {
		t.Fatalf("want steer.suggested, got %s", ev.Type)
	}
	inputs, err := inbox.Drain(context.Background())
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	if len(inputs) != 0 {
		t.Fatalf("non-driver steer reached the agent: %+v", inputs)
	}
	for _, e := range allEvents(t, st, roomID) {
		if e.Type == events.SteerSent {
			t.Fatalf("steer.sent recorded without a driver check (seq %d)", e.Seq)
		}
	}

	// Driver steer: steer.sent in the log and exactly one Kind "steer" input.
	ev, err = svc.Steer(context.Background(), mustUser(t, svc, "arjun"), roomID, "check the topic")
	if err != nil {
		t.Fatalf("driver steer: %v", err)
	}
	if ev.Type != events.SteerSent {
		t.Fatalf("want steer.sent, got %s", ev.Type)
	}
	inputs, err = inbox.Drain(context.Background())
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	if len(inputs) != 1 {
		t.Fatalf("want exactly one agent input, got %+v", inputs)
	}
	if inputs[0].Kind != "steer" || inputs[0].Text != "check the topic" || inputs[0].Actor.ID != "arjun" {
		t.Fatalf("bad agent input: %+v", inputs[0])
	}
}

// TestCreateRoomValidatesPipelineID pins C13 audit item (b): pipeline_id must
// match ^[a-zA-Z0-9_.-]+$ on POST /api/rooms, even with no owners config
// loaded, and an unknown but well-formed id is still rejected once owners are.
func TestCreateRoomValidatesPipelineID(t *testing.T) {
	tests := []struct {
		name    string
		id      string
		owners  bool // load an owners config that only knows "orders"
		wantErr bool
	}{
		// Shape check, owners nil (demo/unit mode): valid charset accepted.
		{name: "letters", id: "orders"},
		{name: "digits", id: "orders2"},
		{name: "underscore", id: "orders_prod"},
		{name: "dot", id: "orders.prod"},
		{name: "dash", id: "orders-prod"},
		// Shape check, owners nil: invalid ids rejected.
		{name: "space", id: "orders prod", wantErr: true},
		{name: "slash", id: "orders/prod", wantErr: true},
		{name: "star", id: "orders*", wantErr: true},
		{name: "semicolon", id: "orders;drop", wantErr: true},
		{name: "empty", id: "", wantErr: true},
		// Owners loaded: known id accepted, unknown well-formed id rejected.
		{name: "known with owners", id: "orders", owners: true},
		{name: "unknown with owners", id: "orders-2", owners: true, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := &clock{t: time.Date(2026, 10, 11, 10, 0, 0, 0, time.UTC)}
			st := memstore.NewWithClock(c.Now)
			deps := room.Deps{Store: st, Users: room.DefaultUsers(), Run: fakeRun, Now: c.Now}
			if tc.owners {
				deps.Owners = &owners.Owners{Pipelines: []owners.Pipeline{{ID: "orders"}}}
			}
			svc := room.New(deps)
			t.Cleanup(svc.Close)

			_, err := svc.CreateRoom(context.Background(), mustUser(t, svc, "arjun"), "Incident", "sev2", tc.id)
			if tc.wantErr {
				wantCode(t, err, "validation")
				return
			}
			if err != nil {
				t.Fatalf("create room with pipeline_id %q: %v", tc.id, err)
			}
		})
	}
}

func TestApproveRequiresLayerOwnership(t *testing.T) {
	act := &fakeAction{name: "postgres_action", layer: "postgres"}
	svc, _, _ := newTestService(t, act)
	roomID := createRoom(t, svc, "arjun").ID

	pid, err := svc.CreateProposal(context.Background(), roomID, events.ProposalCreatedPayload{
		Title:          "fix",
		Steps:          []events.ProposalStep{{Action: "postgres_action", Layer: "postgres", Params: json.RawMessage("{}")}},
		RequiredLayers: []string{"postgres"},
		Risk:           "r",
		Rollback:       "rb",
	}, events.AgentActor)
	if err != nil {
		t.Fatalf("create proposal: %v", err)
	}

	// arjun does not own postgres.
	_, err = svc.ApproveProposal(context.Background(), mustUser(t, svc, "arjun"), pid, "postgres")
	wantCode(t, err, "not_layer_owner")

	// priya owns postgres.
	if _, err = svc.ApproveProposal(context.Background(), mustUser(t, svc, "priya"), pid, "postgres"); err != nil {
		t.Fatalf("approve: %v", err)
	}
}

func TestDoubleApprovalConflict(t *testing.T) {
	svc, _, _ := newTestService(t, &fakeAction{name: "postgres_action", layer: "postgres"})
	roomID := createRoom(t, svc, "arjun").ID
	pid, err := svc.CreateProposal(context.Background(), roomID, events.ProposalCreatedPayload{
		Title:          "fix",
		Steps:          []events.ProposalStep{{Action: "postgres_action", Layer: "postgres", Params: json.RawMessage("{}")}},
		RequiredLayers: []string{"postgres"},
	}, events.AgentActor)
	if err != nil {
		t.Fatalf("create proposal: %v", err)
	}
	if _, err := svc.ApproveProposal(context.Background(), mustUser(t, svc, "priya"), pid, "postgres"); err != nil {
		t.Fatalf("first approve: %v", err)
	}
	_, err = svc.ApproveProposal(context.Background(), mustUser(t, svc, "priya"), pid, "postgres")
	wantCode(t, err, "already_decided")
}

func TestLeaseExpiryAllowsTakeover(t *testing.T) {
	svc, _, c := newTestService(t)
	roomID := createRoom(t, svc, "arjun").ID

	// While the lease is held, a non-driver cannot grant control: 409
	// lease_held, not 403 (docs/api.md).
	_, err := svc.GrantControl(context.Background(), mustUser(t, svc, "kabir"), roomID, "kabir")
	wantCode(t, err, "lease_held")

	// After the lease expires, anyone can take control.
	c.Advance(31 * time.Minute)
	if _, err := svc.GrantControl(context.Background(), mustUser(t, svc, "kabir"), roomID, "kabir"); err != nil {
		t.Fatalf("grant after expiry: %v", err)
	}
	snap, _ := svc.GetRoom(context.Background(), roomID)
	if snap.Driver == nil || snap.Driver.ID != "kabir" {
		t.Fatalf("driver not moved: %+v", snap.Driver)
	}
}

func TestExpiredLeaseDriverStillDrives(t *testing.T) {
	svc, _, c := newTestService(t)
	roomID := createRoom(t, svc, "arjun").ID

	// arjun's lease lapses but nobody replaces him: he is still the driver and
	// his next action renews the lease (docs/api.md).
	c.Advance(31 * time.Minute)
	ev, err := svc.Steer(context.Background(), mustUser(t, svc, "arjun"), roomID, "still here")
	if err != nil {
		t.Fatalf("steer after lease expiry: %v", err)
	}
	if ev.Type != events.SteerSent {
		t.Fatalf("expired-lease driver must steer directly, got %s", ev.Type)
	}
	snap, _ := svc.GetRoom(context.Background(), roomID)
	if snap.Driver == nil || !snap.Driver.LeaseUntil.After(c.Now().Add(room.DriverLease-time.Minute)) {
		t.Fatalf("lease was not renewed: %+v", snap.Driver)
	}
}

func TestCreateHandoffDriverOnly(t *testing.T) {
	svc, _, c := newTestService(t)
	roomID := createRoom(t, svc, "arjun").ID

	// A non-driver cannot trigger a hand-off, even the intended recipient.
	_, err := svc.CreateHandoff(context.Background(), mustUser(t, svc, "priya"), roomID, "kabir", false)
	wantCode(t, err, "not_driver")

	// An expired-lease driver was not replaced, so he may still hand off.
	c.Advance(31 * time.Minute)
	if _, err := svc.CreateHandoff(context.Background(), mustUser(t, svc, "arjun"), roomID, "priya", false); err != nil {
		t.Fatalf("expired-lease driver handoff: %v", err)
	}
}

func TestHandoffSummarySanitized(t *testing.T) {
	long := strings.Repeat("raw tool output ", 200)
	c := &clock{t: time.Date(2026, 10, 11, 10, 0, 0, 0, time.UTC)}
	st := memstore.NewWithClock(c.Now)
	svc := room.New(room.Deps{
		Store: st,
		Users: room.DefaultUsers(),
		Run:   fakeRun,
		Now:   c.Now,
		WriteHandoff: func(ctx context.Context, evs []events.Event, opts agent.Options) (events.HandoffSummary, error) {
			items := make([]string, 0, 40)
			for i := 0; i < 40; i++ {
				items = append(items, long)
			}
			return events.HandoffSummary{WhatBroke: long, Why: long, Done: items, Open: items}, nil
		},
	})
	t.Cleanup(svc.Close)
	roomID := createRoom(t, svc, "arjun").ID

	ev, err := svc.CreateHandoff(context.Background(), mustUser(t, svc, "arjun"), roomID, "priya", false)
	if err != nil {
		t.Fatalf("handoff: %v", err)
	}
	p, err := events.Decode[events.HandoffCreatedPayload](ev)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(p.Summary.WhatBroke) > 600 || len(p.Summary.Why) > 600 {
		t.Fatalf("summary fields not bounded: %d/%d", len(p.Summary.WhatBroke), len(p.Summary.Why))
	}
	if len(p.Summary.Done) > 10 || len(p.Summary.Open) > 10 {
		t.Fatalf("summary lists not bounded: done=%d open=%d", len(p.Summary.Done), len(p.Summary.Open))
	}
	for _, item := range p.Summary.Done {
		if len(item) > 600 {
			t.Fatalf("summary list item not bounded: %d bytes", len(item))
		}
	}
}

func TestExecutorStopsAtFirstFailure(t *testing.T) {
	ok := &fakeAction{name: "ok_action", layer: "postgres"}
	fail := &fakeAction{name: "fail_action", layer: "warehouse", fail: true}
	never := &fakeAction{name: "never_action", layer: "warehouse"}
	svc, st, _ := newTestService(t, ok, fail, never)
	roomID := createRoom(t, svc, "arjun").ID

	pid, err := svc.CreateProposal(context.Background(), roomID, events.ProposalCreatedPayload{
		Title: "fix",
		Steps: []events.ProposalStep{
			{Action: "ok_action", Layer: "postgres", Params: json.RawMessage("{}")},
			{Action: "fail_action", Layer: "warehouse", Params: json.RawMessage("{}")},
			{Action: "never_action", Layer: "warehouse", Params: json.RawMessage("{}")},
		},
		RequiredLayers: []string{"postgres", "warehouse"},
	}, events.AgentActor)
	if err != nil {
		t.Fatalf("create proposal: %v", err)
	}

	if _, err := svc.ApproveProposal(context.Background(), mustUser(t, svc, "priya"), pid, "postgres"); err != nil {
		t.Fatalf("approve postgres: %v", err)
	}
	if _, err := svc.ApproveProposal(context.Background(), mustUser(t, svc, "meera"), pid, "warehouse"); err != nil {
		t.Fatalf("approve warehouse: %v", err)
	}

	// The executor runs asynchronously on the service context; wait for the
	// terminal proposal status (step 1 fails, so the proposal is failed).
	waitFor(t, 5*time.Second, func() bool {
		p, err := st.GetProposal(context.Background(), pid)
		return err == nil && p.Status == "failed"
	})

	executed := map[int]bool{}
	for _, e := range allEvents(t, st, roomID) {
		if e.Type != events.ActionExecuted {
			continue
		}
		p, err := events.Decode[events.ActionExecutedPayload](e)
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		executed[p.StepIndex] = p.OK
	}
	if len(executed) != 2 {
		t.Fatalf("want exactly 2 executed steps, got %d (%v)", len(executed), executed)
	}
	if !executed[0] {
		t.Fatalf("step 0 should succeed: %v", executed)
	}
	if executed[1] {
		t.Fatalf("step 1 should fail: %v", executed)
	}
	if _, ran := executed[2]; ran {
		t.Fatalf("step 2 must not run: %v", executed)
	}
	if ok.Calls() != 1 || fail.Calls() != 1 || never.Calls() != 0 {
		t.Fatalf("unexpected call counts: ok=%d fail=%d never=%d", ok.Calls(), fail.Calls(), never.Calls())
	}
}

func TestCloseRequiresDriver(t *testing.T) {
	svc, _, _ := newTestService(t)
	roomID := createRoom(t, svc, "arjun").ID
	_, err := svc.CloseRoom(context.Background(), mustUser(t, svc, "priya"), roomID, "fixed")
	wantCode(t, err, "not_driver")
}

func TestClosedRoomRejectsWrites(t *testing.T) {
	svc, _, _ := newTestService(t)
	roomID := createRoom(t, svc, "arjun").ID
	if _, err := svc.CloseRoom(context.Background(), mustUser(t, svc, "arjun"), roomID, "fixed"); err != nil {
		t.Fatalf("close: %v", err)
	}
	_, err := svc.AddComment(context.Background(), mustUser(t, svc, "priya"), roomID, "", "hi")
	wantCode(t, err, "room_closed")
	_, err = svc.Steer(context.Background(), mustUser(t, svc, "arjun"), roomID, "hi")
	wantCode(t, err, "room_closed")
}

func TestHandoffMovesDriver(t *testing.T) {
	svc, st, _ := newTestService(t)
	roomID := createRoom(t, svc, "arjun").ID

	ev, err := svc.CreateHandoff(context.Background(), mustUser(t, svc, "arjun"), roomID, "priya", false)
	if err != nil {
		t.Fatalf("handoff: %v", err)
	}
	if ev.Type != events.HandoffCreated {
		t.Fatalf("want handoff.created, got %s", ev.Type)
	}
	p, _ := events.Decode[events.HandoffCreatedPayload](ev)

	// kabir is not the recipient.
	_, err = svc.AcceptHandoff(context.Background(), mustUser(t, svc, "kabir"), p.HandoffID)
	wantCode(t, err, "not_recipient")

	if _, err := svc.AcceptHandoff(context.Background(), mustUser(t, svc, "priya"), p.HandoffID); err != nil {
		t.Fatalf("accept: %v", err)
	}
	snap, _ := svc.GetRoom(context.Background(), roomID)
	if snap.Driver == nil || snap.Driver.ID != "priya" {
		t.Fatalf("driver not moved: %+v", snap.Driver)
	}
	// The old driver stays listed (membership derives from the event log; there
	// is no member-left event), demoted to watching.
	roles := map[string]string{}
	for _, m := range snap.Members {
		roles[m.ID] = m.Role
	}
	if roles["priya"] != "driver" || roles["arjun"] != "watching" {
		t.Fatalf("bad roles after handoff: %+v", roles)
	}
	_ = st
}

func TestRequiredLayersDerivedFromSteps(t *testing.T) {
	pgAct := &fakeAction{name: "pg_act", layer: "postgres"}
	whAct := &fakeAction{name: "wh_act", layer: "warehouse"}
	svc, st, _ := newTestService(t, pgAct, whAct)
	roomID := createRoom(t, svc, "arjun").ID

	pid, err := svc.CreateProposal(context.Background(), roomID, events.ProposalCreatedPayload{
		Title: "fix",
		Steps: []events.ProposalStep{
			{Action: "pg_act", Layer: "postgres", Params: json.RawMessage("{}")},
			{Action: "wh_act", Layer: "warehouse", Params: json.RawMessage("{}")},
		},
		// Wrong: this layer is not touched by any step, so it must be replaced
		// by the derived layers.
		RequiredLayers: []string{"consumer"},
	}, events.AgentActor)
	if err != nil {
		t.Fatalf("create proposal: %v", err)
	}
	prow, err := st.GetProposal(context.Background(), pid)
	if err != nil {
		t.Fatalf("get proposal: %v", err)
	}
	if len(prow.RequiredLayers) != 2 || prow.RequiredLayers[0] != "postgres" || prow.RequiredLayers[1] != "warehouse" {
		t.Fatalf("derived layers wrong: %+v", prow.RequiredLayers)
	}

	// Approving an unrelated layer is rejected.
	_, err = svc.ApproveProposal(context.Background(), mustUser(t, svc, "kabir"), pid, "consumer")
	wantCode(t, err, "validation")

	// Approving only postgres must not execute anything.
	if _, err := svc.ApproveProposal(context.Background(), mustUser(t, svc, "priya"), pid, "postgres"); err != nil {
		t.Fatalf("approve postgres: %v", err)
	}
	if pgAct.Calls() != 0 || whAct.Calls() != 0 {
		t.Fatalf("executor ran before all layers approved: pg=%d wh=%d", pgAct.Calls(), whAct.Calls())
	}

	// Approving warehouse executes both steps.
	if _, err := svc.ApproveProposal(context.Background(), mustUser(t, svc, "meera"), pid, "warehouse"); err != nil {
		t.Fatalf("approve warehouse: %v", err)
	}
	waitFor(t, 5*time.Second, func() bool {
		p, err := st.GetProposal(context.Background(), pid)
		return err == nil && p.Status == "executed"
	})
	if pgAct.Calls() != 1 || whAct.Calls() != 1 {
		t.Fatalf("unexpected call counts: pg=%d wh=%d", pgAct.Calls(), whAct.Calls())
	}
}

func TestExecutorFailureMarksFailed(t *testing.T) {
	failAct := &fakeAction{name: "fail_act", layer: "postgres", fail: true}
	svc, st, _ := newTestService(t, failAct)
	roomID := createRoom(t, svc, "arjun").ID

	pid, err := svc.CreateProposal(context.Background(), roomID, events.ProposalCreatedPayload{
		Title: "fix",
		Steps: []events.ProposalStep{
			{Action: "fail_act", Layer: "postgres", Params: json.RawMessage("{}")},
		},
		RequiredLayers: []string{"postgres"},
	}, events.AgentActor)
	if err != nil {
		t.Fatalf("create proposal: %v", err)
	}
	if _, err := svc.ApproveProposal(context.Background(), mustUser(t, svc, "priya"), pid, "postgres"); err != nil {
		t.Fatalf("approve: %v", err)
	}

	waitFor(t, 5*time.Second, func() bool {
		p, err := st.GetProposal(context.Background(), pid)
		return err == nil && p.Status == "failed"
	})

	executed := map[int]bool{}
	for _, e := range allEvents(t, st, roomID) {
		if e.Type != events.ActionExecuted {
			continue
		}
		p, err := events.Decode[events.ActionExecutedPayload](e)
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		executed[p.StepIndex] = p.OK
	}
	if len(executed) != 1 || executed[0] {
		t.Fatalf("want exactly one failing step, got %v", executed)
	}
}

func TestStaleHandoffRejected(t *testing.T) {
	svc, _, _ := newTestService(t)
	roomID := createRoom(t, svc, "arjun").ID

	ev, err := svc.CreateHandoff(context.Background(), mustUser(t, svc, "arjun"), roomID, "priya", false)
	if err != nil {
		t.Fatalf("handoff: %v", err)
	}
	h, _ := events.Decode[events.HandoffCreatedPayload](ev)

	// arjun hands control to kabir; the pending handoff is now stale.
	if _, err := svc.GrantControl(context.Background(), mustUser(t, svc, "arjun"), roomID, "kabir"); err != nil {
		t.Fatalf("grant: %v", err)
	}
	_, err = svc.AcceptHandoff(context.Background(), mustUser(t, svc, "priya"), h.HandoffID)
	wantCode(t, err, "stale_handoff")
}

func TestAcceptHandoffVoidsOtherPending(t *testing.T) {
	svc, _, _ := newTestService(t)
	roomID := createRoom(t, svc, "arjun").ID

	ev1, err := svc.CreateHandoff(context.Background(), mustUser(t, svc, "arjun"), roomID, "priya", false)
	if err != nil {
		t.Fatalf("handoff 1: %v", err)
	}
	ev2, err := svc.CreateHandoff(context.Background(), mustUser(t, svc, "arjun"), roomID, "kabir", false)
	if err != nil {
		t.Fatalf("handoff 2: %v", err)
	}
	h1, _ := events.Decode[events.HandoffCreatedPayload](ev1)
	h2, _ := events.Decode[events.HandoffCreatedPayload](ev2)

	if _, err := svc.AcceptHandoff(context.Background(), mustUser(t, svc, "priya"), h1.HandoffID); err != nil {
		t.Fatalf("accept 1: %v", err)
	}
	// The second handoff was voided and can no longer be accepted.
	_, err = svc.AcceptHandoff(context.Background(), mustUser(t, svc, "kabir"), h2.HandoffID)
	wantCode(t, err, "already_decided")
}

func TestSendSuggestionForwards(t *testing.T) {
	svc, _, _ := newTestService(t)
	roomID := createRoom(t, svc, "arjun").ID

	sugg, err := svc.Steer(context.Background(), mustUser(t, svc, "priya"), roomID, "try the registry")
	if err != nil {
		t.Fatalf("steer: %v", err)
	}
	sp, _ := events.Decode[events.SteerSuggestedPayload](sugg)

	// non-driver cannot send it.
	_, err = svc.SendSuggestion(context.Background(), mustUser(t, svc, "priya"), roomID, sp.SuggestionID)
	wantCode(t, err, "not_driver")

	ev, err := svc.SendSuggestion(context.Background(), mustUser(t, svc, "arjun"), roomID, sp.SuggestionID)
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	sent, _ := events.Decode[events.SteerSentPayload](ev)
	if sent.SuggestionID != sp.SuggestionID || sent.Text != "try the registry" {
		t.Fatalf("bad steer.sent: %+v", sent)
	}
	snap, _ := svc.GetRoom(context.Background(), roomID)
	if len(snap.PendingSuggestions) != 0 {
		t.Fatalf("suggestion should be consumed: %+v", snap.PendingSuggestions)
	}
}

// unscopedAction implements actions.Action but not actions.Scoped; the room
// service must refuse proposals that use it.
type unscopedAction struct{}

func (unscopedAction) Name() string     { return "unscoped" }
func (unscopedAction) Layer() string    { return "postgres" }
func (unscopedAction) Describe() string { return "unscoped" }
func (unscopedAction) ParamsSchema() map[string]any {
	return map[string]any{"type": "object"}
}
func (unscopedAction) Validate(json.RawMessage) error { return nil }
func (unscopedAction) Execute(context.Context, json.RawMessage) (string, error) {
	return "", nil
}

func TestCreateProposalRejectsUnscopedAction(t *testing.T) {
	svc, _, _ := newTestService(t, unscopedAction{})
	roomID := createRoom(t, svc, "arjun").ID

	_, err := svc.CreateProposal(context.Background(), roomID, events.ProposalCreatedPayload{
		Title: "fix",
		Steps: []events.ProposalStep{{Action: "unscoped", Layer: "postgres", Params: json.RawMessage("{}")}},
	}, events.AgentActor)
	wantCode(t, err, "validation")
}

func TestCreateProposalScopeViolation(t *testing.T) {
	c := &clock{t: time.Date(2026, 10, 11, 10, 0, 0, 0, time.UTC)}
	st := memstore.NewWithClock(c.Now)
	reg := actions.NewRegistry()
	if err := reg.Register(&fakeAction{name: "pg_act", layer: "postgres", scopeErr: errors.New("subject targets another pipeline")}); err != nil {
		t.Fatalf("register: %v", err)
	}
	ownerCfg := &owners.Owners{Pipelines: []owners.Pipeline{{ID: "orders", SourceTables: []string{"public.orders"}}}}
	svc := room.New(room.Deps{
		Store: st, Actions: reg, Users: room.DefaultUsers(), Run: fakeRun, Now: c.Now, Owners: ownerCfg,
	})
	t.Cleanup(svc.Close)

	roomID := createRoom(t, svc, "arjun").ID
	_, err := svc.CreateProposal(context.Background(), roomID, events.ProposalCreatedPayload{
		Title: "fix",
		Steps: []events.ProposalStep{{Action: "pg_act", Layer: "postgres", Params: json.RawMessage("{}")}},
	}, events.AgentActor)
	wantCode(t, err, "validation")

	// POST /api/rooms rejects an unknown pipeline when owners are loaded.
	_, err = svc.CreateRoom(context.Background(), mustUser(t, svc, "arjun"), "t2", "sev2", "not-configured")
	wantCode(t, err, "validation")

	// A room opened by an alert webhook may carry an unknown pipeline; its
	// proposals are disabled because the scope check cannot resolve it.
	snap2, err := svc.CreateRoomFromAlert(context.Background(), "t2", "sev2", "not-configured", json.RawMessage(`{"title":"t2"}`))
	if err != nil {
		t.Fatalf("create room from alert: %v", err)
	}
	_, err = svc.CreateProposal(context.Background(), snap2.ID, events.ProposalCreatedPayload{
		Title: "fix",
		Steps: []events.ProposalStep{{Action: "pg_act", Layer: "postgres", Params: json.RawMessage("{}")}},
	}, events.AgentActor)
	wantCode(t, err, "validation")
}

func TestCreateRoomFromAlertStoresRawBody(t *testing.T) {
	svc, st, _ := newTestService(t)
	raw := json.RawMessage(`{"title":"DB down","severity":"sev1","pipeline_id":"orders","extra":[1,2]}`)

	snap, err := svc.CreateRoomFromAlert(context.Background(), "DB down", "sev1", "orders", raw)
	if err != nil {
		t.Fatalf("create from alert: %v", err)
	}
	if snap.Status != "open" || snap.Driver != nil {
		t.Fatalf("webhook room should be open with no driver: %+v", snap)
	}
	p, err := events.Decode[events.RoomCreatedPayload](allEvents(t, st, snap.ID)[0])
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if string(p.Alert) != string(raw) {
		t.Fatalf("raw alert not stored verbatim:\n got %s\nwant %s", p.Alert, raw)
	}

	// A garbage (non-mapped) body still opens a room with the default pipeline.
	snap2, err := svc.CreateRoomFromAlert(context.Background(), "", "", "", json.RawMessage(`[1,2,3]`))
	if err != nil {
		t.Fatalf("create from array alert: %v", err)
	}
	if snap2.PipelineID != "unknown" || snap2.Severity != "sev3" {
		t.Fatalf("bad defaults: %+v", snap2)
	}
}

type expiredPayload struct {
	ProposalID string `json:"proposal_id"`
	Title      string `json:"title"`
	AgeSeconds int64  `json:"age_seconds"`
}

const expiredType = events.Type("proposal.expired")

func TestProposalExpiryRecordsEventOnce(t *testing.T) {
	c := &clock{t: time.Date(2026, 10, 11, 10, 0, 0, 0, time.UTC)}
	st := memstore.NewWithClock(c.Now)
	reg := actions.NewRegistry()
	if err := reg.Register(&fakeAction{name: "pg_act", layer: "postgres"}); err != nil {
		t.Fatalf("register: %v", err)
	}
	svc := room.New(room.Deps{
		Store: st, Actions: reg, Users: room.DefaultUsers(), Run: fakeRun, Now: c.Now,
		ProposalTTL: 40 * time.Millisecond,
	})
	t.Cleanup(svc.Close)

	roomID := createRoom(t, svc, "arjun").ID
	pid, err := svc.CreateProposal(context.Background(), roomID, events.ProposalCreatedPayload{
		Title: "expiring fix",
		Steps: []events.ProposalStep{{Action: "pg_act", Layer: "postgres", Params: json.RawMessage("{}")}},
	}, events.AgentActor)
	if err != nil {
		t.Fatalf("create proposal: %v", err)
	}

	countExpired := func() []events.Event {
		var out []events.Event
		for _, e := range allEvents(t, st, roomID) {
			if e.Type == expiredType {
				out = append(out, e)
			}
		}
		return out
	}

	// The clock has not moved: the proposal is old only after Advance.
	if got := countExpired(); len(got) != 0 {
		t.Fatalf("premature expiry: %d events", len(got))
	}
	c.Advance(time.Second)
	waitFor(t, 2*time.Second, func() bool { return len(countExpired()) == 1 })

	// Exactly once, even though the sweeper keeps running.
	time.Sleep(60 * time.Millisecond)
	got := countExpired()
	if len(got) != 1 {
		t.Fatalf("expiry must be recorded exactly once, got %d", len(got))
	}
	p, err := events.Decode[expiredPayload](got[0])
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if p.ProposalID != pid || p.Title != "expiring fix" || p.AgeSeconds < 1 {
		t.Fatalf("bad expiry payload: %+v", p)
	}

	// Expiry records a fact only: the proposal stays open for approvals.
	snap, _ := svc.GetRoom(context.Background(), roomID)
	if len(snap.OpenProposals) != 1 {
		t.Fatalf("snapshot must keep the proposal open: %+v", snap.OpenProposals)
	}
}

func TestCloseStopsGoroutinesIdempotent(t *testing.T) {
	act := &fakeAction{name: "pg_act", layer: "postgres"}
	svc, st, _ := newTestService(t, act)
	roomID := createRoom(t, svc, "arjun").ID

	pid, err := svc.CreateProposal(context.Background(), roomID, events.ProposalCreatedPayload{
		Title: "fix",
		Steps: []events.ProposalStep{{Action: "pg_act", Layer: "postgres", Params: json.RawMessage("{}")}},
	}, events.AgentActor)
	if err != nil {
		t.Fatalf("create proposal: %v", err)
	}
	if _, err := svc.ApproveProposal(context.Background(), mustUser(t, svc, "priya"), pid, "postgres"); err != nil {
		t.Fatalf("approve: %v", err)
	}
	waitFor(t, 5*time.Second, func() bool {
		p, err := st.GetProposal(context.Background(), pid)
		return err == nil && p.Status == "executed"
	})

	// At this point the agent supervisor, the room executor and the expiry
	// sweeper are all running. Close cancels the service context and must let
	// every one of them exit promptly; a second Close is a no-op.
	done := make(chan struct{})
	go func() {
		defer close(done)
		svc.Close()
		svc.Close()
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not stop all goroutines within 5s")
	}
}
