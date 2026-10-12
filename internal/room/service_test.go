package room_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
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
	"piperoom/internal/topology"
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

// testOwners is the minimal owners config unit tests scope action steps
// against (the room service now always scope-checks; a nil config fails
// closed).
func testOwners() *owners.Owners {
	return &owners.Owners{Pipelines: []owners.Pipeline{{ID: "orders", SourceTables: []string{"public.orders"}}}}
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
		Owners:       testOwners(),
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

	// Creating the second handoff voided the first (review item 10): it can
	// no longer be accepted, and that is a stale (not an already-decided)
	// handoff.
	_, err = svc.AcceptHandoff(context.Background(), mustUser(t, svc, "priya"), h1.HandoffID)
	wantCode(t, err, "stale_handoff")

	// The newer handoff is still pending and moves the driver role.
	if _, err := svc.AcceptHandoff(context.Background(), mustUser(t, svc, "kabir"), h2.HandoffID); err != nil {
		t.Fatalf("accept 2: %v", err)
	}
	snap, _ := svc.GetRoom(context.Background(), roomID)
	if snap.Driver == nil || snap.Driver.ID != "kabir" {
		t.Fatalf("driver should be kabir: %+v", snap.Driver)
	}
	// Accepting again is already decided.
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

	// The alert webhook never drops an alert: an unknown (but well-formed)
	// pipeline opens a room with proposals disabled (docs/api.md:103); only
	// POST /api/rooms keeps the 400.
	if _, err := svc.CreateRoomFromAlert(context.Background(), "t2", "sev2", "not-configured", json.RawMessage(`{"title":"t2"}`)); err != nil {
		t.Fatalf("webhook with unknown pipeline must open a room: %v", err)
	}
	// A malformed pipeline_id is rejected by shape on both paths.
	_, err = svc.CreateRoomFromAlert(context.Background(), "t2", "sev2", "bad id!", json.RawMessage(`{"title":"t2"}`))
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

	// With the owners config loaded, a garbage (non-mapped) body uses the
	// default pipeline "unknown": the room still opens (docs/api.md:103) with a
	// system message explaining the pipeline is unknown.
	snapUnknown, err := svc.CreateRoomFromAlert(context.Background(), "", "", "", json.RawMessage(`[1,2,3]`))
	if err != nil {
		t.Fatalf("unknown-pipeline webhook must open a room: %v", err)
	}
	if snapUnknown.PipelineID != "unknown" || snapUnknown.Severity != "sev3" {
		t.Fatalf("bad defaults: %+v", snapUnknown)
	}
	sawSystem := false
	for _, e := range allEvents(t, st, snapUnknown.ID) {
		if e.Type == events.AgentMessage && e.Actor.Kind == events.ActorSystem {
			sawSystem = true
		}
	}
	if !sawSystem {
		t.Fatal("no system message on the unknown-pipeline room")
	}

	// Without an owners config (unit mode) the same body opens a room with
	// the defaults.
	bare := room.New(room.Deps{Store: st, Users: room.DefaultUsers(), Run: fakeRun})
	t.Cleanup(bare.Close)
	snap2, err := bare.CreateRoomFromAlert(context.Background(), "", "", "", json.RawMessage(`[1,2,3]`))
	if err != nil {
		t.Fatalf("create from array alert: %v", err)
	}
	if snap2.PipelineID != "unknown" || snap2.Severity != "sev3" {
		t.Fatalf("bad defaults: %+v", snap2)
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

// blockingAction blocks in Execute until its context ends, so a test can close
// the service mid-run.
type blockingAction struct {
	started chan struct{}
	once    sync.Once
}

func newBlockingAction() *blockingAction { return &blockingAction{started: make(chan struct{})} }

func (a *blockingAction) Name() string     { return "blocking_action" }
func (a *blockingAction) Layer() string    { return "postgres" }
func (a *blockingAction) Describe() string { return "blocks until the context ends" }
func (a *blockingAction) ParamsSchema() map[string]any {
	return map[string]any{"type": "object"}
}
func (a *blockingAction) Validate(json.RawMessage) error                    { return nil }
func (a *blockingAction) CheckScope(owners.Pipeline, json.RawMessage) error { return nil }
func (a *blockingAction) Execute(ctx context.Context, _ json.RawMessage) (string, error) {
	a.once.Do(func() { close(a.started) })
	<-ctx.Done()
	return "", ctx.Err()
}

// TestCloseDuringExecutionFinishesStep pins review items 3 and 13: with a
// Close while a step is running, the result writes (step finish, proposal
// status, action.executed event) still land because they are detached from
// the service context (context.WithoutCancel + timeout); the step ends
// "failed", never "started" forever.
func TestCloseDuringExecutionFinishesStep(t *testing.T) {
	act := newBlockingAction()
	svc, st, _ := newTestService(t, act)
	roomID := createRoom(t, svc, "arjun").ID

	pid, err := svc.CreateProposal(context.Background(), roomID, events.ProposalCreatedPayload{
		Title: "fix",
		Steps: []events.ProposalStep{{Action: "blocking_action", Layer: "postgres", Params: json.RawMessage("{}")}},
	}, events.AgentActor)
	if err != nil {
		t.Fatalf("create proposal: %v", err)
	}
	if _, err := svc.ApproveProposal(context.Background(), mustUser(t, svc, "priya"), pid, "postgres"); err != nil {
		t.Fatalf("approve: %v", err)
	}
	select {
	case <-act.started:
	case <-time.After(5 * time.Second):
		t.Fatal("action never started")
	}

	svc.Close()

	if got := st.ProposalStepStatus(pid, 0); got != "failed" {
		t.Fatalf("step state after Close: got %q, want failed", got)
	}
	if p, err := st.GetProposal(context.Background(), pid); err != nil || p.Status != "failed" {
		t.Fatalf("proposal status after Close: %+v err=%v", p, err)
	}
	found := false
	for _, e := range allEvents(t, st, roomID) {
		if e.Type != events.ActionExecuted {
			continue
		}
		p, err := events.Decode[events.ActionExecutedPayload](e)
		if err == nil && p.ProposalID == pid && !p.OK {
			found = true
		}
	}
	if !found {
		t.Fatalf("no failing action.executed recorded for %s", pid)
	}
	snap, err := svc.GetRoom(context.Background(), roomID)
	if err != nil {
		t.Fatalf("get room: %v", err)
	}
	if len(snap.OpenProposals) != 0 {
		t.Fatalf("proposal must leave open_proposals on failure: %+v", snap.OpenProposals)
	}
}

// TestExecutorNeverRunsAPreClaimedStep pins the no-double-run rule end to end
// (review round 2, item 4): a step claimed by an earlier (crashed) run is
// never executed again; it is finished as failed and the proposal is marked
// failed WITH an outcome event (review item 13).
func TestExecutorNeverRunsAPreClaimedStep(t *testing.T) {
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
	// Simulate a crashed earlier run: the step was claimed, never finished.
	if err := st.StartProposalStep(context.Background(), pid, 0); err != nil {
		t.Fatalf("pre-claim step: %v", err)
	}
	if _, err := svc.ApproveProposal(context.Background(), mustUser(t, svc, "priya"), pid, "postgres"); err != nil {
		t.Fatalf("approve: %v", err)
	}

	waitFor(t, 5*time.Second, func() bool {
		p, err := st.GetProposal(context.Background(), pid)
		return err == nil && p.Status == "failed"
	})
	if act.Calls() != 0 {
		t.Fatalf("pre-claimed step ran again: %d calls", act.Calls())
	}
	if got := st.ProposalStepStatus(pid, 0); got != "failed" {
		t.Fatalf("stale claim state: got %q, want failed", got)
	}
	found := false
	for _, e := range allEvents(t, st, roomID) {
		if e.Type != events.ActionExecuted {
			continue
		}
		p, err := events.Decode[events.ActionExecutedPayload](e)
		if err == nil && p.ProposalID == pid && !p.OK {
			found = true
		}
	}
	if !found {
		t.Fatalf("no failing action.executed for the pre-claimed proposal")
	}
	snap, _ := svc.GetRoom(context.Background(), roomID)
	if len(snap.OpenProposals) != 0 {
		t.Fatalf("proposal must leave open_proposals: %+v", snap.OpenProposals)
	}
}

// TestLoadInterruptsStaleProposals pins review item 6: a proposal that was
// fully approved but never finished before a (re)start is marked failed with
// an outcome event on Load instead of staying open forever; a proposal still
// waiting for approvals is untouched.
func TestLoadInterruptsStaleProposals(t *testing.T) {
	pg := &fakeAction{name: "pg_act", layer: "postgres"}
	wh := &fakeAction{name: "wh_act", layer: "warehouse"}
	svc, st, c := newTestService(t, pg, wh)
	roomID := createRoom(t, svc, "arjun").ID

	mk := func(title string, steps ...events.ProposalStep) string {
		t.Helper()
		pid, err := svc.CreateProposal(context.Background(), roomID, events.ProposalCreatedPayload{
			Title: title, Steps: steps,
		}, events.AgentActor)
		if err != nil {
			t.Fatalf("create proposal %s: %v", title, err)
		}
		return pid
	}
	// p1: fully approved, step 0 claimed, never finished — crash mid-run.
	p1 := mk("one", events.ProposalStep{Action: "pg_act", Layer: "postgres", Params: json.RawMessage("{}")})
	// p2: no approvals — must stay open after Load.
	p2 := mk("two", events.ProposalStep{Action: "pg_act", Layer: "postgres", Params: json.RawMessage("{}")})
	// p3: two steps, step 0 recorded done, step 1 claimed but unfinished.
	p3 := mk("three",
		events.ProposalStep{Action: "pg_act", Layer: "postgres", Params: json.RawMessage("{}")},
		events.ProposalStep{Action: "wh_act", Layer: "warehouse", Params: json.RawMessage("{}")},
	)

	// The approvals and p3's finished step 0 happened before the crash; write
	// them straight to the store so no executor runs in this process.
	prior := events.Actor{Kind: events.ActorHuman, ID: "priya", Name: "Priya"}
	for _, a := range []struct {
		typ     events.Type
		payload any
	}{
		{events.ApprovalGiven, events.ApprovalGivenPayload{ProposalID: p1, Layer: "postgres"}},
		{events.ApprovalGiven, events.ApprovalGivenPayload{ProposalID: p3, Layer: "postgres"}},
		{events.ApprovalGiven, events.ApprovalGivenPayload{ProposalID: p3, Layer: "warehouse"}},
		{events.ActionExecuted, events.ActionExecutedPayload{ProposalID: p3, StepIndex: 0, OK: true, Output: "done"}},
	} {
		if _, err := st.Append(context.Background(), roomID, a.typ, prior, a.payload); err != nil {
			t.Fatalf("append %s: %v", a.typ, err)
		}
	}
	if err := st.StartProposalStep(context.Background(), p1, 0); err != nil {
		t.Fatalf("claim p1 step 0: %v", err)
	}
	if err := st.StartProposalStep(context.Background(), p3, 1); err != nil {
		t.Fatalf("claim p3 step 1: %v", err)
	}
	svc.Close() // the old process dies here

	// A new process loads the same store.
	svc2 := room.New(room.Deps{Store: st, Users: room.DefaultUsers(), Run: fakeRun, Now: c.Now})
	t.Cleanup(svc2.Close)
	if err := svc2.Load(context.Background()); err != nil {
		t.Fatalf("load: %v", err)
	}

	for _, pid := range []string{p1, p3} {
		if p, err := st.GetProposal(context.Background(), pid); err != nil || p.Status != "failed" {
			t.Fatalf("proposal after Load: %+v err=%v", p, err)
		}
	}
	if got := st.ProposalStepStatus(p1, 0); got != "failed" {
		t.Fatalf("p1 step 0 after Load: got %q, want failed", got)
	}
	if got := st.ProposalStepStatus(p3, 1); got != "failed" {
		t.Fatalf("p3 step 1 after Load: got %q, want failed", got)
	}

	snap, err := svc2.GetRoom(context.Background(), roomID)
	if err != nil {
		t.Fatalf("get room: %v", err)
	}
	if len(snap.OpenProposals) != 1 || snap.OpenProposals[0].ProposalID != p2 {
		t.Fatalf("only the unapproved proposal stays open: %+v", snap.OpenProposals)
	}

	// The interruption events point at the step that was in flight: 0 for p1
	// (no finished steps), 1 for p3 (step 0 recorded done).
	interrupted := map[string]int{}
	for _, e := range allEvents(t, st, roomID) {
		if e.Type != events.ActionExecuted {
			continue
		}
		p, err := events.Decode[events.ActionExecutedPayload](e)
		if err == nil && !p.OK && strings.Contains(p.Output, "interrupted") {
			interrupted[p.ProposalID] = p.StepIndex
		}
	}
	if interrupted[p1] != 0 || interrupted[p3] != 1 {
		t.Fatalf("interruption steps: %v", interrupted)
	}
}

// TestAgentSinkOnlyAcceptsAgentEvents pins review item 9: the agent Sink
// refuses every non-agent.* event type, so the agent cannot forge
// control-plane events; the agent's own families (internal/agent) still pass.
func TestAgentSinkOnlyAcceptsAgentEvents(t *testing.T) {
	c := &clock{t: time.Date(2026, 10, 11, 10, 0, 0, 0, time.UTC)}
	st := memstore.NewWithClock(c.Now)
	sinks := make(chan agent.Sink, 1)
	svc := room.New(room.Deps{
		Store: st, Users: room.DefaultUsers(), Now: c.Now,
		Run: func(ctx context.Context, _ agent.Alert, deps agent.Deps, _ agent.Options) error {
			sinks <- deps.Sink
			<-ctx.Done()
			return nil
		},
	})
	t.Cleanup(svc.Close)
	roomID := createRoom(t, svc, "arjun").ID

	var sink agent.Sink
	select {
	case sink = <-sinks:
	case <-time.After(5 * time.Second):
		t.Fatal("agent run did not start")
	}

	ctx := context.Background()
	for _, typ := range []events.Type{
		events.ControlGranted, events.ControlRequested, events.ApprovalGiven,
		events.ApprovalRejected, events.RoomClosed, events.HandoffCreated,
		events.ProposalCreated, events.CommentAdded, events.SteerSent,
	} {
		if err := sink.Emit(ctx, typ, map[string]any{}); err == nil {
			t.Fatalf("agent sink accepted %s", typ)
		}
	}
	// The agent's own families still pass; proposals go through
	// Proposals.Create, not the sink.
	if err := sink.Emit(ctx, events.AgentMessage, events.AgentMessagePayload{Text: "hello"}); err != nil {
		t.Fatalf("agent.message refused: %v", err)
	}
	if err := sink.Emit(ctx, events.AgentStatus, events.AgentStatusPayload{State: "working"}); err != nil {
		t.Fatalf("agent.status refused: %v", err)
	}
	evs := allEvents(t, st, roomID)
	last := evs[len(evs)-1]
	if last.Type != events.AgentStatus || last.Actor.Kind != events.ActorAgent {
		t.Fatalf("bad agent event: %+v", last)
	}
}

// TestLeaseRenewalLoggedWhenCloseToExpiry pins review item 11 / docs/api.md:
// renewing while less than 25 minutes remain writes a control.granted with the
// same driver and the new lease_until (at most one per 5 minutes) and logs it;
// renewals with plenty of lease left stay silent.
func TestLeaseRenewalLoggedWhenCloseToExpiry(t *testing.T) {
	c := &clock{t: time.Date(2026, 10, 11, 10, 0, 0, 0, time.UTC)}
	st := memstore.NewWithClock(c.Now)
	var logBuf bytes.Buffer
	svc := room.New(room.Deps{
		Store: st, Users: room.DefaultUsers(), Run: fakeRun, Now: c.Now,
		Logger: slog.New(slog.NewTextHandler(&logBuf, nil)),
	})
	t.Cleanup(svc.Close)
	roomID := createRoom(t, svc, "arjun").ID
	arjun := mustUser(t, svc, "arjun")

	granted := func() []events.ControlGrantedPayload {
		var out []events.ControlGrantedPayload
		for _, e := range allEvents(t, st, roomID) {
			if e.Type != events.ControlGranted {
				continue
			}
			if p, err := events.Decode[events.ControlGrantedPayload](e); err == nil {
				out = append(out, p)
			}
		}
		return out
	}
	steer := func() {
		t.Helper()
		if _, err := svc.Steer(context.Background(), arjun, roomID, "check the topic"); err != nil {
			t.Fatalf("steer: %v", err)
		}
	}

	// t0: the creation grant is the only control.granted; a fresh renewal has
	// ~30 minutes left and stays silent.
	steer()
	if got := granted(); len(got) != 1 {
		t.Fatalf("want only the creation grant, got %d", len(got))
	}

	c.Advance(time.Minute) // t+1: 29 min remain → silent
	steer()
	if got := granted(); len(got) != 1 {
		t.Fatalf("renewal with 29 min left emitted an event (got %d)", len(got))
	}

	// t+11: only 20 minutes remain → the renewal is written to the log with
	// the same driver and the new lease_until, and logged via slog.
	c.Advance(10 * time.Minute)
	steer()
	got := granted()
	if len(got) != 2 {
		t.Fatalf("want a renewal event below 25 min, got %d", len(got))
	}
	if got[1].To.ID != "arjun" || got[1].To.Name != "Arjun" {
		t.Fatalf("renewal must name the current driver: %+v", got[1])
	}
	if want := c.Now().Add(room.DriverLease); !got[1].LeaseUntil.Equal(want) {
		t.Fatalf("lease_until: got %s, want %s", got[1].LeaseUntil, want)
	}
	if !strings.Contains(logBuf.String(), "renewed driver lease") {
		t.Fatalf("renewal not logged: %s", logBuf.String())
	}

	// Immediately after the renewal a full lease remains: no second event.
	steer()
	if got := granted(); len(got) != 2 {
		t.Fatalf("renewal right after an event must stay silent, got %d", len(got))
	}

	// Exactly 5 minutes after the renewal the lease has 25 minutes left → not
	// "less than 25" → still silent (this is the "at most one per 5 minutes"
	// spacing).
	c.Advance(5 * time.Minute)
	steer()
	if got := granted(); len(got) != 2 {
		t.Fatalf("renewal with exactly 25 min left must stay silent, got %d", len(got))
	}

	// Six minutes later less than 25 remain again → the next renewal event.
	c.Advance(6 * time.Minute)
	steer()
	if got := granted(); len(got) != 3 {
		t.Fatalf("want the next renewal event after 5+ silent minutes, got %d", len(got))
	}
}

// closeRoomAction closes its room from inside Execute, so the executor's
// room-closed stop path runs deterministically.
type closeRoomAction struct {
	svc    *room.Service
	roomID string
	user   room.User
}

func (a *closeRoomAction) Name() string     { return "close_room" }
func (a *closeRoomAction) Layer() string    { return "postgres" }
func (a *closeRoomAction) Describe() string { return "closes the room" }
func (a *closeRoomAction) ParamsSchema() map[string]any {
	return map[string]any{"type": "object"}
}
func (a *closeRoomAction) Validate(json.RawMessage) error                    { return nil }
func (a *closeRoomAction) CheckScope(owners.Pipeline, json.RawMessage) error { return nil }
func (a *closeRoomAction) Execute(context.Context, json.RawMessage) (string, error) {
	if _, err := a.svc.CloseRoom(context.Background(), a.user, a.roomID, "closed by the action"); err != nil {
		return "", err
	}
	return "closed", nil
}

// TestExecutorRoomClosedPathRecordsOutcome pins review item 13 for the
// room-closed stop path: the executor must mark the proposal failed with an
// outcome event (so the snapshot sees it) and must not run the remaining
// steps.
func TestExecutorRoomClosedPathRecordsOutcome(t *testing.T) {
	c := &clock{t: time.Date(2026, 10, 11, 10, 0, 0, 0, time.UTC)}
	st := memstore.NewWithClock(c.Now)
	cr := &closeRoomAction{}
	second := &fakeAction{name: "second_act", layer: "postgres"}
	reg := actions.NewRegistry()
	if err := reg.Register(cr); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := reg.Register(second); err != nil {
		t.Fatalf("register: %v", err)
	}
	svc := room.New(room.Deps{Store: st, Actions: reg, Users: room.DefaultUsers(), Owners: testOwners(), Run: fakeRun, Now: c.Now})
	t.Cleanup(svc.Close)

	roomID := createRoom(t, svc, "arjun").ID
	cr.svc = svc
	cr.roomID = roomID
	cr.user = mustUser(t, svc, "arjun")

	pid, err := svc.CreateProposal(context.Background(), roomID, events.ProposalCreatedPayload{
		Title: "fix",
		Steps: []events.ProposalStep{
			{Action: "close_room", Layer: "postgres", Params: json.RawMessage("{}")},
			{Action: "second_act", Layer: "postgres", Params: json.RawMessage("{}")},
		},
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
	if second.Calls() != 0 {
		t.Fatalf("step after the closed room ran: %d calls", second.Calls())
	}
	found := false
	for _, e := range allEvents(t, st, roomID) {
		if e.Type != events.ActionExecuted {
			continue
		}
		p, err := events.Decode[events.ActionExecutedPayload](e)
		if err == nil && p.ProposalID == pid && !p.OK && strings.Contains(p.Output, "room closed") {
			found = true
		}
	}
	if !found {
		t.Fatalf("no room-closed outcome event for %s", pid)
	}
	snap, _ := svc.GetRoom(context.Background(), roomID)
	if len(snap.OpenProposals) != 0 {
		t.Fatalf("proposal must leave open_proposals: %+v", snap.OpenProposals)
	}
	if snap.Status != "closed" {
		t.Fatalf("room should be closed: %+v", snap.Status)
	}
}

// TestScopeCheckFailsClosedWithoutOwners pins LEAD-ARCH round 4, item 3: the
// old "--demo skips the scope check" bypass is gone; a service with no owners
// config refuses to create scoped proposals instead of running them unchecked.
func TestScopeCheckFailsClosedWithoutOwners(t *testing.T) {
	c := &clock{t: time.Date(2026, 10, 11, 10, 0, 0, 0, time.UTC)}
	st := memstore.NewWithClock(c.Now)
	reg := actions.NewRegistry()
	if err := reg.Register(&fakeAction{name: "pg_act", layer: "postgres"}); err != nil {
		t.Fatalf("register: %v", err)
	}
	svc := room.New(room.Deps{Store: st, Actions: reg, Users: room.DefaultUsers(), Run: fakeRun, Now: c.Now})
	t.Cleanup(svc.Close)
	roomID := createRoom(t, svc, "arjun").ID

	_, err := svc.CreateProposal(context.Background(), roomID, events.ProposalCreatedPayload{
		Title: "fix",
		Steps: []events.ProposalStep{{Action: "pg_act", Layer: "postgres", Params: json.RawMessage("{}")}},
	}, events.AgentActor)
	wantCode(t, err, "validation")
	if err == nil || !strings.Contains(err.Error(), "owners") {
		t.Fatalf("want an owners-config error, got %v", err)
	}
}

// TestAgentAutoRestartsAfterFailure pins the arch should-fix "no auto-restart
// of agents": a run that fails transiently is retried (bounded, with backoff)
// and every failure is surfaced as agent.status "error".
func TestAgentAutoRestartsAfterFailure(t *testing.T) {
	c := &clock{t: time.Date(2026, 10, 11, 10, 0, 0, 0, time.UTC)}
	st := memstore.NewWithClock(c.Now)
	var mu sync.Mutex
	attempts := 0
	third := make(chan struct{})
	svc := room.New(room.Deps{
		Store: st, Users: room.DefaultUsers(), Owners: testOwners(), Now: c.Now,
		Run: func(ctx context.Context, _ agent.Alert, _ agent.Deps, _ agent.Options) error {
			mu.Lock()
			attempts++
			n := attempts
			mu.Unlock()
			if n < 3 {
				return errors.New("provider down")
			}
			close(third)
			<-ctx.Done()
			return nil
		},
	})
	t.Cleanup(svc.Close)
	roomID := createRoom(t, svc, "arjun").ID

	select {
	case <-third:
	case <-time.After(15 * time.Second):
		t.Fatal("agent was not restarted after its failures")
	}
	// One agent.status "error" per failed attempt, with the cause.
	waitFor(t, 5*time.Second, func() bool {
		n := 0
		for _, e := range allEvents(t, st, roomID) {
			if e.Type != events.AgentStatus {
				continue
			}
			if p, err := events.Decode[events.AgentStatusPayload](e); err == nil && p.State == "error" {
				n++
			}
		}
		return n == 2
	})
}

// TestTopologyUnknownPipelineNotFound pins docs/api.md: an unknown pipeline is
// a 404 from the API, not a 500 from the builder.
func TestTopologyUnknownPipelineNotFound(t *testing.T) {
	svc, _, _ := newTestService(t)
	_, err := svc.Topology(context.Background(), "nope")
	wantCode(t, err, "not_found")
	if _, err := svc.Topology(context.Background(), "bad id!"); err == nil {
		t.Fatal("malformed pipeline id returned a graph")
	}
	if _, err := svc.Topology(context.Background(), "orders"); err != nil {
		t.Fatalf("known pipeline: %v", err)
	}
}

// TestTraceRejectsMalformedTable pins the 400 (not 500) mapping for a bad
// table argument.
func TestTraceRejectsMalformedTable(t *testing.T) {
	svc, _, _ := newTestService(t)
	roomID := createRoom(t, svc, "arjun").ID
	_, err := svc.Trace(context.Background(), mustUser(t, svc, "arjun"), roomID, "not a table!", "k")
	wantCode(t, err, "validation")
}

// fakeTopo is a topology.Builder whose Trace returns a canned error.
type fakeTopo struct {
	traceErr error
}

func (fakeTopo) Build(ctx context.Context, pipelineID string) (topology.Graph, error) {
	return topology.Graph{PipelineID: pipelineID}, nil
}
func (fakeTopo) Render(topology.Graph) string { return "" }
func (f fakeTopo) Trace(ctx context.Context, pipelineID, table, key string) ([]events.TraceStage, error) {
	if f.traceErr != nil {
		return nil, f.traceErr
	}
	return []events.TraceStage{{Layer: "postgres", Name: table}}, nil
}
func (fakeTopo) LayerOwner(pipelineID, layer string) (string, bool) { return "", false }

// TestTraceOutOfPipelineTableIsValidation pins review item 7: a well-formed
// table outside the pipeline (topology.ErrBadInput) maps to 400 validation,
// not 500.
func TestTraceOutOfPipelineTableIsValidation(t *testing.T) {
	c := &clock{t: time.Date(2026, 10, 11, 10, 0, 0, 0, time.UTC)}
	st := memstore.NewWithClock(c.Now)
	svc := room.New(room.Deps{
		Store: st, Users: room.DefaultUsers(), Owners: testOwners(), Run: fakeRun, Now: c.Now,
		Topology: fakeTopo{traceErr: fmt.Errorf("%w: no connector streams table %q", topology.ErrBadInput, "public.payments")},
	})
	t.Cleanup(svc.Close)
	roomID := createRoom(t, svc, "arjun").ID

	_, err := svc.Trace(context.Background(), mustUser(t, svc, "arjun"), roomID, "public.payments", "1")
	wantCode(t, err, "validation")
}

// TestWebhookUnknownPipelineDisablesProposals pins review item 2: a webhook
// with an unknown pipeline opens the room, but the agent cannot propose a fix
// against a pipeline nothing can be scoped to.
func TestWebhookUnknownPipelineDisablesProposals(t *testing.T) {
	act := &fakeAction{name: "pg_act", layer: "postgres"}
	svc, _, _ := newTestService(t, act)

	snap, err := svc.CreateRoomFromAlert(context.Background(), "", "", "orders-2", json.RawMessage(`{"pipeline_id":"orders-2"}`))
	if err != nil {
		t.Fatalf("webhook with unknown pipeline must open a room: %v", err)
	}
	if snap.PipelineID != "orders-2" {
		t.Fatalf("bad pipeline: %+v", snap)
	}
	_, err = svc.CreateProposal(context.Background(), snap.ID, events.ProposalCreatedPayload{
		Title: "fix",
		Steps: []events.ProposalStep{{Action: "pg_act", Layer: "postgres", Params: json.RawMessage("{}")}},
	}, events.AgentActor)
	wantCode(t, err, "validation")
	if err == nil || !strings.Contains(err.Error(), "proposals are disabled") {
		t.Fatalf("want proposals-disabled error, got %v", err)
	}

	// A known pipeline keeps proposals enabled.
	snap2 := createRoom(t, svc, "arjun")
	if _, err := svc.CreateProposal(context.Background(), snap2.ID, events.ProposalCreatedPayload{
		Title: "fix",
		Steps: []events.ProposalStep{{Action: "pg_act", Layer: "postgres", Params: json.RawMessage("{}")}},
	}, events.AgentActor); err != nil {
		t.Fatalf("known pipeline must allow proposals: %v", err)
	}
}

// TestSendSuggestionAlreadySentConflict pins review item 3 / docs/api.md:51: a
// second send of an already-sent suggestion is 409 already_decided, while a
// suggestion sent to the wrong room stays 404 not_found.
func TestSendSuggestionAlreadySentConflict(t *testing.T) {
	svc, _, _ := newTestService(t)
	roomID := createRoom(t, svc, "arjun").ID

	sugg, err := svc.Steer(context.Background(), mustUser(t, svc, "priya"), roomID, "look here")
	if err != nil {
		t.Fatalf("steer: %v", err)
	}
	sp, _ := events.Decode[events.SteerSuggestedPayload](sugg)

	if _, err := svc.SendSuggestion(context.Background(), mustUser(t, svc, "arjun"), roomID, sp.SuggestionID); err != nil {
		t.Fatalf("send: %v", err)
	}
	_, err = svc.SendSuggestion(context.Background(), mustUser(t, svc, "arjun"), roomID, sp.SuggestionID)
	wantCode(t, err, "already_decided")

	// A suggestion that exists but belongs to a different room is not found.
	room2 := createRoom(t, svc, "arjun").ID
	_, err = svc.SendSuggestion(context.Background(), mustUser(t, svc, "arjun"), room2, sp.SuggestionID)
	wantCode(t, err, "not_found")
}

// TestLoadSkipsAlreadyExecutedProposals pins review item 6: a proposal whose
// store status already says "executed" (its last action.executed append
// failed) must not be re-marked failed by the interruption sweep on Load.
func TestLoadSkipsAlreadyExecutedProposals(t *testing.T) {
	pg := &fakeAction{name: "pg_act", layer: "postgres"}
	svc, st, c := newTestService(t, pg)
	roomID := createRoom(t, svc, "arjun").ID

	pid, err := svc.CreateProposal(context.Background(), roomID, events.ProposalCreatedPayload{
		Title: "fix",
		Steps: []events.ProposalStep{{Action: "pg_act", Layer: "postgres", Params: json.RawMessage("{}")}},
	}, events.AgentActor)
	if err != nil {
		t.Fatalf("create proposal: %v", err)
	}
	// Fully approved in the log, but the action.executed append "failed" while
	// the store status write succeeded: the DB says executed, the snapshot
	// still shows the proposal open.
	prior := events.Actor{Kind: events.ActorHuman, ID: "priya", Name: "Priya"}
	if _, err := st.Append(context.Background(), roomID, events.ApprovalGiven, prior, events.ApprovalGivenPayload{ProposalID: pid, Layer: "postgres"}); err != nil {
		t.Fatalf("append approval: %v", err)
	}
	if err := st.SetProposalStatus(context.Background(), pid, "executed"); err != nil {
		t.Fatalf("set executed: %v", err)
	}
	svc.Close() // the old process dies here

	svc2 := room.New(room.Deps{Store: st, Users: room.DefaultUsers(), Run: fakeRun, Now: c.Now})
	t.Cleanup(svc2.Close)
	if err := svc2.Load(context.Background()); err != nil {
		t.Fatalf("load: %v", err)
	}

	if p, err := st.GetProposal(context.Background(), pid); err != nil || p.Status != "executed" {
		t.Fatalf("proposal after Load: %+v err=%v", p, err)
	}
	for _, e := range allEvents(t, st, roomID) {
		if e.Type != events.ActionExecuted {
			continue
		}
		p, err := events.Decode[events.ActionExecutedPayload](e)
		if err == nil && p.ProposalID == pid && !p.OK && strings.Contains(p.Output, "interrupted") {
			t.Fatalf("executed proposal was re-marked failed by Load")
		}
	}
}

// TestRebuildDriverLeaseUTC pins review item 5: a driver lease read back from
// the store in a non-UTC zone is normalised to UTC when the snapshot is
// rebuilt after a restart (docs/api.md:7).
func TestRebuildDriverLeaseUTC(t *testing.T) {
	svc, st, c := newTestService(t)
	roomID := createRoom(t, svc, "arjun").ID
	svc.Close()

	// A lease persisted by a non-UTC host: later than the control.granted
	// event's lease, but carried in a fixed non-UTC zone.
	later := c.Now().Add(40 * time.Minute).In(time.FixedZone("UTC+1", 3600))
	if err := st.UpdateDriver(context.Background(), roomID, "arjun", "Arjun", later); err != nil {
		t.Fatalf("update driver: %v", err)
	}

	svc2 := room.New(room.Deps{Store: st, Users: room.DefaultUsers(), Run: fakeRun, Now: c.Now})
	t.Cleanup(svc2.Close)
	if err := svc2.Load(context.Background()); err != nil {
		t.Fatalf("load: %v", err)
	}
	snap, err := svc2.GetRoom(context.Background(), roomID)
	if err != nil {
		t.Fatalf("get room: %v", err)
	}
	if snap.Driver == nil {
		t.Fatal("no driver after load")
	}
	if snap.Driver.LeaseUntil.Location() != time.UTC {
		t.Fatalf("lease not UTC after rebuild: %v", snap.Driver.LeaseUntil.Location())
	}
	if !snap.Driver.LeaseUntil.Equal(later.UTC()) {
		t.Fatalf("lease: got %s want %s", snap.Driver.LeaseUntil, later.UTC())
	}
}
