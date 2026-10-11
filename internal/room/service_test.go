package room_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"piperoom/internal/actions"
	"piperoom/internal/agent"
	"piperoom/internal/events"
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
	st := memstore.New()
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

	// While the lease is held, a non-driver cannot grant control.
	_, err := svc.GrantControl(context.Background(), mustUser(t, svc, "kabir"), roomID, "kabir")
	wantCode(t, err, "not_driver")

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
