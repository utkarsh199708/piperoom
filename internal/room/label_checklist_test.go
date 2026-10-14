package room_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"piperoom/internal/events"
	"piperoom/internal/room"
)

// F8d: POST /api/rooms/{id}/label and /checklist (tasks/F8-what-changed-checklist.md).

func closeRoomAs(t *testing.T, svc *room.Service, roomID, userID string) {
	t.Helper()
	if _, err := svc.CloseRoom(context.Background(), mustUser(t, svc, userID), roomID, "fixed"); err != nil {
		t.Fatalf("close: %v", err)
	}
}

func TestLabelRequiresClosedRoom(t *testing.T) {
	svc, st, _ := newTestService(t)
	roomID := createRoom(t, svc, "arjun").ID
	_, err := svc.Label(context.Background(), mustUser(t, svc, "arjun"), roomID, room.LabelInput{Cause: "slot lag"})
	wantCode(t, err, "room_open")
	closeRoomAs(t, svc, roomID, "arjun")
	e, err := svc.Label(context.Background(), mustUser(t, svc, "arjun"), roomID, room.LabelInput{Cause: "slot lag", ActualLayer: "kafka-connect"})
	if err != nil {
		t.Fatalf("label closed room: %v", err)
	}
	if e.Type != events.RoomLabelled {
		t.Fatalf("event type %s", e.Type)
	}
	p, err := events.Decode[events.RoomLabelledPayload](e)
	if err != nil || p.Cause != "slot lag" || p.Identity == nil || p.Identity.Grant == "" {
		t.Fatalf("payload %+v err %v", p, err)
	}
	_, err = svc.Label(context.Background(), mustUser(t, svc, "arjun"), "nope", room.LabelInput{Cause: "x"})
	wantCode(t, err, "not_found")
	// Re-label appends another; the snapshot says labelled.
	if _, err := svc.Label(context.Background(), mustUser(t, svc, "arjun"), roomID, room.LabelInput{Cause: "slot lag, really"}); err != nil {
		t.Fatalf("re-label: %v", err)
	}
	n := 0
	for _, ev := range allEvents(t, st, roomID) {
		if ev.Type == events.RoomLabelled {
			n++
		}
	}
	if n != 2 {
		t.Fatalf("want 2 room.labelled events, got %d", n)
	}
	snap, err := svc.GetRoom(context.Background(), roomID)
	if err != nil || !snap.Labelled {
		t.Fatalf("snapshot labelled = %v, %v", snap.Labelled, err)
	}
}

func TestLabelMember(t *testing.T) {
	svc, _, _ := newTestService(t)
	roomID := createRoom(t, svc, "arjun").ID
	closeRoomAs(t, svc, roomID, "arjun")
	nobody := room.User{ID: "stranger", Name: "Stranger"}
	_, err := svc.Label(context.Background(), nobody, roomID, room.LabelInput{Cause: "x"})
	wantCode(t, err, "not_allowed")
}

func TestLabelValidation(t *testing.T) {
	svc, _, _ := newTestService(t)
	roomID := createRoom(t, svc, "arjun").ID
	closeRoomAs(t, svc, roomID, "arjun")
	u := mustUser(t, svc, "arjun")
	long := strings.Repeat("x", 501)
	for name, in := range map[string]room.LabelInput{
		"empty cause":      {Cause: "  "},
		"long cause":       {Cause: long},
		"long fix":         {Cause: "c", FixThatWorked: long},
		"bad layer":        {Cause: "c", ActualLayer: "moon"},
		"nul in cause":     {Cause: "a\x00b"},
		"unknown proposal": {Cause: "c", FixProposalID: "p_nope"},
	} {
		_, err := svc.Label(context.Background(), u, roomID, in)
		if err == nil {
			t.Errorf("%s: want a validation error", name)
			continue
		}
		wantCode(t, err, "validation")
	}
}

func TestLabelFixProposalMustHaveRun(t *testing.T) {
	ok := &fakeAction{name: "ok_action", layer: "postgres"}
	svc, st, _ := newTestService(t, ok)
	ctx := context.Background()
	roomID := createRoom(t, svc, "arjun").ID
	mk := func(title string) string {
		pid, err := svc.CreateProposal(ctx, roomID, events.ProposalCreatedPayload{
			Title: title, Risk: "low", Rollback: "revert",
			Steps:          []events.ProposalStep{{Action: "ok_action", Layer: "postgres", Params: json.RawMessage("{}")}},
			RequiredLayers: []string{"postgres"},
		}, events.AgentActor)
		if err != nil {
			t.Fatalf("proposal: %v", err)
		}
		return pid
	}
	ran, notRan := mk("ran"), mk("not run")
	if _, err := svc.ApproveProposal(ctx, mustUser(t, svc, "priya"), ran, "postgres", proposalHash(t, svc, ran)); err != nil {
		t.Fatalf("approve: %v", err)
	}
	waitFor(t, 5e9, func() bool { return ok.Calls() == 1 })
	waitFor(t, 5e9, func() bool {
		for _, e := range allEvents(t, st, roomID) {
			if e.Type == events.ActionExecuted {
				return true
			}
		}
		return false
	})
	closeRoomAs(t, svc, roomID, "arjun")
	u := mustUser(t, svc, "arjun")
	_, err := svc.Label(ctx, u, roomID, room.LabelInput{Cause: "c", FixProposalID: notRan})
	wantCode(t, err, "validation")
	if _, err := svc.Label(ctx, u, roomID, room.LabelInput{Cause: "c", FixProposalID: ran}); err != nil {
		t.Fatalf("label with executed proposal: %v", err)
	}
}

// toolResult appends a runbook_lookup result with the given items to the log.
func toolResult(t *testing.T, st interface {
	Append(context.Context, string, events.Type, events.Actor, any) (events.Event, error)
}, roomID, id, tool, data string) {
	t.Helper()
	if _, err := st.Append(context.Background(), roomID, events.AgentToolResult, events.AgentActor, events.AgentToolResultPayload{
		StepID: "s_1", ToolUseID: id, Tool: tool, Summary: "ok", Data: json.RawMessage(data),
	}); err != nil {
		t.Fatalf("append tool result: %v", err)
	}
}

func TestChecklistValidatesToolUse(t *testing.T) {
	svc, st, _ := newTestService(t)
	ctx := context.Background()
	roomID := createRoom(t, svc, "arjun").ID
	toolResult(t, st, roomID, "tu_rb", "runbook_lookup", `{"hits":[{"section":"Failed connector","items":["Check status","Resume the connector"]}]}`)
	toolResult(t, st, roomID, "tu_other", "connect_status", `{"hits":[{"items":["a"]}]}`)
	u := mustUser(t, svc, "kabir")
	in := room.ChecklistInput{StepID: "s_1", ToolUseID: "tu_rb", Hit: 0, Item: 1, Done: true}

	bad := map[string]room.ChecklistInput{
		"other tool":   {StepID: "s_1", ToolUseID: "tu_other", Item: 0, Done: true},
		"unknown id":   {StepID: "s_1", ToolUseID: "tu_zzz", Item: 0, Done: true},
		"hit too big":  {StepID: "s_1", ToolUseID: "tu_rb", Hit: 1, Item: 0, Done: true},
		"item too big": {StepID: "s_1", ToolUseID: "tu_rb", Hit: 0, Item: 2, Done: true},
		"negative":     {StepID: "s_1", ToolUseID: "tu_rb", Hit: 0, Item: -1, Done: true},
		"long note":    {StepID: "s_1", ToolUseID: "tu_rb", Item: 0, Done: true, Note: strings.Repeat("n", 301)},
		"no step id":   {ToolUseID: "tu_rb", Item: 0, Done: true},
	}
	for name, b := range bad {
		_, err := svc.ChecklistStep(ctx, u, roomID, b)
		if err == nil {
			t.Errorf("%s: want error", name)
			continue
		}
		wantCode(t, err, "validation")
	}
	e, err := svc.ChecklistStep(ctx, u, roomID, in)
	if err != nil {
		t.Fatalf("tick: %v", err)
	}
	if e.Type != events.ChecklistStepDone || e.Actor.ID != "kabir" {
		t.Fatalf("event %+v", e)
	}
	// Idempotent: the same state again still appends.
	if _, err := svc.ChecklistStep(ctx, u, roomID, in); err != nil {
		t.Fatalf("tick again: %v", err)
	}
	closeRoomAs(t, svc, roomID, "arjun")
	_, err = svc.ChecklistStep(ctx, u, roomID, in)
	wantCode(t, err, "room_closed")
}

func TestApplyChecklistAndLabel(t *testing.T) {
	snap := room.NewSnapshot()
	ev := func(seq int64, typ events.Type, actor events.Actor, p any) events.Event {
		b, _ := json.Marshal(p)
		return events.Event{Seq: seq, Type: typ, Actor: actor, Payload: b}
	}
	kabir := events.Actor{Kind: events.ActorHuman, ID: "kabir", Name: "Kabir", Verified: true}
	priya := events.Actor{Kind: events.ActorHuman, ID: "priya", Name: "Priya"}
	tick := func(item int, done bool) events.ChecklistStepDonePayload {
		return events.ChecklistStepDonePayload{StepID: "s_1", ToolUseID: "tu_rb", Item: item, Done: done}
	}
	snap = room.Apply(snap, ev(1, events.ChecklistStepDone, kabir, tick(2, true)))
	in := snap
	snap = room.Apply(snap, ev(2, events.ChecklistStepDone, priya, tick(0, true)))
	if len(in.ChecklistDone) != 1 {
		t.Fatalf("Apply mutated its input: %v", in.ChecklistDone)
	}
	if got := snap.ChecklistDone["tu_rb:0:2"]; got.By.ID != "kabir" || !got.By.Verified {
		t.Fatalf("tick by = %+v", got)
	}
	if len(snap.ChecklistDone) != 2 {
		t.Fatalf("want 2 ticks, got %v", snap.ChecklistDone)
	}
	// Last event per key wins: priya re-ticks item 2, then unticks item 0.
	snap = room.Apply(snap, ev(3, events.ChecklistStepDone, priya, tick(2, true)))
	snap = room.Apply(snap, ev(4, events.ChecklistStepDone, kabir, tick(0, false)))
	if got := snap.ChecklistDone["tu_rb:0:2"].By.ID; got != "priya" {
		t.Fatalf("last tick should win, got %q", got)
	}
	if _, ok := snap.ChecklistDone["tu_rb:0:0"]; ok {
		t.Fatalf("untick must remove the key: %v", snap.ChecklistDone)
	}
	if snap.Labelled {
		t.Fatal("labelled before room.labelled")
	}
	snap = room.Apply(snap, ev(5, events.RoomLabelled, priya, events.RoomLabelledPayload{Cause: "c"}))
	if !snap.Labelled {
		t.Fatal("room.labelled must set labelled")
	}
	b, _ := json.Marshal(room.Apply(room.NewSnapshot(), ev(1, events.RoomCreated, priya, events.RoomCreatedPayload{Title: "t", PipelineID: "orders"})))
	if !strings.Contains(string(b), `"checklist_done":{}`) || !strings.Contains(string(b), `"labelled":false`) {
		t.Fatalf("snapshot json lacks the F8 fields or has null: %s", b)
	}
	if c := snap.Clone(); len(c.ChecklistDone) != len(snap.ChecklistDone) {
		t.Fatalf("clone lost ticks")
	}
}

func TestLabelShadowNeedsBlindLabelFirst(t *testing.T) {
	env := newShadowEnv(t, shadowCfg{})
	ctx := context.Background()
	snap := env.open(t, "label-shadow")
	env.waitFinished(t, snap.ID)
	arjun, priya := env.user(t, "arjun"), env.user(t, "priya")
	if _, err := env.svc.GrantControl(ctx, arjun, snap.ID, "arjun"); err != nil {
		t.Fatal(err)
	}
	env.close(t, snap.ID, "schema change")
	_, err := env.svc.Label(ctx, priya, snap.ID, room.LabelInput{Cause: "schema"})
	wantCode(t, err, "not_labelled")
	if _, err := env.svc.ShadowLabel(ctx, priya, snap.ID, "kafka-connect", []string{"schema"}); err != nil {
		t.Fatal(err)
	}
	if _, err := env.svc.Label(ctx, priya, snap.ID, room.LabelInput{Cause: "schema"}); err != nil {
		t.Fatalf("label after the blind label: %v", err)
	}
}
