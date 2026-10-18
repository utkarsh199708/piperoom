package room_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"piperoom/internal/actions"
	"piperoom/internal/events"
	"piperoom/internal/room"
	"piperoom/internal/store/memstore"
)

// Write mode (PIPEROOM_WRITE_MODE, P0 item 1).

// newWriteModeService is newTestService with WriteModeOff and SoloUser set,
// over st (a new store when nil), so a test can restart the service on the
// same log.
func newWriteModeService(t *testing.T, st *memstore.Store, off bool, solo string, act ...actions.Action) (*room.Service, *memstore.Store) {
	t.Helper()
	if st == nil {
		st = memstore.New()
	}
	reg := actions.NewRegistry()
	for _, a := range act {
		if err := reg.Register(a); err != nil {
			t.Fatalf("register action: %v", err)
		}
	}
	svc := room.New(room.Deps{
		Store: st, Actions: reg, Users: room.DefaultUsers(), Owners: testOwners(),
		Run: fakeRun, WriteHandoff: fakeWrite, WriteModeOff: off, SoloUser: solo,
	})
	t.Cleanup(svc.Close)
	return svc, st
}

// writeModeProposal creates a proposal with a postgres and a warehouse step.
func writeModeProposal(t *testing.T, svc *room.Service, roomID string) string {
	t.Helper()
	pid, err := svc.CreateProposal(context.Background(), roomID, events.ProposalCreatedPayload{
		Title: "fix", Risk: "low", Rollback: "revert the change",
		Steps: []events.ProposalStep{
			{Action: "pg_act", Layer: "postgres", Params: json.RawMessage("{}")},
			{Action: "wh_act", Layer: "warehouse", Params: json.RawMessage("{}")},
		},
	}, events.AgentActor)
	if err != nil {
		t.Fatalf("create proposal: %v", err)
	}
	return pid
}

func approveLayer(t *testing.T, svc *room.Service, userID, pid, layer string) {
	t.Helper()
	if _, err := svc.ApproveProposal(context.Background(), mustUser(t, svc, userID), pid, layer, proposalHash(t, svc, pid)); err != nil {
		t.Fatalf("%s approves %s: %v", userID, layer, err)
	}
}

func countEvents(t *testing.T, st *memstore.Store, roomID string, typ events.Type) int {
	t.Helper()
	n := 0
	for _, e := range allEvents(t, st, roomID) {
		if e.Type == typ {
			n++
		}
	}
	return n
}

// viewProposal returns the caller's view of one open proposal.
func viewProposal(t *testing.T, svc *room.Service, userID, roomID, pid string) (room.RoomSnapshot, room.Proposal) {
	t.Helper()
	snap, err := svc.RoomFor(context.Background(), mustUser(t, svc, userID), roomID)
	if err != nil {
		t.Fatalf("room view: %v", err)
	}
	for _, p := range snap.OpenProposals {
		if p.ProposalID == pid {
			return snap, p
		}
	}
	t.Fatalf("proposal %s is not open: %+v", pid, snap.OpenProposals)
	return snap, room.Proposal{}
}

// Off: the last approval runs nothing and writes no action.executed; the
// proposal stays open, and the caller's view says it is blocked by write mode
// while the published snapshot stays as the log folds it.
func TestWriteModeOffApprovedProposalNeverRuns(t *testing.T) {
	pg := &fakeAction{name: "pg_act", layer: "postgres"}
	wh := &fakeAction{name: "wh_act", layer: "warehouse"}
	svc, st := newWriteModeService(t, nil, true, "", pg, wh)
	roomID := createRoom(t, svc, "arjun").ID
	pid := writeModeProposal(t, svc, roomID)

	approveLayer(t, svc, "priya", pid, "postgres")
	if _, p := viewProposal(t, svc, "priya", roomID, pid); p.Blocked != "" {
		t.Fatalf("half-approved proposal blocked = %q, want empty", p.Blocked)
	}
	approveLayer(t, svc, "meera", pid, "warehouse")

	snap, p := viewProposal(t, svc, "meera", roomID, pid)
	if p.Blocked != room.BlockedWriteModeOff {
		t.Fatalf("blocked = %q, want %q", p.Blocked, room.BlockedWriteModeOff)
	}
	if snap.Mode == nil || snap.Mode.Write || snap.Mode.Solo || snap.Mode.AutoApprove == nil || len(snap.Mode.AutoApprove) != 0 {
		t.Fatalf("mode = %+v, want write false, solo false, auto_approve []", snap.Mode)
	}
	raw, err := json.Marshal(snap)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`"mode":{"write":false,"solo":false,"auto_approve":[]}`, `"blocked":"write_mode_off"`} {
		if !strings.Contains(string(raw), want) {
			t.Errorf("view JSON lacks %s: %s", want, raw)
		}
	}
	// The published snapshot is the log's alone.
	pub, err := svc.GetRoom(context.Background(), roomID)
	if err != nil {
		t.Fatal(err)
	}
	if pub.Mode != nil || len(pub.OpenProposals) != 1 || pub.OpenProposals[0].Blocked != "" {
		t.Fatalf("published snapshot carries the view: mode %+v, proposals %+v", pub.Mode, pub.OpenProposals)
	}
	// Approving again is still refused: the layers are decided.
	_, err = svc.ApproveProposal(context.Background(), mustUser(t, svc, "meera"), pid, "warehouse", proposalHash(t, svc, pid))
	wantCode(t, err, "already_decided")

	svc.Close() // waits for every goroutine the service started
	if n := countEvents(t, st, roomID, events.ActionExecuted); n != 0 {
		t.Fatalf("%d action.executed events with write mode off, want 0", n)
	}
	if pg.Calls()+wh.Calls() != 0 {
		t.Fatalf("actions ran with write mode off: pg %d, wh %d", pg.Calls(), wh.Calls())
	}
	if row, err := st.GetProposal(context.Background(), pid); err != nil || row.Status != "open" {
		t.Fatalf("proposal row = %+v (err %v), want status open", row, err)
	}
	if got := st.ProposalStepStatus(pid, 0); got != "" {
		t.Fatalf("step 0 status %q, want never started", got)
	}
}

// On: today's behaviour, the steps run and the view shows nothing blocked.
func TestWriteModeOnRunsApprovedProposal(t *testing.T) {
	pg := &fakeAction{name: "pg_act", layer: "postgres"}
	wh := &fakeAction{name: "wh_act", layer: "warehouse"}
	svc, st := newWriteModeService(t, nil, false, "", pg, wh)
	roomID := createRoom(t, svc, "arjun").ID
	pid := writeModeProposal(t, svc, roomID)

	snap, p := viewProposal(t, svc, "priya", roomID, pid)
	if snap.Mode == nil || !snap.Mode.Write || p.Blocked != "" {
		t.Fatalf("mode %+v blocked %q, want write true and nothing blocked", snap.Mode, p.Blocked)
	}
	approveLayer(t, svc, "priya", pid, "postgres")
	approveLayer(t, svc, "meera", pid, "warehouse")
	waitFor(t, 5*time.Second, func() bool { return countEvents(t, st, roomID, events.ActionExecuted) == 2 })
	if pg.Calls() != 1 || wh.Calls() != 1 {
		t.Fatalf("calls pg %d wh %d, want 1 each", pg.Calls(), wh.Calls())
	}
	if !svc.WriteOn() {
		t.Fatal("WriteOn = false with WriteModeOff unset")
	}
}

// A restart keeps a blocked proposal blocked while write mode stays off; a
// restart with write mode on records it as interrupted and never runs an
// approval given while writes were off.
func TestWriteModeOffSurvivesRestart(t *testing.T) {
	pg := &fakeAction{name: "pg_act", layer: "postgres"}
	wh := &fakeAction{name: "wh_act", layer: "warehouse"}
	svc, st := newWriteModeService(t, nil, true, "", pg, wh)
	roomID := createRoom(t, svc, "arjun").ID
	pid := writeModeProposal(t, svc, roomID)
	approveLayer(t, svc, "priya", pid, "postgres")
	approveLayer(t, svc, "meera", pid, "warehouse")
	svc.Close()

	off, _ := newWriteModeService(t, st, true, "", pg, wh)
	if err := off.Load(context.Background()); err != nil {
		t.Fatalf("load (off): %v", err)
	}
	if _, p := viewProposal(t, off, "priya", roomID, pid); p.Blocked != room.BlockedWriteModeOff {
		t.Fatalf("after an off restart blocked = %q", p.Blocked)
	}
	off.Close()
	if n := countEvents(t, st, roomID, events.ActionExecuted); n != 0 {
		t.Fatalf("an off restart wrote %d action.executed, want 0", n)
	}

	on, _ := newWriteModeService(t, st, false, "", pg, wh)
	if err := on.Load(context.Background()); err != nil {
		t.Fatalf("load (on): %v", err)
	}
	on.Close()
	var outs []events.ActionExecutedPayload
	for _, e := range allEvents(t, st, roomID) {
		if e.Type == events.ActionExecuted {
			p, err := events.Decode[events.ActionExecutedPayload](e)
			if err != nil {
				t.Fatal(err)
			}
			outs = append(outs, p)
		}
	}
	if len(outs) != 1 || outs[0].OK || outs[0].ProposalID != pid || !strings.Contains(outs[0].Output, "interrupted") {
		t.Fatalf("on restart outcome = %+v, want one failed interruption of %s", outs, pid)
	}
	if pg.Calls()+wh.Calls() != 0 {
		t.Fatalf("an approval given with write mode off ran after the switch: pg %d, wh %d", pg.Calls(), wh.Calls())
	}
}

// mode.solo is true for PIPEROOM_SOLO_USER only, per caller.
func TestRoomViewModeSoloPerCaller(t *testing.T) {
	svc, _ := newWriteModeService(t, nil, true, "priya")
	roomID := createRoom(t, svc, "priya").ID
	for user, want := range map[string]bool{"priya": true, "arjun": false} {
		snap, err := svc.RoomFor(context.Background(), mustUser(t, svc, user), roomID)
		if err != nil {
			t.Fatal(err)
		}
		if snap.Mode == nil || snap.Mode.Solo != want || snap.Mode.Write {
			t.Errorf("%s: mode = %+v, want solo %v and write false", user, snap.Mode, want)
		}
	}
	if _, err := svc.RoomFor(context.Background(), mustUser(t, svc, "priya"), "r_missing"); err == nil {
		t.Fatal("RoomFor of a missing room: want an error")
	}
}
