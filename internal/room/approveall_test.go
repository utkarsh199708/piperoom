package room_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"piperoom/internal/actions"
	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/room"
	"piperoom/internal/store"
	"piperoom/internal/store/memstore"
)

// POST /api/proposals/{pid}/approve-all (P0 item 4, P2b-1).

// approveAllOwners names priya on both layers of writeModeProposal, meera on
// warehouse only and kabir on postgres only, so each of them has a different
// answer from approve-all.
func approveAllOwners() *owners.Owners {
	o := testOwners()
	u := func(id string) owners.Principal { return owners.Principal{Kind: owners.KindUser, Name: id} }
	o.Layers = map[string]owners.Principals{
		"postgres":  {u("priya"), u("kabir")},
		"warehouse": {u("priya"), u("meera")},
	}
	return o
}

// newApproveAllService is a service over approveAllOwners with the pg_act and
// wh_act actions of writeModeProposal.
func newApproveAllService(t *testing.T, off bool, solo string) (*room.Service, *memstore.Store, *fakeAction, *fakeAction) {
	t.Helper()
	pg := &fakeAction{name: "pg_act", layer: "postgres"}
	wh := &fakeAction{name: "wh_act", layer: "warehouse"}
	reg := actions.NewRegistry()
	for _, a := range []actions.Action{pg, wh} {
		if err := reg.Register(a); err != nil {
			t.Fatalf("register action: %v", err)
		}
	}
	st := memstore.New()
	svc := room.New(room.Deps{
		Store: st, Actions: reg, Users: room.DefaultUsers(), Owners: approveAllOwners(),
		Run: fakeRun, WriteHandoff: fakeWrite, WriteModeOff: off, SoloUser: solo,
	})
	t.Cleanup(svc.Close)
	return svc, st, pg, wh
}

// approvalsGiven decodes every approval.given of the room, in log order.
func approvalsGiven(t *testing.T, st *memstore.Store, roomID string) []events.ApprovalGivenPayload {
	t.Helper()
	var out []events.ApprovalGivenPayload
	for _, e := range allEvents(t, st, roomID) {
		if e.Type != events.ApprovalGiven {
			continue
		}
		p, err := events.Decode[events.ApprovalGivenPayload](e)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, p)
	}
	return out
}

// checkApprovals asserts the returned events: one approval.given per layer, in
// that order, consecutive in the log (one hold of the room's lock), each with
// the grant and identity.grant given.
func checkApprovals(t *testing.T, evs []events.Event, pid, hash, actor, grant, identGrant string, layers ...string) {
	t.Helper()
	if len(evs) != len(layers) {
		t.Fatalf("got %d events, want %d (%v)", len(evs), len(layers), layers)
	}
	for i, e := range evs {
		p, err := events.Decode[events.ApprovalGivenPayload](e)
		if err != nil {
			t.Fatal(err)
		}
		if e.Type != events.ApprovalGiven || e.Actor.ID != actor || p.ProposalID != pid || p.Layer != layers[i] ||
			p.ParamsHash != hash || p.Grant != grant || p.Identity == nil || p.Identity.Grant != identGrant {
			t.Fatalf("event %d = %s by %s %+v (identity %+v), want approval.given of %s by %s, grant %s, identity.grant %s",
				i, e.Type, e.Actor.ID, p, p.Identity, layers[i], actor, grant, identGrant)
		}
		if i > 0 && e.Seq != evs[i-1].Seq+1 {
			t.Fatalf("event %d has seq %d after %d: something was appended between the layers", i, e.Seq, evs[i-1].Seq)
		}
	}
}

// One person owns both layers: one call records postgres then warehouse with
// grant owner and the steps run, as after the last single approve.
func TestApproveAllOwnerOfEveryLayerRuns(t *testing.T) {
	svc, st, pg, wh := newApproveAllService(t, false, "")
	roomID := createRoom(t, svc, "arjun").ID
	pid := writeModeProposal(t, svc, roomID)
	hash := proposalHash(t, svc, pid)

	evs, err := svc.ApproveAllProposal(context.Background(), mustUser(t, svc, "priya"), pid, hash, "")
	if err != nil {
		t.Fatalf("approve-all: %v", err)
	}
	checkApprovals(t, evs, pid, hash, "priya", events.GrantOwner, "user:priya", "postgres", "warehouse")
	waitFor(t, 5*time.Second, func() bool { return countEvents(t, st, roomID, events.ActionExecuted) == 2 })
	if pg.Calls() != 1 || wh.Calls() != 1 {
		t.Fatalf("calls pg %d wh %d, want 1 each", pg.Calls(), wh.Calls())
	}
	if got := svc.ApprovalsByGrant(); got[events.GrantOwner] != 2 || got[events.GrantSolo] != 0 || len(got) != 4 {
		t.Fatalf("approvals by grant = %v, want owner 2 and all four grants listed", got)
	}
	// Executed: decided.
	_, err = svc.ApproveAllProposal(context.Background(), mustUser(t, svc, "priya"), pid, hash, "")
	wantCode(t, err, "already_decided")
}

// Write mode off: the same call records both layers, nothing runs, the row
// is blocked and a second approve-all is already_decided.
func TestApproveAllWriteModeOffBlocks(t *testing.T) {
	svc, st, pg, wh := newApproveAllService(t, true, "")
	roomID := createRoom(t, svc, "arjun").ID
	pid := writeModeProposal(t, svc, roomID)
	hash := proposalHash(t, svc, pid)

	evs, err := svc.ApproveAllProposal(context.Background(), mustUser(t, svc, "priya"), pid, hash, "")
	if err != nil {
		t.Fatalf("approve-all: %v", err)
	}
	checkApprovals(t, evs, pid, hash, "priya", events.GrantOwner, "user:priya", "postgres", "warehouse")
	if _, p := viewProposal(t, svc, "priya", roomID, pid); p.Blocked != room.BlockedWriteModeOff {
		t.Fatalf("blocked = %q, want %q", p.Blocked, room.BlockedWriteModeOff)
	}
	_, err = svc.ApproveAllProposal(context.Background(), mustUser(t, svc, "priya"), pid, hash, "")
	wantCode(t, err, "already_decided")

	svc.Close() // waits for every goroutine the service started
	if n := countEvents(t, st, roomID, events.ActionExecuted); n != 0 {
		t.Fatalf("%d action.executed with write mode off, want 0", n)
	}
	if pg.Calls()+wh.Calls() != 0 {
		t.Fatalf("actions ran with write mode off: pg %d, wh %d", pg.Calls(), wh.Calls())
	}
	if row, err := st.GetProposal(context.Background(), pid); err != nil || row.Status != store.ProposalStatusBlocked {
		t.Fatalf("proposal row = %+v (err %v), want status %s", row, err, store.ProposalStatusBlocked)
	}
}

// Owning one of the two layers is 403 not_layer_owner naming the other one,
// the first in required_layers order the caller lacks, and nothing is
// recorded, not even the layer the caller owns.
func TestApproveAllPartialOwnerRecordsNothing(t *testing.T) {
	svc, st, _, _ := newApproveAllService(t, false, "")
	roomID := createRoom(t, svc, "arjun").ID
	pid := writeModeProposal(t, svc, roomID)
	hash := proposalHash(t, svc, pid)

	for user, missing := range map[string]string{"meera": "postgres", "kabir": "warehouse", "arjun": "postgres"} {
		_, err := svc.ApproveAllProposal(context.Background(), mustUser(t, svc, user), pid, hash, "")
		wantCode(t, err, "not_layer_owner")
		if !strings.Contains(err.Error(), `layer "`+missing+`"`) {
			t.Errorf("%s: message %q does not name layer %s", user, err, missing)
		}
	}
	if got := approvalsGiven(t, st, roomID); len(got) != 0 {
		t.Fatalf("refused approve-all recorded %+v", got)
	}
	if got := svc.ApprovalsByGrant(); got[events.GrantOwner] != 0 {
		t.Fatalf("approvals by grant = %v after refusals, want none", got)
	}
}

// The solo user approves every layer with one confirm "<pipeline_id>/*" per
// call, grant solo; without it, or with a single-layer confirm, nothing is
// recorded. A stale hash is reported before the confirm, as for approve.
func TestApproveAllSoloConfirm(t *testing.T) {
	svc, st, _, _ := newApproveAllService(t, true, "arjun")
	roomID := createRoom(t, svc, "arjun").ID
	pid := writeModeProposal(t, svc, roomID)
	hash := proposalHash(t, svc, pid)
	arjun := mustUser(t, svc, "arjun")

	_, err := svc.ApproveAllProposal(context.Background(), arjun, pid, "stale", "")
	wantCode(t, err, "params_mismatch")
	for _, confirm := range []string{"", "orders/postgres", "orders/", "other/*"} {
		_, err := svc.ApproveAllProposal(context.Background(), arjun, pid, hash, confirm)
		wantCode(t, err, "validation")
		if !strings.Contains(err.Error(), `"orders/*"`) {
			t.Errorf("confirm %q: message %q does not say what to type", confirm, err)
		}
	}
	if got := approvalsGiven(t, st, roomID); len(got) != 0 {
		t.Fatalf("approve-all without the confirm recorded %+v", got)
	}

	evs, err := svc.ApproveAllProposal(context.Background(), arjun, pid, hash, "orders/*")
	if err != nil {
		t.Fatalf("solo approve-all: %v", err)
	}
	checkApprovals(t, evs, pid, hash, "arjun", events.GrantSolo, "solo", "postgres", "warehouse")
	if got := svc.ApprovalsByGrant(); got[events.GrantSolo] != 2 || got[events.GrantOwner] != 0 {
		t.Fatalf("approvals by grant = %v, want solo 2", got)
	}
}

// params_hash: missing is 400 validation, different is 409 params_mismatch;
// neither records anything.
func TestApproveAllParamsHash(t *testing.T) {
	svc, st, _, _ := newApproveAllService(t, false, "")
	roomID := createRoom(t, svc, "arjun").ID
	pid := writeModeProposal(t, svc, roomID)
	priya := mustUser(t, svc, "priya")

	_, err := svc.ApproveAllProposal(context.Background(), priya, pid, "", "")
	wantCode(t, err, "validation")
	_, err = svc.ApproveAllProposal(context.Background(), priya, pid, "sha256:not-this-one", "")
	wantCode(t, err, "params_mismatch")
	if got := approvalsGiven(t, st, roomID); len(got) != 0 {
		t.Fatalf("approve-all with a bad hash recorded %+v", got)
	}
}

// A layer another owner approved before the call is skipped: approve-all
// records the rest only, and the proposal runs once.
func TestApproveAllSkipsLayerAlreadyApproved(t *testing.T) {
	svc, st, pg, wh := newApproveAllService(t, false, "")
	roomID := createRoom(t, svc, "arjun").ID
	pid := writeModeProposal(t, svc, roomID)
	hash := proposalHash(t, svc, pid)
	approveLayer(t, svc, "meera", pid, "warehouse")

	evs, err := svc.ApproveAllProposal(context.Background(), mustUser(t, svc, "priya"), pid, hash, "")
	if err != nil {
		t.Fatalf("approve-all: %v", err)
	}
	checkApprovals(t, evs, pid, hash, "priya", events.GrantOwner, "user:priya", "postgres")
	got := approvalsGiven(t, st, roomID)
	if len(got) != 2 || got[0].Layer != "warehouse" || got[0].Identity.Grant != "user:meera" || got[0].Grant != events.GrantOwner || got[1].Layer != "postgres" {
		t.Fatalf("log approvals = %+v, want meera's warehouse (grant owner, the single approve path) then priya's postgres", got)
	}
	waitFor(t, 5*time.Second, func() bool { return countEvents(t, st, roomID, events.ActionExecuted) == 2 })
	if pg.Calls() != 1 || wh.Calls() != 1 {
		t.Fatalf("calls pg %d wh %d, want 1 each", pg.Calls(), wh.Calls())
	}
}

// A proposal with a rejected layer is decided: 409 already_decided, nothing
// recorded.
func TestApproveAllRejectedProposal(t *testing.T) {
	svc, st, _, _ := newApproveAllService(t, false, "")
	roomID := createRoom(t, svc, "arjun").ID
	pid := writeModeProposal(t, svc, roomID)
	hash := proposalHash(t, svc, pid)
	if _, err := svc.RejectProposal(context.Background(), mustUser(t, svc, "meera"), pid, "warehouse", "not now", hash); err != nil {
		t.Fatalf("reject: %v", err)
	}
	_, err := svc.ApproveAllProposal(context.Background(), mustUser(t, svc, "priya"), pid, hash, "")
	wantCode(t, err, "already_decided")
	if got := approvalsGiven(t, st, roomID); len(got) != 0 {
		t.Fatalf("approve-all of a rejected proposal recorded %+v", got)
	}
}

// A closed room takes no approval: 409 room_closed.
func TestApproveAllClosedRoom(t *testing.T) {
	svc, st, _, _ := newApproveAllService(t, false, "")
	roomID := createRoom(t, svc, "arjun").ID
	pid := writeModeProposal(t, svc, roomID)
	hash := proposalHash(t, svc, pid)
	if _, err := svc.CloseRoom(context.Background(), mustUser(t, svc, "arjun"), roomID, "fixed by hand"); err != nil {
		t.Fatalf("close: %v", err)
	}
	_, err := svc.ApproveAllProposal(context.Background(), mustUser(t, svc, "priya"), pid, hash, "")
	wantCode(t, err, "room_closed")
	if got := approvalsGiven(t, st, roomID); len(got) != 0 {
		t.Fatalf("approve-all in a closed room recorded %+v", got)
	}
}

// An unknown proposal is 404, as for approve.
func TestApproveAllUnknownProposal(t *testing.T) {
	svc, _, _, _ := newApproveAllService(t, false, "")
	_, err := svc.ApproveAllProposal(context.Background(), mustUser(t, svc, "priya"), "p_missing", "x", "")
	wantCode(t, err, "not_found")
}
