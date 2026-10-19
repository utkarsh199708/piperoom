package room

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"piperoom/internal/actions"
	"piperoom/internal/events"
	"piperoom/internal/store"
	"piperoom/internal/store/memstore"
)

// Reject on a fallback layer (P2b-3b, ruling on #609): whoever may approve a
// layer may reject it, so the room's driver rejects a fallback_layers entry
// with grant "fallback", re-checked when approval.rejected is appended.

func (r *fallbackRig) rejections(roomID string) []events.ApprovalRejectedPayload {
	r.t.Helper()
	evs, err := r.ms.Since(context.Background(), roomID, 0, 1000)
	if err != nil {
		r.t.Fatal(err)
	}
	var out []events.ApprovalRejectedPayload
	for _, e := range evs {
		if e.Type == events.ApprovalRejected {
			p, err := events.Decode[events.ApprovalRejectedPayload](e)
			if err != nil {
				r.t.Fatal(err)
			}
			out = append(out, p)
		}
	}
	return out
}

func (r *fallbackRig) status(pid string) string {
	r.t.Helper()
	p, err := r.ms.GetProposal(context.Background(), pid)
	if err != nil {
		r.t.Fatal(err)
	}
	return p.Status
}

// The driver rejects the fallback layer with grant "fallback" and the
// proposal is rejected; the owner of another layer, a member with no layer
// and a viewer are refused on it; the driver still cannot reject an owned
// layer.
func TestFallbackReject(t *testing.T) {
	ctx := context.Background()
	r := newFallbackRig(t, memstore.New(), fallbackOwners())
	roomID, pid, hash := r.room()

	viewer := User{ID: "vic", Name: "Vic", Groups: []string{"viewers"}}
	for _, u := range []User{r.user("priya"), r.user("meera"), viewer} {
		_, err := r.svc.RejectProposal(ctx, u, pid, "warehouse", "no", hash)
		if err == nil {
			t.Fatalf("%s rejected the fallback layer without being the driver", u.ID)
		}
		var re *Error
		if !errors.As(err, &re) || (re.Code != codeNotLayerOwner && re.Status != 404) {
			t.Fatalf("%s: err = %v, want not_layer_owner", u.ID, err)
		}
	}
	_, err := r.svc.RejectProposal(ctx, r.user("arjun"), pid, "postgres", "no", hash)
	wantRoomCode(t, err, codeNotLayerOwner)
	if got := r.rejections(roomID); len(got) != 0 {
		t.Fatalf("refusals recorded %+v", got)
	}
	if s := r.status(pid); s != "open" {
		t.Fatalf("status after refusals = %q, want open", s)
	}

	ev, err := r.svc.RejectProposal(ctx, r.user("arjun"), pid, "warehouse", "not now", hash)
	if err != nil {
		t.Fatalf("driver rejects fallback layer: %v", err)
	}
	p, _ := events.Decode[events.ApprovalRejectedPayload](ev)
	if ev.Type != events.ApprovalRejected || p.Layer != "warehouse" || p.Reason != "not now" ||
		p.Identity == nil || p.Identity.Grant != events.GrantFallback || ev.Actor.ID != "arjun" {
		t.Fatalf("rejection = %+v by %s (identity %+v), want warehouse grant fallback by arjun", p, ev.Actor.ID, p.Identity)
	}
	if s := r.status(pid); s != "rejected" {
		t.Fatalf("status = %q, want rejected", s)
	}
	snap, err := r.svc.GetRoom(ctx, roomID)
	if err != nil {
		t.Fatal(err)
	}
	for _, op := range snap.OpenProposals {
		if op.ProposalID == pid {
			t.Fatalf("rejected proposal still open in the snapshot: %+v", op)
		}
	}
	// Decided: the owner can no longer approve her layer.
	_, err = r.svc.ApproveProposal(ctx, r.user("priya"), pid, "postgres", hash)
	wantRoomCode(t, err, codeAlreadyDecided)
	if r.pg.count() != 0 || r.wh.count() != 0 {
		t.Fatal("a rejected proposal ran")
	}
}

// A driver change before the rejection: the old driver is refused, the new
// one may reject.
func TestFallbackRejectDriverChanged(t *testing.T) {
	ctx := context.Background()
	r := newFallbackRig(t, memstore.New(), fallbackOwners())
	roomID, pid, hash := r.room()
	if _, err := r.svc.GrantControl(ctx, r.user("arjun"), roomID, "meera"); err != nil {
		t.Fatal(err)
	}
	_, err := r.svc.RejectProposal(ctx, r.user("arjun"), pid, "warehouse", "no", hash)
	wantRoomCode(t, err, codeNotLayerOwner)
	ev, err := r.svc.RejectProposal(ctx, r.user("meera"), pid, "warehouse", "no", hash)
	if err != nil {
		t.Fatalf("new driver: %v", err)
	}
	if p, _ := events.Decode[events.ApprovalRejectedPayload](ev); p.Identity == nil || p.Identity.Grant != events.GrantFallback {
		t.Fatalf("identity = %+v, want grant fallback", p.Identity)
	}
}

// Shared store: another replica logs a driver change after this one checked
// the old driver and before its append. The conditional append re-checks
// against the new driver and refuses; nothing is logged, the approvals row is
// removed again and the proposal stays open, so the new driver can reject.
func TestFallbackRejectRecheckedAtAppend(t *testing.T) {
	ctx := context.Background()
	hs := &approvalStoreHook{leaseStore: newLeaseStore()}
	r := newFallbackRig(t, hs, fallbackOwners())
	if r.svc.coord == nil {
		t.Fatal("hook store not seen as a coordinator")
	}
	roomID, pid, hash := r.room()
	meera := r.user("meera")
	var once sync.Once
	hs.onApproval = func(a store.ApprovalRow) {
		once.Do(func() {
			if _, err := hs.leaseStore.Append(ctx, roomID, events.ControlGranted, r.svc.humanActor(r.user("arjun")), events.ControlGrantedPayload{
				To: r.svc.humanActor(meera), LeaseUntil: time.Now().Add(DriverLease),
			}); err != nil {
				t.Error(err)
			}
		})
	}
	_, err := r.svc.RejectProposal(ctx, r.user("arjun"), pid, "warehouse", "no", hash)
	wantRoomCode(t, err, codeNotLayerOwner)
	if got := r.rejections(roomID); len(got) != 0 {
		t.Fatalf("old driver's rejection logged: %+v", got)
	}
	if s := r.status(pid); s != "open" {
		t.Fatalf("status after the refused append = %q, want open", s)
	}
	ev, err := r.svc.RejectProposal(ctx, meera, pid, "warehouse", "no", hash)
	if err != nil {
		t.Fatalf("new driver after the refused append: %v", err)
	}
	if ev.Actor.ID != "meera" {
		t.Fatalf("actor = %s, want meera", ev.Actor.ID)
	}
	if s := r.status(pid); s != "rejected" {
		t.Fatalf("status = %q, want rejected", s)
	}
}

// An owner who is not the driver still rejects her own layer, with her owner
// grant, as before the fallback.
func TestFallbackRejectOwnerUnchanged(t *testing.T) {
	ctx := context.Background()
	r := newFallbackRig(t, memstore.New(), fallbackOwners())
	_, pid, hash := r.room()
	ev, err := r.svc.RejectProposal(ctx, r.user("priya"), pid, "postgres", "no", hash)
	if err != nil {
		t.Fatalf("owner rejects her layer: %v", err)
	}
	if p, _ := events.Decode[events.ApprovalRejectedPayload](ev); p.Identity == nil || p.Identity.Grant != "user:priya" {
		t.Fatalf("identity = %+v, want grant user:priya", p.Identity)
	}
	if s := r.status(pid); s != "rejected" {
		t.Fatalf("status = %q, want rejected", s)
	}
}

// The solo user rejects the fallback layer as before, with grant "solo",
// without being the driver.
func TestFallbackRejectSoloUnchanged(t *testing.T) {
	ctx := context.Background()
	st := memstore.New()
	r := &fallbackRig{t: t, ms: st, pg: &pgAct{}, wh: &whAct{}}
	reg := actions.NewRegistry()
	for _, a := range []actions.Action{r.pg, r.wh} {
		if err := reg.Register(a); err != nil {
			t.Fatal(err)
		}
	}
	r.svc = New(Deps{Store: st, Actions: reg, Users: fallbackUsers(t), Owners: fallbackOwners(), Run: idleRun,
		ViewerGroups: []string{"viewers"}, SoloUser: "meera"})
	t.Cleanup(r.svc.Close)
	if err := r.svc.Load(ctx); err != nil {
		t.Fatal(err)
	}
	_, pid, hash := r.room()
	ev, err := r.svc.RejectProposal(ctx, r.user("meera"), pid, "warehouse", "no", hash)
	if err != nil {
		t.Fatalf("solo user rejects: %v", err)
	}
	if p, _ := events.Decode[events.ApprovalRejectedPayload](ev); p.Identity == nil || p.Identity.Grant != events.GrantSolo {
		t.Fatalf("identity = %+v, want grant solo", p.Identity)
	}
}
