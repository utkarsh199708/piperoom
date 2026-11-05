package room

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"piperoom/internal/actions"
	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/store"
	"piperoom/internal/store/memstore"
	"piperoom/internal/tools"
)

// Unowned-layer fallback (P2b-3): fallback_layers on the proposal, the
// driver's grant "fallback", re-checked at append.

// whAct is a warehouse-layer action that counts its runs.
type whAct struct {
	mu   sync.Mutex
	runs int
}

func (*whAct) Name() string                                      { return "wh_act" }
func (*whAct) Layer() string                                     { return "warehouse" }
func (*whAct) Describe() string                                  { return "wh_act" }
func (*whAct) ParamsSchema() map[string]any                      { return map[string]any{"type": "object"} }
func (*whAct) Validate(context.Context, json.RawMessage) error   { return nil }
func (*whAct) CheckScope(owners.Pipeline, json.RawMessage) error { return nil }
func (a *whAct) Execute(context.Context, json.RawMessage) (string, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.runs++
	return "done", nil
}

func (a *whAct) count() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.runs
}

func (a *pgAct) count() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.runs
}

func fbUser(name string) owners.Principal { return owners.Principal{Kind: owners.KindUser, Name: name} }

// fallbackOwners names priya on postgres and nobody on warehouse.
func fallbackOwners() *owners.Owners {
	return &owners.Owners{
		Pipelines: []owners.Pipeline{{ID: "orders", SourceTables: []string{"public.orders"}}},
		Layers:    map[string]owners.Principals{"postgres": {fbUser("priya")}},
	}
}

// fallbackUsers: nobody claims warehouse in users.yaml either, so it has no
// approver at all; meera is a member with no layer.
func fallbackUsers(t *testing.T) *Users {
	t.Helper()
	u, err := ParseUsers([]byte(`users:
  - id: arjun
    name: Arjun
    layers: [kafka]
  - id: priya
    name: Priya
    layers: [postgres]
  - id: meera
    name: Meera
`))
	if err != nil {
		t.Fatal(err)
	}
	return u
}

type fallbackRig struct {
	t   *testing.T
	svc *Service
	ms  store.Store
	pg  *pgAct
	wh  *whAct
}

func newFallbackRig(t *testing.T, st store.Store, own *owners.Owners) *fallbackRig {
	t.Helper()
	r := &fallbackRig{t: t, ms: st, pg: &pgAct{}, wh: &whAct{}}
	reg := actions.NewRegistry()
	for _, a := range []actions.Action{r.pg, r.wh} {
		if err := reg.Register(a); err != nil {
			t.Fatal(err)
		}
	}
	r.svc = New(Deps{Store: st, Actions: reg, Users: fallbackUsers(t), Owners: own, Run: idleRun,
		ViewerGroups: []string{"viewers"}})
	t.Cleanup(r.svc.Close)
	if err := r.svc.Load(context.Background()); err != nil {
		t.Fatal(err)
	}
	return r
}

func (r *fallbackRig) user(id string) User {
	r.t.Helper()
	u, ok := r.svc.LookupUser(id)
	if !ok {
		r.t.Fatalf("no user %s", id)
	}
	return u
}

// room opens a room driven by arjun and proposes a postgres + warehouse fix.
func (r *fallbackRig) room() (roomID, pid, hash string) {
	r.t.Helper()
	ctx := context.Background()
	snap, err := r.svc.CreateRoom(ctx, r.user("arjun"), "Incident", "sev2", "orders")
	if err != nil {
		r.t.Fatal(err)
	}
	pid, err = r.svc.CreateProposal(ctx, snap.ID, events.ProposalCreatedPayload{
		Title: "fix", Risk: "low", Rollback: "revert the change",
		Steps: []events.ProposalStep{
			{Action: "pg_act", Layer: "postgres", Params: json.RawMessage("{}")},
			{Action: "wh_act", Layer: "warehouse", Params: json.RawMessage("{}")},
		},
	}, events.AgentActor)
	if err != nil {
		r.t.Fatal(err)
	}
	p := r.proposal(snap.ID, pid)
	return snap.ID, pid, p.ParamsHash
}

func (r *fallbackRig) proposal(roomID, pid string) Proposal {
	r.t.Helper()
	snap, err := r.svc.GetRoom(context.Background(), roomID)
	if err != nil {
		r.t.Fatal(err)
	}
	for _, p := range snap.OpenProposals {
		if p.ProposalID == pid {
			return p
		}
	}
	r.t.Fatalf("proposal %s not open", pid)
	return Proposal{}
}

func (r *fallbackRig) approvals(roomID string) []events.ApprovalGivenPayload {
	r.t.Helper()
	evs, err := r.ms.Since(context.Background(), roomID, 0, 1000)
	if err != nil {
		r.t.Fatal(err)
	}
	var out []events.ApprovalGivenPayload
	for _, e := range evs {
		if e.Type == events.ApprovalGiven {
			p, err := events.Decode[events.ApprovalGivenPayload](e)
			if err != nil {
				r.t.Fatal(err)
			}
			out = append(out, p)
		}
	}
	return out
}

func wantRoomCode(t *testing.T, err error, code string) {
	t.Helper()
	var re *Error
	if !errors.As(err, &re) || re.Code != code {
		t.Fatalf("err = %v, want room error %q", err, code)
	}
}

// fallbackLayersFor lists only the layers nobody may approve, and fails
// closed without an owners config or for an unknown pipeline.
func TestFallbackLayersFor(t *testing.T) {
	required := []string{"postgres", "warehouse", "consumer"}
	pipeOver := func(o *owners.Owners, layer string, r owners.LayerRights) *owners.Owners {
		o.Pipelines[0].Owners = map[string]owners.LayerRights{layer: r}
		return o
	}
	cases := []struct {
		name     string
		own      *owners.Owners
		users    *Users
		pipeline string
		want     []string
	}{
		{"no owners config: no fallback", nil, nil, "orders", []string{}},
		{"unknown pipeline: no fallback", fallbackOwners(), nil, "nope", []string{}},
		{"empty pipeline id: no fallback", fallbackOwners(), nil, "", []string{}},
		{"layers: entry owns postgres", fallbackOwners(), nil, "orders", []string{"warehouse", "consumer"}},
		{"pipeline owners: approver owns warehouse", pipeOver(fallbackOwners(), "warehouse",
			owners.LayerRights{Approvers: owners.Principals{fbUser("meera")}}), nil, "orders", []string{"consumer"}},
		{"pipeline owners: backup counts", pipeOver(fallbackOwners(), "warehouse",
			owners.LayerRights{Backup: owners.Principals{fbUser("meera")}}), nil, "orders", []string{"consumer"}},
		{"pipeline owners: entry with nobody replaces layers:", pipeOver(fallbackOwners(), "postgres",
			owners.LayerRights{}), nil, "orders", []string{"postgres", "warehouse", "consumer"}},
		{"users.yaml layers claim owns the layer", fallbackOwners(), DefaultUsers(), "orders", []string{}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := &Service{owners: c.own, users: c.users}
			got := s.fallbackLayersFor(c.pipeline, required)
			if got == nil || !slices.Equal(got, c.want) {
				t.Fatalf("fallbackLayersFor = %#v, want %#v", got, c.want)
			}
		})
	}
}

// The proposal carries the unowned layer in proposal.created and the
// snapshot; a proposal whose every layer is owned carries [] in the snapshot
// and nothing in the event.
func TestFallbackLayersOnProposal(t *testing.T) {
	r := newFallbackRig(t, memstore.New(), fallbackOwners())
	roomID, pid, _ := r.room()
	if got := r.proposal(roomID, pid).FallbackLayers; !slices.Equal(got, []string{"warehouse"}) {
		t.Fatalf("snapshot fallback_layers = %v, want [warehouse]", got)
	}
	evs, err := r.ms.Since(context.Background(), roomID, 0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range evs {
		if e.Type == events.ProposalCreated {
			p, _ := events.Decode[events.ProposalCreatedPayload](e)
			if !slices.Equal(p.FallbackLayers, []string{"warehouse"}) {
				t.Fatalf("proposal.created fallback_layers = %v, want [warehouse]", p.FallbackLayers)
			}
		}
	}

	own := fallbackOwners()
	own.Layers["warehouse"] = owners.Principals{fbUser("meera")}
	r2 := newFallbackRig(t, memstore.New(), own)
	roomID2, pid2, _ := r2.room()
	p := r2.proposal(roomID2, pid2)
	b, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	if p.FallbackLayers == nil || len(p.FallbackLayers) != 0 || !strings.Contains(string(b), `"fallback_layers":[]`) {
		t.Fatalf("owned proposal: fallback_layers %#v, json %s; want []", p.FallbackLayers, b)
	}
	evs, _ = r2.ms.Since(context.Background(), roomID2, 0, 1000)
	for _, e := range evs {
		if e.Type == events.ProposalCreated && strings.Contains(string(e.Payload), "fallback_layers") {
			t.Fatalf("owned proposal.created carries fallback_layers: %s", e.Payload)
		}
	}
}

// approve: the driver approves the fallback layer with grant "fallback"; the
// owner of another layer, a member with no layer and a viewer are refused on
// it; the driver still cannot approve an owned layer.
func TestFallbackApprove(t *testing.T) {
	ctx := context.Background()
	r := newFallbackRig(t, memstore.New(), fallbackOwners())
	roomID, pid, hash := r.room()

	viewer := User{ID: "vic", Name: "Vic", Groups: []string{"viewers"}}
	for _, u := range []User{r.user("priya"), r.user("meera"), viewer} {
		_, err := r.svc.ApproveProposal(ctx, u, pid, "warehouse", hash)
		if err == nil {
			t.Fatalf("%s approved the fallback layer without being the driver", u.ID)
		}
		var re *Error
		if !errors.As(err, &re) || (re.Code != codeNotLayerOwner && re.Status != 404) {
			t.Fatalf("%s: err = %v, want not_layer_owner", u.ID, err)
		}
	}
	_, err := r.svc.ApproveProposal(ctx, r.user("arjun"), pid, "postgres", hash)
	wantRoomCode(t, err, codeNotLayerOwner)
	if got := r.approvals(roomID); len(got) != 0 {
		t.Fatalf("refusals recorded %+v", got)
	}

	ev, err := r.svc.ApproveProposal(ctx, r.user("arjun"), pid, "warehouse", hash)
	if err != nil {
		t.Fatalf("driver approves fallback layer: %v", err)
	}
	p, _ := events.Decode[events.ApprovalGivenPayload](ev)
	if p.Grant != events.GrantFallback || p.Identity == nil || p.Identity.Grant != events.GrantFallback || ev.Actor.ID != "arjun" {
		t.Fatalf("approval = %+v by %s (identity %+v), want grant fallback by arjun", p, ev.Actor.ID, p.Identity)
	}
	if got := r.svc.ApprovalsByGrant(); got[events.GrantFallback] != 1 {
		t.Fatalf("approvals by grant = %v, want fallback 1", got)
	}
	// The owned layer still needs its owner; then the steps run.
	if _, err := r.svc.ApproveProposal(ctx, r.user("priya"), pid, "postgres", hash); err != nil {
		t.Fatal(err)
	}
	waitForCond(t, 5*time.Second, func() bool { return r.pg.count() == 1 && r.wh.count() == 1 }, "both steps run")
}

// A driver change before the approval: the old driver is refused, the new
// one may approve.
func TestFallbackDriverChanged(t *testing.T) {
	ctx := context.Background()
	r := newFallbackRig(t, memstore.New(), fallbackOwners())
	roomID, pid, hash := r.room()
	if _, err := r.svc.GrantControl(ctx, r.user("arjun"), roomID, "meera"); err != nil {
		t.Fatal(err)
	}
	_, err := r.svc.ApproveProposal(ctx, r.user("arjun"), pid, "warehouse", hash)
	wantRoomCode(t, err, codeNotLayerOwner)
	ev, err := r.svc.ApproveProposal(ctx, r.user("meera"), pid, "warehouse", hash)
	if err != nil {
		t.Fatalf("new driver: %v", err)
	}
	if p, _ := events.Decode[events.ApprovalGivenPayload](ev); p.Grant != events.GrantFallback {
		t.Fatalf("grant = %q, want fallback", p.Grant)
	}
}

// approve-all: the fallback layer counts for the driver only. A non-driver
// owner of the other layer gets 403 naming the fallback layer and nothing is
// recorded; the driver without the owned layer gets 403 naming it; a driver
// who owns the other layer records owner + fallback and the steps run.
func TestFallbackApproveAll(t *testing.T) {
	ctx := context.Background()
	r := newFallbackRig(t, memstore.New(), fallbackOwners())
	roomID, pid, hash := r.room()

	_, err := r.svc.ApproveAllProposal(ctx, r.user("priya"), pid, hash, "")
	wantRoomCode(t, err, codeNotLayerOwner)
	if !strings.Contains(err.Error(), `layer "warehouse"`) {
		t.Fatalf("message %q does not name warehouse", err)
	}
	_, err = r.svc.ApproveAllProposal(ctx, r.user("arjun"), pid, hash, "")
	wantRoomCode(t, err, codeNotLayerOwner)
	if !strings.Contains(err.Error(), `layer "postgres"`) {
		t.Fatalf("message %q does not name postgres", err)
	}
	if got := r.approvals(roomID); len(got) != 0 {
		t.Fatalf("refused approve-all recorded %+v", got)
	}

	if _, err := r.svc.GrantControl(ctx, r.user("arjun"), roomID, "priya"); err != nil {
		t.Fatal(err)
	}
	evs, err := r.svc.ApproveAllProposal(ctx, r.user("priya"), pid, hash, "")
	if err != nil {
		t.Fatalf("driver-owner approve-all: %v", err)
	}
	want := []struct{ layer, grant, ident string }{
		{"postgres", events.GrantOwner, "user:priya"},
		{"warehouse", events.GrantFallback, events.GrantFallback},
	}
	if len(evs) != len(want) {
		t.Fatalf("got %d events, want %d", len(evs), len(want))
	}
	for i, e := range evs {
		p, _ := events.Decode[events.ApprovalGivenPayload](e)
		if p.Layer != want[i].layer || p.Grant != want[i].grant || p.Identity == nil || p.Identity.Grant != want[i].ident {
			t.Fatalf("event %d = %+v (identity %+v), want %+v", i, p, p.Identity, want[i])
		}
	}
	waitForCond(t, 5*time.Second, func() bool { return r.pg.count() == 1 && r.wh.count() == 1 }, "both steps run")
}

// approvalStoreHook runs onApproval after the approvals row is written and
// before approval.given is appended: the window between the request's check
// and the append.
type approvalStoreHook struct {
	*leaseStore
	onApproval func(store.ApprovalRow)
}

func (h *approvalStoreHook) RecordApproval(ctx context.Context, a store.ApprovalRow) error {
	if err := h.leaseStore.RecordApproval(ctx, a); err != nil {
		return err
	}
	if h.onApproval != nil {
		h.onApproval(a)
	}
	return nil
}

// Shared store: another replica logs a driver change after this one checked
// the old driver and before its append. The conditional append re-checks
// against the new driver and refuses; nothing is recorded and the approvals
// row is removed again, so the new driver can approve.
func TestFallbackRecheckedAtAppend(t *testing.T) {
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
	_, err := r.svc.ApproveProposal(ctx, r.user("arjun"), pid, "warehouse", hash)
	wantRoomCode(t, err, codeNotLayerOwner)
	if got := r.approvals(roomID); len(got) != 0 {
		t.Fatalf("old driver's approval recorded: %+v", got)
	}
	if got := r.svc.ApprovalsByGrant(); got[events.GrantFallback] != 0 {
		t.Fatalf("approvals by grant = %v, want no fallback", got)
	}
	if _, err := r.svc.ApproveProposal(ctx, meera, pid, "warehouse", hash); err != nil {
		t.Fatalf("new driver after the refused append: %v", err)
	}
}

// checkFallbackLocked refuses a viewer even when the snapshot names them the
// driver, and a layer that is not in the proposal's fallback_layers.
func TestCheckFallbackLocked(t *testing.T) {
	s := New(Deps{Store: memstore.New(), Users: fallbackUsers(t), Owners: fallbackOwners(), ViewerGroups: []string{"viewers"}})
	defer s.Close()
	st := &roomState{snap: NewSnapshot()}
	st.snap.PipelineID = "orders"
	st.snap.OpenProposals = []Proposal{{ProposalID: "p_1", RequiredLayers: []string{"postgres", "warehouse"}, FallbackLayers: []string{"warehouse"}}}
	viewer := User{ID: "vic", Groups: []string{"viewers"}}
	cases := []struct {
		name   string
		driver string
		user   User
		layer  string
		ok     bool
	}{
		{"driver on fallback layer", "meera", User{ID: "meera"}, "warehouse", true},
		{"viewer as driver", "vic", viewer, "warehouse", false},
		{"driver on owned layer", "meera", User{ID: "meera"}, "postgres", false},
		{"driver on a layer not required", "meera", User{ID: "meera"}, "consumer", false},
		{"no driver", "", User{ID: "meera"}, "warehouse", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			st.snap.Driver = nil
			if c.driver != "" {
				st.snap.Driver = &Driver{ID: c.driver}
			}
			err := s.checkFallbackLocked(st, "p_1", c.user, c.layer)
			if (err == nil) != c.ok {
				t.Fatalf("err = %v, want ok %v", err, c.ok)
			}
			if err != nil {
				wantRoomCode(t, err, codeNotLayerOwner)
			}
		})
	}
	if got := approvalGrant(events.GrantFallback); got != events.GrantFallback {
		t.Fatalf("approvalGrant(fallback) = %q", got)
	}
}

// scopeAct records the pipeline scope its Validate sees.
type scopeAct struct {
	mu   sync.Mutex
	seen owners.Pipeline
	ok   bool
}

func (*scopeAct) Name() string                                      { return "scope_act" }
func (*scopeAct) Layer() string                                     { return "postgres" }
func (*scopeAct) Describe() string                                  { return "scope_act" }
func (*scopeAct) ParamsSchema() map[string]any                      { return map[string]any{"type": "object"} }
func (*scopeAct) CheckScope(owners.Pipeline, json.RawMessage) error { return nil }
func (*scopeAct) Execute(context.Context, json.RawMessage) (string, error) {
	return "done", nil
}
func (a *scopeAct) Validate(ctx context.Context, _ json.RawMessage) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.seen, a.ok = tools.PipelineFrom(ctx)
	return nil
}

// Validate at creation runs under the room's whole pipeline, as the executor
// does: the owners.yaml entry, not only its id.
func TestCreateProposalValidateSeesPipeline(t *testing.T) {
	ctx := context.Background()
	act := &scopeAct{}
	reg := actions.NewRegistry()
	if err := reg.Register(act); err != nil {
		t.Fatal(err)
	}
	svc := New(Deps{Store: memstore.New(), Actions: reg, Users: DefaultUsers(), Owners: fallbackOwners(), Run: idleRun})
	defer svc.Close()
	arjun, _ := svc.LookupUser("arjun")
	snap, err := svc.CreateRoom(ctx, arjun, "Incident", "sev2", "orders")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.CreateProposal(ctx, snap.ID, events.ProposalCreatedPayload{
		Title: "fix", Risk: "low", Rollback: "revert the change",
		Steps: []events.ProposalStep{{Action: "scope_act", Layer: "postgres", Params: json.RawMessage("{}")}},
	}, events.AgentActor); err != nil {
		t.Fatal(err)
	}
	act.mu.Lock()
	defer act.mu.Unlock()
	if !act.ok || act.seen.ID != "orders" || !slices.Equal(act.seen.SourceTables, []string{"public.orders"}) {
		t.Fatalf("Validate saw pipeline %+v (ok %v), want the orders entry", act.seen, act.ok)
	}
}
