package room_test

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"piperoom/internal/actions"
	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/room"
	"piperoom/internal/store"
	"piperoom/internal/store/memstore"
)

// Security audit 2026-10 (reviews/security-audit-2026-10.md): attacks on the
// approval chain and on users.yaml. A passing test is a defended attack.

// mutStore applies mutate to every proposal row it returns, the way a
// scripted or partial database edit would.
type mutStore struct {
	*memstore.Store
	mu     sync.Mutex
	mutate func(*store.ProposalRow)
}

func (m *mutStore) set(f func(*store.ProposalRow)) {
	m.mu.Lock()
	m.mutate = f
	m.mu.Unlock()
}

func (m *mutStore) GetProposal(ctx context.Context, id string) (store.ProposalRow, error) {
	p, err := m.Store.GetProposal(ctx, id)
	if err != nil {
		return p, err
	}
	m.mu.Lock()
	f := m.mutate
	m.mu.Unlock()
	if f != nil {
		f(&p)
	}
	return p, nil
}

func mutService(t *testing.T, acts ...actions.Action) (*room.Service, *mutStore) {
	t.Helper()
	m := &mutStore{Store: memstore.New()}
	reg := actions.NewRegistry()
	for _, a := range acts {
		if err := reg.Register(a); err != nil {
			t.Fatal(err)
		}
	}
	svc := room.New(room.Deps{Store: m, Actions: reg, Users: room.DefaultUsers(), Owners: testOwners(), Run: fakeRun, WriteHandoff: fakeWrite})
	t.Cleanup(svc.Close)
	return svc, m
}

// rehash recomputes the stored params_hash for the (edited) row: an editor who
// knows the public formula.
func rehash(t *testing.T, p *store.ProposalRow) {
	var steps []events.ProposalStep
	if err := json.Unmarshal(p.Steps, &steps); err != nil {
		t.Error(err)
		return
	}
	h, err := room.ComputeParamsHash(p.RoomID, p.ProposalID, p.Title, p.Risk, p.Rollback, p.RequiredLayers, steps)
	if err != nil {
		t.Error(err)
		return
	}
	p.ParamsHash = h
}

func waitStatus(t *testing.T, m *mutStore, pid, status string) {
	t.Helper()
	waitFor(t, 5*time.Second, func() bool {
		p, err := m.Store.GetProposal(context.Background(), pid)
		return err == nil && p.Status == status
	})
}

// A-PH-1 (fixed). Two layers must approve. priya approves the row as created
// (hash H1). Then the steps and the stored hash are edited together (H2), and
// a second approver who knows the formula echoes H2. Before the fix the
// executor compared only the row and the last approval, so the edited steps
// ran with priya's approval of H1 on record. Now every approval must carry the
// hash that runs.
func TestAttack_ParamsHashEditBetweenTwoApprovals(t *testing.T) {
	pg := &fakeAction{name: "pg_act", layer: "postgres"}
	kf := &fakeAction{name: "kf_act", layer: "kafka"}
	svc, m := mutService(t, pg, kf)
	ctx := context.Background()
	roomID := createRoom(t, svc, "arjun").ID
	pid, err := svc.CreateProposal(ctx, roomID, events.ProposalCreatedPayload{
		Title: "two layers", Risk: "low", Rollback: "undo",
		Steps: []events.ProposalStep{
			{Action: "pg_act", Layer: "postgres", Params: json.RawMessage(`{"table":"public.orders"}`), Description: "pg"},
			{Action: "kf_act", Layer: "kafka", Params: json.RawMessage(`{"topic":"pg.public.orders"}`), Description: "kf"},
		},
	}, events.AgentActor)
	if err != nil {
		t.Fatal(err)
	}
	h1 := openProposal(t, svc, roomID, pid).ParamsHash
	if _, err := svc.ApproveProposal(ctx, mustUser(t, svc, "priya"), pid, "postgres", h1); err != nil {
		t.Fatal(err)
	}

	m.set(func(p *store.ProposalRow) {
		p.Steps = []byte(strings.ReplaceAll(string(p.Steps), "orders", "customers"))
		rehash(t, p)
	})
	edited, err := m.GetProposal(ctx, pid)
	if err != nil {
		t.Fatal(err)
	}
	h2 := edited.ParamsHash
	if h2 == h1 {
		t.Fatal("test setup: the edit did not change the hash")
	}
	if _, err := svc.ApproveProposal(ctx, mustUser(t, svc, "arjun"), pid, "kafka", h2); err != nil {
		t.Fatalf("second approval: %v", err)
	}
	waitStatus(t, m, pid, "failed")
	if pg.Calls()+kf.Calls() != 0 {
		t.Fatalf("edited steps ran: pg %d, kafka %d", pg.Calls(), kf.Calls())
	}
	res := eventsOf[events.ActionExecutedPayload](t, svc, roomID, events.ActionExecuted)
	if len(res) != 1 || res[0].OK || res[0].Output != "params hash mismatch" {
		t.Fatalf("action.executed = %+v", res)
	}
}

// A-PH-2. Every part of the hashed object edited in the row after the last
// approval's echo was checked (the stored hash left as it was, so the approve
// check passes): the executor recomputes and refuses. Nothing runs.
func TestAttack_ParamsHashBypassBetweenApproveAndExecute(t *testing.T) {
	edits := map[string]func(*store.ProposalRow){
		"params": func(p *store.ProposalRow) {
			p.Steps = []byte(strings.ReplaceAll(string(p.Steps), "orders", "customers"))
		},
		"description": func(p *store.ProposalRow) {
			p.Steps = []byte(strings.ReplaceAll(string(p.Steps), `"alter"`, `"harmless"`))
		},
		"title":           func(p *store.ProposalRow) { p.Title += " " },
		"risk":            func(p *store.ProposalRow) { p.Risk = "none" },
		"rollback":        func(p *store.ProposalRow) { p.Rollback = "" },
		"required layers": func(p *store.ProposalRow) { p.RequiredLayers = []string{} },
		"room id":         func(p *store.ProposalRow) { p.RoomID = "r_other" },
		"appended step": func(p *store.ProposalRow) {
			s := strings.TrimSuffix(string(p.Steps), "]")
			p.Steps = []byte(s + `,{"action":"pg_act","layer":"postgres","params":{"table":"public.orders"},"description":"extra"}]`)
		},
		"key case in params": func(p *store.ProposalRow) {
			p.Steps = []byte(strings.Replace(string(p.Steps), `"table"`, `"Table"`, 1))
		},
		"steps not json": func(p *store.ProposalRow) { p.Steps = []byte(`{`) },
		"steps null":     func(p *store.ProposalRow) { p.Steps = []byte(`null`) },
	}
	for name, edit := range edits {
		t.Run(name, func(t *testing.T) {
			pg := &fakeAction{name: "pg_act", layer: "postgres"}
			svc, m := mutService(t, pg)
			ctx := context.Background()
			roomID := createRoom(t, svc, "arjun").ID
			pid, err := svc.CreateProposal(ctx, roomID, pgPayload(`{"table":"public.orders"}`), events.AgentActor)
			if err != nil {
				t.Fatal(err)
			}
			hash := openProposal(t, svc, roomID, pid).ParamsHash
			m.set(edit)
			if _, err := svc.ApproveProposal(ctx, mustUser(t, svc, "priya"), pid, "postgres", hash); err != nil {
				// Refusing at approval time is also a defence.
				return
			}
			waitStatus(t, m, pid, "failed")
			if pg.Calls() != 0 {
				t.Fatalf("the action ran %d times on an edited row", pg.Calls())
			}
		})
	}
}

// A-AP-2. Approval forgery at the service: a user who owns no layer of the
// proposal, a user approving a layer the proposal does not need, an agent
// that sets required_layers or params_hash itself, a closed room.
func TestAttack_ApprovalForgeryAtService(t *testing.T) {
	pg := &fakeAction{name: "pg_act", layer: "postgres"}
	svc, _, _ := newTestService(t, pg)
	ctx := context.Background()
	roomID := createRoom(t, svc, "arjun").ID

	forged := pgPayload(`{"table":"public.orders"}`)
	forged.RequiredLayers = []string{"warehouse"} // the agent names an easier approver
	forged.ParamsHash = strings.Repeat("0", 64)   // and its own hash
	pid, err := svc.CreateProposal(ctx, roomID, forged, events.AgentActor)
	if err != nil {
		t.Fatal(err)
	}
	p := openProposal(t, svc, roomID, pid)
	if len(p.RequiredLayers) != 1 || p.RequiredLayers[0] != "postgres" {
		t.Fatalf("required layers = %v: the agent chose them", p.RequiredLayers)
	}
	if p.ParamsHash == forged.ParamsHash {
		t.Fatal("the agent chose the hash")
	}

	// meera owns warehouse, which this proposal does not need.
	_, err = svc.ApproveProposal(ctx, mustUser(t, svc, "meera"), pid, "warehouse", p.ParamsHash)
	wantRoomError(t, err, 400, "validation")
	// arjun owns no layer of it.
	_, err = svc.ApproveProposal(ctx, mustUser(t, svc, "arjun"), pid, "postgres", p.ParamsHash)
	wantRoomError(t, err, 403, "not_layer_owner")

	if _, err := svc.CloseRoom(ctx, mustUser(t, svc, "arjun"), roomID, "done"); err != nil {
		t.Fatal(err)
	}
	_, err = svc.ApproveProposal(ctx, mustUser(t, svc, "priya"), pid, "postgres", p.ParamsHash)
	if err == nil {
		t.Fatal("approved in a closed room")
	}
	if pg.Calls() != 0 {
		t.Fatal("ran")
	}
}

// A-USR-1 and A-USR-2 (fixed), and the users.yaml edge cases that were already
// refused. A repeated id let BySub/ByEmail and Get disagree; an unknown key
// such as `subject:` silently dropped a sub pin.
func TestAttack_UsersYAMLEdgeCases(t *testing.T) {
	bad := map[string]string{
		"repeated id (pin bypass)": `
users:
  - {id: priya, email: priya@example.com}
  - {id: priya, sub: real-priya, layers: [postgres]}`,
		"id differing only in case":    "users:\n  - {id: priya}\n  - {id: Priya}",
		"id equal under Unicode folds": "users:\n  - {id: kabir}\n  - {id: \"\u212aabir\"}",
		"unknown key subject":          "users:\n  - {id: priya, subject: real-priya}",
		"unknown key emial":            "users:\n  - {id: priya, emial: priya@example.com}",
		"duplicate yaml key":           "users:\n  - id: priya\n    sub: a\n    sub: b",
		"duplicate yaml id key":        "users:\n  - id: priya\n    id: admin",
		"empty id":                     "users:\n  - {id: \"\"}",
		"id with a colon":              "users:\n  - {id: \"group:x\"}",
		"id with a space":              "users:\n  - {id: \"pri ya\"}",
		"id with a tab":                "users:\n  - {id: \"priya\\t\"}",
		"id with a no-break space":     "users:\n  - {id: \"priya\u00a0\"}",
		"derived id prefix":            "users:\n  - {id: u_0123456789ab}",
		"email with a space":           "users:\n  - {id: a, email: \"a@example.com \"}",
		"non-ascii email":              "users:\n  - {id: a, email: \"\u0430@example.com\"}",
		"email without @":              "users:\n  - {id: a, email: example.com}",
		"same email, other case":       "users:\n  - {id: a, email: A@example.com}\n  - {id: b, email: a@EXAMPLE.com}",
		"same sub":                     "users:\n  - {id: a, sub: s}\n  - {id: b, sub: s}",
	}
	for name, doc := range bad {
		if _, err := room.ParseUsers([]byte(doc)); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}

	u, err := room.ParseUsers([]byte(`
users:
  - {id: priya, email: priya@example.com}
  - {id: kabir, sub: sub-kabir}
`))
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range []string{"", " priya@example.com", "priya@example.com ", "pr\u0456ya@example.com", "priya@example.com\u200b", "PRIYA@EXAMPLE.CO"} {
		if got, ok := u.ByEmail(e); ok {
			t.Errorf("ByEmail(%q) = %s", e, got.ID)
		}
	}
	if got, ok := u.ByEmail("PRIYA@example.COM"); !ok || got.ID != "priya" {
		t.Error("ASCII case-insensitive email match broke")
	}
	for _, s := range []string{"", "sub-kabir ", "SUB-KABIR"} {
		if got, ok := u.BySub(s); ok {
			t.Errorf("BySub(%q) = %s", s, got.ID)
		}
	}
	for _, id := range []string{"Priya", "priya ", "pr\u0456ya", ""} {
		if _, ok := u.Get(id); ok {
			t.Errorf("Get(%q) found a user", id)
		}
	}
	// An empty file is no users, not an error.
	if _, err := room.ParseUsers(nil); err != nil {
		t.Errorf("empty file: %v", err)
	}
}

// The shipped users files still load with the stricter parser.
func TestAttack_ShippedUsersFilesStillLoad(t *testing.T) {
	for _, p := range []string{"../../dev/users.yaml", "../../deploy/config/users.example.yaml"} {
		if _, err := room.LoadUsers(p); err != nil {
			t.Errorf("%s: %v", p, err)
		}
	}
}

// A-AP-3 (fixed). A user who lists a layer in users.yaml must be a user:
// principal of it in owners.yaml even when the layer also lists groups: the
// server cannot vouch for group membership at start-up. ValidateUsers refuses
// to start and names the user and the layer; and should such a config reach
// the service anyway, the users.yaml layer grants nothing (design section 2.3,
// revision 5; the approve path is covered over HTTP in
// httpapi TestLifecycle_UsersLayerOverGroupLayer).
func TestAttack_UsersYAMLLayerOverGroupLayer(t *testing.T) {
	users, err := room.ParseUsers([]byte("users:\n  - {id: kabir, layers: [consumer]}\n  - {id: mallory, layers: [consumer]}\n"))
	if err != nil {
		t.Fatal(err)
	}
	own := &owners.Owners{Layers: map[string]owners.Principals{
		"consumer": {{Kind: owners.KindUser, Name: "kabir"}, {Kind: owners.KindGroup, Name: "consumers"}},
	}}
	err = room.ValidateUsers(users, own)
	if err == nil || !strings.Contains(err.Error(), `"mallory"`) || !strings.Contains(err.Error(), `"consumer"`) {
		t.Fatalf("ValidateUsers = %v, want an error naming mallory and consumer", err)
	}
	// The layer the user really owns still validates.
	okUsers, _ := room.ParseUsers([]byte("users:\n  - {id: kabir, layers: [consumer]}\n"))
	if err := room.ValidateUsers(okUsers, own); err != nil {
		t.Fatalf("kabir alone: %v", err)
	}
	svc := room.New(room.Deps{Store: memstore.New(), Users: users, Owners: own, Run: fakeRun, WriteHandoff: fakeWrite})
	t.Cleanup(svc.Close)
	mallory, _ := svc.LookupUser("mallory")
	if got := svc.LayersFor(mallory); len(got) != 0 {
		t.Fatalf("mallory may approve %v without the start check, want nothing", got)
	}
	kabir, _ := svc.LookupUser("kabir")
	if got := svc.LayersFor(kabir); len(got) != 1 || got[0] != "consumer" {
		t.Fatalf("kabir may approve %v, want [consumer]", got)
	}
}
