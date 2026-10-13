package room_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gowebpki/jcs"

	"piperoom/internal/actions"
	"piperoom/internal/events"
	"piperoom/internal/room"
	"piperoom/internal/store"
	"piperoom/internal/store/memstore"
)

// params_hash (docs/design/auth.md section 3): computed at creation over the
// section 3.1 object, echoed on approve and reject, re-checked by the executor.

// independentHash rebuilds the section 3.1 object from its parts and hashes it
// with jcs directly, so the test does not trust room.ComputeParamsHash.
func independentHash(t *testing.T, roomID, proposalID, title, risk, rollback string, layers []string, steps []events.ProposalStep) string {
	t.Helper()
	raw, err := json.Marshal(map[string]any{
		"v": 2, "room_id": roomID, "proposal_id": proposalID, "title": title, "risk": risk,
		"rollback": rollback, "required_layers": layers, "steps": steps,
	})
	if err != nil {
		t.Fatal(err)
	}
	canon, err := jcs.Transform(raw)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(canon)
	return hex.EncodeToString(sum[:])
}

func pgPayload(params string) events.ProposalCreatedPayload {
	return events.ProposalCreatedPayload{
		Title: "widen column", Risk: "low", Rollback: "narrow it again",
		Steps: []events.ProposalStep{{Action: "pg_act", Layer: "postgres", Params: json.RawMessage(params), Description: "alter"}},
	}
}

func wantRoomError(t *testing.T, err error, status int, code string) {
	t.Helper()
	var re *room.Error
	if !errors.As(err, &re) || re.Status != status || re.Code != code {
		t.Fatalf("err = %v, want %d %s", err, status, code)
	}
}

func openProposal(t *testing.T, svc *room.Service, roomID, pid string) room.Proposal {
	t.Helper()
	snap, err := svc.GetRoom(context.Background(), roomID)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range snap.OpenProposals {
		if p.ProposalID == pid {
			return p
		}
	}
	t.Fatalf("proposal %s is not open", pid)
	return room.Proposal{}
}

func eventsOf[T any](t *testing.T, svc *room.Service, roomID string, typ events.Type) []T {
	t.Helper()
	evs, _, err := svc.Events(context.Background(), roomID, 0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	var out []T
	for _, ev := range evs {
		if ev.Type == typ {
			p, err := events.Decode[T](ev)
			if err != nil {
				t.Fatal(err)
			}
			out = append(out, p)
		}
	}
	return out
}

func TestCreateProposalComputesTheHash(t *testing.T) {
	pg := &fakeAction{name: "pg_act", layer: "postgres"}
	svc, _, _ := newTestService(t, pg)
	ctx := context.Background()
	roomID := createRoom(t, svc, "arjun").ID

	// A hash the caller supplies is overwritten.
	in := pgPayload(`{"table":"public.orders","n":1.50,"b":true}`)
	in.ParamsHash = "deadbeef"
	pid, err := svc.CreateProposal(ctx, roomID, in, events.AgentActor)
	if err != nil {
		t.Fatal(err)
	}
	p := openProposal(t, svc, roomID, pid)
	want := independentHash(t, roomID, pid, in.Title, in.Risk, in.Rollback, []string{"postgres"}, in.Steps)
	if p.ParamsHash != want {
		t.Fatalf("snapshot params_hash = %q, want %q (a caller value must be ignored)", p.ParamsHash, want)
	}
	created := eventsOf[events.ProposalCreatedPayload](t, svc, roomID, events.ProposalCreated)
	if len(created) != 1 || created[0].ParamsHash != want {
		t.Fatalf("proposal.created params_hash = %+v, want %s", created, want)
	}

	// Key order and whitespace in params do not change the hash (JCS), the
	// proposal id does.
	same := pgPayload(`{ "b": true, "n": 1.5, "table": "public.orders" }`)
	pid2, err := svc.CreateProposal(ctx, roomID, same, events.AgentActor)
	if err != nil {
		t.Fatal(err)
	}
	if got, wantSame := openProposal(t, svc, roomID, pid2).ParamsHash, independentHash(t, roomID, pid2, same.Title, same.Risk, same.Rollback, []string{"postgres"}, same.Steps); got != wantSame {
		t.Fatalf("hash %s != %s", got, wantSame)
	}
	if openProposal(t, svc, roomID, pid2).ParamsHash == p.ParamsHash {
		t.Fatal("two proposals share a hash although their ids differ")
	}
}

// The number rule (design 3.1): a number a double cannot carry exactly is a 400
// before anything is stored.
func TestCreateProposalNumberRule(t *testing.T) {
	pg := &fakeAction{name: "pg_act", layer: "postgres"}
	svc, st, _ := newTestService(t, pg)
	ctx := context.Background()
	roomID := createRoom(t, svc, "arjun").ID

	bad := map[string]string{
		"2^53+1":                `{"offset":9007199254740993}`,
		"negative 2^53+1":       `{"offset":-9007199254740993}`,
		"nested in an array":    `{"xs":[1,{"y":9007199254740993}]}`,
		"twenty digits":         `{"n":12345678901234567890}`,
		"more than 17 digits":   `{"n":0.12345678901234567890123}`,
		"outside the double":    `{"n":1e400}`,
		"duplicate object keys": `{"a":1,"a":2}`,
	}
	for name, params := range bad {
		_, err := svc.CreateProposal(ctx, roomID, pgPayload(params), events.AgentActor)
		if err == nil {
			t.Errorf("%s: want 400, proposal created", name)
			continue
		}
		wantRoomError(t, err, 400, "validation")
	}
	snap, _ := svc.GetRoom(ctx, roomID)
	if len(snap.OpenProposals) != 0 {
		t.Fatalf("refused proposals left %d open proposals", len(snap.OpenProposals))
	}
	if evs := eventsOf[events.ProposalCreatedPayload](t, svc, roomID, events.ProposalCreated); len(evs) != 0 {
		t.Fatalf("refused proposals left %d proposal.created events", len(evs))
	}
	_ = st

	// 2^53 itself, and a value quoted as a string, are fine.
	for _, params := range []string{`{"offset":9007199254740992}`, `{"offset":"9007199254740993"}`, `{"n":0.1,"m":-1.5e-7,"k":1e21}`} {
		if _, err := svc.CreateProposal(ctx, roomID, pgPayload(params), events.AgentActor); err != nil {
			t.Errorf("params %s: %v", params, err)
		}
	}
}

func approvalSetup(t *testing.T) (*room.Service, string, string, *fakeAction) {
	t.Helper()
	pg := &fakeAction{name: "pg_act", layer: "postgres"}
	svc, _, _ := newTestService(t, pg)
	roomID := createRoom(t, svc, "arjun").ID
	pid, err := svc.CreateProposal(context.Background(), roomID, pgPayload(`{"table":"public.orders"}`), events.AgentActor)
	if err != nil {
		t.Fatal(err)
	}
	return svc, roomID, pid, pg
}

func TestApproveAndRejectNeedTheRightHash(t *testing.T) {
	svc, roomID, pid, pg := approvalSetup(t)
	ctx := context.Background()
	priya := mustUser(t, svc, "priya")
	hash := openProposal(t, svc, roomID, pid).ParamsHash

	_, err := svc.ApproveProposal(ctx, priya, pid, "postgres", "")
	wantRoomError(t, err, 400, "validation")
	_, err = svc.RejectProposal(ctx, priya, pid, "postgres", "no", "")
	wantRoomError(t, err, 400, "validation")
	wrong := strings.Repeat("a", 64)
	_, err = svc.ApproveProposal(ctx, priya, pid, "postgres", wrong)
	wantRoomError(t, err, 409, "params_mismatch")
	_, err = svc.RejectProposal(ctx, priya, pid, "postgres", "no", wrong)
	wantRoomError(t, err, 409, "params_mismatch")
	if n := len(eventsOf[events.ApprovalGivenPayload](t, svc, roomID, events.ApprovalGiven)); n != 0 {
		t.Fatalf("%d approvals recorded by refused calls", n)
	}
	// The refused calls did not decide the layer.
	if _, err := svc.ApproveProposal(ctx, priya, pid, "postgres", hash); err != nil {
		t.Fatalf("approve with the right hash: %v", err)
	}
	waitFor(t, 5*time.Second, func() bool { return pg.Calls() == 1 })
}

// The identity on approval events: an OIDC session's facts are copied, the
// grant is added, a user with no session is "dev" with no session id.
func TestApprovalEventsCarryIdentity(t *testing.T) {
	svc, roomID, pid, _ := approvalSetup(t)
	ctx := context.Background()
	hash := openProposal(t, svc, roomID, pid).ParamsHash

	oidcUser := mustUser(t, svc, "priya")
	local := time.FixedZone("x", 5*3600+1800)
	oidcUser.Identity = events.Identity{
		Iss: "https://idp.example", Sub: "sub-priya", SessionID: "ses_01ABC", Method: "oidc",
		AuthTime: time.Date(2026, 10, 13, 15, 30, 0, 0, local),
	}
	if _, err := svc.ApproveProposal(ctx, oidcUser, pid, "postgres", hash); err != nil {
		t.Fatal(err)
	}
	given := eventsOf[events.ApprovalGivenPayload](t, svc, roomID, events.ApprovalGiven)
	if len(given) != 1 || given[0].ParamsHash != hash || given[0].Identity == nil {
		t.Fatalf("approval.given = %+v", given)
	}
	id := given[0].Identity
	if id.Iss != "https://idp.example" || id.Sub != "sub-priya" || id.SessionID != "ses_01ABC" || id.Method != "oidc" || id.Grant != "user:priya" {
		t.Fatalf("identity = %+v", id)
	}
	if want := time.Date(2026, 10, 13, 10, 0, 0, 0, time.UTC); !id.AuthTime.Equal(want) || id.AuthTime.Location() != time.UTC {
		t.Fatalf("auth_time = %v, want %v in UTC", id.AuthTime, want)
	}

	// A rejection of another proposal by a user with no session.
	pid2, err := svc.CreateProposal(ctx, roomID, pgPayload(`{"table":"public.orders","x":2}`), events.AgentActor)
	if err != nil {
		t.Fatal(err)
	}
	h2 := openProposal(t, svc, roomID, pid2).ParamsHash
	if _, err := svc.RejectProposal(ctx, mustUser(t, svc, "priya"), pid2, "postgres", "not now", h2); err != nil {
		t.Fatal(err)
	}
	rej := eventsOf[events.ApprovalRejectedPayload](t, svc, roomID, events.ApprovalRejected)
	if len(rej) != 1 || rej[0].ParamsHash != h2 || rej[0].Identity == nil {
		t.Fatalf("approval.rejected = %+v", rej)
	}
	if r := rej[0].Identity; r.Method != "dev" || r.SessionID != "" || r.Grant != "user:priya" || r.Iss != "" || !r.AuthTime.IsZero() {
		t.Fatalf("no-session identity = %+v, want method dev, no session id", r)
	}
}

// wrapStore lets a test edit or hide what the store holds, the way a database
// edit or an old row would.
type wrapStore struct {
	*memstore.Store
	tamper    atomic.Bool // GetProposal returns steps with "orders" changed to "customers"
	rehash    atomic.Bool // ... and a params_hash recomputed for the edited steps (an editor who also fixes the hash)
	clearHash atomic.Bool // ... and an empty params_hash
	bigNumber atomic.Bool // GetProposal returns steps holding a number above 2^53 (an unhashable row)
	conflict  atomic.Bool // SetProposalParamsHash answers a conflict (another writer set a different hash)
	legacy    atomic.Bool // new proposals are stored with no hash, as before auth PR 4
}

func (w *wrapStore) GetProposal(ctx context.Context, id string) (store.ProposalRow, error) {
	p, err := w.Store.GetProposal(ctx, id)
	if err != nil {
		return p, err
	}
	if w.tamper.Load() {
		p.Steps = bytes.ReplaceAll(p.Steps, []byte("orders"), []byte("customers"))
	}
	if w.bigNumber.Load() {
		p.Steps = bytes.ReplaceAll(p.Steps, []byte(`"public.orders"`), []byte(`9007199254740993`))
	}
	if w.rehash.Load() {
		var steps []events.ProposalStep
		if err := json.Unmarshal(p.Steps, &steps); err != nil {
			return p, err
		}
		h, err := room.ComputeParamsHash(p.RoomID, p.ProposalID, p.Title, p.Risk, p.Rollback, p.RequiredLayers, steps)
		if err != nil {
			return p, err
		}
		p.ParamsHash = h
	}
	if w.clearHash.Load() {
		p.ParamsHash = ""
	}
	return p, nil
}

func (w *wrapStore) SetProposalParamsHash(ctx context.Context, id, hash string) error {
	if w.conflict.Load() {
		return store.ErrParamsHashConflict
	}
	return w.Store.SetProposalParamsHash(ctx, id, hash)
}

func (w *wrapStore) CreateProposal(ctx context.Context, p store.ProposalRow) error {
	if w.legacy.Load() {
		p.ParamsHash = ""
	}
	return w.Store.CreateProposal(ctx, p)
}

func (w *wrapStore) Append(ctx context.Context, roomID string, typ events.Type, actor events.Actor, payload any) (events.Event, error) {
	if pl, ok := payload.(events.ProposalCreatedPayload); ok && w.legacy.Load() {
		pl.ParamsHash = ""
		payload = pl
	}
	return w.Store.Append(ctx, roomID, typ, actor, payload)
}

func wrappedService(t *testing.T, act actions.Action) (*room.Service, *wrapStore) {
	t.Helper()
	w := &wrapStore{Store: memstore.New()}
	reg := actions.NewRegistry()
	if err := reg.Register(act); err != nil {
		t.Fatal(err)
	}
	svc := room.New(room.Deps{
		Store: w, Actions: reg, Users: room.DefaultUsers(), Owners: testOwners(), Run: fakeRun, WriteHandoff: fakeWrite,
	})
	t.Cleanup(svc.Close)
	return svc, w
}

// Design 3.2: the executor recomputes the hash from the stored row before each
// Execute. A row edited after approval is refused: the step fails with
// "params hash mismatch", the action never runs, the proposal is failed.
func TestExecutorRefusesAnEditedRow(t *testing.T) {
	pg := &fakeAction{name: "pg_act", layer: "postgres"}
	svc, w := wrappedService(t, pg)
	ctx := context.Background()
	roomID := createRoom(t, svc, "arjun").ID
	pid, err := svc.CreateProposal(ctx, roomID, pgPayload(`{"table":"public.orders"}`), events.AgentActor)
	if err != nil {
		t.Fatal(err)
	}
	hash := openProposal(t, svc, roomID, pid).ParamsHash

	w.tamper.Store(true) // the row changes after creation, before the last approval
	if _, err := svc.ApproveProposal(ctx, mustUser(t, svc, "priya"), pid, "postgres", hash); err != nil {
		t.Fatalf("approve: %v", err)
	}
	waitFor(t, 5*time.Second, func() bool {
		p, err := w.Store.GetProposal(ctx, pid)
		return err == nil && p.Status == "failed"
	})
	if pg.Calls() != 0 {
		t.Fatalf("the action ran %d times on an edited row", pg.Calls())
	}
	res := eventsOf[events.ActionExecutedPayload](t, svc, roomID, events.ActionExecuted)
	if len(res) != 1 || res[0].OK || res[0].Output != "params hash mismatch" || res[0].ParamsHash != hash {
		t.Fatalf("action.executed = %+v, want a failed step with output %q and params_hash %s", res, "params hash mismatch", hash)
	}
}

// The unedited path: action.executed carries the hash.
func TestActionExecutedCarriesTheHash(t *testing.T) {
	svc, roomID, pid, pg := approvalSetup(t)
	ctx := context.Background()
	hash := openProposal(t, svc, roomID, pid).ParamsHash
	if _, err := svc.ApproveProposal(ctx, mustUser(t, svc, "priya"), pid, "postgres", hash); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 5*time.Second, func() bool {
		return len(eventsOf[events.ActionExecutedPayload](t, svc, roomID, events.ActionExecuted)) == 1
	})
	res := eventsOf[events.ActionExecutedPayload](t, svc, roomID, events.ActionExecuted)
	if !res[0].OK || res[0].ParamsHash != hash || pg.Calls() != 1 {
		t.Fatalf("action.executed = %+v calls=%d", res, pg.Calls())
	}
}

// Old data: a proposal created before auth PR 4 has no hash in its row or its
// event. It still shows a hash in the snapshot, it can be approved with that
// hash, the hash is then stored, and it runs.
func TestOldProposalWithoutHashCanBeApproved(t *testing.T) {
	pg := &fakeAction{name: "pg_act", layer: "postgres"}
	svc, w := wrappedService(t, pg)
	ctx := context.Background()
	roomID := createRoom(t, svc, "arjun").ID
	w.legacy.Store(true)
	pid, err := svc.CreateProposal(ctx, roomID, pgPayload(`{"table":"public.orders"}`), events.AgentActor)
	if err != nil {
		t.Fatal(err)
	}
	w.legacy.Store(false)
	if row, _ := w.Store.GetProposal(ctx, pid); row.ParamsHash != "" {
		t.Fatalf("test setup: the row has hash %q, want none", row.ParamsHash)
	}
	if created := eventsOf[events.ProposalCreatedPayload](t, svc, roomID, events.ProposalCreated); created[0].ParamsHash != "" {
		t.Fatalf("test setup: the event has hash %q, want none", created[0].ParamsHash)
	}

	// The snapshot (and a restart that rebuilds it from the log) shows the value
	// creation would have stored.
	p := openProposal(t, svc, roomID, pid)
	want := independentHash(t, roomID, pid, p.Title, p.Risk, p.Rollback, p.RequiredLayers, p.Steps)
	if p.ParamsHash != want {
		t.Fatalf("snapshot hash of an old proposal = %q, want %q", p.ParamsHash, want)
	}

	// A wrong hash is still a mismatch; the right one approves, stores the hash, and the step runs.
	_, err = svc.ApproveProposal(ctx, mustUser(t, svc, "priya"), pid, "postgres", strings.Repeat("b", 64))
	wantRoomError(t, err, 409, "params_mismatch")
	if _, err := svc.ApproveProposal(ctx, mustUser(t, svc, "priya"), pid, "postgres", p.ParamsHash); err != nil {
		t.Fatalf("approve an old proposal: %v", err)
	}
	waitFor(t, 5*time.Second, func() bool { return pg.Calls() == 1 })
	row, _ := w.Store.GetProposal(ctx, pid)
	if row.ParamsHash != want {
		t.Fatalf("stored hash = %q, want %q", row.ParamsHash, want)
	}
	given := eventsOf[events.ApprovalGivenPayload](t, svc, roomID, events.ApprovalGiven)
	if len(given) != 1 || given[0].ParamsHash != want {
		t.Fatalf("approval.given = %+v", given)
	}
	apps, err := w.Store.ListApprovals(ctx, pid)
	if err != nil || len(apps) != 1 || apps[0].ParamsHash != want || apps[0].IdentityMethod != "dev" || apps[0].IdentityGrant != "user:priya" {
		t.Fatalf("approvals row = %+v, %v", apps, err)
	}
}

// Export access (design 3.4) at the service: approvers and backups of any layer
// and auditors; not the driver, a viewer or a plain member.
func TestCheckExportAccess(t *testing.T) {
	svc, _, _ := newTestService(t)
	ctx := context.Background()
	roomID := createRoom(t, svc, "arjun").ID
	// Dev mode: users.yaml layers decide. Every default user owns a layer.
	for _, id := range []string{"arjun", "priya", "meera", "kabir"} {
		if err := svc.CheckExportAccess(ctx, mustUser(t, svc, id), roomID); err != nil {
			t.Errorf("%s: %v", id, err)
		}
	}
	// A person with no layer is refused, even as the room's driver.
	nobody := room.User{ID: "nobody", Name: "Nobody", Layers: []string{}}
	wantRoomError(t, svc.CheckExportAccess(ctx, nobody, roomID), 403, "not_allowed")
	if _, err := svc.GetRoom(ctx, roomID); err != nil {
		t.Fatal(err)
	}
}
