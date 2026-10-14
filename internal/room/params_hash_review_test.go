package room_test

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"piperoom/internal/actions"
	"piperoom/internal/events"
	"piperoom/internal/room"
	"piperoom/internal/store/memstore"
)

// Tests for the security review of PR 67: the executor keeps the approved hash,
// keys are case-sensitive, an unhashable old proposal can only be rejected.

// hookAction is a postgres action whose first run calls onFirst, so a test can
// edit the stored row while a proposal is part-way through its steps.
type hookAction struct {
	*fakeAction
	onFirst func()
	once    atomic.Bool
}

func (h *hookAction) Execute(ctx context.Context, p json.RawMessage) (string, error) {
	if h.once.CompareAndSwap(false, true) && h.onFirst != nil {
		h.onFirst()
	}
	return h.fakeAction.Execute(ctx, p)
}

func twoStepPayload() events.ProposalCreatedPayload {
	step := func(n string) events.ProposalStep {
		return events.ProposalStep{Action: "pg_act", Layer: "postgres", Params: json.RawMessage(`{"table":"public.orders","n":"` + n + `"}`), Description: "d" + n}
	}
	return events.ProposalCreatedPayload{Title: "two steps", Risk: "low", Rollback: "undo", Steps: []events.ProposalStep{step("1"), step("2")}}
}

// hookedService runs a two-step proposal whose first step triggers onFirst.
func hookedService(t *testing.T, onFirst func(w *wrapStore)) (*room.Service, *wrapStore, *hookAction, string, string, string) {
	t.Helper()
	w := &wrapStore{Store: memstore.New()}
	act := &hookAction{fakeAction: &fakeAction{name: "pg_act", layer: "postgres"}}
	act.onFirst = func() { onFirst(w) }
	reg := actions.NewRegistry()
	if err := reg.Register(act); err != nil {
		t.Fatal(err)
	}
	svc := room.New(room.Deps{Store: w, Actions: reg, Users: room.DefaultUsers(), Owners: testOwners(), Run: fakeRun, WriteHandoff: fakeWrite})
	t.Cleanup(svc.Close)
	ctx := context.Background()
	roomID := createRoom(t, svc, "arjun").ID
	pid, err := svc.CreateProposal(ctx, roomID, twoStepPayload(), events.AgentActor)
	if err != nil {
		t.Fatal(err)
	}
	hash := openProposal(t, svc, roomID, pid).ParamsHash
	return svc, w, act, roomID, pid, hash
}

func waitFailed(t *testing.T, w *wrapStore, pid string) {
	t.Helper()
	waitFor(t, 5*time.Second, func() bool {
		p, err := w.Store.GetProposal(context.Background(), pid)
		return err == nil && p.Status == "failed"
	})
}

// An editor who changes the steps AND the stored params_hash after the last
// approval gets the new hash past the row check. The executor also holds the
// hash that was approved, so step 1 is refused.
func TestExecutorRefusesStepsAndHashEditedTogether(t *testing.T) {
	svc, w, act, roomID, pid, hash := hookedService(t, func(w *wrapStore) { w.tamper.Store(true); w.rehash.Store(true) })
	if _, err := svc.ApproveProposal(context.Background(), mustUser(t, svc, "priya"), pid, "postgres", hash); err != nil {
		t.Fatal(err)
	}
	waitFailed(t, w, pid)
	if act.Calls() != 1 {
		t.Fatalf("the action ran %d times, want only step 0", act.Calls())
	}
	res := eventsOf[events.ActionExecutedPayload](t, svc, roomID, events.ActionExecuted)
	if len(res) != 2 || !res[0].OK || res[1].OK || res[1].Output != "params hash mismatch" || res[1].ParamsHash != hash {
		t.Fatalf("action.executed = %+v, want step 0 ok and step 1 refused with the approved hash %s", res, hash)
	}
}

// Steps edited and the stored hash cleared after the approval: refused. An
// empty stored hash is a mismatch, never a cue to compute one.
func TestExecutorTreatsAnEmptyRowHashAsAMismatch(t *testing.T) {
	svc, w, act, roomID, pid, hash := hookedService(t, func(w *wrapStore) { w.tamper.Store(true); w.clearHash.Store(true) })
	if _, err := svc.ApproveProposal(context.Background(), mustUser(t, svc, "priya"), pid, "postgres", hash); err != nil {
		t.Fatal(err)
	}
	waitFailed(t, w, pid)
	res := eventsOf[events.ActionExecutedPayload](t, svc, roomID, events.ActionExecuted)
	if act.Calls() != 1 || len(res) != 2 || res[1].OK || res[1].Output != "params hash mismatch" {
		t.Fatalf("calls %d, results %+v", act.Calls(), res)
	}
}

// The same edit made before the approval (steps and hash both changed in the
// row) cannot be approved: the approver's echo is the hash they saw.
func TestApproveRefusesARowEditedWithItsHash(t *testing.T) {
	svc, w, act, _, pid, hash := hookedService(t, func(*wrapStore) {})
	w.tamper.Store(true)
	w.rehash.Store(true)
	_, err := svc.ApproveProposal(context.Background(), mustUser(t, svc, "priya"), pid, "postgres", hash)
	wantRoomError(t, err, 409, "params_mismatch")
	if act.Calls() != 0 {
		t.Fatal("ran")
	}
}

// strictAction declares its parameters, so CreateProposal checks key names.
type strictAction struct{ *fakeAction }

func (strictAction) ParamsSchema() map[string]any {
	return map[string]any{
		"type": "object", "additionalProperties": false,
		"properties": map[string]any{
			"key":  map[string]any{"type": "string"},
			"n":    map[string]any{"type": "number"},
			"opts": map[string]any{"type": "object", "properties": map[string]any{"x": map[string]any{"type": "number"}}},
			"list": map[string]any{"type": "array", "items": map[string]any{"type": "object", "properties": map[string]any{"v": map[string]any{"type": "string"}}}},
		},
	}
}

// Go's json decoder ignores key case, so {"key":"shown","Key":"hidden"} would
// run a value the approver never saw. Keys must be exactly the schema's names.
func TestCreateProposalRefusesCaseFoldedAndUnknownKeys(t *testing.T) {
	svc, _, _ := newTestService(t, strictAction{&fakeAction{name: "pg_act", layer: "postgres"}})
	ctx := context.Background()
	roomID := createRoom(t, svc, "arjun").ID

	bad := map[string]string{
		"two keys differing in case":    `{"key":"shown","Key":"hidden"}`,
		"the Kelvin sign for k":         `{"key":"shown","Key":"hidden"}`,
		"wrong case alone":              `{"Key":"hidden"}`,
		"an unknown key":                `{"key":"a","extra":1}`,
		"an unknown nested key":         `{"key":"a","opts":{"y":1}}`,
		"case-folded duplicates nested": `{"key":"a","opts":{"x":1,"X":2}}`,
		"a wrong-case key in an array":  `{"list":[{"v":"a"},{"V":"b"}]}`,
		"upper and lower n":             `{"n":1,"N":2}`,
	}
	for name, params := range bad {
		_, err := svc.CreateProposal(ctx, roomID, pgPayload(params), events.AgentActor)
		if err == nil {
			t.Errorf("%s: proposal created", name)
			continue
		}
		wantRoomError(t, err, 400, "validation")
	}
	snap, _ := svc.GetRoom(ctx, roomID)
	if len(snap.OpenProposals) != 0 {
		t.Fatalf("%d proposals stored", len(snap.OpenProposals))
	}
	for _, params := range []string{`{"key":"a","n":1}`, `{"opts":{"x":2},"list":[{"v":"b"}]}`, `{}`} {
		if _, err := svc.CreateProposal(ctx, roomID, pgPayload(params), events.AgentActor); err != nil {
			t.Errorf("%s: %v", params, err)
		}
	}
}

// Without a property list the only rule is: no two keys equal under case
// folding, at any depth.
func TestCreateProposalRefusesFoldedDuplicatesAtAnyDepth(t *testing.T) {
	svc, _, _ := newTestService(t, &fakeAction{name: "pg_act", layer: "postgres"})
	ctx := context.Background()
	roomID := createRoom(t, svc, "arjun").ID
	for _, params := range []string{`{"a":{"b":[{"c":1,"C":2}]}}`, `{"A":1,"a":2}`, `{"K":1,"k":2}`, `{"ſ":1,"s":2}`} {
		_, err := svc.CreateProposal(ctx, roomID, pgPayload(params), events.AgentActor)
		if err == nil {
			t.Errorf("%s: proposal created", params)
			continue
		}
		wantRoomError(t, err, 400, "validation")
	}
	if _, err := svc.CreateProposal(ctx, roomID, pgPayload(`{"a":{"b":1},"c":{"b":2}}`), events.AgentActor); err != nil {
		t.Fatal(err)
	}
}

// A rejection with an empty params_hash is allowed only for a proposal that
// cannot be hashed, and is recorded with an empty hash. Approve always needs one.
func TestRejectWithEmptyHashOnlyForAnUnhashableProposal(t *testing.T) {
	pg := &fakeAction{name: "pg_act", layer: "postgres"}
	svc, w := wrappedService(t, pg)
	ctx := context.Background()
	roomID := createRoom(t, svc, "arjun").ID
	priya := mustUser(t, svc, "priya")

	// A normal proposal: an empty hash is 400 for approve and for reject.
	pid, err := svc.CreateProposal(ctx, roomID, pgPayload(`{"table":"public.orders"}`), events.AgentActor)
	if err != nil {
		t.Fatal(err)
	}
	_, err = svc.RejectProposal(ctx, priya, pid, "postgres", "no", "")
	wantRoomError(t, err, 400, "validation")
	_, err = svc.ApproveProposal(ctx, priya, pid, "postgres", "")
	wantRoomError(t, err, 400, "validation")

	// An old row that cannot be hashed: approve is refused, reject with "" works.
	w.legacy.Store(true)
	bad, err := svc.CreateProposal(ctx, roomID, pgPayload(`{"table":"public.orders","y":1}`), events.AgentActor)
	if err != nil {
		t.Fatal(err)
	}
	w.legacy.Store(false)
	w.bigNumber.Store(true)
	shown := openProposal(t, svc, roomID, bad).ParamsHash
	_, err = svc.ApproveProposal(ctx, priya, bad, "postgres", shown)
	wantRoomError(t, err, 400, "validation")
	_, err = svc.RejectProposal(ctx, priya, bad, "postgres", "no", shown)
	wantRoomError(t, err, 400, "validation") // a hash was sent but cannot match: only "" is accepted
	if _, err := svc.RejectProposal(ctx, priya, bad, "postgres", "unhashable", ""); err != nil {
		t.Fatalf("reject an unhashable proposal with an empty hash: %v", err)
	}
	rej := eventsOf[events.ApprovalRejectedPayload](t, svc, roomID, events.ApprovalRejected)
	if len(rej) != 1 || rej[0].ParamsHash != "" || rej[0].Identity == nil || rej[0].Identity.Method != "dev" {
		t.Fatalf("approval.rejected = %+v", rej)
	}
	apps, _ := w.Store.ListApprovals(ctx, bad)
	if len(apps) != 1 || apps[0].ParamsHash != "" || apps[0].Decision != "rejected" {
		t.Fatalf("approval row = %+v", apps)
	}
}

// If another writer set a different hash on an old row meanwhile, the store
// says so and the approver gets 409 params_mismatch.
func TestOldProposalHashConflictIsAMismatch(t *testing.T) {
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
	w.conflict.Store(true)
	_, err = svc.ApproveProposal(ctx, mustUser(t, svc, "priya"), pid, "postgres", openProposal(t, svc, roomID, pid).ParamsHash)
	wantRoomError(t, err, 409, "params_mismatch")
	if pg.Calls() != 0 {
		t.Fatal("ran")
	}
}
