package room

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"piperoom/internal/actions"
	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/store"
	"piperoom/internal/store/memstore"
)

// sweepWindowStore is a memstore that says it is shared (store.Coordinator)
// and records the unclaimed window of each stale-sweep query. Coordinator
// methods the test does not reach are left nil and panic if called.
type sweepWindowStore struct {
	*memstore.Store
	store.Coordinator
	mu        sync.Mutex
	unclaimed []time.Duration
}

func (s *sweepWindowStore) StaleProposals(_ context.Context, _, unclaimed time.Duration, _ int) ([]store.StaleProposal, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.unclaimed = append(s.unclaimed, unclaimed)
	return nil, nil
}

func (s *sweepWindowStore) ExpireRoomLeases(context.Context, string) error { return nil }

// With write mode off the stale sweep never lists a fully approved, unclaimed
// proposal: it is blocked, not abandoned by a dead replica. With write mode
// on the window is the configured one, as before.
func TestSweepUnclaimedWindowFollowsWriteMode(t *testing.T) {
	for _, tc := range []struct {
		off  bool
		want time.Duration
	}{{false, DefaultUnclaimedStale}, {true, writeOffUnclaimedStale}} {
		st := &sweepWindowStore{Store: memstore.New()}
		s := New(Deps{Store: st, WriteModeOff: tc.off})
		s.sweepStaleProposals()
		s.Close()
		if len(st.unclaimed) != 1 || st.unclaimed[0] != tc.want {
			t.Errorf("off=%v: unclaimed windows %v, want [%v]", tc.off, st.unclaimed, tc.want)
		}
	}
	// The window must stay far beyond any real wait, and fit the store's
	// millisecond parameter.
	if writeOffUnclaimedStale < 50*365*24*time.Hour || writeOffUnclaimedStale.Milliseconds() <= 0 {
		t.Fatalf("writeOffUnclaimedStale = %v", writeOffUnclaimedStale)
	}
}

// A proposal that reaches the executor some other way than the queue is
// refused before its claim: nothing is written.
func TestExecuteProposalRefusesWithWriteModeOff(t *testing.T) {
	st := memstore.New()
	s := New(Deps{Store: st, WriteModeOff: true})
	defer s.Close()
	err := s.executeProposal(store.ProposalRow{ProposalID: "p_x", RoomID: "r_x", Steps: []byte(`[{"action":"a","layer":"postgres"}]`)}, false)
	if !errors.Is(err, errWriteModeOff) {
		t.Fatalf("executeProposal = %v, want errWriteModeOff", err)
	}
	if got := st.ProposalStepStatus("p_x", 0); got != "" {
		t.Fatalf("step 0 status %q, want nothing recorded", got)
	}
	if s.WriteOn() {
		t.Fatal("WriteOn = true with WriteModeOff")
	}
}

// pgAct is a postgres-layer action that counts its runs.
type pgAct struct {
	mu   sync.Mutex
	runs int
}

func (*pgAct) Name() string                                      { return "pg_act" }
func (*pgAct) Layer() string                                     { return "postgres" }
func (*pgAct) Describe() string                                  { return "pg_act" }
func (*pgAct) ParamsSchema() map[string]any                      { return map[string]any{"type": "object"} }
func (*pgAct) Validate(context.Context, json.RawMessage) error   { return nil }
func (*pgAct) CheckScope(owners.Pipeline, json.RawMessage) error { return nil }
func (a *pgAct) Execute(context.Context, json.RawMessage) (string, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.runs++
	return "done", nil
}

// Shared store, off then on: a proposal fully approved with write mode off is
// marked blocked in its row; a replica with write mode on records it as
// approved while write mode was off (not as a server that stopped), and never
// runs it. A never-claimed proposal that was not blocked keeps the old
// message. The fake's stale list stands in for the stale SQL (run against
// Postgres in internal/store's integration test): its WriteBlocked comes from
// the row the off replica wrote.
func TestSharedStoreWriteOffProposalRecordedOnceWriteOn(t *testing.T) {
	ctx := context.Background()
	ls := newLeaseStore()
	act := &pgAct{}
	newSvc := func(replica string, off bool) *Service {
		reg := actions.NewRegistry()
		if err := reg.Register(act); err != nil {
			t.Fatal(err)
		}
		s := New(Deps{Store: ls, Actions: reg, Users: DefaultUsers(), Owners: tenantOwners(), ReplicaID: replica, WriteModeOff: off,
			Run: idleRun, ReplicaTimings: ReplicaTimings{LeaseTTL: 600 * time.Millisecond, LeaseRenew: 100 * time.Millisecond,
				TakeoverScan: 200 * time.Millisecond, ExecSweep: 200 * time.Millisecond}})
		t.Cleanup(s.Close)
		if err := s.Load(ctx); err != nil {
			t.Fatal(err)
		}
		return s
	}

	off := newSvc("replica-off", true)
	roomID := createRooms(t, off, 1)[0]
	pid, err := off.CreateProposal(ctx, roomID, events.ProposalCreatedPayload{
		Title: "fix", Risk: "low", Rollback: "revert the change",
		Steps: []events.ProposalStep{{Action: "pg_act", Layer: "postgres", Params: json.RawMessage("{}")}},
	}, events.AgentActor)
	if err != nil {
		t.Fatal(err)
	}
	snap, err := off.GetRoom(ctx, roomID)
	if err != nil {
		t.Fatal(err)
	}
	priya, _ := off.LookupUser("priya")
	if _, err := off.ApproveProposal(ctx, priya, pid, "postgres", snap.OpenProposals[0].ParamsHash); err != nil {
		t.Fatal(err)
	}
	row, err := ls.GetProposal(ctx, pid)
	if err != nil || row.Status != store.ProposalStatusBlocked || row.RoomID != roomID {
		t.Fatalf("row after an off approval = %+v (err %v), want status %s", row, err, store.ProposalStatusBlocked)
	}
	off.Close()

	ls.lmu.Lock()
	ls.rows["p_crash"] = store.ProposalRow{ProposalID: "p_crash", RoomID: roomID, Status: "open", Steps: []byte("[]")}
	ls.stale = []store.StaleProposal{
		{ProposalID: pid, RoomID: roomID, WriteBlocked: row.Status == store.ProposalStatusBlocked},
		{ProposalID: "p_crash", RoomID: roomID},
	}
	ls.lmu.Unlock()
	newSvc("replica-on", false)

	outputs := func() map[string]events.ActionExecutedPayload {
		out := map[string]events.ActionExecutedPayload{}
		evs, _ := ls.Store.Since(ctx, roomID, 0, 1000)
		for _, e := range evs {
			if p, ok := decode[events.ActionExecutedPayload](e); ok && e.Type == events.ActionExecuted {
				out[p.ProposalID] = p
			}
		}
		return out
	}
	pollUntil(t, func() bool { return len(outputs()) == 2 })
	got := outputs()
	if p := got[pid]; p.OK || p.OutcomeUnknown || p.Output != writeOffNotRunOutput {
		t.Fatalf("approved-while-off outcome = %+v, want failed with %q", p, writeOffNotRunOutput)
	}
	if p := got["p_crash"]; p.OK || p.Output == writeOffNotRunOutput {
		t.Fatalf("never-claimed outcome = %+v, want the stopped-server message", p)
	}
	act.mu.Lock()
	defer act.mu.Unlock()
	if act.runs != 0 {
		t.Fatalf("an approval given with write mode off ran %d times after the switch", act.runs)
	}
}
