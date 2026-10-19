package room

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"piperoom/internal/actions"
	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/store"
	"piperoom/internal/store/memstore"
)

// gateAct is a postgres action that records, at every run, whether every
// required layer of the proposal had approved it in the store at that moment.
type gateAct struct {
	st         store.Store
	pid        atomic.Value // string
	runs       atomic.Int32
	unapproved atomic.Int32
}

func (*gateAct) Name() string                                      { return "pg_gate" }
func (*gateAct) Layer() string                                     { return "postgres" }
func (*gateAct) Describe() string                                  { return "pg_gate" }
func (*gateAct) ParamsSchema() map[string]any                      { return map[string]any{"type": "object"} }
func (*gateAct) Validate(context.Context, json.RawMessage) error   { return nil }
func (*gateAct) CheckScope(owners.Pipeline, json.RawMessage) error { return nil }
func (a *gateAct) Execute(ctx context.Context, _ json.RawMessage) (string, error) {
	a.runs.Add(1)
	pid, _ := a.pid.Load().(string)
	p, err := a.st.GetProposal(ctx, pid)
	if err != nil {
		a.unapproved.Add(1)
		return "done", nil
	}
	apps, err := a.st.ListApprovals(ctx, pid)
	// Partial or rejected decisions count as unapproved too.
	if err != nil || !layersApproved(p.RequiredLayers, apps) {
		a.unapproved.Add(1)
	}
	return "done", nil
}

type gateRig struct {
	svc *Service
	ms  *memstore.Store
	act *gateAct
	pid string
	row store.ProposalRow
}

func newGateRig(t *testing.T) *gateRig {
	t.Helper()
	ms := memstore.New()
	act := &gateAct{st: ms}
	reg := actions.NewRegistry()
	if err := reg.Register(act); err != nil {
		t.Fatal(err)
	}
	svc := New(Deps{Store: ms, Actions: reg, Users: fallbackUsers(t), Owners: fallbackOwners(), Run: idleRun})
	t.Cleanup(svc.Close)
	ctx := context.Background()
	if err := svc.Load(ctx); err != nil {
		t.Fatal(err)
	}
	arjun, _ := svc.LookupUser("arjun")
	snap, err := svc.CreateRoom(ctx, arjun, "Incident", "sev2", "orders")
	if err != nil {
		t.Fatal(err)
	}
	pid, err := svc.CreateProposal(ctx, snap.ID, events.ProposalCreatedPayload{
		Title: "fix", Risk: "low", Rollback: "revert the change",
		Steps: []events.ProposalStep{{Action: "pg_gate", Layer: "postgres", Params: json.RawMessage("{}")}},
	}, events.AgentActor)
	if err != nil {
		t.Fatal(err)
	}
	act.pid.Store(pid)
	row, err := ms.GetProposal(ctx, pid)
	if err != nil {
		t.Fatal(err)
	}
	return &gateRig{svc: svc, ms: ms, act: act, pid: pid, row: row}
}

func (r *gateRig) approve(t *testing.T) error {
	t.Helper()
	priya, _ := r.svc.LookupUser("priya")
	_, err := r.svc.ApproveProposal(context.Background(), priya, r.pid, "postgres", r.row.ParamsHash)
	return err
}

func (r *gateRig) waitExecuted(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if p, err := r.ms.GetProposal(context.Background(), r.pid); err == nil && p.Status == "executed" {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	p, _ := r.ms.GetProposal(context.Background(), r.pid)
	t.Fatalf("proposal status %q, want executed", p.Status)
}

// An executor that reaches a proposal before its owners approved it runs
// nothing and records nothing: the proposal stays open, the owner's approval
// then goes through, and the step runs once, after it.
func TestExecutorRefusesUnapprovedProposal(t *testing.T) {
	r := newGateRig(t)
	if err := r.svc.executeProposal(r.row, false); err != nil {
		t.Fatalf("executeProposal: %v", err)
	}
	if n := r.act.runs.Load(); n != 0 {
		t.Fatalf("the action ran %d time(s) on a proposal nobody approved", n)
	}
	p, err := r.ms.GetProposal(context.Background(), r.pid)
	if err != nil || p.Status != "open" {
		t.Fatalf("proposal = %q (%v), want still open", p.Status, err)
	}
	if got := r.ms.ProposalStepStatus(r.pid, 0); got != "" {
		t.Fatalf("step 0 status %q, want nothing recorded", got)
	}
	if err := r.approve(t); err != nil {
		t.Fatalf("approve after the refused run: %v", err)
	}
	r.waitExecuted(t)
	if n, bad := r.act.runs.Load(), r.act.unapproved.Load(); n != 1 || bad != 0 {
		t.Fatalf("runs = %d (unapproved %d), want 1 (0)", n, bad)
	}
}

// Many executors racing the approval (TestReplicasExecutorExactlyOnce's
// shape): none of them runs the step before the approval, and none of them
// decides the proposal so that the approval is refused.
func TestExecutorRaceWithApprovalNeverRunsUnapproved(t *testing.T) {
	r := newGateRig(t)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			// Errors are expected here after the approval: the
			// single-process store has no claim, so a late executor
			// finds the step already started. Before the approval
			// there must be none, which the counts below check.
			_ = r.svc.executeProposal(r.row, false)
		}()
	}
	close(start)
	if err := r.approve(t); err != nil {
		t.Fatalf("approve: %v", err)
	}
	wg.Wait()
	if bad := r.act.unapproved.Load(); bad != 0 {
		t.Fatalf("the action ran %d time(s) before the proposal was approved", bad)
	}
}

// The executor's last guard: even with the claim already held (the queue
// path, executeProposal(row, true)), recheckStep refuses a step on a proposal
// that is not approved in full. Execute is never called; the step is recorded
// failed with notApprovedOutput.
func TestRecheckStepRefusesUnapprovedClaimedProposal(t *testing.T) {
	for _, tc := range []struct {
		name     string
		decision string // "" records nothing
	}{
		{"no approval", ""},
		{"rejected", "rejected"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := newGateRig(t)
			ctx := context.Background()
			if tc.decision != "" {
				// Straight into the store: the room's reject path would
				// also mark the proposal rejected, and this test is about
				// the executor's own check.
				if err := r.ms.RecordApproval(ctx, store.ApprovalRow{ProposalID: r.pid, Layer: "postgres", Decision: tc.decision, ParamsHash: r.row.ParamsHash}); err != nil {
					t.Fatal(err)
				}
			}
			if err := r.svc.executeProposal(r.row, true); err != nil {
				t.Fatalf("executeProposal: %v", err)
			}
			if n := r.act.runs.Load(); n != 0 {
				t.Fatalf("Execute ran %d time(s) on a proposal that is not approved in full", n)
			}
			if got := r.ms.ProposalStepStatus(r.pid, 0); got != "failed" {
				t.Fatalf("step 0 status %q, want failed", got)
			}
			evs, err := r.ms.Since(ctx, r.row.RoomID, 0, 1000)
			if err != nil {
				t.Fatal(err)
			}
			var outs []string
			for _, e := range evs {
				if e.Type != events.ActionExecuted {
					continue
				}
				p, ok := decode[events.ActionExecutedPayload](e)
				if !ok {
					t.Fatalf("undecodable action.executed: %+v", e)
				}
				if p.OK {
					t.Fatalf("action.executed ok for an unapproved proposal: %+v", p)
				}
				outs = append(outs, p.Output)
			}
			if len(outs) != 1 || outs[0] != notApprovedOutput {
				t.Fatalf("action.executed outputs %q, want [%q]", outs, notApprovedOutput)
			}
			if p, err := r.ms.GetProposal(ctx, r.pid); err != nil || p.Status != "failed" {
				t.Fatalf("proposal %q (%v), want failed", p.Status, err)
			}
		})
	}
}

// layersApproved: every required layer needs an approved decision; none
// required is never approved.
func TestLayersApproved(t *testing.T) {
	ok := func(l string) store.ApprovalRow { return store.ApprovalRow{Layer: l, Decision: "approved"} }
	for _, tc := range []struct {
		name     string
		required []string
		apps     []store.ApprovalRow
		want     bool
	}{
		{"none required", nil, []store.ApprovalRow{ok("kafka")}, false},
		{"no decisions", []string{"kafka"}, nil, false},
		{"all approved", []string{"kafka", "postgres"}, []store.ApprovalRow{ok("postgres"), ok("kafka")}, true},
		{"one missing", []string{"kafka", "postgres"}, []store.ApprovalRow{ok("kafka")}, false},
		{"one rejected", []string{"kafka"}, []store.ApprovalRow{{Layer: "kafka", Decision: "rejected"}}, false},
		{"other layer approved", []string{"kafka"}, []store.ApprovalRow{ok("warehouse")}, false},
	} {
		if got := layersApproved(tc.required, tc.apps); got != tc.want {
			t.Errorf("%s: got %v, want %v", tc.name, got, tc.want)
		}
	}
}
