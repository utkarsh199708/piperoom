//go:build integration

package room

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"piperoom/internal/actions"
	"piperoom/internal/events"
	"piperoom/internal/store"
)

var errLostWrite = errors.New("simulated lost side-table write")

// lostWritePG is a Postgres store whose rooms-row close and "rejected"
// proposal status writes fail while failing is set. Everything else,
// coordination included, is the embedded store's.
type lostWritePG struct {
	*store.PG
	failing atomic.Bool
}

func (f *lostWritePG) CloseRoom(ctx context.Context, id string, closedAt time.Time) error {
	if f.failing.Load() {
		return errLostWrite
	}
	return f.PG.CloseRoom(ctx, id, closedAt)
}

func (f *lostWritePG) SetProposalStatus(ctx context.Context, proposalID, status string) error {
	if f.failing.Load() && status == "rejected" {
		return errLostWrite
	}
	return f.PG.SetProposalStatus(ctx, proposalID, status)
}

// On Postgres with two replicas: replica A loses the rooms-row close and the
// proposals-row rejection; replica B, loading the rooms from the log, repairs
// the closed row and refuses an approval of the rejected proposal (and repairs
// that row too).
func TestReplicasLogWinsOverLostSideWrites(t *testing.T) {
	schema := pgSchema(t)
	log := newAgentLog()
	acts := func(d *Deps) {
		reg := actions.NewRegistry()
		for _, a := range []actions.Action{&countAction{}, &itAction{}} {
			if err := reg.Register(a); err != nil {
				t.Fatal(err)
			}
		}
		d.Actions = reg
	}
	ast := &lostWritePG{PG: pgReplicaStore(t, schema, store.Options{})}
	a := newReplica(t, ast, "r0", log, nil, acts)
	ctx := context.Background()

	closed, err := a.CreateRoom(ctx, user(t, a, "arjun"), "lag", "sev2", "orders")
	if err != nil {
		t.Fatal(err)
	}
	open, err := a.CreateRoom(ctx, user(t, a, "arjun"), "slot", "sev2", "orders")
	if err != nil {
		t.Fatal(err)
	}
	pid, err := a.CreateProposal(ctx, open.ID, events.ProposalCreatedPayload{
		Title: "fix",
		Steps: []events.ProposalStep{
			{Action: "count_act", Layer: "postgres", Params: json.RawMessage("{}")},
			{Action: "it_fix", Layer: "kafka", Params: json.RawMessage("{}")},
		},
		Risk: "low", Rollback: "undo",
	}, events.AgentActor)
	if err != nil {
		t.Fatal(err)
	}
	row, err := ast.GetProposal(ctx, pid)
	if err != nil {
		t.Fatal(err)
	}

	ast.failing.Store(true)
	if _, err := a.CloseRoom(ctx, user(t, a, "arjun"), closed.ID, "fixed"); err != nil {
		t.Fatalf("close: %v", err)
	}
	if _, err := a.RejectProposal(ctx, user(t, a, "priya"), pid, "postgres", "no", row.ParamsHash); err != nil {
		t.Fatalf("reject: %v", err)
	}
	if r, _ := ast.GetRoom(ctx, closed.ID); r.Status != "open" {
		t.Fatalf("room row = %q, want open (the write failed)", r.Status)
	}
	if p, _ := ast.GetProposal(ctx, pid); p.Status != "open" {
		t.Fatalf("proposal row = %q, want open (the write failed)", p.Status)
	}
	_, err = a.ApproveProposal(ctx, user(t, a, "arjun"), pid, "kafka", row.ParamsHash)
	if re := (*Error)(nil); !errors.As(err, &re) || re.Code != codeAlreadyDecided {
		t.Fatalf("approve on A = %v, want already_decided", err)
	}

	b := newReplica(t, pgReplicaStore(t, schema, store.Options{}), "r1", log, nil, acts)
	snap, err := b.GetRoom(ctx, closed.ID)
	if err != nil || snap.ClosedAt == nil {
		t.Fatalf("room on B = %+v (%v), want closed", snap, err)
	}
	r, err := ast.GetRoom(ctx, closed.ID)
	if err != nil || r.Status != "closed" || r.ClosedAt == nil || !r.ClosedAt.Equal(*snap.ClosedAt) {
		t.Fatalf("room row after B's load = %+v (%v), want closed at %v", r, err, snap.ClosedAt)
	}

	_, err = b.ApproveProposal(ctx, user(t, b, "arjun"), pid, "kafka", row.ParamsHash)
	if re := (*Error)(nil); !errors.As(err, &re) || re.Code != codeAlreadyDecided {
		t.Fatalf("approve on B = %v, want already_decided", err)
	}
	if p, _ := ast.GetProposal(ctx, pid); p.Status != "rejected" {
		t.Fatalf("proposal row after B's refusal = %q, want rejected", p.Status)
	}
	apps, err := ast.ListApprovals(ctx, pid)
	if err != nil || len(apps) != 1 || apps[0].Decision != "rejected" {
		t.Fatalf("approval rows = %+v (%v), want only the rejection", apps, err)
	}
}
