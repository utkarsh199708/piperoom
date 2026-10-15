//go:build integration

package store_test

import (
	"context"
	"encoding/json"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"piperoom/internal/actions"
	"piperoom/internal/agent"
	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/room"
)

// pgAction is a postgres-layer action that counts its runs.
type pgAction struct {
	runs atomic.Int32
	hook func() // called at the start of the first run
}

func (a *pgAction) Name() string     { return "pg_act" }
func (a *pgAction) Layer() string    { return "postgres" }
func (a *pgAction) Describe() string { return "test" }
func (a *pgAction) ParamsSchema() map[string]any {
	return map[string]any{"type": "object"}
}
func (a *pgAction) Validate(context.Context, json.RawMessage) error   { return nil }
func (a *pgAction) CheckScope(owners.Pipeline, json.RawMessage) error { return nil }
func (a *pgAction) Execute(context.Context, json.RawMessage) (string, error) {
	if a.runs.Add(1) == 1 && a.hook != nil {
		a.hook()
	}
	return "done", nil
}

// On real Postgres: jsonb drops whitespace and reorders keys, and the hash of
// the row read back must still equal the hash computed at creation (design
// 3.1), so an untouched proposal runs; a row edited with SQL after creation is
// refused by the executor (design 3.2).
func TestPostgresParamsHashRoundTripAndExecutorRefusal(t *testing.T) {
	s, url, schema := requireStoreSchema(t)
	ctx := context.Background()
	act := &pgAction{}
	reg := actions.NewRegistry()
	if err := reg.Register(act); err != nil {
		t.Fatal(err)
	}
	svc := room.New(room.Deps{
		Store: s, Actions: reg, Users: room.DefaultUsers(),
		Owners: &owners.Owners{Pipelines: []owners.Pipeline{{ID: "orders", SourceTables: []string{"public.orders"}}}},
		Run: func(ctx context.Context, _ agent.Alert, _ agent.Deps, _ agent.Options) error {
			<-ctx.Done()
			return nil
		},
	})
	t.Cleanup(svc.Close)
	arjun, _ := svc.LookupUser("arjun")
	priya, _ := svc.LookupUser("priya")
	snap, err := svc.CreateRoom(ctx, arjun, "t", "sev2", "orders")
	if err != nil {
		t.Fatal(err)
	}
	hashOf := func(pid string) string {
		cur, err := svc.GetRoom(ctx, snap.ID)
		if err != nil {
			t.Fatal(err)
		}
		for _, p := range cur.OpenProposals {
			if p.ProposalID == pid {
				return p.ParamsHash
			}
		}
		t.Fatalf("proposal %s is not open", pid)
		return ""
	}
	propose := func(params string) string {
		pid, err := svc.CreateProposal(ctx, snap.ID, events.ProposalCreatedPayload{
			Title: "fix <b> & é", Risk: "low", Rollback: "undo",
			Steps: []events.ProposalStep{{Action: "pg_act", Layer: "postgres", Params: json.RawMessage(params), Description: "d"}},
		}, events.AgentActor)
		if err != nil {
			t.Fatal(err)
		}
		return pid
	}
	waitStatus := func(pid, want string) {
		t.Helper()
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			if p, err := s.GetProposal(ctx, pid); err == nil && p.Status == want {
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
		t.Fatalf("proposal %s did not reach status %s", pid, want)
	}

	// 1. Whitespace, key order, a trailing-zero decimal and non-ASCII text all
	// change in jsonb; the hash must not.
	pidA := propose(`{ "z": "<b>&é", "n": 1.50, "table": "public.orders", "list": [3, 2, 1] }`)
	hashA := hashOf(pidA)
	row, err := s.GetProposal(ctx, pidA)
	if err != nil || row.ParamsHash != hashA {
		t.Fatalf("stored hash = %q, %v; snapshot shows %q", row.ParamsHash, err, hashA)
	}
	if _, err := svc.ApproveProposal(ctx, priya, pidA, "postgres", hashA); err != nil {
		t.Fatal(err)
	}
	waitStatus(pidA, "executed")
	if act.runs.Load() != 1 {
		t.Fatalf("runs = %d, want 1", act.runs.Load())
	}
	apps, err := s.ListApprovals(ctx, pidA)
	if err != nil || len(apps) != 1 || apps[0].ParamsHash != hashA || apps[0].IdentityMethod != "dev" || apps[0].IdentityGrant != "user:priya" {
		t.Fatalf("approval row = %+v, %v", apps, err)
	}

	// 2. A SQL edit of the steps after creation: the approval still matches the
	// stored hash, but the executor recomputes from the row and refuses.
	pidB := propose(`{"table":"public.orders"}`)
	hashB := hashOf(pidB)
	conn, err := pgx.Connect(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	tag, err := conn.Exec(ctx, `UPDATE `+pgx.Identifier{schema, "proposals"}.Sanitize()+
		` SET steps = replace(steps::text, 'public.orders', 'public.customers')::jsonb WHERE proposal_id = $1`, pidB)
	if err != nil || tag.RowsAffected() != 1 {
		t.Fatalf("edit row: %v (rows %d)", err, tag.RowsAffected())
	}
	if _, err := svc.ApproveProposal(ctx, priya, pidB, "postgres", hashB); err != nil {
		t.Fatal(err)
	}
	waitStatus(pidB, "failed")
	if act.runs.Load() != 1 {
		t.Fatalf("the action ran on an edited row (runs = %d)", act.runs.Load())
	}
	evs, _, err := svc.Events(ctx, snap.ID, 0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	var last events.ActionExecutedPayload
	for _, ev := range evs {
		if ev.Type == events.ActionExecuted {
			last, _ = events.Decode[events.ActionExecutedPayload](ev)
		}
	}
	if last.ProposalID != pidB || last.OK || last.Output != "params hash mismatch" || last.ParamsHash != hashB {
		t.Fatalf("last action.executed = %+v", last)
	}

	// 3. An old row (no stored hash) can still be approved: the hash is filled on first use.
	pidC := propose(`{"table":"public.orders","old":true}`)
	hashC := hashOf(pidC)
	if _, err := conn.Exec(ctx, `UPDATE `+pgx.Identifier{schema, "proposals"}.Sanitize()+` SET params_hash = '' WHERE proposal_id = $1`, pidC); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.ApproveProposal(ctx, priya, pidC, "postgres", hashC); err != nil {
		t.Fatalf("approve an old row: %v", err)
	}
	waitStatus(pidC, "executed")
	if row, _ := s.GetProposal(ctx, pidC); row.ParamsHash != hashC || !strings.HasPrefix(row.ParamsHash, hashC[:8]) {
		t.Fatalf("hash after first use = %q, want %q", row.ParamsHash, hashC)
	}
}
