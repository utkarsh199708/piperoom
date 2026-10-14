//go:build integration

package store_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"piperoom/internal/actions"
	"piperoom/internal/agent"
	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/room"
)

// On real Postgres: someone edits the steps AND the stored params_hash (with
// the correct hash for the edit) after the last approval, between step 0 and
// step 1. The row check alone would pass; the executor also holds the hash the
// approvers approved, so step 1 is refused (review of PR 67).
func TestPostgresExecutorRefusesStepsAndHashEditedTogether(t *testing.T) {
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
	step := func(n string) events.ProposalStep {
		return events.ProposalStep{Action: "pg_act", Layer: "postgres", Params: json.RawMessage(`{"table":"public.orders","n":"` + n + `"}`), Description: "d"}
	}
	pid, err := svc.CreateProposal(ctx, snap.ID, events.ProposalCreatedPayload{
		Title: "two steps", Risk: "low", Rollback: "undo", Steps: []events.ProposalStep{step("1"), step("2")},
	}, events.AgentActor)
	if err != nil {
		t.Fatal(err)
	}
	cur, err := svc.GetRoom(ctx, snap.ID)
	if err != nil {
		t.Fatal(err)
	}
	hash := cur.OpenProposals[0].ParamsHash

	conn, err := pgx.Connect(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	table := pgx.Identifier{schema, "proposals"}.Sanitize()
	act.hook = func() {
		// Edit the steps, then store the correct hash for the edited row.
		if _, err := conn.Exec(ctx, `UPDATE `+table+` SET steps = replace(steps::text, 'public.orders', 'public.customers')::jsonb WHERE proposal_id = $1`, pid); err != nil {
			t.Error(err)
			return
		}
		row, err := s.GetProposal(ctx, pid)
		if err != nil {
			t.Error(err)
			return
		}
		var steps []events.ProposalStep
		if err := json.Unmarshal(row.Steps, &steps); err != nil {
			t.Error(err)
			return
		}
		h, err := room.ComputeParamsHash(row.RoomID, row.ProposalID, row.Title, row.Risk, row.Rollback, row.RequiredLayers, steps)
		if err != nil {
			t.Error(err)
			return
		}
		if _, err := conn.Exec(ctx, `UPDATE `+table+` SET params_hash = $2 WHERE proposal_id = $1`, pid, h); err != nil {
			t.Error(err)
		}
	}
	if _, err := svc.ApproveProposal(ctx, priya, pid, "postgres", hash); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if p, err := s.GetProposal(ctx, pid); err == nil && p.Status == "failed" {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if p, _ := s.GetProposal(ctx, pid); p.Status != "failed" {
		t.Fatalf("status = %q, want failed", p.Status)
	}
	if act.runs.Load() != 1 {
		t.Fatalf("the action ran %d times, want only step 0", act.runs.Load())
	}
	evs, _, err := svc.Events(ctx, snap.ID, 0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	var res []events.ActionExecutedPayload
	for _, ev := range evs {
		if ev.Type == events.ActionExecuted {
			p, _ := events.Decode[events.ActionExecutedPayload](ev)
			res = append(res, p)
		}
	}
	if len(res) != 2 || !res[0].OK || res[1].OK || res[1].Output != "params hash mismatch" || res[1].ParamsHash != hash {
		t.Fatalf("action.executed = %+v", res)
	}
}
