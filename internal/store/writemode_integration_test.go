//go:build integration

package store_test

import (
	"context"
	"testing"
	"time"

	"piperoom/internal/store"
)

// A proposal fully approved while write mode was off (status blocked) is
// never listed under the write-off unclaimed window, cannot be claimed, and
// under a normal window is listed flagged WriteBlocked, next to an approved,
// unclaimed open one that is not, and is failed by exactly one interrupt. A
// blocked row in a closed room is never listed and stays blocked.
func TestStaleWriteBlockedProposal(t *testing.T) {
	s, _, schema := requireStoreSchema(t)
	roomID := createTestRoom(t, s)
	closedRoom := createTestRoom(t, s)
	ctx := context.Background()
	for pid, room := range map[string]string{"p_blocked": roomID, "p_open": roomID, "p_closed": closedRoom} {
		createTestProposal(t, s, room, pid)
		if err := s.RecordApproval(ctx, store.ApprovalRow{ProposalID: pid, Layer: "postgres", Decision: "approved"}); err != nil {
			t.Fatal(err)
		}
	}
	for _, pid := range []string{"p_blocked", "p_closed"} {
		if err := s.SetProposalStatus(ctx, pid, store.ProposalStatusBlocked); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.CloseRoom(ctx, closedRoom, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	execSQL(t, schema, `UPDATE approvals SET decided_at = now() - interval '10 minutes'`)

	century := 100 * 365 * 24 * time.Hour // room.writeOffUnclaimedStale
	if sp, err := s.StaleProposals(ctx, time.Minute, century, 10); err != nil || len(sp) != 0 {
		t.Fatalf("write-off window lists %+v (err %v), want none", sp, err)
	}
	if ok, err := s.InterruptProposal(ctx, "p_blocked", "sweeper", time.Minute, century); err != nil || ok {
		t.Fatalf("write-off window interrupt: ok=%v err=%v, want false", ok, err)
	}
	if ok, _ := s.ClaimProposal(ctx, "p_blocked", "executor"); ok {
		t.Fatal("an executor claimed a blocked proposal")
	}

	sp, err := s.StaleProposals(ctx, time.Minute, time.Minute, 10)
	if err != nil || len(sp) != 2 {
		t.Fatalf("stale proposals: %+v %v, want both", sp, err)
	}
	for _, p := range sp {
		if p.ClaimedBy != "" || p.RoomID != roomID || p.WriteBlocked != (p.ProposalID == "p_blocked") {
			t.Fatalf("stale proposal %+v: want unclaimed, WriteBlocked only for p_blocked", p)
		}
	}
	if ok, err := s.InterruptProposal(ctx, "p_blocked", "sweeper", time.Minute, time.Minute); err != nil || !ok {
		t.Fatalf("interrupt: ok=%v err=%v", ok, err)
	}
	if ok, _ := s.InterruptProposal(ctx, "p_blocked", "sweeper-2", time.Minute, time.Minute); ok {
		t.Fatal("a blocked proposal was interrupted twice")
	}
	if row, _ := s.GetProposal(ctx, "p_blocked"); row.Status != "failed" {
		t.Fatalf("status %q, want failed", row.Status)
	}
	if ok, err := s.InterruptProposal(ctx, "p_closed", "sweeper", time.Minute, time.Minute); err != nil || ok {
		t.Fatalf("interrupt in a closed room: ok=%v err=%v, want false", ok, err)
	}
	if row, _ := s.GetProposal(ctx, "p_closed"); row.Status != store.ProposalStatusBlocked {
		t.Fatalf("closed room's row status %q, want %s", row.Status, store.ProposalStatusBlocked)
	}
}
