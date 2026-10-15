//go:build integration

package store_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"piperoom/internal/store"
)

// A withdrawal and the executor's claim race on two replicas: exactly one
// wins, every round (QA G9). The loser of a withdrawal sees ErrProposalRunning.
func TestWithdrawRacesClaim(t *testing.T) {
	a, _, schema := requireStoreSchema(t)
	b := openReplica(t, schema, store.Options{})
	roomID := createTestRoom(t, a)
	ctx := context.Background()
	for round := 0; round < 20; round++ {
		pid := fmt.Sprintf("p_race_%d", round)
		createTestProposal(t, a, roomID, pid)
		var withdrawn, claimed atomic.Int32
		var wg sync.WaitGroup
		start := make(chan struct{})
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			err := a.WithdrawProposal(ctx, pid)
			switch {
			case err == nil:
				withdrawn.Add(1)
			case errors.Is(err, store.ErrProposalRunning):
			default:
				t.Errorf("withdraw: %v", err)
			}
		}()
		go func() {
			defer wg.Done()
			<-start
			ok, err := b.ClaimProposal(ctx, pid, "executor-b")
			if err != nil {
				t.Errorf("claim: %v", err)
			}
			if ok {
				claimed.Add(1)
			}
		}()
		close(start)
		wg.Wait()
		if withdrawn.Load()+claimed.Load() != 1 {
			t.Fatalf("round %d: withdrawn %d, claimed %d; want exactly one winner", round, withdrawn.Load(), claimed.Load())
		}
		p, err := a.GetProposal(ctx, pid)
		if err != nil {
			t.Fatal(err)
		}
		if want := map[bool]string{true: store.ProposalWithdrawn, false: "open"}[withdrawn.Load() == 1]; p.Status != want {
			t.Fatalf("round %d: status %q, want %q", round, p.Status, want)
		}
	}
}

// The refusals: a started step, a decided or withdrawn proposal and an
// unknown id change nothing and say why.
func TestWithdrawRefusals(t *testing.T) {
	s, _, _ := requireStoreSchema(t)
	roomID := createTestRoom(t, s)
	ctx := context.Background()

	createTestProposal(t, s, roomID, "p_open")
	if err := s.WithdrawProposal(ctx, "p_open"); err != nil {
		t.Fatalf("withdraw open: %v", err)
	}
	if err := s.WithdrawProposal(ctx, "p_open"); !errors.Is(err, store.ErrProposalWithdrawn) {
		t.Fatalf("second withdraw: %v, want ErrProposalWithdrawn", err)
	}

	createTestProposal(t, s, roomID, "p_step")
	if err := s.StartProposalStep(ctx, "p_step", 0); err != nil {
		t.Fatal(err)
	}
	if err := s.WithdrawProposal(ctx, "p_step"); !errors.Is(err, store.ErrProposalRunning) {
		t.Fatalf("withdraw started: %v, want ErrProposalRunning", err)
	}

	createTestProposal(t, s, roomID, "p_done")
	if err := s.SetProposalStatus(ctx, "p_done", "rejected"); err != nil {
		t.Fatal(err)
	}
	if err := s.WithdrawProposal(ctx, "p_done"); !errors.Is(err, store.ErrProposalDecided) {
		t.Fatalf("withdraw rejected: %v, want ErrProposalDecided", err)
	}
	if err := s.WithdrawProposal(ctx, "p_missing"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("withdraw missing: %v, want ErrNotFound", err)
	}
	// A withdrawn proposal can no longer be claimed.
	if ok, err := s.ClaimProposal(ctx, "p_open", "late"); err != nil || ok {
		t.Fatalf("claim of a withdrawn proposal: %v %v", ok, err)
	}
	for id, want := range map[string]string{"p_open": store.ProposalWithdrawn, "p_step": "open", "p_done": "rejected"} {
		p, err := s.GetProposal(ctx, id)
		if err != nil || p.Status != want {
			t.Fatalf("%s: status %q (%v), want %q", id, p.Status, err, want)
		}
	}
}
