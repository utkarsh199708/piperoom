//go:build integration

package store_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"piperoom/internal/store"
	"piperoom/internal/testenv"
)

// ClaimProposal claims only a proposal every required layer approved. Each
// refused case would be claimable if the claim's approval rule lost one of
// its conditions: the decision filter (rejected), the layer match (approval on
// another layer), the per-layer check (partial), or the "no layers is never
// approved" rule (empty array, jsonb null).
func TestClaimProposalRequiresFullApproval(t *testing.T) {
	s, _, _ := requireStoreSchema(t)
	roomID := createTestRoom(t, s)
	ctx := context.Background()
	type decision struct{ layer, decision string }
	for _, tc := range []struct {
		name      string
		required  []string
		decisions []decision
	}{
		{"partial approval", []string{"kafka", "postgres"}, []decision{{"kafka", "approved"}}},
		{"only layer rejected", []string{"kafka"}, []decision{{"kafka", "rejected"}}},
		{"one approved, one rejected", []string{"kafka", "postgres"}, []decision{{"kafka", "approved"}, {"postgres", "rejected"}}},
		{"approval on a layer that is not required", []string{"kafka"}, []decision{{"warehouse", "approved"}}},
		{"no required layers (empty array)", []string{}, []decision{{"kafka", "approved"}}},
		{"no required layers (jsonb null)", nil, []decision{{"kafka", "approved"}}},
		{"no decisions at all", []string{"kafka"}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pid := testenv.Name("p")
			if err := s.CreateProposal(ctx, store.ProposalRow{
				ProposalID: pid, RoomID: roomID, Title: "fix", Status: "open",
				RequiredLayers: tc.required, Steps: []byte(`[{"action":"pg_act","layer":"postgres"}]`),
			}); err != nil {
				t.Fatalf("create proposal: %v", err)
			}
			for _, d := range tc.decisions {
				if err := s.RecordApproval(ctx, store.ApprovalRow{ProposalID: pid, Layer: d.layer, Decision: d.decision}); err != nil {
					t.Fatalf("record %s %s: %v", d.layer, d.decision, err)
				}
			}
			ok, err := s.ClaimProposal(ctx, pid, "executor")
			if err != nil {
				t.Fatalf("claim: %v, want a refusal without an error", err)
			}
			if ok {
				t.Fatal("claimed a proposal that is not approved in full")
			}
			p, err := s.GetProposal(ctx, pid)
			if err != nil || p.Status != "open" {
				t.Fatalf("proposal after the refused claim: %+v %v, want open", p, err)
			}
		})
	}
	// The stale sweep shares the rule: a jsonb-null row must not fail its
	// query (it used to call jsonb_array_length on it).
	if _, err := s.StaleProposals(ctx, time.Millisecond, time.Millisecond, 100); err != nil {
		t.Fatalf("stale proposals with a null-layers row present: %v", err)
	}

	// Approved by every required layer: exactly one of many concurrent
	// claimers gets it.
	pid := testenv.Name("p")
	if err := s.CreateProposal(ctx, store.ProposalRow{
		ProposalID: pid, RoomID: roomID, Title: "fix", Status: "open",
		RequiredLayers: []string{"kafka", "postgres"}, Steps: []byte(`[{"action":"pg_act","layer":"postgres"}]`),
	}); err != nil {
		t.Fatalf("create proposal: %v", err)
	}
	for _, l := range []string{"kafka", "postgres"} {
		if err := s.RecordApproval(ctx, store.ApprovalRow{ProposalID: pid, Layer: l, Decision: "approved"}); err != nil {
			t.Fatal(err)
		}
	}
	var won atomic.Int32
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			ok, err := s.ClaimProposal(ctx, pid, "executor")
			if err != nil {
				t.Errorf("claim: %v", err)
			}
			if ok {
				won.Add(1)
			}
		}()
	}
	close(start)
	wg.Wait()
	if n := won.Load(); n != 1 {
		t.Fatalf("%d claims of a fully approved proposal succeeded, want exactly 1", n)
	}
}
