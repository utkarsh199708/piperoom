package memstore

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"piperoom/internal/store"
)

// TestWithdrawProposal pins the refusals both stores share
// (store.WithdrawRefusal): only an open proposal with no started step is
// withdrawn, and a refusal changes nothing.
func TestWithdrawProposal(t *testing.T) {
	tests := []struct {
		name       string
		status     string
		startStep  bool
		missing    bool
		cancelled  bool
		want       error
		wantStatus string
	}{
		{name: "open", status: "open", wantStatus: store.ProposalWithdrawn},
		{name: "already withdrawn", status: store.ProposalWithdrawn, want: store.ErrProposalWithdrawn, wantStatus: store.ProposalWithdrawn},
		{name: "rejected", status: "rejected", want: store.ErrProposalDecided, wantStatus: "rejected"},
		{name: "executed", status: "executed", want: store.ErrProposalDecided, wantStatus: "executed"},
		{name: "failed", status: "failed", want: store.ErrProposalDecided, wantStatus: "failed"},
		{name: "step started", status: "open", startStep: true, want: store.ErrProposalRunning, wantStatus: "open"},
		{name: "unknown proposal", missing: true, want: store.ErrNotFound},
		{name: "cancelled context", status: "open", cancelled: true, want: context.Canceled, wantStatus: "open"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := New()
			roomID := newTestRoom(t, s)
			ctx := context.Background()
			if !tc.missing {
				if err := s.CreateProposal(ctx, store.ProposalRow{
					ProposalID: "p1", RoomID: roomID, Title: "fix", Status: tc.status,
					RequiredLayers: []string{"postgres"}, Steps: json.RawMessage(`[]`), Risk: "r", Rollback: "rb",
				}); err != nil {
					t.Fatal(err)
				}
			}
			if tc.startStep {
				if err := s.StartProposalStep(ctx, "p1", 0); err != nil {
					t.Fatal(err)
				}
			}
			callCtx := ctx
			if tc.cancelled {
				c, cancel := context.WithCancel(ctx)
				cancel()
				callCtx = c
			}
			err := s.WithdrawProposal(callCtx, "p1")
			if tc.want == nil && err != nil {
				t.Fatalf("withdraw: %v", err)
			}
			if tc.want != nil && !errors.Is(err, tc.want) {
				t.Fatalf("withdraw = %v, want %v", err, tc.want)
			}
			if tc.missing {
				return
			}
			p, err := s.GetProposal(ctx, "p1")
			if err != nil {
				t.Fatal(err)
			}
			if p.Status != tc.wantStatus {
				t.Fatalf("status = %q, want %q", p.Status, tc.wantStatus)
			}
		})
	}
}
