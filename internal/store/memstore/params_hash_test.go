package memstore_test

import (
	"context"
	"errors"
	"testing"

	"piperoom/internal/store"
	"piperoom/internal/store/memstore"
)

// SetProposalParamsHash fills an empty hash, accepts the same value again, and
// answers ErrParamsHashConflict for a different one (it never replaces).
func TestSetProposalParamsHashConflict(t *testing.T) {
	s := memstore.New()
	ctx := context.Background()
	if err := s.CreateProposal(ctx, store.ProposalRow{ProposalID: "p1", RoomID: "r", Title: "t", Status: "open"}); err != nil {
		t.Fatal(err)
	}
	if err := s.SetProposalParamsHash(ctx, "p1", "aaaa"); err != nil {
		t.Fatal(err)
	}
	if err := s.SetProposalParamsHash(ctx, "p1", "aaaa"); err != nil {
		t.Fatalf("same value: %v", err)
	}
	if err := s.SetProposalParamsHash(ctx, "p1", "bbbb"); !errors.Is(err, store.ErrParamsHashConflict) {
		t.Fatalf("different value: want ErrParamsHashConflict, got %v", err)
	}
	if p, _ := s.GetProposal(ctx, "p1"); p.ParamsHash != "aaaa" {
		t.Fatalf("hash = %q, the first value must stay", p.ParamsHash)
	}
	if err := s.SetProposalParamsHash(ctx, "nope", "aaaa"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("missing proposal: %v", err)
	}
}
