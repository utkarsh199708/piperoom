//go:build integration

package store_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"piperoom/internal/store"
	"piperoom/internal/testenv"
)

// Auth PR 4 columns (migration 00008): proposals.params_hash and the approval
// identity columns round-trip, an old row (no hash) reads back empty and can be
// filled once, and the auth time comes back in UTC.
func TestParamsHashAndApprovalIdentityColumns(t *testing.T) {
	s, _ := requireStore(t)
	ctx := context.Background()
	roomID := createTestRoom(t, s)
	hash := "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

	pid := testenv.Name("p_")
	if err := s.CreateProposal(ctx, store.ProposalRow{
		ProposalID: pid, RoomID: roomID, Title: "fix", Status: "open", ParamsHash: hash,
		RequiredLayers: []string{"postgres", "kafka"}, Steps: []byte(`[{"action":"pg_act","layer":"postgres"}]`),
	}); err != nil {
		t.Fatal(err)
	}
	p, err := s.GetProposal(ctx, pid)
	if err != nil || p.ParamsHash != hash {
		t.Fatalf("params_hash = %q, %v; want %q", p.ParamsHash, err, hash)
	}

	// A proposal from before the migration has no hash; it is filled once and never replaced.
	oldPid := testenv.Name("p_")
	createTestProposal(t, s, roomID, oldPid)
	if p, _ := s.GetProposal(ctx, oldPid); p.ParamsHash != "" {
		t.Fatalf("a row with no hash reads back %q", p.ParamsHash)
	}
	if err := s.SetProposalParamsHash(ctx, oldPid, hash); err != nil {
		t.Fatal(err)
	}
	if err := s.SetProposalParamsHash(ctx, oldPid, hash); err != nil { // the same value again is a no-op
		t.Fatal(err)
	}
	if err := s.SetProposalParamsHash(ctx, oldPid, "ffff"); !errors.Is(err, store.ErrParamsHashConflict) {
		t.Fatalf("a different hash: want ErrParamsHashConflict, got %v", err)
	}
	if p, _ := s.GetProposal(ctx, oldPid); p.ParamsHash != hash {
		t.Fatalf("hash = %q, want the first value to stay", p.ParamsHash)
	}
	if err := s.SetProposalParamsHash(ctx, testenv.Name("p_"), hash); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("missing proposal: want ErrNotFound, got %v", err)
	}

	// Approvals: the identity columns round-trip.
	authTime := time.Date(2026, 10, 13, 15, 30, 0, 0, time.FixedZone("x", 5*3600+1800))
	oidc := store.ApprovalRow{
		ProposalID: pid, Layer: "postgres", Decision: "approved", ParamsHash: hash,
		IdentityIss: "https://idp.example", IdentitySub: "sub-1", IdentitySessionID: "ses_01ABC",
		IdentityMethod: "oidc", IdentityGrant: "group:oncall", IdentityAuthTime: authTime,
	}
	if err := s.RecordApproval(ctx, oidc); err != nil {
		t.Fatal(err)
	}
	// A dev decision: no session, no auth time (NULL).
	dev := store.ApprovalRow{ProposalID: pid, Layer: "kafka", Decision: "rejected", Reason: "no", ParamsHash: hash, IdentityMethod: "dev", IdentityGrant: "solo"}
	if err := s.RecordApproval(ctx, dev); err != nil {
		t.Fatal(err)
	}
	got, err := s.ListApprovals(ctx, pid)
	if err != nil || len(got) != 2 {
		t.Fatalf("approvals = %+v, %v", got, err)
	}
	// ordered by layer: kafka, postgres
	k, pg := got[0], got[1]
	if k.Layer != "kafka" || k.Decision != "rejected" || k.Reason != "no" || k.ParamsHash != hash ||
		k.IdentityMethod != "dev" || k.IdentityGrant != "solo" || k.IdentitySessionID != "" || k.IdentityIss != "" || !k.IdentityAuthTime.IsZero() {
		t.Fatalf("dev row = %+v", k)
	}
	if pg.ParamsHash != hash || pg.IdentityIss != oidc.IdentityIss || pg.IdentitySub != "sub-1" || pg.IdentitySessionID != "ses_01ABC" ||
		pg.IdentityMethod != "oidc" || pg.IdentityGrant != "group:oncall" {
		t.Fatalf("oidc row = %+v", pg)
	}
	if !pg.IdentityAuthTime.Equal(authTime) || pg.IdentityAuthTime.Location() != time.UTC {
		t.Fatalf("auth time = %v, want %v in UTC", pg.IdentityAuthTime, authTime)
	}

	// A second decision on a layer is still refused, and text Postgres cannot store is unstorable.
	if err := s.RecordApproval(ctx, oidc); !errors.Is(err, store.ErrAlreadyExists) {
		t.Fatalf("duplicate: %v", err)
	}
	bad := store.ApprovalRow{ProposalID: pid, Layer: "consumer", Decision: "approved", IdentitySub: "a\x00b"}
	if err := s.RecordApproval(ctx, bad); !errors.Is(err, store.ErrUnstorable) {
		t.Fatalf("NUL in identity_sub: want ErrUnstorable, got %v", err)
	}
}
