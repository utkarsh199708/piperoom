//go:build integration

package store_test

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"piperoom/internal/auth"
	"piperoom/internal/testenv"
)

func TestSessionsStoreRoundTrip(t *testing.T) {
	s, _ := requireStore(t)
	sess := s.Sessions()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	_, hash, err := auth.NewToken()
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	id := testenv.Name("ses")
	want := auth.Session{
		ID: id, TokenHash: hash, UserID: "priya", Name: "Priya",
		Iss: "https://idp.example", Sub: "sub-1", Groups: []string{"dataeng", "oncall"}, GroupsMissing: true,
		AuthTime: now.Add(-time.Minute), AMR: []string{"pwd", "mfa"}, ACR: "urn:acr:2", Method: auth.MethodOIDC,
		CreatedAt: now, LastSeenAt: now, ExpiresAt: now.Add(12 * time.Hour),
	}
	if _, err := sess.ByTokenHash(ctx, hash); !errors.Is(err, auth.ErrNotFound) {
		t.Fatalf("before create: %v, want ErrNotFound", err)
	}
	if err := sess.Create(ctx, want); err != nil {
		t.Fatalf("create: %v", err)
	}
	got, err := sess.ByTokenHash(ctx, hash)
	if err != nil {
		t.Fatalf("get by hash: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("round trip differs:\n got %+v\nwant %+v", got, want)
	}
	byID, err := sess.ByID(ctx, id)
	if err != nil || !reflect.DeepEqual(byID, want) {
		t.Fatalf("by id = %+v, %v", byID, err)
	}
	if _, err := sess.ByID(ctx, testenv.Name("nope")); !errors.Is(err, auth.ErrNotFound) {
		t.Fatalf("missing id: %v, want ErrNotFound", err)
	}

	// Duplicate id and duplicate token hash are refused.
	if err := sess.Create(ctx, want); err == nil {
		t.Fatal("duplicate create succeeded")
	}

	// Touch moves last_seen_at; Revoke sets revoked_at once.
	later := now.Add(3 * time.Minute)
	if err := sess.Touch(ctx, id, later); err != nil {
		t.Fatal(err)
	}
	first := now.Add(4 * time.Minute)
	if err := sess.Revoke(ctx, id, first); err != nil {
		t.Fatal(err)
	}
	if err := sess.Revoke(ctx, id, first.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	got, err = sess.ByTokenHash(ctx, hash)
	if err != nil {
		t.Fatal(err)
	}
	if !got.LastSeenAt.Equal(later) || !got.RevokedAt.Equal(first) {
		t.Fatalf("after touch+revoke: last_seen %v revoked %v, want %v / %v (revoke keeps the first time)", got.LastSeenAt, got.RevokedAt, later, first)
	}
	if err := got.Check(later, 8*time.Hour); !errors.Is(err, auth.ErrRevoked) {
		t.Fatalf("Check on a revoked row = %v, want ErrRevoked", err)
	}
	// A missing session is not an error for Touch or Revoke.
	if err := sess.Touch(ctx, testenv.Name("nope"), later); err != nil {
		t.Fatalf("touch missing: %v", err)
	}
	if err := sess.Revoke(ctx, testenv.Name("nope"), later); err != nil {
		t.Fatalf("revoke missing: %v", err)
	}
}

// A session with empty groups and no auth_time reads back as empty and zero,
// not NULL surprises, and a bad method is refused by the table.
func TestSessionsStoreDefaultsAndMethodCheck(t *testing.T) {
	s, _ := requireStore(t)
	sess := s.Sessions()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	_, hash, _ := auth.NewToken()
	now := time.Now().UTC().Truncate(time.Microsecond)
	d := auth.Session{ID: testenv.Name("ses"), TokenHash: hash, UserID: "arjun", Method: auth.MethodDev,
		CreatedAt: now, LastSeenAt: now, ExpiresAt: now.Add(time.Hour)}
	if err := sess.Create(ctx, d); err != nil {
		t.Fatalf("create: %v", err)
	}
	got, err := sess.ByTokenHash(ctx, hash)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Groups) != 0 || len(got.AMR) != 0 || !got.AuthTime.IsZero() || !got.RevokedAt.IsZero() || got.GroupsMissing {
		t.Fatalf("defaults wrong: %+v", got)
	}

	_, hash2, _ := auth.NewToken()
	bad := d
	bad.ID, bad.TokenHash, bad.Method = testenv.Name("ses"), hash2, "saml"
	if err := sess.Create(ctx, bad); err == nil {
		t.Fatal("create with method saml succeeded; the CHECK should refuse it")
	}
}
