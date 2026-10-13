package auth_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"piperoom/internal/auth"
)

func TestTokenRoundTrip(t *testing.T) {
	tok, hash, err := auth.NewToken()
	if err != nil {
		t.Fatal(err)
	}
	if len(tok) != 43 || len(hash) != 32 {
		t.Fatalf("token %d chars, hash %d bytes; want 43 and 32", len(tok), len(hash))
	}
	got, ok := auth.HashToken(tok)
	if !ok || string(got) != string(hash) {
		t.Fatalf("HashToken did not reproduce the stored hash")
	}
	tok2, _, _ := auth.NewToken()
	if tok == tok2 {
		t.Fatal("two tokens are equal")
	}
	for _, bad := range []string{"", "junk", tok[:42], tok + "A", "!!!!" + tok[4:]} {
		if _, ok := auth.HashToken(bad); ok {
			t.Fatalf("HashToken(%q) accepted a malformed token", bad)
		}
	}
}

func TestSessionCheck(t *testing.T) {
	now := time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC)
	live := auth.Session{CreatedAt: now.Add(-time.Hour), LastSeenAt: now.Add(-time.Minute), ExpiresAt: now.Add(time.Hour)}
	if err := live.Check(now, 8*time.Hour); err != nil {
		t.Fatalf("live: %v", err)
	}
	idle := live
	idle.LastSeenAt = now.Add(-8 * time.Hour)
	if err := idle.Check(now, 8*time.Hour); !errors.Is(err, auth.ErrExpired) {
		t.Fatalf("idle: %v", err)
	}
	abs := live
	abs.ExpiresAt = now
	if err := abs.Check(now, 8*time.Hour); !errors.Is(err, auth.ErrExpired) {
		t.Fatalf("absolute: %v", err)
	}
	rev := live
	rev.RevokedAt = now.Add(-time.Second)
	if err := rev.Check(now, 8*time.Hour); !errors.Is(err, auth.ErrRevoked) {
		t.Fatalf("revoked: %v", err)
	}
}

func TestMemStore(t *testing.T) {
	ctx := context.Background()
	m := auth.NewMemStore()
	now := time.Now().UTC().Truncate(time.Second)
	_, hash, _ := auth.NewToken()
	s := auth.Session{ID: "ses_1", TokenHash: hash, UserID: "priya", Method: auth.MethodDev, Groups: []string{"g"},
		CreatedAt: now, LastSeenAt: now, ExpiresAt: now.Add(time.Hour)}
	if err := m.Create(ctx, s); err != nil {
		t.Fatal(err)
	}
	if err := m.Create(ctx, s); err == nil {
		t.Fatal("duplicate create succeeded")
	}
	got, err := m.ByTokenHash(ctx, hash)
	if err != nil || got.ID != "ses_1" || got.UserID != "priya" {
		t.Fatalf("ByTokenHash = %+v, %v", got, err)
	}
	got.Groups[0] = "mutated" // a returned copy must not alias the store
	if again, _ := m.ByID(ctx, "ses_1"); again.Groups[0] != "g" {
		t.Fatal("store aliases the returned slice")
	}
	if _, err := m.ByTokenHash(ctx, []byte("nope")); !errors.Is(err, auth.ErrNotFound) {
		t.Fatalf("unknown hash: %v", err)
	}
	later := now.Add(2 * time.Minute)
	if err := m.Touch(ctx, "ses_1", later); err != nil {
		t.Fatal(err)
	}
	if err := m.Revoke(ctx, "ses_1", later); err != nil {
		t.Fatal(err)
	}
	if err := m.Revoke(ctx, "ses_1", later.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	got, _ = m.ByID(ctx, "ses_1")
	if !got.LastSeenAt.Equal(later) || !got.RevokedAt.Equal(later) {
		t.Fatalf("after touch+revoke: %+v (revoke must keep the first time)", got)
	}
	if err := m.Touch(ctx, "ses_missing", later); err != nil {
		t.Fatalf("touch of a missing session: %v", err)
	}
}
