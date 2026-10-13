//go:build integration

package store_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"piperoom/internal/auth"
	"piperoom/internal/testenv"
)

// Sessions revoked, or past expires_at, by more than the grace are deleted;
// live and recently dead ones stay. Each test has its own schema, so the counts
// are exact.
func TestPruneSessionsOnPostgres(t *testing.T) {
	s, _ := requireStore(t)
	sess := s.Sessions()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	now := time.Now().UTC().Truncate(time.Microsecond)
	day := 24 * time.Hour

	ids := map[string]string{}
	add := func(name string, expires, revoked time.Time) {
		t.Helper()
		_, h, err := auth.NewToken()
		if err != nil {
			t.Fatal(err)
		}
		id := testenv.Name("ses")
		ids[name] = id
		if err := sess.Create(ctx, auth.Session{
			ID: id, TokenHash: h, UserID: "u-" + id, Method: auth.MethodDev,
			CreatedAt: now.Add(-30 * day), LastSeenAt: now.Add(-30 * day), ExpiresAt: expires, RevokedAt: revoked,
		}); err != nil {
			t.Fatalf("create %s: %v", name, err)
		}
	}
	add("live", now.Add(time.Hour), time.Time{})
	add("expired-8d", now.Add(-8*day), time.Time{})
	add("expired-6d", now.Add(-6*day), time.Time{})
	add("revoked-8d", now.Add(time.Hour), now.Add(-8*day))
	add("revoked-6d", now.Add(time.Hour), now.Add(-6*day))
	add("revoked-just-now", now.Add(time.Hour), now.Add(-time.Minute))
	add("expired-8d-and-revoked-6d", now.Add(-8*day), now.Add(-6*day))

	n, err := sess.PruneSessions(ctx, now, auth.SessionGrace)
	if err != nil {
		t.Fatalf("prune: %v", err)
	}
	if n != 3 {
		t.Fatalf("pruned %d sessions, want 3", n)
	}
	for name, wantGone := range map[string]bool{
		"live": false, "expired-6d": false, "revoked-6d": false, "revoked-just-now": false,
		"expired-8d": true, "revoked-8d": true, "expired-8d-and-revoked-6d": true,
	} {
		_, err := sess.ByID(ctx, ids[name])
		if gone := errors.Is(err, auth.ErrNotFound); gone != wantGone {
			t.Errorf("%s: gone = %v (err %v), want %v", name, gone, err, wantGone)
		}
	}
	if n, err := sess.PruneSessions(ctx, now, auth.SessionGrace); err != nil || n != 0 {
		t.Fatalf("second prune = %d, %v; want 0", n, err)
	}
}

// login_states rows at or past expires_at are deleted; live rows stay.
func TestPruneLoginStatesOnPostgres(t *testing.T) {
	s, _ := requireStore(t)
	ls := s.LoginStates()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	now := time.Now().UTC().Truncate(time.Microsecond)
	// Created "two hours ago", when none had expired, so Create's own prune
	// removes nothing; this test is about the retention job's prune.
	created := now.Add(-2 * time.Hour)

	ids := map[string]string{}
	for name, exp := range map[string]time.Time{
		"live":      now.Add(5 * time.Minute),
		"expired":   now.Add(-time.Minute),
		"long-gone": now.Add(-time.Hour),
		"at-expiry": now,
	} {
		id := testenv.Name("ls")
		ids[name] = id
		if err := ls.Create(ctx, auth.LoginState{ID: id, State: "s", Nonce: "n", Verifier: "v", ReturnPath: "/", ExpiresAt: exp}, "", created); err != nil {
			t.Fatalf("create %s: %v", name, err)
		}
	}
	n, err := ls.PruneLoginStates(ctx, now)
	if err != nil || n != 3 {
		t.Fatalf("pruned %d, %v; want 3", n, err)
	}
	if _, err := ls.Take(ctx, ids["live"], now); err != nil {
		t.Fatalf("a live row must survive the prune: %v", err)
	}
	if n, err := ls.PruneLoginStates(ctx, now); err != nil || n != 0 {
		t.Fatalf("second prune = %d, %v; want 0", n, err)
	}
}
