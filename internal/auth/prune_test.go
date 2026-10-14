package auth_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"piperoom/internal/auth"
)

func TestMemStorePruneSessions(t *testing.T) {
	ctx := context.Background()
	m := auth.NewMemStore()
	now := time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC)
	day := 24 * time.Hour

	hashes := map[string][]byte{}
	add := func(id string, expires time.Time, revoked time.Time) {
		t.Helper()
		_, h, err := auth.NewToken()
		if err != nil {
			t.Fatal(err)
		}
		hashes[id] = h
		s := auth.Session{ID: id, TokenHash: h, UserID: "u-" + id, Method: auth.MethodDev,
			CreatedAt: now.Add(-30 * day), LastSeenAt: now.Add(-30 * day), ExpiresAt: expires, RevokedAt: revoked}
		if err := m.Create(ctx, s); err != nil {
			t.Fatal(err)
		}
	}
	add("live", now.Add(time.Hour), time.Time{})
	add("expired-8d", now.Add(-8*day), time.Time{})
	add("expired-6d", now.Add(-6*day), time.Time{})
	add("revoked-8d", now.Add(time.Hour), now.Add(-8*day))
	add("revoked-6d", now.Add(time.Hour), now.Add(-6*day))
	add("revoked-just-now", now.Add(time.Hour), now.Add(-time.Minute))
	add("expired-8d-and-revoked-6d", now.Add(-8*day), now.Add(-6*day))

	n, err := m.PruneSessions(ctx, now, auth.SessionGrace)
	if err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Fatalf("pruned %d sessions, want 3", n)
	}
	for id, wantGone := range map[string]bool{
		"live": false, "expired-6d": false, "revoked-6d": false, "revoked-just-now": false,
		"expired-8d": true, "revoked-8d": true, "expired-8d-and-revoked-6d": true,
	} {
		_, byID := m.ByID(ctx, id)
		_, byHash := m.ByTokenHash(ctx, hashes[id])
		if gone := errors.Is(byID, auth.ErrNotFound); gone != wantGone || errors.Is(byHash, auth.ErrNotFound) != wantGone {
			t.Errorf("%s: gone = %v, want %v", id, gone, wantGone)
		}
	}
	if n, _ := m.PruneSessions(ctx, now, auth.SessionGrace); n != 0 {
		t.Fatalf("second prune deleted %d, want 0", n)
	}
}

func TestMemLoginStatesPrune(t *testing.T) {
	ctx := context.Background()
	m := auth.NewMemLoginStates()
	now := time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC)
	// All rows are created "an hour ago", when none of them had expired, so
	// Create's own prune removes nothing.
	created := now.Add(-time.Hour)
	for id, exp := range map[string]time.Time{
		"live":       now.Add(5 * time.Minute),
		"just-live":  now.Add(time.Nanosecond),
		"at-expiry":  now,
		"long-gone":  now.Add(-30 * time.Minute),
		"just-gone2": now.Add(-time.Second),
	} {
		if err := m.Create(ctx, auth.LoginState{ID: id, ExpiresAt: exp}, "", created); err != nil {
			t.Fatal(err)
		}
	}
	n, err := m.PruneLoginStates(ctx, now)
	if err != nil || n != 3 {
		t.Fatalf("pruned %d, %v; want 3", n, err)
	}
	if m.Len() != 2 {
		t.Fatalf("%d rows left, want 2", m.Len())
	}
	if _, err := m.Take(ctx, "live", now.Add(-time.Minute)); err != nil {
		t.Fatalf("a live row must survive: %v", err)
	}
}
