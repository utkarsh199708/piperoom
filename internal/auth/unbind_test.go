package auth_test

import (
	"context"
	"testing"
	"time"

	"piperoom/internal/auth"
)

// A-ID-5: Unbind revokes the user's live sessions and the sessions of the
// removed (iss, sub), and nobody else's.
func TestMemIdentitiesUnbindRevokesSessions(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	sessions := auth.NewMemStore()
	ids := auth.NewMemIdentities().WithSessions(sessions)
	const iss = "https://idp.example"
	if err := ids.Bind(ctx, iss, "sub-1", "priya", "p@example.com"); err != nil {
		t.Fatal(err)
	}
	add := func(id, user, i, s, method string, expires time.Time, revoked bool) {
		t.Helper()
		sess := auth.Session{ID: id, TokenHash: []byte(id), UserID: user, Iss: i, Sub: s, Method: method,
			CreatedAt: now, LastSeenAt: now, ExpiresAt: expires}
		if err := sessions.Create(ctx, sess); err != nil {
			t.Fatal(err)
		}
		if revoked {
			if err := sessions.Revoke(ctx, id, now.Add(-time.Minute)); err != nil {
				t.Fatal(err)
			}
		}
	}
	live := now.Add(time.Hour)
	add("by-user", "priya", iss, "sub-1", auth.MethodOIDC, live, false)
	add("dev-same-user", "priya", "", "", auth.MethodDev, live, false)
	add("by-pair-other-user", "mallory", iss, "sub-1", auth.MethodOIDC, live, false)
	add("other", "kabir", iss, "sub-2", auth.MethodOIDC, live, false)
	add("other-dev", "kabir", "", "", auth.MethodDev, live, false)
	add("already-revoked", "priya", iss, "sub-1", auth.MethodOIDC, live, true)
	add("expired", "priya", iss, "sub-1", auth.MethodOIDC, now.Add(-time.Second), false)

	res, err := ids.Unbind(ctx, "priya", now)
	if err != nil || res.Unbound != 1 || res.Revoked != 3 {
		t.Fatalf("Unbind = %+v, %v; want 1 binding, 3 sessions", res, err)
	}
	for id, wantRevoked := range map[string]bool{
		"by-user": true, "dev-same-user": true, "by-pair-other-user": true,
		"other": false, "other-dev": false,
	} {
		got, err := sessions.ByID(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if revoked := !got.RevokedAt.IsZero(); revoked != wantRevoked {
			t.Errorf("session %s revoked = %v, want %v", id, revoked, wantRevoked)
		}
	}

	// No binding left: only the user's own sessions are touched, and an empty
	// (iss, sub) pair never matches other users' dev sessions.
	add("late", "ghost", "", "", auth.MethodDev, live, false)
	res, err = ids.Unbind(ctx, "ghost", now)
	if err != nil || res.Unbound != 0 || res.Revoked != 1 {
		t.Fatalf("Unbind(ghost) = %+v, %v; want 0 bindings, 1 session", res, err)
	}
	if got, _ := sessions.ByID(ctx, "other-dev"); !got.RevokedAt.IsZero() {
		t.Error("an empty (iss, sub) pair revoked another user's dev session")
	}
}
