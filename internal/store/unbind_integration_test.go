//go:build integration

package store_test

import (
	"context"
	"testing"
	"time"

	"piperoom/internal/auth"
	"piperoom/internal/testenv"
)

// A-ID-5 on Postgres: Unbind deletes the row and revokes the user's live
// sessions and those of the removed (iss, sub) in one transaction.
func TestIdentitiesUnbindRevokesSessions(t *testing.T) {
	s, _ := requireStore(t)
	ids, sessions := s.Identities(), s.Sessions()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	now := time.Now().UTC().Truncate(time.Microsecond)
	iss := "https://idp.example/" + testenv.Name("iss")
	user, other, ghost := testenv.Name("user"), testenv.Name("other"), testenv.Name("ghost")
	if err := ids.Bind(ctx, iss, "sub-1", user, "u@example.com"); err != nil {
		t.Fatal(err)
	}
	add := func(name, uid, i, sub, method string, expires time.Time, revoked bool) string {
		t.Helper()
		_, hash, err := auth.NewToken()
		if err != nil {
			t.Fatal(err)
		}
		id := testenv.Name(name)
		sess := auth.Session{ID: id, TokenHash: hash, UserID: uid, Iss: i, Sub: sub, Method: method,
			CreatedAt: now, LastSeenAt: now, ExpiresAt: expires}
		if err := sessions.Create(ctx, sess); err != nil {
			t.Fatalf("create %s: %v", name, err)
		}
		if revoked {
			if err := sessions.Revoke(ctx, id, now.Add(-time.Minute)); err != nil {
				t.Fatal(err)
			}
		}
		return id
	}
	live := now.Add(time.Hour)
	byUser := add("ses-user", user, iss, "sub-1", auth.MethodOIDC, live, false)
	devSame := add("ses-dev", user, "", "", auth.MethodDev, live, false)
	byPair := add("ses-pair", other, iss, "sub-1", auth.MethodOIDC, live, false)
	bystander := add("ses-by", other, iss, "sub-2", auth.MethodOIDC, live, false)
	bystanderDev := add("ses-bydev", other, "", "", auth.MethodDev, live, false)
	add("ses-rev", user, iss, "sub-1", auth.MethodOIDC, live, true)
	add("ses-exp", user, iss, "sub-1", auth.MethodOIDC, now.Add(-time.Second), false)

	res, err := ids.Unbind(ctx, user, now)
	if err != nil || res.Unbound != 1 || res.Revoked != 3 {
		t.Fatalf("Unbind = %+v, %v; want 1 binding, 3 sessions", res, err)
	}
	for id, want := range map[string]bool{byUser: true, devSame: true, byPair: true, bystander: false, bystanderDev: false} {
		got, err := sessions.ByID(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if revoked := !got.RevokedAt.IsZero(); revoked != want {
			t.Errorf("session %s revoked = %v, want %v", id, revoked, want)
		}
	}
	if _, err := ids.UserFor(ctx, iss, "sub-1"); err == nil {
		t.Error("the identities row is still there")
	}

	// No binding: the user's own sessions go, an empty (iss, sub) never matches
	// somebody else's dev session.
	add("ses-ghost", ghost, "", "", auth.MethodDev, live, false)
	res, err = ids.Unbind(ctx, ghost, now)
	if err != nil || res.Unbound != 0 || res.Revoked != 1 {
		t.Fatalf("Unbind(ghost) = %+v, %v; want 0 bindings, 1 session", res, err)
	}
	if got, _ := sessions.ByID(ctx, bystanderDev); !got.RevokedAt.IsZero() {
		t.Error("an empty (iss, sub) pair revoked another user's dev session")
	}
}
