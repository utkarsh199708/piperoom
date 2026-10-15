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

// LiveSessions returns the newest live session of each user, with the verified
// email that sessions.email keeps for GET /api/people.
func TestSessionsLiveSessionsAndEmail(t *testing.T) {
	s, _ := requireStore(t)
	sess := s.Sessions()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	now := time.Now().UTC().Truncate(time.Microsecond)
	live := auth.Liveness{Idle: time.Hour}
	a, b, dead := "a-"+testenv.Name("user"), "b-"+testenv.Name("user"), "c-"+testenv.Name("user")

	add := func(user, email string, created, lastSeen, expires time.Time, revoke bool) string {
		t.Helper()
		_, hash, _ := auth.NewToken()
		id := testenv.Name("ses")
		if err := sess.Create(ctx, auth.Session{
			ID: id, TokenHash: hash, UserID: user, Name: user, Email: email, Method: auth.MethodOIDC,
			CreatedAt: created, LastSeenAt: lastSeen, ExpiresAt: expires,
		}); err != nil {
			t.Fatal(err)
		}
		if revoke {
			_ = sess.Revoke(ctx, id, now)
		}
		return id
	}
	add(a, "old@example.com", now.Add(-2*time.Hour), now.Add(-time.Minute), now.Add(time.Hour), false)
	newA := add(a, "a@example.com", now.Add(-time.Hour), now.Add(-time.Minute), now.Add(time.Hour), false)
	newB := add(b, "", now.Add(-time.Minute), now, now.Add(time.Hour), false)
	add(dead, "dead@example.com", now.Add(-time.Hour), now, now.Add(time.Hour), true)

	got, err := sess.LiveSessions(ctx, now, live, 100)
	if err != nil {
		t.Fatal(err)
	}
	// Other tests share nothing (own schema), so these are all the live sessions.
	if len(got) != 2 || got[0].ID != newA || got[1].ID != newB {
		t.Fatalf("live sessions = %+v, want [%s %s] ordered by user id", got, newA, newB)
	}
	if got[0].Email != "a@example.com" || got[1].Email != "" {
		t.Fatalf("emails = %q, %q", got[0].Email, got[1].Email)
	}
	if lim, err := sess.LiveSessions(ctx, now, live, 1); err != nil || len(lim) != 1 || lim[0].UserID != a {
		t.Fatalf("limit 1 = %+v, %v", lim, err)
	}
	// The email also comes back through the token-hash lookup.
	byID, err := sess.ByID(ctx, newA)
	if err != nil || byID.Email != "a@example.com" {
		t.Fatalf("ByID = %+v, %v", byID, err)
	}
}

// #417: the SQL applies auth.Liveness. In oidc mode a newer dev-login row does
// not shadow a user's oidc session, and neither it nor a row past the current
// max is listed or counted under the LIMIT. In dev mode any method is live.
func TestSessionsLivenessRule(t *testing.T) {
	s, _ := requireStore(t)
	sess := s.Sessions()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	now := time.Now().UTC().Truncate(time.Microsecond)
	own, dev, old := "a-"+testenv.Name("user"), "b-"+testenv.Name("user"), "c-"+testenv.Name("user")

	add := func(user, method string, created time.Time) string {
		t.Helper()
		_, hash, _ := auth.NewToken()
		id := testenv.Name("ses")
		if err := sess.Create(ctx, auth.Session{
			ID: id, TokenHash: hash, UserID: user, Name: user, Method: method,
			CreatedAt: created, LastSeenAt: now, ExpiresAt: now.Add(time.Hour),
		}); err != nil {
			t.Fatal(err)
		}
		return id
	}
	oidcRow := add(own, auth.MethodOIDC, now.Add(-time.Hour))
	devRow := add(own, auth.MethodDev, now.Add(-time.Minute))
	add(dev, auth.MethodDev, now.Add(-time.Minute))
	add(old, auth.MethodOIDC, now.Add(-13*time.Hour))

	oidc := auth.Liveness{Idle: time.Hour, Max: 12 * time.Hour, Method: auth.MethodOIDC}
	if got, err := sess.LiveByUserID(ctx, own, now, oidc); err != nil || got.ID != oidcRow {
		t.Fatalf("LiveByUserID oidc = %q, %v; want the oidc row %s", got.ID, err, oidcRow)
	}
	for _, u := range []string{dev, old} {
		if _, err := sess.LiveByUserID(ctx, u, now, oidc); !errors.Is(err, auth.ErrNotFound) {
			t.Fatalf("LiveByUserID(%s) oidc err = %v, want ErrNotFound", u, err)
		}
	}
	if got, err := sess.LiveSessions(ctx, now, oidc, 1); err != nil || len(got) != 1 || got[0].ID != oidcRow {
		t.Fatalf("LiveSessions oidc limit 1 = %+v, %v; want only %s", got, err, oidcRow)
	}
	if got, err := sess.LiveSessions(ctx, now, oidc, 100); err != nil || len(got) != 1 {
		t.Fatalf("LiveSessions oidc = %+v, %v; want one", got, err)
	}

	devMode := auth.Liveness{Idle: time.Hour, Max: 12 * time.Hour}
	if got, err := sess.LiveByUserID(ctx, own, now, devMode); err != nil || got.ID != devRow {
		t.Fatalf("LiveByUserID dev = %q, %v; want the newest row %s", got.ID, err, devRow)
	}
	if got, err := sess.LiveSessions(ctx, now, devMode, 100); err != nil || len(got) != 2 {
		t.Fatalf("LiveSessions dev = %+v, %v; want %s and %s", got, err, own, dev)
	}
}
