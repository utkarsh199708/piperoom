//go:build integration

package store_test

import (
	"context"
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
	idle := time.Hour
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

	got, err := sess.LiveSessions(ctx, now, idle, 100)
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
	if lim, err := sess.LiveSessions(ctx, now, idle, 1); err != nil || len(lim) != 1 || lim[0].UserID != a {
		t.Fatalf("limit 1 = %+v, %v", lim, err)
	}
	// The email also comes back through the token-hash lookup.
	byID, err := sess.ByID(ctx, newA)
	if err != nil || byID.Email != "a@example.com" {
		t.Fatalf("ByID = %+v, %v", byID, err)
	}
}
