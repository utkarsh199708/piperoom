//go:build integration

package store_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"piperoom/internal/auth"
	"piperoom/internal/testenv"
)

func TestLoginStatesStore(t *testing.T) {
	s, _ := requireStore(t)
	ls := s.LoginStates()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	now := time.Now().UTC().Truncate(time.Microsecond)

	row := func(id string, expires time.Time) auth.LoginState {
		return auth.LoginState{ID: id, State: "state-" + id, Nonce: "nonce-" + id, Verifier: "verifier-" + id, ReturnPath: "/rooms/r_x?tab=1", ExpiresAt: expires}
	}

	t.Run("round trip and single use", func(t *testing.T) {
		id := testenv.Name("ls")
		want := row(id, now.Add(auth.LoginStateTTL))
		if err := ls.Create(ctx, want, "", now); err != nil {
			t.Fatalf("create: %v", err)
		}
		got, err := ls.Take(ctx, id, now)
		if err != nil || got != want {
			t.Fatalf("take = %+v, %v; want %+v", got, err, want)
		}
		// DELETE ... RETURNING: the row is gone, so a replayed callback finds nothing.
		if _, err := ls.Take(ctx, id, now); !errors.Is(err, auth.ErrLoginStateNotFound) {
			t.Fatalf("second take = %v, want ErrLoginStateNotFound", err)
		}
	})

	t.Run("an expired row is not returned", func(t *testing.T) {
		id := testenv.Name("ls")
		if err := ls.Create(ctx, row(id, now.Add(-time.Second)), "", now.Add(-time.Hour)); err != nil {
			t.Fatal(err)
		}
		if _, err := ls.Take(ctx, id, now); !errors.Is(err, auth.ErrLoginStateNotFound) {
			t.Fatalf("take of an expired row = %v", err)
		}
	})

	t.Run("create prunes expired rows and replaces the cookie's old row", func(t *testing.T) {
		oldExpired := testenv.Name("ls")
		old := testenv.Name("ls")
		fresh := testenv.Name("ls")
		if err := ls.Create(ctx, row(oldExpired, now.Add(-time.Minute)), "", now.Add(-time.Hour)); err != nil {
			t.Fatal(err)
		}
		if err := ls.Create(ctx, row(old, now.Add(time.Minute)), "", now); err != nil {
			t.Fatal(err)
		}
		if err := ls.Create(ctx, row(fresh, now.Add(time.Minute)), old, now); err != nil {
			t.Fatal(err)
		}
		if _, err := ls.Take(ctx, old, now); !errors.Is(err, auth.ErrLoginStateNotFound) {
			t.Fatalf("the replaced row is still there: %v", err)
		}
		// The expired row was deleted by the prune, not only hidden: a Take with a
		// clock before its expiry would otherwise still find it.
		if _, err := ls.Take(ctx, oldExpired, now.Add(-2*time.Minute)); !errors.Is(err, auth.ErrLoginStateNotFound) {
			t.Fatalf("the expired row survived the prune: %v", err)
		}
		if _, err := ls.Take(ctx, fresh, now); err != nil {
			t.Fatalf("the fresh row: %v", err)
		}
	})

	t.Run("a duplicate id fails and keeps the old row", func(t *testing.T) {
		id := testenv.Name("ls")
		other := testenv.Name("ls")
		_ = ls.Create(ctx, row(id, now.Add(time.Minute)), "", now)
		_ = ls.Create(ctx, row(other, now.Add(time.Minute)), "", now)
		// Replacing "other" while inserting an id that already exists must not
		// lose "other": the delete and the insert are one transaction.
		if err := ls.Create(ctx, row(id, now.Add(time.Minute)), other, now); err == nil {
			t.Fatal("a duplicate id was inserted")
		}
		if _, err := ls.Take(ctx, other, now); err != nil {
			t.Fatalf("the row to replace was lost with the failed insert: %v", err)
		}
	})

	t.Run("concurrent callbacks: exactly one wins", func(t *testing.T) {
		id := testenv.Name("ls")
		if err := ls.Create(ctx, row(id, now.Add(time.Minute)), "", now); err != nil {
			t.Fatal(err)
		}
		var wins atomic.Int32
		var wg sync.WaitGroup
		for i := 0; i < 8; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if _, err := ls.Take(ctx, id, now); err == nil {
					wins.Add(1)
				}
			}()
		}
		wg.Wait()
		if wins.Load() != 1 {
			t.Fatalf("%d callers got the row, want exactly 1", wins.Load())
		}
	})
}

func TestIdentitiesStore(t *testing.T) {
	s, _ := requireStore(t)
	ids := s.Identities()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	iss := "https://idp.example/" + testenv.Name("iss")
	user1, user2 := testenv.Name("user"), testenv.Name("user")

	if _, err := ids.UserFor(ctx, iss, "sub-1"); !errors.Is(err, auth.ErrIdentityNotFound) {
		t.Fatalf("empty: %v", err)
	}
	if err := ids.Bind(ctx, iss, "sub-1", user1, "a@example.com"); err != nil {
		t.Fatalf("bind: %v", err)
	}
	if got, err := ids.UserFor(ctx, iss, "sub-1"); err != nil || got != user1 {
		t.Fatalf("UserFor = %q, %v", got, err)
	}
	// The same pair for the same user again is fine.
	if err := ids.Bind(ctx, iss, "sub-1", user1, "a@example.com"); err != nil {
		t.Fatalf("rebind: %v", err)
	}
	// A recycled email: the user is already bound to another sub.
	if err := ids.Bind(ctx, iss, "sub-2", user1, "a@example.com"); !errors.Is(err, auth.ErrIdentityConflict) {
		t.Fatalf("second sub for the same user: %v, want ErrIdentityConflict", err)
	}
	// A pair that belongs to someone else.
	if err := ids.Bind(ctx, iss, "sub-1", user2, "b@example.com"); !errors.Is(err, auth.ErrIdentityConflict) {
		t.Fatalf("a taken pair for another user: %v, want ErrIdentityConflict", err)
	}
	// The same sub under another issuer is another identity.
	if err := ids.Bind(ctx, iss+"/other", "sub-1", user2, "b@example.com"); err != nil {
		t.Fatalf("other issuer: %v", err)
	}
	// Unbind frees the user for a new account.
	if r, err := ids.Unbind(ctx, user1, time.Now()); err != nil || r.Unbound != 1 {
		t.Fatalf("unbind = %+v, %v", r, err)
	}
	if r, err := ids.Unbind(ctx, user1, time.Now()); err != nil || r.Unbound != 0 {
		t.Fatalf("second unbind = %+v, %v", r, err)
	}
	if _, err := ids.UserFor(ctx, iss, "sub-1"); !errors.Is(err, auth.ErrIdentityNotFound) {
		t.Fatalf("after unbind: %v", err)
	}
	if err := ids.Bind(ctx, iss, "sub-2", user1, "a@example.com"); err != nil {
		t.Fatalf("bind after unbind: %v", err)
	}

	// Two first logins of the same person racing: only one pair binds.
	user3 := testenv.Name("user")
	var ok atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			sub := "race-" + string(rune('a'+i))
			if err := ids.Bind(ctx, iss, sub, user3, "c@example.com"); err == nil {
				ok.Add(1)
			} else if !errors.Is(err, auth.ErrIdentityConflict) {
				t.Errorf("race bind: %v", err)
			}
		}(i)
	}
	wg.Wait()
	if ok.Load() != 1 {
		t.Fatalf("%d racing binds won, want exactly 1", ok.Load())
	}
}

func TestSessionsLiveByUserID(t *testing.T) {
	s, _ := requireStore(t)
	sess := s.Sessions()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	now := time.Now().UTC().Truncate(time.Microsecond)
	user := testenv.Name("user")

	add := func(created, lastSeen, expires time.Time, revoke bool) string {
		t.Helper()
		_, hash, _ := auth.NewToken()
		id := testenv.Name("ses")
		if err := sess.Create(ctx, auth.Session{
			ID: id, TokenHash: hash, UserID: user, Name: id, Groups: []string{"g"}, Method: auth.MethodOIDC,
			CreatedAt: created, LastSeenAt: lastSeen, ExpiresAt: expires,
		}); err != nil {
			t.Fatal(err)
		}
		if revoke {
			_ = sess.Revoke(ctx, id, now)
		}
		return id
	}
	idle := time.Hour
	if _, err := sess.LiveByUserID(ctx, user, now, idle); !errors.Is(err, auth.ErrNotFound) {
		t.Fatalf("no sessions: %v", err)
	}
	add(now.Add(-5*time.Hour), now.Add(-4*time.Hour), now.Add(time.Hour), false) // idle too long
	add(now.Add(-4*time.Hour), now, now.Add(-time.Minute), false)                // past the absolute cap
	add(now.Add(-3*time.Hour), now, now.Add(time.Hour), true)                    // revoked
	if _, err := sess.LiveByUserID(ctx, user, now, idle); !errors.Is(err, auth.ErrNotFound) {
		t.Fatalf("only dead sessions: %v, want ErrNotFound", err)
	}
	older := add(now.Add(-2*time.Hour), now.Add(-time.Minute), now.Add(time.Hour), false)
	newer := add(now.Add(-time.Hour), now.Add(-time.Minute), now.Add(time.Hour), false)
	got, err := sess.LiveByUserID(ctx, user, now, idle)
	if err != nil || got.ID != newer {
		t.Fatalf("live = %q, %v; want the newest live session %q (older %q)", got.ID, err, newer, older)
	}
	if _, err := sess.LiveByUserID(ctx, testenv.Name("user"), now, idle); !errors.Is(err, auth.ErrNotFound) {
		t.Fatalf("other user: %v", err)
	}
}
