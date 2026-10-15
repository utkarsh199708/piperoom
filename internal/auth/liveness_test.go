package auth_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"piperoom/internal/auth"
)

// #417: one rule decides whether a stored session may be used, for requests
// and for the room's person lookup alike.
func TestLivenessUsable(t *testing.T) {
	now := time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC)
	sess := func(method string, created, expires time.Time) auth.Session {
		return auth.Session{Method: method, CreatedAt: created, LastSeenAt: now.Add(-time.Minute), ExpiresAt: expires}
	}
	fresh := sess(auth.MethodOIDC, now.Add(-time.Hour), now.Add(time.Hour))
	devFresh := sess(auth.MethodDev, now.Add(-time.Hour), now.Add(time.Hour))
	overMax := sess(auth.MethodOIDC, now.Add(-13*time.Hour), now.Add(time.Hour))
	expired := sess(auth.MethodOIDC, now.Add(-time.Hour), now.Add(-time.Second))
	revoked := fresh
	revoked.RevokedAt = now.Add(-time.Second)
	idle := fresh
	idle.LastSeenAt = now.Add(-9 * time.Hour)

	oidc := auth.Settings{Mode: auth.ModeOIDC, Idle: 8 * time.Hour, Max: 12 * time.Hour}.Liveness()
	dev := auth.Settings{Mode: auth.ModeDev, Idle: 8 * time.Hour, Max: 12 * time.Hour}.Liveness()
	cases := []struct {
		name string
		live auth.Liveness
		s    auth.Session
		want bool
	}{
		{"oidc: usable oidc session", oidc, fresh, true},
		{"oidc: dev-login session", oidc, devFresh, false},
		{"oidc: past the current max", oidc, overMax, false},
		{"oidc: expired", oidc, expired, false},
		{"oidc: revoked", oidc, revoked, false},
		{"oidc: idle", oidc, idle, false},
		{"dev: dev-login session", dev, devFresh, true},
		{"dev: oidc session", dev, fresh, true},
		{"dev: past the current max", dev, overMax, false},
		{"dev: expired", dev, expired, false},
		{"no max: past 12 h", auth.Liveness{Idle: 8 * time.Hour}, overMax, true},
	}
	for _, c := range cases {
		if got := c.live.Usable(c.s, now); got != c.want {
			t.Errorf("%s: Usable = %v, want %v", c.name, got, c.want)
		}
	}
}

// MemStore applies the rule before choosing the newest session, so a newer
// unusable row neither shadows an older usable one nor is listed.
func TestMemStoreLiveness(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC)
	m := auth.NewMemStore()
	add := func(id, user, name, method string, created time.Time) {
		t.Helper()
		_, hash, err := auth.NewToken()
		if err != nil {
			t.Fatal(err)
		}
		if err := m.Create(ctx, auth.Session{ID: id, TokenHash: hash, UserID: user, Name: name, Method: method,
			CreatedAt: created, LastSeenAt: now, ExpiresAt: now.Add(time.Hour)}); err != nil {
			t.Fatal(err)
		}
	}
	add("s1", "u_real", "Real", auth.MethodOIDC, now.Add(-time.Hour))
	add("s2", "u_real", "Ghost", auth.MethodDev, now) // newer, dev
	add("s3", "u_dev", "Dev", auth.MethodDev, now)
	add("s4", "u_old", "Old", auth.MethodOIDC, now.Add(-13*time.Hour))

	oidc := auth.Liveness{Idle: 8 * time.Hour, Max: 12 * time.Hour, Method: auth.MethodOIDC}
	if s, err := m.LiveByUserID(ctx, "u_real", now, oidc); err != nil || s.ID != "s1" {
		t.Fatalf("LiveByUserID(u_real) = %q, %v; want s1", s.ID, err)
	}
	for _, u := range []string{"u_dev", "u_old"} {
		if _, err := m.LiveByUserID(ctx, u, now, oidc); !errors.Is(err, auth.ErrNotFound) {
			t.Errorf("LiveByUserID(%s) err = %v, want ErrNotFound", u, err)
		}
	}
	list, err := m.LiveSessions(ctx, now, oidc, 0)
	if err != nil || len(list) != 1 || list[0].ID != "s1" {
		t.Fatalf("LiveSessions(oidc) = %+v, %v; want only s1", list, err)
	}

	// Dev mode is unchanged: any method, so the newest row wins.
	dev := auth.Liveness{Idle: 8 * time.Hour, Max: 12 * time.Hour}
	if s, err := m.LiveByUserID(ctx, "u_real", now, dev); err != nil || s.ID != "s2" {
		t.Fatalf("dev LiveByUserID(u_real) = %q, %v; want s2", s.ID, err)
	}
	if list, err := m.LiveSessions(ctx, now, dev, 0); err != nil || len(list) != 2 {
		t.Fatalf("dev LiveSessions = %+v, %v; want u_dev and u_real", list, err)
	}
}
