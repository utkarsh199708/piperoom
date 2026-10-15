package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"piperoom/internal/auth"
	"piperoom/internal/owners"
	"piperoom/internal/room"
	"piperoom/internal/store/memstore"
)

// The room is held to the same session rule as every request (#417): the
// server's room.Deps.SessionLive is authSettings.Liveness(), so a group
// member whose session is idle past PIPEROOM_SESSION_IDLE, or a dev session
// in oidc mode, is not a person the room can give control or hand off to.
// A zero or stub rule would accept both.
func TestRoomSessionLiveFromAuthSettings(t *testing.T) {
	settings := auth.Settings{Mode: auth.ModeOIDC, Idle: 10 * time.Minute, Max: 8 * time.Hour, ViewerGroups: []string{"viewers"}}
	d := withRoomAuth(room.Deps{}, settings)
	if d.SessionLive != settings.Liveness() || d.SessionLive == (auth.Liveness{}) {
		t.Fatalf("room SessionLive = %+v, want %+v", d.SessionLive, settings.Liveness())
	}

	path := filepath.Join(t.TempDir(), "owners.yaml")
	if err := os.WriteFile(path, []byte("pipelines:\n  - id: orders\n    topic_prefix: pg\n    source_tables: [public.orders]\n    connectors: [c1]\nlayers:\n  kafka: group:oncall\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	own, err := owners.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	now := time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC)
	sessions := auth.NewMemStore()
	add := func(user, method string, lastSeen time.Time) {
		t.Helper()
		_, hash, err := auth.NewToken()
		if err != nil {
			t.Fatal(err)
		}
		if err := sessions.Create(ctx, auth.Session{ID: "ses_" + user, TokenHash: hash, UserID: user, Name: user, Groups: []string{"oncall"},
			Method: method, CreatedAt: now.Add(-time.Hour), LastSeenAt: lastSeen, ExpiresAt: now.Add(time.Hour)}); err != nil {
			t.Fatal(err)
		}
	}
	add("u_live", auth.MethodOIDC, now.Add(-time.Minute))
	add("u_idle", auth.MethodOIDC, now.Add(-30*time.Minute))
	add("u_dev", auth.MethodDev, now.Add(-time.Minute))

	// newRoomService is the constructor main uses: the settings are a required
	// argument, so main cannot build the room without the rule.
	svc := newRoomService(room.Deps{
		Store: memstore.New(), Users: room.DefaultUsers(), Owners: own, Sessions: sessions,
		Now: func() time.Time { return now },
	}, settings, &usageRelay{})
	t.Cleanup(svc.Close)
	if _, err := svc.LookupPerson(ctx, "u_live"); err != nil {
		t.Fatalf("live session: %v", err)
	}
	for _, id := range []string{"u_idle", "u_dev"} {
		if _, err := svc.LookupPerson(ctx, id); err == nil {
			t.Errorf("%s: a session failing the auth liveness rule is live for the room", id)
		}
	}
}
