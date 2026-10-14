package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"piperoom/internal/auth"
)

func TestUnbindIdentity(t *testing.T) {
	ctx := context.Background()
	sessions := auth.NewMemStore()
	ids := auth.NewMemIdentities().WithSessions(sessions)
	if err := ids.Bind(ctx, "https://idp", "sub-1", "priya", "priya@example.com"); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	if err := sessions.Create(ctx, auth.Session{ID: "ses_1", TokenHash: []byte("h1"), UserID: "priya", Iss: "https://idp", Sub: "sub-1",
		Method: auth.MethodOIDC, CreatedAt: now, LastSeenAt: now, ExpiresAt: now.Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}

	var out bytes.Buffer
	if err := unbindIdentity(ctx, ids, "priya", &out); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), `unbound "priya"`) || !strings.Contains(out.String(), `revoked 1 live session(s) of "priya"`) {
		t.Fatalf("output = %q", out.String())
	}
	if got, _ := sessions.ByID(ctx, "ses_1"); got.RevokedAt.IsZero() {
		t.Fatal("the session is still live")
	}
	if _, err := ids.UserFor(ctx, "https://idp", "sub-1"); err == nil {
		t.Fatal("the row is still there")
	}

	// Nothing to delete is not an error, and says so.
	out.Reset()
	if err := unbindIdentity(ctx, ids, "priya", &out); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "nothing unbound") {
		t.Fatalf("output = %q", out.String())
	}
}

func TestIdentitiesCmdUsage(t *testing.T) {
	for _, args := range [][]string{nil, {"unbind"}, {"bind", "priya"}, {"unbind", "priya", "extra"}, {"unbind", ""}} {
		err := identitiesCmd(context.Background(), &stack{}, args, &bytes.Buffer{})
		if err == nil || !strings.Contains(err.Error(), "usage: devctl identities unbind <user>") {
			t.Errorf("args %q: err = %v, want the usage line", args, err)
		}
	}
}
