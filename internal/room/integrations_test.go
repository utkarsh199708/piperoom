package room_test

import (
	"context"
	"strings"
	"testing"

	"piperoom/internal/events"
	"piperoom/internal/integrations/spec"
	"piperoom/internal/room"
)

func TestIntegrationMessage(t *testing.T) {
	for _, c := range []struct {
		e    spec.AuditEntry
		want string
		ok   bool
	}{
		{spec.AuditEntry{Action: spec.AuditUpdated, Actor: "priya", Changed: []string{"password", "url"}}, "Kafka Connect integration updated by priya (password, url)", true},
		{spec.AuditEntry{Action: spec.AuditDisabled, Actor: "priya", Changed: []string{}}, "Kafka Connect integration disabled by priya", true},
		{spec.AuditEntry{Action: spec.AuditTested, Actor: "priya"}, "", false},
	} {
		got, ok := room.IntegrationMessage(c.e, "Kafka Connect")
		if got != c.want || ok != c.ok {
			t.Fatalf("IntegrationMessage(%v) = %q, %v", c.e.Action, got, ok)
		}
	}
}

func systemMessages(t *testing.T, svc *room.Service, roomID string) []string {
	t.Helper()
	evs, _, err := svc.Events(context.Background(), roomID, 0, 500)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, e := range evs {
		if e.Type == events.AgentMessage && e.Actor.Kind == events.ActorSystem {
			out = append(out, string(e.Payload))
		}
	}
	return out
}

func TestIntegrationChangedPostsInOpenRoomsOnly(t *testing.T) {
	svc, _, _ := newTestService(t)
	ctx := context.Background()
	arjun, _ := svc.LookupUser("arjun")
	open, err := svc.CreateRoom(ctx, arjun, "open", "sev2", "orders")
	if err != nil {
		t.Fatal(err)
	}
	closed, err := svc.CreateRoom(ctx, arjun, "closed", "sev2", "orders")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.CloseRoom(ctx, arjun, closed.ID, "done"); err != nil {
		t.Fatal(err)
	}
	e := spec.AuditEntry{WorkspaceID: spec.DefaultWorkspace, IntegrationID: "int_1", Action: spec.AuditUpdated, Actor: "priya", Changed: []string{"url"}}
	svc.IntegrationChanged(ctx, "other-ws", e, "Kafka Connect")
	if n := len(systemMessages(t, svc, open.ID)); n != 0 {
		t.Fatalf("another workspace's change was posted: %d", n)
	}
	svc.IntegrationChanged(ctx, spec.DefaultWorkspace, e, "Kafka Connect")
	got := systemMessages(t, svc, open.ID)
	if len(got) != 1 || !strings.Contains(got[0], "Kafka Connect integration updated by priya (url)") {
		t.Fatalf("open room: %v", got)
	}
	for _, m := range systemMessages(t, svc, closed.ID) {
		if strings.Contains(m, "integration") {
			t.Fatalf("closed room got the notice: %v", m)
		}
	}
}

// A request cancelled before the notice is posted still reaches every open
// room: the change is already saved.
func TestIntegrationChangedIgnoresCancelledRequest(t *testing.T) {
	svc, _, _ := newTestService(t)
	arjun, _ := svc.LookupUser("arjun")
	var ids []string
	for _, title := range []string{"a", "b", "c"} {
		r, err := svc.CreateRoom(context.Background(), arjun, title, "sev2", "orders")
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, r.ID)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	e := spec.AuditEntry{WorkspaceID: spec.DefaultWorkspace, IntegrationID: "int_1", Action: spec.AuditDisabled, Actor: "priya"}
	svc.IntegrationChanged(ctx, spec.DefaultWorkspace, e, "Kafka")
	for _, id := range ids {
		if got := systemMessages(t, svc, id); len(got) != 1 {
			t.Fatalf("room %s: %v", id, got)
		}
	}
}
