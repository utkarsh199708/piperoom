//go:build integration

package store_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"testing"
	"time"

	"piperoom/internal/agent"
	"piperoom/internal/alertin"
	"piperoom/internal/events"
	"piperoom/internal/room"
	"piperoom/internal/store"
	"piperoom/internal/testenv"
)

// Real Postgres: an alert body that jsonb cannot hold is refused with a 400
// before any write, so no rooms row and no event exist afterwards, while a body
// that only looks similar is stored. Run against a throwaway Postgres.
func TestAlertBodyGuardLeavesNoRowsOnPostgres(t *testing.T) {
	s, _ := requireStore(t)
	svc := room.New(room.Deps{
		Store: s,
		Users: room.DefaultUsers(),
		Run: func(ctx context.Context, _ agent.Alert, _ agent.Deps, _ agent.Options) error {
			<-ctx.Done()
			return nil
		},
	})
	t.Cleanup(svc.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	rows := func() int {
		t.Helper()
		rs, err := s.ListRooms(ctx)
		if err != nil {
			t.Fatalf("store: list rooms: %v", err)
		}
		return len(rs)
	}
	refused := func(err error) {
		t.Helper()
		var re *room.Error
		if !errors.As(err, &re) || re.Status != http.StatusBadRequest || re.Code != "validation" {
			t.Fatalf("err = %v, want a 400 validation error", err)
		}
	}

	bad := map[string]string{
		"nul escape":         `{"title":"x\u0000y","pipeline_id":"orders"}`,
		"unpaired surrogate": `{"title":"\ud83d","pipeline_id":"orders"}`,
		"invalid utf-8":      "{\"title\":\"x\xffy\",\"pipeline_id\":\"orders\"}",
	}
	for name, body := range bad {
		_, _, err := svc.CreateRoomFromAlert(ctx, "lag", "sev2", "orders", json.RawMessage(body))
		refused(err)
		_, _, err = svc.OpenAlertRoom(ctx, alertin.Alert{
			Source: alertin.Generic, Status: alertin.StatusFiring, Title: "lag", Severity: "sev2",
			PipelineID: "orders", DedupKey: "generic:" + name, Raw: json.RawMessage(body),
		})
		refused(err)
	}
	_, err := svc.CreateRoom(ctx, room.User{ID: "arjun", Name: "Arjun"}, "a\x00b", "sev2", "orders")
	refused(err)
	if n := rows(); n != 0 {
		t.Fatalf("refused bodies left %d rooms rows, want 0", n)
	}

	// A valid surrogate pair and an escaped backslash are storable.
	_, created, err := svc.CreateRoomFromAlert(ctx, "lag", "sev2", "orders",
		json.RawMessage(`{"title":"😀 \\u0000","pipeline_id":"orders"}`))
	if err != nil || !created {
		t.Fatalf("storable body: created=%v err=%v", created, err)
	}
	if n := rows(); n != 1 {
		t.Fatalf("want 1 room row, got %d", n)
	}
}

// DeleteEmptyRoom removes a rooms row that has no events (the leftover of a
// failed room.created append) and keeps a room that has events.
func TestDeleteEmptyRoom(t *testing.T) {
	s, _ := requireStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	empty := testenv.Name("empty")
	if err := s.CreateRoom(ctx, store.Room{ID: empty, Title: "t", Severity: "sev2", PipelineID: "orders", Status: "open", OpenedAt: time.Now().UTC()}); err != nil {
		t.Fatalf("store: create room: %v", err)
	}
	if err := s.DeleteEmptyRoom(ctx, empty); err != nil {
		t.Fatalf("store: delete empty room: %v", err)
	}
	if _, err := s.GetRoom(ctx, empty); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("store: get deleted room: err = %v, want ErrNotFound", err)
	}
	if err := s.DeleteEmptyRoom(ctx, empty); err != nil {
		t.Fatalf("store: deleting a missing room must be a no-op: %v", err)
	}

	full := createTestRoom(t, s)
	if _, err := s.Append(ctx, full, events.RoomCreated, events.Actor{Kind: events.ActorSystem, ID: "system", Name: "System"}, events.RoomCreatedPayload{Title: "t"}); err != nil {
		t.Fatalf("store: append: %v", err)
	}
	if err := s.DeleteEmptyRoom(ctx, full); !errors.Is(err, store.ErrRoomNotEmpty) {
		t.Fatalf("store: delete room with events: err = %v, want ErrRoomNotEmpty", err)
	}
	if _, err := s.GetRoom(ctx, full); err != nil {
		t.Fatalf("store: room with events must stay: %v", err)
	}
}
