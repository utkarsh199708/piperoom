//go:build integration

package store_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"piperoom/internal/events"
	"piperoom/internal/store"
	"piperoom/internal/testenv"
)

// createAlertRoom makes a room and its room.created event holding dedupKey.
func createAlertRoom(ctx context.Context, t *testing.T, s *store.PG, pipeline, dedupKey string, openedAt time.Time) string {
	t.Helper()
	id := testenv.Name("store")
	if err := s.CreateRoom(ctx, store.Room{
		ID: id, Title: "alert", Severity: "sev2", PipelineID: pipeline, Status: "open", OpenedAt: openedAt,
	}); err != nil {
		t.Fatalf("store: create room: %v", err)
	}
	sys := events.Actor{Kind: events.ActorSystem, ID: "system", Name: "System"}
	if _, err := s.Append(ctx, id, events.RoomCreated, sys, events.RoomCreatedPayload{
		Title: "alert", Severity: "sev2", PipelineID: pipeline, AlertSource: "watcher", DedupKey: dedupKey,
	}); err != nil {
		t.Fatalf("store: append room.created: %v", err)
	}
	return id
}

// LastRoomByDedupKey finds the newest room whose room.created holds the key,
// open or closed, and reports ErrNotFound for an unknown or empty key. Rooms
// without a dedup_key, and other keys, never match.
func TestLastRoomByDedupKey(t *testing.T) {
	s, _ := requireStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	key := "orders|" + testenv.Name("watcher")
	base := time.Now().UTC().Truncate(time.Millisecond)

	if _, err := s.LastRoomByDedupKey(ctx, key); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("before any room: err = %v, want ErrNotFound", err)
	}
	if _, err := s.LastRoomByDedupKey(ctx, ""); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("empty key: err = %v, want ErrNotFound", err)
	}

	// Noise: a room with no key, and a room under another key.
	createTestRoom(t, s)
	createAlertRoom(ctx, t, s, "orders", "orders|other-"+testenv.Name("k"), base)

	older := createAlertRoom(ctx, t, s, "orders", key, base.Add(-2*time.Hour))
	got, err := s.LastRoomByDedupKey(ctx, key)
	if err != nil || got.ID != older || got.Status != "open" || got.ClosedAt != nil {
		t.Fatalf("one open room: %+v err=%v, want open %s", got, err, older)
	}

	closedAt := base.Add(-time.Hour)
	if err := s.CloseRoom(ctx, older, closedAt); err != nil {
		t.Fatalf("close: %v", err)
	}
	got, err = s.LastRoomByDedupKey(ctx, key)
	if err != nil || got.ID != older || got.Status != "closed" || got.ClosedAt == nil || !got.ClosedAt.Equal(closedAt) {
		t.Fatalf("closed room: %+v err=%v, want closed %s at %v", got, err, older, closedAt)
	}

	newer := createAlertRoom(ctx, t, s, "orders", key, base)
	got, err = s.LastRoomByDedupKey(ctx, key)
	if err != nil || got.ID != newer || got.Status != "open" || got.PipelineID != "orders" {
		t.Fatalf("two rooms: %+v err=%v, want newest %s", got, err, newer)
	}
}
