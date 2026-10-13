package memstore

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"piperoom/internal/events"
	"piperoom/internal/store"
)

func roomAt(id string, opened time.Time) store.Room {
	return store.Room{ID: id, Title: id, Severity: "sev2", PipelineID: "orders", Status: "open", OpenedAt: opened}
}

func TestCreateRoomRules(t *testing.T) {
	ctx := context.Background()
	s := New()
	at := time.Date(2026, 10, 12, 9, 0, 0, 0, time.UTC)

	if err := s.CreateRoom(ctx, roomAt("r1", at)); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := s.CreateRoom(ctx, roomAt("r1", at)); err == nil {
		t.Fatal("duplicate room id was accepted")
	}
	// A NUL byte cannot be stored in Postgres text; the in-memory store must
	// refuse it the same way and must not keep a half-made room.
	bad := roomAt("r2", at)
	bad.Title = "bad\x00title"
	if err := s.CreateRoom(ctx, bad); err == nil {
		t.Fatal("unstorable title was accepted")
	}
	if _, err := s.GetRoom(ctx, "r2"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("refused room is visible: err = %v", err)
	}

	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if err := s.CreateRoom(cancelled, roomAt("r3", at)); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled ctx: err = %v, want context.Canceled", err)
	}
}

func TestCreateRoomStartsAtSeqZero(t *testing.T) {
	s := New()
	r := roomAt("r1", time.Now())
	r.LastSeq = 17 // a caller cannot pre-set the sequence
	if err := s.CreateRoom(context.Background(), r); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetRoom(context.Background(), "r1")
	if err != nil {
		t.Fatal(err)
	}
	if got.LastSeq != 0 {
		t.Fatalf("LastSeq = %d, want 0", got.LastSeq)
	}
}

func TestGetRoomUnknown(t *testing.T) {
	if _, err := New().GetRoom(context.Background(), "nope"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("err = %v, want store.ErrNotFound", err)
	}
}

func TestListRoomsNewestFirstThenID(t *testing.T) {
	ctx := context.Background()
	s := New()
	old := time.Date(2026, 10, 10, 8, 0, 0, 0, time.UTC)
	newer := time.Date(2026, 10, 12, 8, 0, 0, 0, time.UTC)
	for _, r := range []store.Room{
		roomAt("b-old", old),
		roomAt("a-new", newer),
		roomAt("c-new", newer), // same opened_at as a-new: id breaks the tie
	} {
		if err := s.CreateRoom(ctx, r); err != nil {
			t.Fatal(err)
		}
	}
	rooms, err := s.ListRooms(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	for _, r := range rooms {
		ids = append(ids, r.ID)
	}
	if want := []string{"a-new", "c-new", "b-old"}; !slices.Equal(ids, want) {
		t.Fatalf("order = %v, want %v", ids, want)
	}
}

func TestDeleteEmptyRoom(t *testing.T) {
	ctx := context.Background()
	s := New()
	at := time.Date(2026, 10, 12, 9, 0, 0, 0, time.UTC)
	for _, id := range []string{"empty", "busy"} {
		if err := s.CreateRoom(ctx, roomAt(id, at)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.Append(ctx, "busy", events.RoomCreated, testActor(), events.RoomCreatedPayload{Title: "busy"}); err != nil {
		t.Fatal(err)
	}

	if err := s.DeleteEmptyRoom(ctx, "empty"); err != nil {
		t.Fatalf("delete empty: %v", err)
	}
	if _, err := s.GetRoom(ctx, "empty"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("empty room still present: %v", err)
	}

	err := s.DeleteEmptyRoom(ctx, "busy")
	if !errors.Is(err, store.ErrRoomNotEmpty) {
		t.Fatalf("delete room with events: err = %v, want store.ErrRoomNotEmpty", err)
	}
	if _, err := s.GetRoom(ctx, "busy"); err != nil {
		t.Fatalf("room with events was removed: %v", err)
	}

	if err := s.DeleteEmptyRoom(ctx, "never-existed"); err != nil {
		t.Fatalf("delete unknown room: err = %v, want nil (idempotent)", err)
	}
}

func TestLastRoomByDedupKey(t *testing.T) {
	ctx := context.Background()
	s := New()
	older := time.Date(2026, 10, 10, 8, 0, 0, 0, time.UTC)
	newer := time.Date(2026, 10, 12, 8, 0, 0, 0, time.UTC)

	open := func(id string, at time.Time, dedup string) {
		t.Helper()
		if err := s.CreateRoom(ctx, roomAt(id, at)); err != nil {
			t.Fatal(err)
		}
		if _, err := s.Append(ctx, id, events.RoomCreated, testActor(), events.RoomCreatedPayload{Title: id, DedupKey: dedup}); err != nil {
			t.Fatal(err)
		}
	}
	open("first", older, "watcher:orders")
	open("second", newer, "watcher:orders")
	open("other", newer, "watcher:payments")

	got, err := s.LastRoomByDedupKey(ctx, "watcher:orders")
	if err != nil {
		t.Fatalf("LastRoomByDedupKey: %v", err)
	}
	if got.ID != "second" {
		t.Fatalf("got room %q, want the newest with the key (second)", got.ID)
	}

	for _, key := range []string{"watcher:unknown", ""} {
		if _, err := s.LastRoomByDedupKey(ctx, key); !errors.Is(err, store.ErrNotFound) {
			t.Fatalf("key %q: err = %v, want store.ErrNotFound", key, err)
		}
	}

	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := s.LastRoomByDedupKey(cancelled, "watcher:orders"); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled ctx: err = %v, want context.Canceled", err)
	}
}

func TestUpdateDriverAndCloseRoom(t *testing.T) {
	ctx := context.Background()
	s := New()
	at := time.Date(2026, 10, 12, 9, 0, 0, 0, time.UTC)
	if err := s.CreateRoom(ctx, roomAt("r1", at)); err != nil {
		t.Fatal(err)
	}

	lease := at.Add(15 * time.Minute)
	if err := s.UpdateDriver(ctx, "r1", "u9", "Meera", lease); err != nil {
		t.Fatalf("UpdateDriver: %v", err)
	}
	r, _ := s.GetRoom(ctx, "r1")
	if r.DriverID != "u9" || r.DriverName != "Meera" || r.DriverLeaseUntil == nil || !r.DriverLeaseUntil.Equal(lease) {
		t.Fatalf("driver not recorded: %+v", r)
	}

	if err := s.UpdateDriver(ctx, "nope", "u9", "Meera", lease); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("UpdateDriver unknown room: err = %v, want ErrNotFound", err)
	}

	closedAt := at.Add(time.Hour)
	if err := s.CloseRoom(ctx, "r1", closedAt); err != nil {
		t.Fatalf("CloseRoom: %v", err)
	}
	r, _ = s.GetRoom(ctx, "r1")
	if r.Status != "closed" || r.ClosedAt == nil || !r.ClosedAt.Equal(closedAt) {
		t.Fatalf("room not closed: %+v", r)
	}

	if err := s.CloseRoom(ctx, "nope", closedAt); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("CloseRoom unknown room: err = %v, want ErrNotFound", err)
	}
}
