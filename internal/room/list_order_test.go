package room_test

import (
	"context"
	"slices"
	"sort"
	"testing"
	"time"
)

// TestListRoomsDeterministicTiebreak pins the tiebreaker added for KAN-382:
// two rooms can share an opened_at (the store keeps microsecond precision),
// and the map iteration order of publishedRooms must not leak into the list.
// ListRooms must return the same order on every call, with equal opened_at
// broken by id ascending.
func TestListRoomsDeterministicTiebreak(t *testing.T) {
	svc, _, _ := newTestService(t)
	ctx := context.Background()
	u := mustUser(t, svc, "arjun")
	// The test clock is fixed, so every room shares the same opened_at.
	for _, title := range []string{"one", "two", "three"} {
		if _, err := svc.CreateRoom(ctx, u, title, "sev2", "orders"); err != nil {
			t.Fatal(err)
		}
	}

	first, err := svc.ListRooms(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 3 {
		t.Fatalf("got %d rooms, want 3", len(first))
	}
	for i := 1; i < len(first); i++ {
		if !first[i-1].OpenedAt.Equal(first[i].OpenedAt) {
			t.Fatalf("test clock did not produce equal opened_at: %v vs %v", first[i-1].OpenedAt, first[i].OpenedAt)
		}
		if first[i-1].ID >= first[i].ID {
			t.Fatalf("equal opened_at not broken by id: %q then %q", first[i-1].ID, first[i].ID)
		}
	}

	// The order must not vary between calls: publishedRooms iterates a map, so
	// a list that only ties on opened_at would shuffle with each call.
	for range 20 {
		next, err := svc.ListRooms(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for i := range first {
			if next[i].ID != first[i].ID {
				t.Fatalf("list order changed between calls: call 1[%d]=%q, later[%d]=%q", i, first[i].ID, i, next[i].ID)
			}
		}
	}
}

// TestListRoomsFullOrder pins every key of the list order, not only the id
// tiebreaker: open rooms precede closed ones even when the closed room is
// newer, rooms are newest opened_at first, and an equal opened_at is broken
// by id ascending.
func TestListRoomsFullOrder(t *testing.T) {
	svc, _, c := newTestService(t)
	ctx := context.Background()
	u := mustUser(t, svc, "arjun")

	old, err := svc.CreateRoom(ctx, u, "old", "sev2", "orders")
	if err != nil {
		t.Fatal(err)
	}
	c.Advance(time.Minute)
	tied1, err := svc.CreateRoom(ctx, u, "tied one", "sev2", "orders")
	if err != nil {
		t.Fatal(err)
	}
	tied2, err := svc.CreateRoom(ctx, u, "tied two", "sev2", "orders")
	if err != nil {
		t.Fatal(err)
	}
	c.Advance(time.Minute)
	newestClosed, err := svc.CreateRoom(ctx, u, "newest closed", "sev2", "orders")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.CloseRoom(ctx, u, newestClosed.ID, "done"); err != nil {
		t.Fatal(err)
	}

	// Both tied rooms share an opened_at; the id is their only difference.
	tied := []string{tied1.ID, tied2.ID}
	sort.Strings(tied)
	want := []string{tied[0], tied[1], old.ID, newestClosed.ID}

	for range 5 {
		got, err := svc.ListRooms(ctx)
		if err != nil {
			t.Fatal(err)
		}
		ids := make([]string, 0, len(got))
		for _, r := range got {
			ids = append(ids, r.ID)
		}
		if !slices.Equal(ids, want) {
			t.Fatalf("order = %v, want %v (open newest first, ties by id, closed last)", ids, want)
		}
	}
}
