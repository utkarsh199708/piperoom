package room

import (
	"context"
	"testing"
	"time"

	"piperoom/internal/store/memstore"
)

// recountOpen is the slow, authoritative count OpenRooms must always equal.
func recountOpen(s *Service) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, st := range s.rooms {
		if s.roomOpen(st) {
			n++
		}
	}
	return n
}

// OpenRooms is a counter, never a stale zero and never a wait on s.mu: right
// after Load it is already right, even while another goroutine holds the lock.
func TestOpenRoomsCorrectAfterLoadWhileLockHeld(t *testing.T) {
	st := memstore.New()
	first := New(Deps{Store: st, Users: DefaultUsers()})
	u, ok := first.LookupUser("arjun")
	if !ok {
		t.Fatal("user arjun missing")
	}
	ctx := context.Background()
	var ids []string
	for i := 0; i < 3; i++ {
		snap, err := first.CreateRoom(ctx, u, "t", "sev2", "orders")
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, snap.ID)
	}
	if _, err := first.CloseRoom(ctx, u, ids[0], "done"); err != nil {
		t.Fatal(err)
	}
	first.Close()

	// A fresh service over the same store: the cache is empty until Load.
	s := New(Deps{Store: st, Users: DefaultUsers()})
	t.Cleanup(s.Close)
	if err := s.Load(ctx); err != nil {
		t.Fatalf("Load: %v", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	got := make(chan int, 1)
	go func() { got <- s.OpenRooms() }()
	select {
	case n := <-got:
		if n != 2 {
			t.Fatalf("OpenRooms with s.mu held right after Load = %d, want 2", n)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("OpenRooms waited on s.mu")
	}
}

// The counter follows create, close and delete: after each step it equals a
// recount of the cache.
func TestOpenRoomsTracksCreateCloseDelete(t *testing.T) {
	s := New(Deps{Store: memstore.New(), Users: DefaultUsers()})
	t.Cleanup(s.Close)
	u, _ := s.LookupUser("arjun")
	ctx := context.Background()
	check := func(step string, want int) {
		t.Helper()
		if got, re := s.OpenRooms(), recountOpen(s); got != want || re != want {
			t.Fatalf("%s: OpenRooms = %d, recount = %d, want %d", step, got, re, want)
		}
	}
	check("empty", 0)
	a, err := s.CreateRoom(ctx, u, "a", "sev2", "orders")
	if err != nil {
		t.Fatal(err)
	}
	b, err := s.CreateRoom(ctx, u, "b", "sev2", "orders")
	if err != nil {
		t.Fatal(err)
	}
	check("two created", 2)
	if _, err := s.CloseRoom(ctx, u, a.ID, "done"); err != nil {
		t.Fatal(err)
	}
	check("one closed", 1)
	s.mu.Lock()
	s.uncacheRoomLocked(b.ID)
	s.mu.Unlock()
	check("open room dropped from cache", 0)
	s.mu.Lock()
	s.uncacheRoomLocked(a.ID)
	s.mu.Unlock()
	check("closed room dropped from cache", 0)
}
