package room

import (
	"testing"
	"time"

	"piperoom/internal/store/memstore"
)

// A scrape must not wait on s.mu, which is held across store appends: with the
// lock held (a slow database) OpenRooms returns the last count at once.
func TestOpenRoomsDoesNotWaitOnServiceLock(t *testing.T) {
	s := New(Deps{Store: memstore.New(), Users: DefaultUsers()})
	t.Cleanup(s.Close)
	s.rooms["r1"] = &roomState{snap: RoomSnapshot{Status: "open"}}
	if n := s.OpenRooms(); n != 1 {
		t.Fatalf("OpenRooms = %d, want 1", n)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	got := make(chan int, 1)
	go func() { got <- s.OpenRooms() }()
	select {
	case n := <-got:
		if n != 1 {
			t.Errorf("OpenRooms with s.mu held = %d, want the last count 1", n)
		}
	case <-time.After(2 * time.Second):
		t.Error("OpenRooms waited on s.mu")
	}
}
