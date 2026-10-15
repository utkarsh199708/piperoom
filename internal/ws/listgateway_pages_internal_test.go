package ws

import (
	"testing"
	"time"

	"piperoom/internal/room"
	"piperoom/internal/store/memstore"
)

// TestListConnPushesPageAndFlags: an owner.paged (or new title flags) that
// changes nothing else still reaches the list socket, or the web PagedBanner
// never fires live (Q2 F4).
func TestListConnPushesPageAndFlags(t *testing.T) {
	now := time.Date(2026, 10, 14, 3, 0, 0, 0, time.UTC)
	svc := room.New(room.Deps{Store: memstore.New()})
	defer svc.Close()
	g := &Gateway{svc: svc, now: func() time.Time { return now }}
	c := &client{send: make(chan []byte, 16)}
	l := &listConn{g: g, c: c, known: map[string][32]byte{}, sent: map[string]time.Time{}, pending: map[string]*room.RoomSnapshot{}}

	snap := room.NewSnapshot()
	snap.ID, snap.Status, snap.Title = "r_1", "open", "a"
	s1 := snap
	l.changed(&s1)
	now = now.Add(time.Hour)
	s2 := snap
	s2.Pages = []room.Page{{Layer: "consumer", Owners: []string{"kabir"}, At: now}}
	l.changed(&s2)
	if len(c.send) != 2 {
		t.Fatalf("frames after a page = %d, want 2", len(c.send))
	}
	now = now.Add(time.Hour)
	s3 := s2
	s3.TitleFlags = []string{"instruction"}
	l.changed(&s3)
	if len(c.send) != 3 {
		t.Fatalf("frames after title flags = %d, want 3", len(c.send))
	}
}
