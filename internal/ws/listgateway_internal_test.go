package ws

import (
	"encoding/json"
	"testing"
	"time"

	"piperoom/internal/room"
	"piperoom/internal/store/memstore"
)

// TestListConnCoalesces: a room changing twice within listCoalesce sends one
// frame now and the newest state when the wait is over; a change that only
// touches fields outside the digest sends nothing.
func TestListConnCoalesces(t *testing.T) {
	now := time.Date(2026, 10, 14, 3, 0, 0, 0, time.UTC)
	svc := room.New(room.Deps{Store: memstore.New()})
	defer svc.Close()
	g := &Gateway{svc: svc, now: func() time.Time { return now }}
	c := &client{send: make(chan []byte, 16)}
	l := &listConn{g: g, c: c, known: map[string][32]byte{}, sent: map[string]time.Time{}, pending: map[string]*room.RoomSnapshot{}}

	snap := room.NewSnapshot()
	snap.ID, snap.Status, snap.Title = "r_1", "open", "a"
	s1 := snap
	if !l.changed(&s1) || len(c.send) != 1 {
		t.Fatalf("first change: %d frames", len(c.send))
	}
	s2 := snap
	s2.LastSeq = 9 // not in the digest
	l.changed(&s2)
	if len(c.send) != 1 || len(l.pending) != 0 {
		t.Fatal("a last_seq-only change was sent or held")
	}
	s3 := snap
	s3.Title = "b"
	l.changed(&s3)
	s4 := snap
	s4.Title = "c"
	l.changed(&s4)
	if len(c.send) != 1 || l.pending["r_1"] != &s4 {
		t.Fatalf("changes within %s: %d frames, pending %v", listCoalesce, len(c.send), l.pending)
	}
	if d, ok := l.nextDue(); !ok || d != listCoalesce {
		t.Fatalf("next due = %s %v", d, ok)
	}
	now = now.Add(listCoalesce)
	if !l.flushDue() || len(c.send) != 2 || len(l.pending) != 0 {
		t.Fatalf("after the wait: %d frames, pending %d", len(c.send), len(l.pending))
	}
	<-c.send
	var f listMsg
	if err := json.Unmarshal(<-c.send, &f); err != nil || f.Type != "room" || f.Room.Title != "c" {
		t.Fatalf("flushed frame = %+v %v", f, err)
	}
	// Deleting a known room sends room_deleted once; an unknown one nothing.
	l.deleted("r_1")
	l.deleted("r_1")
	l.deleted("r_x")
	if len(c.send) != 1 {
		t.Fatalf("deletions sent %d frames", len(c.send))
	}
}

// TestListConnSendsLastFix: a fix finishing and its verdict change only
// last_fix, and the list's Status cell reads it (KAN-594), so each sends a
// frame. The same state again sends nothing.
func TestListConnSendsLastFix(t *testing.T) {
	now := time.Date(2026, 10, 14, 3, 0, 0, 0, time.UTC)
	svc := room.New(room.Deps{Store: memstore.New()})
	defer svc.Close()
	g := &Gateway{svc: svc, now: func() time.Time { return now }}
	c := &client{send: make(chan []byte, 16)}
	l := &listConn{g: g, c: c, known: map[string][32]byte{}, sent: map[string]time.Time{}, pending: map[string]*room.RoomSnapshot{}}

	snap := room.NewSnapshot()
	snap.ID, snap.Status, snap.Title = "r_1", "open", "a"
	verified := &room.LastFix{ProposalID: "p_1", Seq: 3, Steps: 1, Verification: &room.FixVerification{Recovered: true, Verdict: "recovered: ok"}}
	for i, fix := range []*room.LastFix{nil, {ProposalID: "p_1", Seq: 3, Steps: 1}, verified, verified} {
		s := snap
		s.LastFix = fix
		l.changed(&s)
		want := min(i+1, 3)
		if len(c.send) != want {
			t.Fatalf("change %d: %d frames, want %d", i, len(c.send), want)
		}
		now = now.Add(listCoalesce)
	}
}
