package slackbridge

import (
	"context"
	"fmt"
	"testing"
	"time"

	"piperoom/internal/events"
)

// sizes reports how many entries the pager keeps per room-keyed map.
func (p *pager) sizes() (last, roomHits, seen int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.last), len(p.roomHits), len(p.seen)
}

func newPrunePager(t *testing.T, now *time.Time, l PagingLimits) *pager {
	t.Helper()
	p, err := newPager(PagingOptions{Layers: map[string][]string{"k": {owner1}}, Limits: l})
	if err != nil {
		t.Fatal(err)
	}
	p.now = func() time.Time { return *now }
	return p
}

func pageOnce(p *pager, room, reason string) bool {
	k := pageKey{room, owner1, reason}
	if !p.check(k, nil) {
		return false
	}
	p.record([]pageKey{k})
	return true
}

// 10 000 rooms, each idle past the longest window before the next one pages:
// the maps must not keep them.
func TestPagerPrunesIdleRooms(t *testing.T) {
	now := time.Unix(1700000000, 0)
	p := newPrunePager(t, &now, PagingLimits{UserPerHour: 1 << 30, RoomPerHour: 1 << 30})
	for i := 0; i < 10000; i++ {
		if !pageOnce(p, fmt.Sprintf("room-%d", i), "x") {
			t.Fatalf("room %d denied", i)
		}
		now = now.Add(2 * time.Hour)
	}
	last, hits, seen := p.sizes()
	if last > 1 || hits > 1 || seen > maxSeenPages {
		t.Errorf("kept last=%d roomHits=%d seen=%d after 10000 idle rooms", last, hits, seen)
	}
}

// A burst of rooms inside one window cannot grow the maps past the cap, and a
// room that pages after the burst is still rate limited.
func TestPagerCapsMapsAndKeepsActiveRoomLimited(t *testing.T) {
	now := time.Unix(1700000000, 0)
	p := newPrunePager(t, &now, PagingLimits{UserPerHour: 1 << 30, RoomPerHour: 2})
	for i := 0; i < 10000; i++ {
		now = now.Add(time.Millisecond)
		pageOnce(p, fmt.Sprintf("room-%d", i), "x")
		last, hits, seen := p.sizes()
		if last > maxTrackedRooms || hits > maxTrackedRooms || seen > maxSeenPages {
			t.Fatalf("after %d rooms: last=%d roomHits=%d seen=%d", i+1, last, hits, seen)
		}
	}
	now = now.Add(time.Second)
	if !pageOnce(p, "active", "a") {
		t.Fatal("active room's first page denied")
	}
	if pageOnce(p, "active", "b") {
		t.Error("per-user-per-room gap lost for the active room")
	}
	now = now.Add(11 * time.Minute)
	if !pageOnce(p, "active", "c") {
		t.Fatal("second page after the gap denied")
	}
	now = now.Add(11 * time.Minute)
	if pageOnce(p, "active", "d") {
		t.Error("room hourly cap lost for the active room")
	}
}

func TestPagerForgetRoom(t *testing.T) {
	now := time.Unix(1700000000, 0)
	p := newPrunePager(t, &now, PagingLimits{})
	pageOnce(p, "r1", "x")
	pageOnce(p, "r2", "x")
	p.forgetRoom("r1")
	if last, hits, seen := p.sizes(); last != 1 || hits != 1 || seen != 1 {
		t.Errorf("after forget: last=%d roomHits=%d seen=%d, want 1 each", last, hits, seen)
	}
	if !p.check(pageKey{"r1", owner1, "x"}, nil) {
		t.Error("a forgotten room's state still blocks")
	}
	if p.check(pageKey{"r2", owner1, "y"}, nil) {
		t.Error("another room lost its gap")
	}
}

// The server allows a re-nudge exactly 10 minutes after the previous one, but
// the bridge stamps the page after the Slack post. That lag must not turn the
// boundary re-nudge into a reply-only message.
func TestPagerGapBoundary(t *testing.T) {
	now := time.Unix(1700000000, 0)
	p := newPrunePager(t, &now, PagingLimits{})
	pageOnce(p, "r1", "a")
	for _, c := range []struct {
		after time.Duration
		want  bool
	}{{9 * time.Minute, false}, {10*time.Minute - 3*time.Second, false}, {10*time.Minute - time.Second, true}, {10 * time.Minute, true}} {
		now = time.Unix(1700000000, 0).Add(c.after)
		if got := p.check(pageKey{"r1", owner1, "b"}, nil); got != c.want {
			t.Errorf("after %v: check=%v, want %v", c.after, got, c.want)
		}
	}
}

// Defaults: an unconfigured pager has a 10-minute gap.
func TestPagerDefaultInterval(t *testing.T) {
	p, err := newPager(pagingOpts())
	if err != nil {
		t.Fatal(err)
	}
	if p.lim.UserRoomInterval != 10*time.Minute || p.lim.UserPerHour != 6 || p.lim.RoomPerHour != 20 {
		t.Errorf("defaults = %+v", p.lim)
	}
}

// Closing a room drops its pager state; other rooms keep theirs.
func TestBridgeForgetsClosedRoom(t *testing.T) {
	created := ev(1, events.RoomCreated, "ana", events.RoomCreatedPayload{Title: "t"})
	prop := ev(2, events.ProposalCreated, "agent", events.ProposalCreatedPayload{ProposalID: "p1", Title: "t", RequiredLayers: []string{"kafka"}})
	closed := ev(3, events.RoomClosed, "ana", events.RoomClosedPayload{Resolution: "done"})
	fs := &fakeSlack{}
	b, _, _ := newBridge(t, &fakeStore{evs: []events.Event{created, prop, closed}}, fs, func(o *Options) { o.Paging = pagingOpts() })
	b.pager.record([]pageKey{{"other", owner1, "x"}})
	if err := b.Run(context.Background(), "r1"); err != nil {
		t.Fatal(err)
	}
	if n, _ := mentionCount(fs.reqs); n != 1 {
		t.Fatalf("pages = %d, want 1", n)
	}
	if last, hits, seen := b.pager.sizes(); last != 1 || hits != 1 || seen != 1 {
		t.Errorf("after close: last=%d roomHits=%d seen=%d, want only the other room", last, hits, seen)
	}
}
