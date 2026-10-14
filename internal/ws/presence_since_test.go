package ws

import (
	"encoding/json"
	"testing"
	"time"
)

// fakeClock is the gateway's now for presence tests.
type fakeClock struct{ t time.Time }

func (f *fakeClock) now() time.Time { return f.t }

func newPresenceGateway(clk *fakeClock) *Gateway {
	return &Gateway{now: clk.now, rooms: map[string]*roomHub{}}
}

func addConn(g *Gateway, roomID, user string) *client {
	c := &client{userID: user, userName: user, state: "viewing", lastSeen: g.now(), stateSince: g.now(), send: make(chan []byte, 8)}
	hub := g.rooms[roomID]
	if hub == nil {
		hub = &roomHub{conns: map[*client]struct{}{}}
		g.rooms[roomID] = hub
	}
	hub.conns[c] = struct{}{}
	return c
}

func memberOf(t *testing.T, g *Gateway, roomID, id string) Member {
	t.Helper()
	for _, m := range g.rooms[roomID].members() {
		if m.ID == id {
			return m
		}
	}
	t.Fatalf("%s is not a member", id)
	return Member{}
}

func TestPresenceSinceRegisterAndAwayFlip(t *testing.T) {
	clk := &fakeClock{t: time.Date(2026, 10, 11, 10, 0, 0, 0, time.UTC)}
	g := newPresenceGateway(clk)
	c := addConn(g, "r1", "priya")
	if m := memberOf(t, g, "r1", "priya"); m.State != "viewing" || !m.Since.Equal(clk.t) {
		t.Fatalf("registered: %+v", m)
	}

	// Silent for 44 s: still viewing. At 46 s: away, since = the flip time.
	clk.t = clk.t.Add(44 * time.Second)
	g.markAway()
	if m := memberOf(t, g, "r1", "priya"); m.State != "viewing" {
		t.Fatalf("44s: %+v", m)
	}
	clk.t = clk.t.Add(2 * time.Second)
	g.markAway()
	flip := clk.t
	if m := memberOf(t, g, "r1", "priya"); m.State != "away" || !m.Since.Equal(flip) {
		t.Fatalf("46s: %+v", m)
	}

	// A repeated tick does not move since.
	clk.t = clk.t.Add(12 * time.Minute)
	g.markAway()
	if m := memberOf(t, g, "r1", "priya"); !m.Since.Equal(flip) {
		t.Fatalf("since moved on a repeat tick: %+v", m)
	}

	// Back: viewing, since = now.
	g.setPresence("r1", c, "viewing")
	if m := memberOf(t, g, "r1", "priya"); m.State != "viewing" || !m.Since.Equal(clk.t) {
		t.Fatalf("back: %+v", m)
	}
	// Setting the state it already has changes nothing.
	prev := clk.t
	clk.t = clk.t.Add(time.Minute)
	g.setPresence("r1", c, "viewing")
	if m := memberOf(t, g, "r1", "priya"); !m.Since.Equal(prev) {
		t.Fatalf("no-op presence moved since: %+v", m)
	}
}

func TestPresenceSinceAcrossTwoConnections(t *testing.T) {
	clk := &fakeClock{t: time.Date(2026, 10, 11, 10, 0, 0, 0, time.UTC)}
	g := newPresenceGateway(clk)
	t0 := clk.t
	a := addConn(g, "r1", "priya")
	clk.t = t0.Add(5 * time.Minute)
	b := addConn(g, "r1", "priya")

	// Both viewing: the earliest.
	if m := memberOf(t, g, "r1", "priya"); m.State != "viewing" || !m.Since.Equal(t0) {
		t.Fatalf("both viewing: %+v", m)
	}
	// Both away: the earliest away stamp.
	g.setPresence("r1", a, "away")
	aAway := clk.t
	clk.t = t0.Add(8 * time.Minute)
	g.setPresence("r1", b, "away")
	if m := memberOf(t, g, "r1", "priya"); m.State != "away" || !m.Since.Equal(aAway) {
		t.Fatalf("both away: %+v", m)
	}
	// A second tab opening flips her to here, since = its connect time.
	clk.t = t0.Add(20 * time.Minute)
	addConn(g, "r1", "priya")
	if m := memberOf(t, g, "r1", "priya"); m.State != "viewing" || !m.Since.Equal(clk.t) {
		t.Fatalf("new tab: %+v", m)
	}
}

func TestPresenceFrameCarriesSince(t *testing.T) {
	clk := &fakeClock{t: time.Date(2026, 10, 11, 10, 52, 0, 0, time.UTC)}
	g := newPresenceGateway(clk)
	c := addConn(g, "r1", "priya")
	g.broadcastLocked("r1")
	var msg struct {
		Type    string `json:"type"`
		Members []struct {
			Since string `json:"since"`
		} `json:"members"`
	}
	if err := json.Unmarshal(<-c.send, &msg); err != nil {
		t.Fatal(err)
	}
	if msg.Type != "presence" || len(msg.Members) != 1 || msg.Members[0].Since != "2026-10-11T10:52:00Z" {
		t.Fatalf("frame: %+v", msg)
	}
}
