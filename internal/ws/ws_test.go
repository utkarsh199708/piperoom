package ws

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"

	"piperoom/internal/events"
	"piperoom/internal/room"
)

const (
	testRoom    = "r_1"
	waitTimeout = 5 * time.Second
)

var testUsers = map[string]room.User{
	"alice": {ID: "alice", Name: "Alice"},
	"bob":   {ID: "bob", Name: "Bob"},
}

// ---- fakes ----

// fakeLog is an in-memory event log with the Subscribe semantics of
// events.Store: replay after N, then live, channel closed when ctx ends.
type fakeLog struct {
	mu     sync.Mutex
	events []events.Event
	subs   map[*fakeSub]struct{}
	active atomic.Int32 // subscriptions whose ctx has not ended yet
	err    error        // returned by Subscribe when set
}

type fakeSub struct {
	roomID string
	ch     chan events.Event
}

func newFakeLog() *fakeLog { return &fakeLog{subs: make(map[*fakeSub]struct{})} }

func (l *fakeLog) Subscribe(ctx context.Context, roomID string, after int64) (<-chan events.Event, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.err != nil {
		return nil, l.err
	}
	s := &fakeSub{roomID: roomID, ch: make(chan events.Event, 4096)}
	for _, e := range l.events {
		if e.RoomID == roomID && e.Seq > after {
			s.ch <- e
		}
	}
	l.subs[s] = struct{}{}
	l.active.Add(1)
	go func() {
		<-ctx.Done()
		l.mu.Lock()
		delete(l.subs, s)
		close(s.ch)
		l.mu.Unlock()
		l.active.Add(-1)
	}()
	return s.ch, nil
}

// Publish appends an event to the log and sends it to live subscribers.
func (l *fakeLog) Publish(e events.Event) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.events = append(l.events, e)
	for s := range l.subs {
		if s.roomID == e.RoomID {
			s.ch <- e
		}
	}
}

func ev(seq int64) events.Event {
	return events.Event{
		RoomID:  testRoom,
		Seq:     seq,
		Type:    events.CommentAdded,
		Actor:   events.Actor{Kind: events.ActorHuman, ID: "alice", Name: "Alice"},
		Payload: json.RawMessage(`{"text":"event ` + itoa(seq) + `"}`),
		At:      time.Date(2026, 10, 11, 10, 0, int(seq), 0, time.UTC),
	}
}

func itoa(n int64) string {
	b, _ := json.Marshal(n)
	return string(b)
}

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

// ---- environment ----

type env struct {
	h     *Handler
	log   *fakeLog
	clock *fakeClock
	srv   *httptest.Server
}

func newEnv(t *testing.T, mod func(*Config)) *env {
	t.Helper()
	e := &env{log: newFakeLog(), clock: &fakeClock{t: time.Date(2026, 10, 11, 10, 0, 0, 0, time.UTC)}}
	cfg := Config{
		Subscribe: e.log.Subscribe,
		Authenticate: func(id string) (room.User, bool) {
			u, ok := testUsers[id]
			return u, ok
		},
		RoomExists:    func(_ context.Context, id string) bool { return id == testRoom },
		Logger:        slog.New(slog.NewTextHandler(io.Discard, nil)),
		Now:           e.clock.Now,
		SweepInterval: 5 * time.Millisecond,
		PingInterval:  -1,
	}
	if mod != nil {
		mod(&cfg)
	}
	e.h = New(cfg)
	mux := http.NewServeMux()
	mux.Handle("GET /api/rooms/{id}/ws", e.h)
	e.srv = httptest.NewServer(mux)
	// Cleanups run last-in first-out: Shutdown first, so no handler outlives the server.
	t.Cleanup(e.srv.Close)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if err := e.h.Shutdown(ctx); err != nil {
			t.Errorf("Shutdown: %v", err)
		}
	})
	return e
}

// shutdownAsync starts Shutdown and returns a channel for its result.
func (e *env) shutdownAsync() <-chan error {
	out := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		out <- e.h.Shutdown(ctx)
	}()
	return out
}

func (e *env) url(roomID, query string) string {
	return "ws" + strings.TrimPrefix(e.srv.URL, "http") + "/api/rooms/" + roomID + "/ws?" + query
}

// client wraps a test connection and remembers the latest presence list.
type client struct {
	t    *testing.T
	conn *websocket.Conn
}

type frame struct {
	Type    string           `json:"type"`
	Event   events.Event     `json:"event"`
	Members []PresenceMember `json:"members"`
	Error   struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

func (e *env) dial(t *testing.T, roomID, user string, after int64) *client {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()
	conn, resp, err := websocket.Dial(ctx, e.url(roomID, "user="+user+"&after="+itoa(after)), nil)
	if err != nil {
		status := 0
		if resp != nil {
			status = resp.StatusCode
		}
		t.Fatalf("dial %s as %s: %v (status %d)", roomID, user, err, status)
	}
	t.Cleanup(func() { _ = conn.CloseNow() })
	return &client{t: t, conn: conn}
}

// read returns the next frame or the read error.
func (c *client) read() (frame, error) {
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()
	_, data, err := c.conn.Read(ctx)
	if err != nil {
		return frame{}, err
	}
	var f frame
	if err := json.Unmarshal(data, &f); err != nil {
		c.t.Fatalf("bad frame %q: %v", data, err)
	}
	return f, nil
}

// events reads until n event frames arrived (skipping presence) and returns
// their sequence numbers.
func (c *client) events(n int) []int64 {
	c.t.Helper()
	var seqs []int64
	for len(seqs) < n {
		f, err := c.read()
		if err != nil {
			c.t.Fatalf("read after %d events: %v", len(seqs), err)
		}
		if f.Type == "event" {
			seqs = append(seqs, f.Event.Seq)
		}
	}
	return seqs
}

// waitPresence reads until a presence frame equal to want arrives.
func (c *client) waitPresence(want ...PresenceMember) {
	c.t.Helper()
	var got [][]PresenceMember
	for {
		f, err := c.read()
		if err != nil {
			c.t.Fatalf("waiting for presence %v: %v (saw %v)", want, err, got)
		}
		if f.Type != "presence" {
			continue
		}
		got = append(got, f.Members)
		if len(f.Members) == len(want) {
			same := true
			for i := range want {
				if f.Members[i] != want[i] {
					same = false
				}
			}
			if same {
				return
			}
		}
	}
}

func (c *client) send(s string) {
	c.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()
	if err := c.conn.Write(ctx, websocket.MessageText, []byte(s)); err != nil {
		c.t.Fatalf("send %q: %v", s, err)
	}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(waitTimeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func equalSeqs(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func viewing(id, name string) PresenceMember {
	return PresenceMember{ID: id, Name: name, State: StateViewing}
}

func away(id, name string) PresenceMember {
	return PresenceMember{ID: id, Name: name, State: StateAway}
}

// ---- tests: before the upgrade ----

func TestRejectsBeforeUpgrade(t *testing.T) {
	e := newEnv(t, nil)
	cases := []struct {
		name     string
		path     string
		prepare  func()
		status   int
		wantCode string
	}{
		{"unknown user", "/api/rooms/r_1/ws?user=mallory", nil, 401, "unauthenticated"},
		{"missing user", "/api/rooms/r_1/ws", nil, 401, "unauthenticated"},
		{"unknown room", "/api/rooms/r_nope/ws?user=alice", nil, 404, "not_found"},
		{"bad after", "/api/rooms/r_1/ws?user=alice&after=abc", nil, 400, "bad_request"},
		{"negative after", "/api/rooms/r_1/ws?user=alice&after=-1", nil, 400, "bad_request"},
		{"subscribe fails", "/api/rooms/r_1/ws?user=alice", func() { e.log.err = errors.New("db down") }, 503, "unavailable"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.prepare != nil {
				tc.prepare()
				defer func() { e.log.err = nil }()
			}
			resp, err := http.Get(e.srv.URL + tc.path)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != tc.status {
				t.Fatalf("status = %d, want %d", resp.StatusCode, tc.status)
			}
			var body struct {
				Error struct{ Code, Message string } `json:"error"`
			}
			if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
				t.Fatalf("body is not the error JSON: %v", err)
			}
			if body.Error.Code != tc.wantCode {
				t.Fatalf("code = %q, want %q", body.Error.Code, tc.wantCode)
			}
		})
	}
	if n := e.log.active.Load(); n != 0 {
		t.Fatalf("%d subscriptions left open by rejected requests", n)
	}
}

func TestOriginCheck(t *testing.T) {
	e := newEnv(t, nil)
	cases := []struct {
		origin string
		ok     bool
	}{
		{"http://localhost:5173", true},
		{"http://127.0.0.1:5173", true},
		{"", true}, // not a browser
		{"http://evil.example", false},
		{"http://localhost:9999", false},
	}
	for _, tc := range cases {
		t.Run(tc.origin, func(t *testing.T) {
			hdr := http.Header{}
			if tc.origin != "" {
				hdr.Set("Origin", tc.origin)
			}
			ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
			defer cancel()
			conn, resp, err := websocket.Dial(ctx, e.url(testRoom, "user=alice"), &websocket.DialOptions{HTTPHeader: hdr})
			if tc.ok {
				if err != nil {
					t.Fatalf("dial: %v", err)
				}
				_ = conn.CloseNow()
				return
			}
			if err == nil {
				_ = conn.CloseNow()
				t.Fatal("dial succeeded for a foreign origin")
			}
			if resp == nil || resp.StatusCode != http.StatusForbidden {
				t.Fatalf("response = %v, want 403", resp)
			}
		})
	}
}

// ---- tests: replay and live ----

func TestReplayThenLive(t *testing.T) {
	cases := []struct {
		name  string
		after int64
		want  []int64
	}{
		{"full replay", 0, []int64{1, 2, 3, 4, 5, 6, 7}},
		{"after 2", 2, []int64{3, 4, 5, 6, 7}},
		{"after the last seq", 5, []int64{6, 7}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := newEnv(t, nil)
			for i := int64(1); i <= 5; i++ {
				e.log.Publish(ev(i))
			}
			c := e.dial(t, testRoom, "alice", tc.after)
			// Live events arrive after the replay, even if published before the client read.
			e.log.Publish(ev(6))
			e.log.Publish(ev(7))
			got := c.events(len(tc.want))
			if !equalSeqs(got, tc.want) {
				t.Fatalf("seqs = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestEventFrameCarriesTheEvent(t *testing.T) {
	e := newEnv(t, nil)
	e.log.Publish(ev(1))
	c := e.dial(t, testRoom, "alice", 0)
	for {
		f, err := c.read()
		if err != nil {
			t.Fatal(err)
		}
		if f.Type != "event" {
			continue
		}
		want := ev(1)
		if f.Event.RoomID != want.RoomID || f.Event.Seq != 1 || f.Event.Type != want.Type ||
			f.Event.Actor != want.Actor || !f.Event.At.Equal(want.At) ||
			string(f.Event.Payload) != string(want.Payload) {
			t.Fatalf("event = %+v, want %+v", f.Event, want)
		}
		return
	}
}

func TestReconnectWithAfterSkipsNothingAndRepeatsNothing(t *testing.T) {
	e := newEnv(t, nil)
	e.log.Publish(ev(1))
	e.log.Publish(ev(2))
	e.log.Publish(ev(3))
	first := e.dial(t, testRoom, "alice", 0)
	if got := first.events(3); !equalSeqs(got, []int64{1, 2, 3}) {
		t.Fatalf("first connection saw %v", got)
	}
	_ = first.conn.Close(websocket.StatusNormalClosure, "")
	waitFor(t, "first connection to end", func() bool { return e.log.active.Load() == 0 })

	// Events that happen while the client is away are replayed on reconnect.
	e.log.Publish(ev(4))
	e.log.Publish(ev(5))
	second := e.dial(t, testRoom, "alice", 3)
	e.log.Publish(ev(6))
	if got := second.events(3); !equalSeqs(got, []int64{4, 5, 6}) {
		t.Fatalf("second connection saw %v, want [4 5 6]", got)
	}
}

func TestDuplicateEventsFromTheStoreAreDropped(t *testing.T) {
	feed := make(chan events.Event, 8)
	for _, s := range []int64{1, 2, 2, 3, 1, 4} {
		feed <- ev(s)
	}
	e := newEnv(t, func(c *Config) {
		c.Subscribe = func(context.Context, string, int64) (<-chan events.Event, error) { return feed, nil }
	})
	c := e.dial(t, testRoom, "alice", 0)
	feed <- ev(4) // a repeat of the last event, then a new one
	feed <- ev(5)
	if got := c.events(5); !equalSeqs(got, []int64{1, 2, 3, 4, 5}) {
		t.Fatalf("seqs = %v, want [1 2 3 4 5]", got)
	}
}

func TestEndedFeedClosesWithTryAgainLater(t *testing.T) {
	feed := make(chan events.Event, 1)
	e := newEnv(t, func(c *Config) {
		c.Subscribe = func(context.Context, string, int64) (<-chan events.Event, error) { return feed, nil }
	})
	c := e.dial(t, testRoom, "alice", 0)
	c.waitPresence(viewing("alice", "Alice"))
	// The store gave up on this subscriber: the client must be told to come back.
	close(feed)
	for {
		if _, err := c.read(); err != nil {
			if got := websocket.CloseStatus(err); got != websocket.StatusTryAgainLater {
				t.Fatalf("close status = %d (%v), want %d", got, err, websocket.StatusTryAgainLater)
			}
			return
		}
	}
}

func TestSubscriptionEndsWithTheConnection(t *testing.T) {
	e := newEnv(t, nil)
	c := e.dial(t, testRoom, "alice", 0)
	waitFor(t, "subscription to open", func() bool { return e.log.active.Load() == 1 })
	_ = c.conn.Close(websocket.StatusNormalClosure, "bye")
	waitFor(t, "subscription context to end", func() bool { return e.log.active.Load() == 0 })
}

// ---- tests: presence ----

func TestPresenceJoinAndLeave(t *testing.T) {
	e := newEnv(t, nil)
	a := e.dial(t, testRoom, "alice", 0)
	a.waitPresence(viewing("alice", "Alice"))

	b := e.dial(t, testRoom, "bob", 0)
	b.waitPresence(viewing("alice", "Alice"), viewing("bob", "Bob"))
	a.waitPresence(viewing("alice", "Alice"), viewing("bob", "Bob"))
	if got := e.h.PresenceMembers(testRoom); len(got) != 2 {
		t.Fatalf("PresenceMembers = %v, want 2 members", got)
	}

	_ = b.conn.Close(websocket.StatusNormalClosure, "")
	a.waitPresence(viewing("alice", "Alice"))
	waitFor(t, "bob to leave presence", func() bool { return len(e.h.PresenceMembers(testRoom)) == 1 })

	_ = a.conn.Close(websocket.StatusNormalClosure, "")
	waitFor(t, "room presence to empty", func() bool { return len(e.h.PresenceMembers(testRoom)) == 0 })
	if got := e.h.PresenceMembers(testRoom); got == nil {
		t.Fatal("PresenceMembers returned nil, want an empty slice")
	}
}

func TestPresenceIsPerRoom(t *testing.T) {
	e := newEnv(t, func(c *Config) {
		c.RoomExists = func(_ context.Context, id string) bool { return id == testRoom || id == "r_2" }
	})
	e.dial(t, testRoom, "alice", 0)
	e.dial(t, "r_2", "bob", 0)
	waitFor(t, "both rooms to have a member", func() bool {
		return len(e.h.PresenceMembers(testRoom)) == 1 && len(e.h.PresenceMembers("r_2")) == 1
	})
	if got := e.h.PresenceMembers(testRoom); got[0].ID != "alice" {
		t.Fatalf("room 1 members = %v", got)
	}
}

func TestPresenceGoesAwayAfterSilenceAndBack(t *testing.T) {
	e := newEnv(t, nil)
	a := e.dial(t, testRoom, "alice", 0)
	a.waitPresence(viewing("alice", "Alice"))

	// 44 s of silence is still fine; 46 s is not.
	e.clock.Advance(44 * time.Second)
	time.Sleep(30 * time.Millisecond) // several sweeps
	if got := e.h.PresenceMembers(testRoom); got[0].State != StateViewing {
		t.Fatalf("after 44s: %v, want viewing", got)
	}
	e.clock.Advance(2 * time.Second)
	a.waitPresence(away("alice", "Alice"))

	// A heartbeat brings the member back.
	a.send(`{"type":"presence","state":"viewing"}`)
	a.waitPresence(viewing("alice", "Alice"))
}

func TestPresenceExplicitAway(t *testing.T) {
	e := newEnv(t, nil)
	a := e.dial(t, testRoom, "alice", 0)
	b := e.dial(t, testRoom, "bob", 0)
	a.waitPresence(viewing("alice", "Alice"), viewing("bob", "Bob"))

	b.send(`{"type":"presence","state":"away"}`)
	a.waitPresence(viewing("alice", "Alice"), away("bob", "Bob"))
	b.send(`{"type":"presence","state":"viewing"}`)
	a.waitPresence(viewing("alice", "Alice"), viewing("bob", "Bob"))
}

func TestPresenceHeartbeatKeepsMemberViewing(t *testing.T) {
	e := newEnv(t, nil)
	a := e.dial(t, testRoom, "alice", 0)
	a.waitPresence(viewing("alice", "Alice"))
	for range 4 {
		e.clock.Advance(30 * time.Second)
		a.send(`{"type":"presence","state":"viewing"}`)
		time.Sleep(20 * time.Millisecond)
		if got := e.h.PresenceMembers(testRoom); got[0].State != StateViewing {
			t.Fatalf("member went away despite heartbeats: %v", got)
		}
	}
}

func TestSeveralTabsAreOneMember(t *testing.T) {
	e := newEnv(t, nil)
	tab1 := e.dial(t, testRoom, "alice", 0)
	tab1.waitPresence(viewing("alice", "Alice"))
	tab2 := e.dial(t, testRoom, "alice", 0)
	waitFor(t, "second tab to join", func() bool {
		e.h.hub.mu.Lock()
		defer e.h.hub.mu.Unlock()
		return len(e.h.hub.rooms[testRoom].conns) == 2
	})
	if got := e.h.PresenceMembers(testRoom); len(got) != 1 {
		t.Fatalf("two tabs gave %v, want one member", got)
	}

	tab1.send(`{"type":"presence","state":"away"}`)
	time.Sleep(30 * time.Millisecond)
	if got := e.h.PresenceMembers(testRoom); got[0].State != StateViewing {
		t.Fatalf("one tab away, one viewing: %v, want viewing", got)
	}
	tab2.send(`{"type":"presence","state":"away"}`)
	waitFor(t, "member to be away", func() bool { return e.h.PresenceMembers(testRoom)[0].State == StateAway })

	_ = tab1.conn.Close(websocket.StatusNormalClosure, "")
	waitFor(t, "first tab to leave", func() bool {
		e.h.hub.mu.Lock()
		defer e.h.hub.mu.Unlock()
		return len(e.h.hub.rooms[testRoom].conns) == 1
	})
	if got := e.h.PresenceMembers(testRoom); len(got) != 1 {
		t.Fatalf("after closing one tab: %v, want the member to stay", got)
	}
}

func TestBadClientMessagesGetAnErrorAndKeepTheConnection(t *testing.T) {
	e := newEnv(t, nil)
	a := e.dial(t, testRoom, "alice", 0)
	a.waitPresence(viewing("alice", "Alice"))

	for _, msg := range []string{
		`not json`,
		`{"type":"nope"}`,
		`{"type":"presence","state":"sleeping"}`,
		`{"type":"presence"}`,
		`[]`,
	} {
		a.send(msg)
		for {
			f, err := a.read()
			if err != nil {
				t.Fatalf("connection closed after %q: %v", msg, err)
			}
			if f.Type == "error" {
				if f.Error.Code != "bad_request" || f.Error.Message == "" {
					t.Fatalf("error frame for %q = %+v", msg, f.Error)
				}
				break
			}
		}
	}
	// Still alive and still able to change state.
	a.send(`{"type":"presence","state":"away"}`)
	a.waitPresence(away("alice", "Alice"))
}

func TestOversizedClientMessageClosesTheConnection(t *testing.T) {
	e := newEnv(t, nil)
	a := e.dial(t, testRoom, "alice", 0)
	a.send(`{"type":"presence","state":"` + strings.Repeat("x", 8<<10) + `"}`)
	for {
		if _, err := a.read(); err != nil {
			if got := websocket.CloseStatus(err); got != websocket.StatusMessageTooBig {
				t.Fatalf("close status = %d (%v), want %d", got, err, websocket.StatusMessageTooBig)
			}
			return
		}
	}
}

// ---- tests: slow clients ----

func TestSlowClientIsClosedWithPolicyViolation(t *testing.T) {
	e := newEnv(t, func(c *Config) {
		c.SendBuffer = 2
		c.SlowGrace = 2 * time.Millisecond
	})
	// A writer that takes 40 ms per frame stands in for a client that cannot keep up.
	e.h.beforeWrite = func() { time.Sleep(40 * time.Millisecond) }
	const total = 60
	for i := int64(1); i <= total; i++ {
		e.log.Publish(ev(i))
	}
	a := e.dial(t, testRoom, "alice", 0)

	var seqs []int64
	for {
		f, err := a.read()
		if err != nil {
			if got := websocket.CloseStatus(err); got != websocket.StatusPolicyViolation {
				t.Fatalf("close status = %d (%v), want %d", got, err, websocket.StatusPolicyViolation)
			}
			break
		}
		if f.Type == "event" {
			seqs = append(seqs, f.Event.Seq)
		}
	}
	if len(seqs) >= total {
		t.Fatalf("client received all %d events; it should have been dropped", total)
	}
	// What it did get is an in-order prefix, so reconnecting with after=<last> is lossless.
	for i, s := range seqs {
		if s != int64(i+1) {
			t.Fatalf("seqs = %v, want a prefix of 1..%d", seqs, total)
		}
	}
	waitFor(t, "dropped client to leave presence", func() bool { return len(e.h.PresenceMembers(testRoom)) == 0 })
	waitFor(t, "dropped client's subscription to end", func() bool { return e.log.active.Load() == 0 })
}

func TestLongReplayToAHealthyClientIsNotDropped(t *testing.T) {
	// More events than the send buffer holds, written slowly but steadily: the
	// grace period must let the client catch up instead of closing it.
	e := newEnv(t, func(c *Config) {
		c.SendBuffer = 4
		c.SlowGrace = 2 * time.Second
	})
	e.h.beforeWrite = func() { time.Sleep(2 * time.Millisecond) }
	const total = 100
	for i := int64(1); i <= total; i++ {
		e.log.Publish(ev(i))
	}
	a := e.dial(t, testRoom, "alice", 0)
	seqs := a.events(total)
	for i, s := range seqs {
		if s != int64(i+1) {
			t.Fatalf("seq %d at position %d", s, i)
		}
	}
}

func TestEnqueuePolicy(t *testing.T) {
	newTestConn := func(grace time.Duration) *conn {
		h := New(Config{SendBuffer: 2, SlowGrace: grace, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
		t.Cleanup(h.stop)
		return newConn(h, nil, testRoom, testUsers["alice"])
	}

	t.Run("full buffer with no grace closes at once", func(t *testing.T) {
		c := newTestConn(-1)
		if !c.enqueue([]byte("1")) || !c.enqueue([]byte("2")) {
			t.Fatal("enqueue into free space failed")
		}
		if c.enqueue([]byte("3")) {
			t.Fatal("enqueue into a full buffer succeeded")
		}
		<-c.done
		if c.code != websocket.StatusPolicyViolation {
			t.Fatalf("code = %d, want %d", c.code, websocket.StatusPolicyViolation)
		}
	})

	t.Run("a slot freed within the grace period is used", func(t *testing.T) {
		c := newTestConn(2 * time.Second)
		c.enqueue([]byte("1"))
		c.enqueue([]byte("2"))
		go func() {
			time.Sleep(20 * time.Millisecond)
			<-c.send
		}()
		if !c.enqueue([]byte("3")) {
			t.Fatal("enqueue failed although a slot freed in time")
		}
		select {
		case <-c.done:
			t.Fatal("connection was closed")
		default:
		}
	})

	t.Run("a stuck client is closed after the grace period", func(t *testing.T) {
		c := newTestConn(30 * time.Millisecond)
		c.enqueue([]byte("1"))
		c.enqueue([]byte("2"))
		start := time.Now()
		if c.enqueue([]byte("3")) {
			t.Fatal("enqueue succeeded with a stuck client")
		}
		if d := time.Since(start); d < 30*time.Millisecond {
			t.Fatalf("gave up after %v, before the grace period", d)
		}
		<-c.done
		if c.code != websocket.StatusPolicyViolation {
			t.Fatalf("code = %d", c.code)
		}
	})

	t.Run("a closed connection stops waiting", func(t *testing.T) {
		c := newTestConn(10 * time.Second)
		c.enqueue([]byte("1"))
		c.enqueue([]byte("2"))
		go func() {
			time.Sleep(20 * time.Millisecond)
			c.fail(websocket.StatusGoingAway, "x")
		}()
		if c.enqueue([]byte("3")) {
			t.Fatal("enqueue succeeded on a closed connection")
		}
		if c.code != websocket.StatusGoingAway {
			t.Fatalf("code = %d, want the first failure to win", c.code)
		}
	})
}

// ---- tests: shutdown and goroutines ----

func TestShutdownClosesConnectionsWithGoingAway(t *testing.T) {
	e := newEnv(t, nil)
	a := e.dial(t, testRoom, "alice", 0)
	a.waitPresence(viewing("alice", "Alice"))

	// The client must keep reading while the server shuts down: the close
	// handshake waits for its reply (a browser answers on its own).
	shutdownErr := e.shutdownAsync()
	for {
		if _, err := a.read(); err != nil {
			if got := websocket.CloseStatus(err); got != websocket.StatusGoingAway {
				t.Fatalf("close status = %d (%v), want %d", got, err, websocket.StatusGoingAway)
			}
			break
		}
	}
	if err := <-shutdownErr; err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if n := e.log.active.Load(); n != 0 {
		t.Fatalf("%d subscriptions still open after Shutdown", n)
	}

	resp, err := http.Get(e.srv.URL + "/api/rooms/r_1/ws?user=alice")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status after Shutdown = %d, want 503", resp.StatusCode)
	}
}

func TestGoroutinesEndWhenConnectionsClose(t *testing.T) {
	e := newEnv(t, func(c *Config) { c.PingInterval = 20 * time.Millisecond })
	// Warm up: start the HTTP server and client machinery once so it is part of the baseline.
	w := e.dial(t, testRoom, "alice", 0)
	w.waitPresence(viewing("alice", "Alice"))
	_ = w.conn.Close(websocket.StatusNormalClosure, "")
	waitFor(t, "warm-up connection to end", func() bool {
		return e.log.active.Load() == 0 && len(e.h.PresenceMembers(testRoom)) == 0
	})
	waitFor(t, "goroutines to settle", func() bool { return settled(5) })
	base := runtime.NumGoroutine()

	for i := range 8 {
		c := e.dial(t, testRoom, "bob", 0)
		c.waitPresence(viewing("bob", "Bob"))
		e.log.Publish(ev(int64(i + 1))) // exercise the pump and writer too
		_ = c.conn.Close(websocket.StatusNormalClosure, "")
	}
	waitFor(t, "connections to end", func() bool {
		return e.log.active.Load() == 0 && len(e.h.PresenceMembers(testRoom)) == 0
	})
	waitFor(t, "goroutine count to return to the baseline", func() bool {
		return runtime.NumGoroutine() <= base
	})

	// And a server-side stop ends them too.
	a := e.dial(t, testRoom, "alice", 0)
	a.waitPresence(viewing("alice", "Alice"))
	shutdownErr := e.shutdownAsync()
	for {
		if _, err := a.read(); err != nil {
			break
		}
	}
	if err := <-shutdownErr; err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	waitFor(t, "goroutine count to return to the baseline after Shutdown", func() bool {
		return runtime.NumGoroutine() <= base
	})
}

// settled reports whether the goroutine count has been stable for n looks.
func settled(n int) bool {
	last := runtime.NumGoroutine()
	for range n {
		time.Sleep(10 * time.Millisecond)
		cur := runtime.NumGoroutine()
		if cur != last {
			return false
		}
		last = cur
	}
	return true
}
