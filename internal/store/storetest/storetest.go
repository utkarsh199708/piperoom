// Package storetest is the behaviour suite every store.Store implementation must
// pass. memstore runs it as a unit test and the Postgres store runs it against a
// real database, so the fake used by other packages' tests cannot drift from the
// real thing.
package storetest

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"piperoom/internal/events"
	"piperoom/internal/store"
)

// Factory returns a store ready to use. It may be backed by a shared database:
// the suite only ever touches rooms it creates itself, named it_<random>.
type Factory func(t *testing.T) store.Store

// waitFor bounds every blocking receive so a broken store fails fast.
const waitFor = 10 * time.Second

// firstErr records the first error reported by any goroutine. (atomic.Value
// would panic on two different concrete error types.)
type firstErr struct {
	mu  sync.Mutex
	err error
}

func (f *firstErr) set(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err == nil {
		f.err = err
	}
}

func (f *firstErr) get() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.err
}

var human = events.Actor{Kind: events.ActorHuman, ID: "u-ada", Name: "Ada"}

// Run executes the whole suite as subtests of t.
func Run(t *testing.T, newStore Factory) {
	t.Helper()
	cases := []struct {
		name string
		fn   func(t *testing.T, s store.Store)
	}{
		{"AppendAssignsGaplessSeq", testAppendSeq},
		{"AppendReturnsWhatIsStored", testAppendReturnsStored},
		{"AppendUnknownRoom", testAppendUnknownRoom},
		{"AppendPayloadForms", testAppendPayloadForms},
		{"CreateRoomDuplicate", testCreateRoomDuplicate},
		{"RoomIDsOldestFirst", testRoomIDsOrder},
		{"SinceWindowAndLimits", testSinceWindow},
		{"SinceCapsLimit", testSinceCapsLimit},
		{"SinceEmptyRoom", testSinceEmptyRoom},
		{"SinceUnknownRoom", testSinceUnknownRoom},
		{"ConcurrentAppendsHaveNoGaps", testConcurrentAppends},
		{"SubscribeReplaysThenGoesLive", testSubscribeReplayThenLive},
		{"SubscribeFromLastSeqSeesOnlyNew", testSubscribeFromLast},
		{"SubscribeMidStreamSeesEachEventOnce", testSubscribeMidStream},
		{"SubscribeManyConsumersAgree", testSubscribeMany},
		{"SubscribeUnknownRoom", testSubscribeUnknownRoom},
		{"SubscribeCancelClosesChannel", testSubscribeCancel},
		{"SubscribeIgnoresOtherRooms", testSubscribeIsolation},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			c.fn(t, newStore(t))
		})
	}
}

// NewRoomID returns a unique room id; tests on a shared database never collide.
func NewRoomID() string {
	b := make([]byte, 5)
	_, _ = rand.Read(b)
	return "it_" + hex.EncodeToString(b)
}

func newRoom(t *testing.T, s store.Store) string {
	t.Helper()
	id := NewRoomID()
	if err := s.CreateRoom(context.Background(), id); err != nil {
		t.Fatalf("CreateRoom(%s): %v", id, err)
	}
	return id
}

func comment(i int) events.CommentAddedPayload {
	return events.CommentAddedPayload{CommentID: fmt.Sprintf("c%d", i), Text: fmt.Sprintf("comment %d", i)}
}

func appendN(t *testing.T, s store.Store, roomID string, n int) []events.Event {
	t.Helper()
	out := make([]events.Event, 0, n)
	for i := 1; i <= n; i++ {
		ev, err := s.Append(context.Background(), roomID, events.CommentAdded, human, comment(i))
		if err != nil {
			t.Fatalf("Append #%d: %v", i, err)
		}
		out = append(out, ev)
	}
	return out
}

// expectSeqs fails unless got has exactly the seqs from..to in order.
func expectSeqs(t *testing.T, got []events.Event, from, to int64) {
	t.Helper()
	want := int(to - from + 1)
	if to < from {
		want = 0
	}
	if len(got) != want {
		t.Fatalf("got %d events %v, want %d (seq %d..%d)", len(got), seqsOf(got), want, from, to)
	}
	for i, ev := range got {
		if ev.Seq != from+int64(i) {
			t.Fatalf("event %d has seq %d, want %d (all: %v)", i, ev.Seq, from+int64(i), seqsOf(got))
		}
	}
}

func seqsOf(evs []events.Event) []int64 {
	out := make([]int64, len(evs))
	for i, e := range evs {
		out[i] = e.Seq
	}
	return out
}

// collect reads n events from ch, failing on timeout or early close.
func collect(t *testing.T, ch <-chan events.Event, n int) []events.Event {
	t.Helper()
	out := make([]events.Event, 0, n)
	timer := time.NewTimer(waitFor)
	defer timer.Stop()
	for len(out) < n {
		select {
		case ev, ok := <-ch:
			if !ok {
				t.Fatalf("channel closed after %d of %d events (seqs %v)", len(out), n, seqsOf(out))
			}
			out = append(out, ev)
		case <-timer.C:
			t.Fatalf("timed out after %d of %d events (seqs %v)", len(out), n, seqsOf(out))
		}
	}
	return out
}

// expectNothing fails if an event arrives within d.
func expectNothing(t *testing.T, ch <-chan events.Event, d time.Duration) {
	t.Helper()
	select {
	case ev, ok := <-ch:
		if ok {
			t.Fatalf("unexpected event seq %d", ev.Seq)
		}
		t.Fatal("channel closed unexpectedly")
	case <-time.After(d):
	}
}

func expectClosed(t *testing.T, ch <-chan events.Event) {
	t.Helper()
	timer := time.NewTimer(waitFor)
	defer timer.Stop()
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				return
			}
		case <-timer.C:
			t.Fatal("channel not closed in time")
		}
	}
}

func testAppendSeq(t *testing.T, s store.Store) {
	room := newRoom(t, s)
	evs := appendN(t, s, room, 5)
	expectSeqs(t, evs, 1, 5)
}

func testAppendReturnsStored(t *testing.T, s store.Store) {
	ctx := context.Background()
	room := newRoom(t, s)
	ev, err := s.Append(ctx, room, events.CommentAdded, human, events.CommentAddedPayload{
		CommentID: "c1", StepID: "s2", Text: "hello @agent", MentionsAgent: true,
	})
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if ev.RoomID != room || ev.Seq != 1 || ev.Type != events.CommentAdded || ev.Actor != human {
		t.Fatalf("returned event has wrong header: %+v", ev)
	}
	if ev.At.IsZero() || ev.At.Location() != time.UTC {
		t.Fatalf("At must be set and in UTC, got %v (%v)", ev.At, ev.At.Location())
	}
	p, err := events.Decode[events.CommentAddedPayload](ev)
	if err != nil || p.CommentID != "c1" || p.StepID != "s2" || p.Text != "hello @agent" || !p.MentionsAgent {
		t.Fatalf("payload did not round-trip: %+v, %v", p, err)
	}

	got, err := s.Since(ctx, room, 0, 10)
	if err != nil || len(got) != 1 {
		t.Fatalf("Since: %v, %d events", err, len(got))
	}
	g := got[0]
	if g.RoomID != ev.RoomID || g.Seq != ev.Seq || g.Type != ev.Type || g.Actor != ev.Actor ||
		!g.At.Equal(ev.At) || g.At.Location() != time.UTC || !bytes.Equal(g.Payload, ev.Payload) {
		t.Fatalf("Since returned a different event than Append did:\n append: %+v\n since:  %+v", ev, g)
	}
}

func testAppendUnknownRoom(t *testing.T, s store.Store) {
	_, err := s.Append(context.Background(), NewRoomID(), events.CommentAdded, human, comment(1))
	if !errors.Is(err, store.ErrRoomNotFound) {
		t.Fatalf("want ErrRoomNotFound, got %v", err)
	}
}

func testAppendPayloadForms(t *testing.T, s store.Store) {
	ctx := context.Background()
	room := newRoom(t, s)

	raw := json.RawMessage(`{"alert":{"name":"lag","labels":["a","b"],"n":3}}`)
	ev, err := s.Append(ctx, room, events.RoomCreated, events.Actor{Kind: events.ActorSystem, ID: "system", Name: "System"}, raw)
	if err != nil {
		t.Fatalf("Append(RawMessage): %v", err)
	}
	if !jsonEqual(t, ev.Payload, raw) {
		t.Fatalf("RawMessage payload changed: %s", ev.Payload)
	}
	// nil payloads become JSON null rather than failing.
	if ev, err = s.Append(ctx, room, events.ControlRequested, human, nil); err != nil {
		t.Fatalf("Append(nil): %v", err)
	} else if !jsonEqual(t, ev.Payload, json.RawMessage("null")) {
		t.Fatalf("nil payload stored as %s", ev.Payload)
	}
	if _, err = s.Append(ctx, room, events.ControlRequested, human, events.ControlRequestedPayload{}); err != nil {
		t.Fatalf("Append(struct{}): %v", err)
	}

	// A payload that cannot be JSON must fail without burning a seq.
	if _, err = s.Append(ctx, room, events.CommentAdded, human, json.RawMessage(`{not json`)); err == nil {
		t.Fatal("invalid RawMessage was accepted")
	}
	if _, err = s.Append(ctx, room, events.CommentAdded, human, make(chan int)); err == nil {
		t.Fatal("unmarshalable payload was accepted")
	}
	next, err := s.Append(ctx, room, events.CommentAdded, human, comment(1))
	if err != nil {
		t.Fatalf("Append after failures: %v", err)
	}
	if next.Seq != 4 {
		t.Fatalf("failed appends must not consume a seq: next seq = %d, want 4", next.Seq)
	}

	// Untrusted text can hold a NUL, which Postgres jsonb refuses; the store
	// must still accept the event (NUL is replaced by U+FFFD).
	nul, err := s.Append(ctx, room, events.AgentMessage, events.AgentActor, events.AgentMessagePayload{Text: "a\x00b"})
	if err != nil {
		t.Fatalf("Append(payload with NUL): %v", err)
	}
	if p, err := events.Decode[events.AgentMessagePayload](nul); err != nil || p.Text != "a\ufffdb" {
		t.Fatalf("NUL payload came back as %q, %v; want %q", p.Text, err, "a\ufffdb")
	}

	// A large payload (a long tool result) round-trips intact.
	big := events.AgentMessagePayload{Text: string(bytes.Repeat([]byte("x\u00e9 "), 60_000))}
	evBig, err := s.Append(ctx, room, events.AgentMessage, events.AgentActor, big)
	if err != nil {
		t.Fatalf("Append(large payload): %v", err)
	}
	got, err := s.Since(ctx, room, evBig.Seq-1, 1)
	if err != nil || len(got) != 1 {
		t.Fatalf("Since after large payload: %v, %d events", err, len(got))
	}
	if p, err := events.Decode[events.AgentMessagePayload](got[0]); err != nil || p.Text != big.Text {
		t.Fatalf("large payload was altered (%d bytes back, %v)", len(p.Text), err)
	}
}

func jsonEqual(t *testing.T, a, b json.RawMessage) bool {
	t.Helper()
	var va, vb any
	if err := json.Unmarshal(a, &va); err != nil {
		t.Fatalf("not JSON: %s: %v", a, err)
	}
	if err := json.Unmarshal(b, &vb); err != nil {
		t.Fatalf("not JSON: %s: %v", b, err)
	}
	ja, _ := json.Marshal(va)
	jb, _ := json.Marshal(vb)
	return bytes.Equal(ja, jb)
}

func testCreateRoomDuplicate(t *testing.T, s store.Store) {
	room := newRoom(t, s)
	if err := s.CreateRoom(context.Background(), room); !errors.Is(err, store.ErrRoomExists) {
		t.Fatalf("want ErrRoomExists, got %v", err)
	}
	if err := s.CreateRoom(context.Background(), ""); err == nil {
		t.Fatal("empty room id was accepted")
	}
}

func testRoomIDsOrder(t *testing.T, s store.Store) {
	ctx := context.Background()
	var mine []string
	for range 3 {
		mine = append(mine, newRoom(t, s))
		time.Sleep(3 * time.Millisecond) // distinct created_at even on coarse clocks
	}
	all, err := s.RoomIDs(ctx)
	if err != nil {
		t.Fatalf("RoomIDs: %v", err)
	}
	if all == nil {
		t.Fatal("RoomIDs returned nil")
	}
	// Other subtests create rooms concurrently, so compare only our own, in order.
	var got []string
	for _, id := range all {
		for _, m := range mine {
			if id == m {
				got = append(got, id)
			}
		}
	}
	if fmt.Sprint(got) != fmt.Sprint(mine) {
		t.Fatalf("rooms not listed oldest first: got %v, want %v", got, mine)
	}
}

func testSinceWindow(t *testing.T, s store.Store) {
	ctx := context.Background()
	room := newRoom(t, s)
	appendN(t, s, room, 5)

	tests := []struct {
		name           string
		after          int64
		limit          int
		wantFrom, want int64 // wantFrom..want inclusive; want < wantFrom means empty
	}{
		{"everything", 0, 0, 1, 5},
		{"negative after is zero", -4, 0, 1, 5},
		{"negative limit is the default", 0, -1, 1, 5},
		{"after 2", 2, 0, 3, 5},
		{"after 2 limit 2", 2, 2, 3, 4},
		{"limit 1", 0, 1, 1, 1},
		{"after the last", 5, 0, 6, 5},
		{"after far beyond the last", 99, 10, 100, 99},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := s.Since(ctx, room, tc.after, tc.limit)
			if err != nil {
				t.Fatalf("Since: %v", err)
			}
			if got == nil {
				t.Fatal("Since returned a nil slice")
			}
			expectSeqs(t, got, tc.wantFrom, tc.want)
		})
	}
}

func testSinceCapsLimit(t *testing.T, s store.Store) {
	ctx := context.Background()
	room := newRoom(t, s)
	const total = 1010
	var wg sync.WaitGroup
	errs := make(chan error, 10)
	for w := range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range total / 10 {
				if _, err := s.Append(ctx, room, events.CommentAdded, human, comment(w*1000+i)); err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("Append: %v", err)
	}

	for _, tc := range []struct{ limit, want int }{{0, 500}, {-3, 500}, {7, 7}, {1000, 1000}, {5000, 1000}} {
		got, err := s.Since(ctx, room, 0, tc.limit)
		if err != nil {
			t.Fatalf("Since limit %d: %v", tc.limit, err)
		}
		if len(got) != tc.want {
			t.Fatalf("Since limit %d returned %d events, want %d", tc.limit, len(got), tc.want)
		}
	}
}

func testSinceEmptyRoom(t *testing.T, s store.Store) {
	room := newRoom(t, s)
	got, err := s.Since(context.Background(), room, 0, 0)
	if err != nil {
		t.Fatalf("Since on an empty room: %v", err)
	}
	if got == nil || len(got) != 0 {
		t.Fatalf("want empty non-nil slice, got %#v", got)
	}
}

func testSinceUnknownRoom(t *testing.T, s store.Store) {
	_, err := s.Since(context.Background(), NewRoomID(), 0, 10)
	if !errors.Is(err, store.ErrRoomNotFound) {
		t.Fatalf("want ErrRoomNotFound, got %v", err)
	}
}

func testConcurrentAppends(t *testing.T, s store.Store) {
	ctx := context.Background()
	room := newRoom(t, s)
	const goroutines, each = 20, 10

	var (
		wg     sync.WaitGroup
		mu     sync.Mutex
		seen   = map[int64]int{}
		failed firstErr
	)
	for g := range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range each {
				ev, err := s.Append(ctx, room, events.CommentAdded, human, comment(g*100+i))
				if err != nil {
					failed.set(err)
					return
				}
				mu.Lock()
				seen[ev.Seq]++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if err := failed.get(); err != nil {
		t.Fatalf("Append: %v", err)
	}

	for seq := int64(1); seq <= goroutines*each; seq++ {
		if seen[seq] != 1 {
			t.Fatalf("seq %d was handed out %d times", seq, seen[seq])
		}
	}
	if len(seen) != goroutines*each {
		t.Fatalf("got %d distinct seqs, want %d", len(seen), goroutines*each)
	}

	got, err := s.Since(ctx, room, 0, 1000)
	if err != nil {
		t.Fatalf("Since: %v", err)
	}
	expectSeqs(t, got, 1, goroutines*each)
	// Seq order must match time order: the next append happens after the previous.
	for i := 1; i < len(got); i++ {
		if got[i].At.Before(got[i-1].At) {
			t.Fatalf("seq %d is stamped %v, before seq %d at %v", got[i].Seq, got[i].At, got[i-1].Seq, got[i-1].At)
		}
	}
}

func testSubscribeReplayThenLive(t *testing.T, s store.Store) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	room := newRoom(t, s)
	appendN(t, s, room, 5)

	ch, err := s.Subscribe(ctx, room, 2)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	expectSeqs(t, collect(t, ch, 3), 3, 5) // replay
	expectNothing(t, ch, 100*time.Millisecond)

	if _, err := s.Append(ctx, room, events.AgentMessage, events.AgentActor, events.AgentMessagePayload{Text: "live"}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	live := collect(t, ch, 1)
	expectSeqs(t, live, 6, 6)
	if live[0].Type != events.AgentMessage || live[0].Actor != events.AgentActor {
		t.Fatalf("live event lost its fields: %+v", live[0])
	}
}

func testSubscribeFromLast(t *testing.T, s store.Store) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	room := newRoom(t, s)
	appendN(t, s, room, 3)

	ch, err := s.Subscribe(ctx, room, 3)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	expectNothing(t, ch, 150*time.Millisecond)
	appendN(t, s, room, 2)
	expectSeqs(t, collect(t, ch, 2), 4, 5)
}

// testSubscribeMidStream is the headline guarantee: writers are busy, a
// subscriber joins in the middle, and it still sees every event exactly once, in
// order, from the seq it asked for.
func testSubscribeMidStream(t *testing.T, s store.Store) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	room := newRoom(t, s)
	const writers, each = 4, 150
	const total = writers * each

	var (
		appended atomic.Int64
		wg       sync.WaitGroup
		failed   firstErr
	)
	for w := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range each {
				if _, err := s.Append(ctx, room, events.CommentAdded, human, comment(w*1000+i)); err != nil {
					failed.set(err)
					return
				}
				appended.Add(1)
			}
		}()
	}

	// Join once the stream is demonstrably in flight, but not finished.
	for appended.Load() < 40 && failed.get() == nil {
		time.Sleep(time.Millisecond)
	}
	const after = 10
	ch, err := s.Subscribe(ctx, room, after)
	if err != nil {
		t.Fatalf("Subscribe mid-stream: %v", err)
	}
	got := collect(t, ch, total-after)
	wg.Wait()
	if err := failed.get(); err != nil {
		t.Fatalf("Append: %v", err)
	}
	expectSeqs(t, got, after+1, total)
	expectNothing(t, ch, 150*time.Millisecond) // nothing repeated afterwards
}

func testSubscribeMany(t *testing.T, s store.Store) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	room := newRoom(t, s)
	appendN(t, s, room, 3)

	const subscribers = 5
	chans := make([]<-chan events.Event, subscribers)
	for i := range chans {
		ch, err := s.Subscribe(ctx, room, 0)
		if err != nil {
			t.Fatalf("Subscribe %d: %v", i, err)
		}
		chans[i] = ch
	}
	appendN(t, s, room, 20)
	for i, ch := range chans {
		t.Run(fmt.Sprintf("subscriber %d", i), func(t *testing.T) {
			expectSeqs(t, collect(t, ch, 23), 1, 23)
		})
	}
}

func testSubscribeUnknownRoom(t *testing.T, s store.Store) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if _, err := s.Subscribe(ctx, NewRoomID(), 0); !errors.Is(err, store.ErrRoomNotFound) {
		t.Fatalf("want ErrRoomNotFound, got %v", err)
	}
}

func testSubscribeCancel(t *testing.T, s store.Store) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	room := newRoom(t, s)
	appendN(t, s, room, 2)
	ch, err := s.Subscribe(ctx, room, 0)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	expectSeqs(t, collect(t, ch, 2), 1, 2)
	cancel()
	expectClosed(t, ch)
}

func testSubscribeIsolation(t *testing.T, s store.Store) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	a, b := newRoom(t, s), newRoom(t, s)
	chA, err := s.Subscribe(ctx, a, 0)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	appendN(t, s, b, 3) // seq restarts at 1 in every room and must not leak into A
	expectNothing(t, chA, 150*time.Millisecond)
	appendN(t, s, a, 2)
	got := collect(t, chA, 2)
	expectSeqs(t, got, 1, 2)
	for _, ev := range got {
		if ev.RoomID != a {
			t.Fatalf("event from room %s delivered to a subscriber of %s", ev.RoomID, a)
		}
	}
}
