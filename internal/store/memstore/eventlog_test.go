package memstore

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"piperoom/internal/events"
	"piperoom/internal/store"
)

// newLogRoom returns a store with rooms r1 and r2 and a fixed clock.
func newLogStore(t *testing.T) (*Store, time.Time) {
	t.Helper()
	at := time.Date(2026, 10, 12, 9, 30, 0, 0, time.UTC)
	s := NewWithClock(func() time.Time { return at })
	for _, id := range []string{"r1", "r2"} {
		if err := s.CreateRoom(context.Background(), store.Room{ID: id, Title: id, Severity: "sev2", PipelineID: "orders", Status: "open"}); err != nil {
			t.Fatalf("create %s: %v", id, err)
		}
	}
	return s, at
}

func appendN(t *testing.T, s *Store, roomID string, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		if _, err := s.Append(context.Background(), roomID, events.CommentAdded, testActor(), map[string]int{"i": i}); err != nil {
			t.Fatalf("append %s #%d: %v", roomID, i, err)
		}
	}
}

func seqs(evs []events.Event) []int64 {
	out := make([]int64, 0, len(evs))
	for _, e := range evs {
		out = append(out, e.Seq)
	}
	return out
}

func TestAppendSeqIsGaplessPerRoom(t *testing.T) {
	s, _ := newLogStore(t)
	ctx := context.Background()
	var r1, r2 []int64
	// Interleave the rooms: each room counts on its own.
	for i := 0; i < 3; i++ {
		e, err := s.Append(ctx, "r1", events.RoomCreated, testActor(), map[string]int{"i": i})
		if err != nil {
			t.Fatal(err)
		}
		r1 = append(r1, e.Seq)
		if i < 2 {
			e, err := s.Append(ctx, "r2", events.RoomCreated, testActor(), map[string]int{"i": i})
			if err != nil {
				t.Fatal(err)
			}
			r2 = append(r2, e.Seq)
		}
	}
	if !slices.Equal(r1, []int64{1, 2, 3}) || !slices.Equal(r2, []int64{1, 2}) {
		t.Fatalf("seqs r1=%v r2=%v, want 1,2,3 and 1,2", r1, r2)
	}
	meta, err := s.GetRoom(ctx, "r1")
	if err != nil {
		t.Fatal(err)
	}
	if meta.LastSeq != 3 {
		t.Fatalf("room LastSeq = %d, want 3", meta.LastSeq)
	}
}

func TestAppendEventFields(t *testing.T) {
	s, at := newLogStore(t)
	actor := events.Actor{Kind: events.ActorHuman, ID: "u7", Name: "Kabir"}
	e, err := s.Append(context.Background(), "r1", events.CommentAdded, actor, map[string]string{"text": "hi"})
	if err != nil {
		t.Fatal(err)
	}
	if e.RoomID != "r1" || e.Seq != 1 || e.Type != events.CommentAdded || e.Actor != actor {
		t.Fatalf("event = %+v", e)
	}
	if string(e.Payload) != `{"text":"hi"}` {
		t.Fatalf("payload = %s, want the JSON of the argument", e.Payload)
	}
	if !e.At.Equal(at) || e.At.Location() != time.UTC {
		t.Fatalf("At = %v (%v), want %v in UTC", e.At, e.At.Location(), at)
	}
}

func TestAppendTimestampIsUTCWhateverTheClockZone(t *testing.T) {
	ist := time.FixedZone("IST", 5*3600+30*60)
	s := NewWithClock(func() time.Time { return time.Date(2026, 10, 12, 15, 0, 0, 0, ist) })
	if err := s.CreateRoom(context.Background(), store.Room{ID: "r1", Title: "t", Severity: "sev2", PipelineID: "p", Status: "open"}); err != nil {
		t.Fatal(err)
	}
	e, err := s.Append(context.Background(), "r1", events.RoomCreated, testActor(), map[string]int{})
	if err != nil {
		t.Fatal(err)
	}
	if e.At.Location() != time.UTC || e.At.Hour() != 9 || e.At.Minute() != 30 {
		t.Fatalf("At = %v, want 09:30 UTC", e.At)
	}
}

func TestAppendFailuresDoNotConsumeSeq(t *testing.T) {
	ctx := context.Background()
	cancelled, cancel := context.WithCancel(ctx)
	cancel()

	tests := []struct {
		name    string
		ctx     context.Context
		room    string
		payload any
		wantErr error
		wantMsg string
	}{
		{name: "unknown room", ctx: ctx, room: "nope", payload: map[string]int{}, wantErr: store.ErrNotFound},
		{name: "cancelled context", ctx: cancelled, room: "r1", payload: map[string]int{}, wantErr: context.Canceled},
		{name: "unmarshallable payload", ctx: ctx, room: "r1", payload: make(chan int), wantMsg: "marshal payload"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, _ := newLogStore(t)
			_, err := s.Append(tt.ctx, tt.room, events.CommentAdded, testActor(), tt.payload)
			if err == nil {
				t.Fatal("want error")
			}
			if tt.wantErr != nil && !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if tt.wantMsg != "" && !strings.Contains(err.Error(), tt.wantMsg) {
				t.Fatalf("err = %v, want it to mention %q", err, tt.wantMsg)
			}
			// The failed append must not leave a gap: the next event is seq 1.
			e, err := s.Append(ctx, "r1", events.CommentAdded, testActor(), map[string]int{})
			if err != nil {
				t.Fatalf("append after failure: %v", err)
			}
			if e.Seq != 1 {
				t.Fatalf("seq after failed append = %d, want 1", e.Seq)
			}
		})
	}
}

func TestSinceTable(t *testing.T) {
	s, _ := newLogStore(t)
	appendN(t, s, "r1", 5)
	ctx := context.Background()

	tests := []struct {
		name  string
		after int64
		limit int
		want  []int64
	}{
		{name: "from the start", after: 0, limit: 10, want: []int64{1, 2, 3, 4, 5}},
		{name: "limit cuts the page", after: 0, limit: 2, want: []int64{1, 2}},
		{name: "after a middle seq", after: 2, limit: 2, want: []int64{3, 4}},
		{name: "page reaches the end", after: 3, limit: 10, want: []int64{4, 5}},
		{name: "after the last seq is empty", after: 5, limit: 10, want: []int64{}},
		{name: "after beyond the last seq is empty", after: 99, limit: 10, want: []int64{}},
		{name: "limit zero is empty", after: 0, limit: 0, want: []int64{}},
		{name: "negative limit is empty", after: 0, limit: -1, want: []int64{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := s.Since(ctx, "r1", tt.after, tt.limit)
			if err != nil {
				t.Fatalf("Since: %v", err)
			}
			if got == nil {
				t.Fatal("Since returned nil, want an empty slice")
			}
			if !slices.Equal(seqs(got), tt.want) {
				t.Fatalf("seqs = %v, want %v", seqs(got), tt.want)
			}
		})
	}
}

func TestSinceErrors(t *testing.T) {
	s, _ := newLogStore(t)
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := s.Since(context.Background(), "nope", 0, 10); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("unknown room: err = %v, want store.ErrNotFound", err)
	}
	if _, err := s.Since(cancelled, "r1", 0, 10); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled ctx: err = %v, want context.Canceled", err)
	}
}

func TestSinceReturnsIndependentSlice(t *testing.T) {
	s, _ := newLogStore(t)
	appendN(t, s, "r1", 2)
	ctx := context.Background()

	first, err := s.Since(ctx, "r1", 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	first[0].Seq = 99 // a caller changing its copy must not change the log

	again, err := s.Since(ctx, "r1", 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(seqs(again), []int64{1, 2}) {
		t.Fatalf("log changed by caller: seqs = %v", seqs(again))
	}
}

// recvSeqs reads exactly n events from ch, failing the test if they do not
// arrive in time.
func recvSeqs(t *testing.T, ch <-chan events.Event, n int) []int64 {
	t.Helper()
	var out []int64
	timeout := time.After(5 * time.Second)
	for len(out) < n {
		select {
		case e, ok := <-ch:
			if !ok {
				t.Fatalf("channel closed after %d of %d events", len(out), n)
			}
			out = append(out, e.Seq)
		case <-timeout:
			t.Fatalf("timed out after %d of %d events: %v", len(out), n, out)
		}
	}
	return out
}

func TestSubscribeReplayThenLive(t *testing.T) {
	s, _ := newLogStore(t)
	appendN(t, s, "r1", 3)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch, err := s.Subscribe(ctx, "r1", 1)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if got := recvSeqs(t, ch, 2); !slices.Equal(got, []int64{2, 3}) {
		t.Fatalf("replay = %v, want [2 3]", got)
	}

	appendN(t, s, "r1", 2)
	if got := recvSeqs(t, ch, 2); !slices.Equal(got, []int64{4, 5}) {
		t.Fatalf("live = %v, want [4 5]", got)
	}

	cancel()
	for range ch { // drain anything in flight; the channel must close
	}
}

func TestSubscribeClosesWhenContextEnds(t *testing.T) {
	s, _ := newLogStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	ch, err := s.Subscribe(ctx, "r1", 0)
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	timeout := time.After(5 * time.Second)
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				return
			}
		case <-timeout:
			t.Fatal("channel did not close after the context was cancelled")
		}
	}
}

func TestSubscribeErrors(t *testing.T) {
	s, _ := newLogStore(t)
	if _, err := s.Subscribe(context.Background(), "nope", 0); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("unknown room: err = %v, want store.ErrNotFound", err)
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.Subscribe(cancelled, "r1", 0); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled ctx: err = %v, want context.Canceled", err)
	}
}

func TestSubscribeNoGapsOrRepeatsUnderConcurrentAppend(t *testing.T) {
	// The contract: no event is skipped or repeated, even when appends race
	// with the replay and the live phase.
	s, _ := newLogStore(t)
	appendN(t, s, "r1", 10)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch, err := s.Subscribe(ctx, "r1", 0)
	if err != nil {
		t.Fatal(err)
	}
	const total = 200
	done := make(chan error, 1)
	go func() {
		for i := 10; i < total; i++ {
			if _, err := s.Append(context.Background(), "r1", events.CommentAdded, testActor(), map[string]int{"i": i}); err != nil {
				done <- fmt.Errorf("append %d: %w", i, err)
				return
			}
		}
		done <- nil
	}()
	got := recvSeqs(t, ch, total)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	for i, seq := range got {
		if seq != int64(i+1) {
			t.Fatalf("event %d has seq %d, want %d (gap or repeat)", i, seq, i+1)
		}
	}
}
