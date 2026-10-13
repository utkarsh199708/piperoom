package memstore

import (
	"context"
	"errors"
	"testing"
	"time"

	"piperoom/internal/events"
	"piperoom/internal/store"
)

// TestSinceMatchesLinearScan pins Since to its original definition (every
// event with Seq > afterSeq, in order, at most limit) for every afterSeq and
// limit around the edges, so the indexed lookup cannot drift from it.
func TestSinceMatchesLinearScan(t *testing.T) {
	s := New()
	roomID := newTestRoom(t, s)
	ctx := context.Background()
	const n = 37
	for i := 0; i < n; i++ {
		if _, err := s.Append(ctx, roomID, events.CommentAdded, testActor(), events.CommentAddedPayload{CommentID: "c", Text: "x"}); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	all, err := s.Since(ctx, roomID, 0, n+10)
	if err != nil {
		t.Fatalf("since all: %v", err)
	}
	if len(all) != n {
		t.Fatalf("want %d events, got %d", n, len(all))
	}
	reference := func(after int64, limit int) []events.Event {
		out := []events.Event{}
		for _, e := range all {
			if e.Seq > after {
				out = append(out, e)
				if len(out) >= limit {
					break
				}
			}
		}
		return out
	}
	for _, after := range []int64{-5, 0, 1, 2, 18, 35, 36, 37, 38, 1000} {
		for _, limit := range []int{1, 2, 5, 36, 37, 38, 1000} {
			got, err := s.Since(ctx, roomID, after, limit)
			if err != nil {
				t.Fatalf("since(%d, %d): %v", after, limit, err)
			}
			want := reference(after, limit)
			if got == nil || len(got) != len(want) {
				t.Fatalf("since(%d, %d): got %d events (nil=%v), want %d", after, limit, len(got), got == nil, len(want))
			}
			for i := range want {
				if got[i].Seq != want[i].Seq || got[i].Type != want[i].Type {
					t.Fatalf("since(%d, %d)[%d]: got seq %d, want %d", after, limit, i, got[i].Seq, want[i].Seq)
				}
			}
		}
	}
}

// TestSinceReturnsCopy checks the caller cannot change the stored log through
// the returned slice.
func TestSinceReturnsCopy(t *testing.T) {
	s := New()
	roomID := newTestRoom(t, s)
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		if _, err := s.Append(ctx, roomID, events.CommentAdded, testActor(), events.CommentAddedPayload{CommentID: "c", Text: "x"}); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	got, err := s.Since(ctx, roomID, 0, 10)
	if err != nil {
		t.Fatalf("since: %v", err)
	}
	got[0].Seq = 99
	_ = append(got[:1], events.Event{Seq: 42})
	again, err := s.Since(ctx, roomID, 0, 10)
	if err != nil {
		t.Fatalf("since again: %v", err)
	}
	for i, e := range again {
		if e.Seq != int64(i+1) {
			t.Fatalf("stored log changed: event %d has seq %d", i, e.Seq)
		}
	}
}

// TestSubscribeSlowReaderGetsEverything appends far more events than the
// subscription buffer holds before reading any, then checks the subscriber
// still sees every seq exactly once and in order: a full channel only makes
// the subscriber goroutine wait, it never drops.
func TestSubscribeSlowReaderGetsEverything(t *testing.T) {
	s := New()
	roomID := newTestRoom(t, s)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	const before, live = 3, 10 * store.SubscribeBuffer
	for i := 0; i < before; i++ {
		if _, err := s.Append(ctx, roomID, events.CommentAdded, testActor(), events.CommentAddedPayload{CommentID: "c", Text: "x"}); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	ch, err := s.Subscribe(ctx, roomID, 1)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	for i := 0; i < live; i++ {
		if _, err := s.Append(ctx, roomID, events.CommentAdded, testActor(), events.CommentAddedPayload{CommentID: "c", Text: "x"}); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	want := int64(2)
	timeout := time.After(5 * time.Second)
	for want <= before+live {
		select {
		case e, ok := <-ch:
			if !ok {
				t.Fatalf("channel closed at seq %d", want)
			}
			if e.Seq != want {
				t.Fatalf("got seq %d, want %d", e.Seq, want)
			}
			want++
		case <-timeout:
			t.Fatalf("timed out waiting for seq %d", want)
		}
	}
}

func TestSinceErrors(t *testing.T) {
	s := New()
	newTestRoom(t, s)
	if _, err := s.Since(context.Background(), "nope", 0, 10); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("unknown room: got %v, want ErrNotFound", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.Since(ctx, "r1", 0, 10); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled ctx: got %v, want context.Canceled", err)
	}
}
