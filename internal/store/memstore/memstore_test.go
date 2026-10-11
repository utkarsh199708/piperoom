package memstore

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"piperoom/internal/events"
	"piperoom/internal/store"
	"piperoom/internal/store/storetest"
)

var actor = events.Actor{Kind: events.ActorHuman, ID: "u-ada", Name: "Ada"}

// subscriberCount reports live subscribers so tests can check cleanup.
func (s *Store) subscriberCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, r := range s.rooms {
		n += len(r.subs)
	}
	return n
}

// The whole behaviour suite also runs against Postgres (pg_integration_test.go
// in package store); passing it here is what makes memstore a faithful fake.
func TestStoreSuite(t *testing.T) {
	storetest.Run(t, func(t *testing.T) store.Store { return New() })
}

func TestClockIsUsedAndUTC(t *testing.T) {
	zone := time.FixedZone("UTC+5", 5*3600)
	base := time.Date(2026, 3, 1, 9, 0, 0, 0, zone)
	calls := 0
	s := NewWithClock(func() time.Time {
		calls++
		return base.Add(time.Duration(calls) * time.Minute)
	})
	ctx := context.Background()
	if err := s.CreateRoom(ctx, "r1"); err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 3; i++ {
		ev, err := s.Append(ctx, "r1", events.ControlRequested, actor, nil)
		if err != nil {
			t.Fatal(err)
		}
		want := base.Add(time.Duration(i) * time.Minute)
		if !ev.At.Equal(want) {
			t.Fatalf("event %d stamped %v, want %v", i, ev.At, want)
		}
		if ev.At.Location() != time.UTC {
			t.Fatalf("event %d At is not in UTC: %v", i, ev.At.Location())
		}
	}
}

func TestNilClockFallsBackToNow(t *testing.T) {
	s := NewWithClock(nil)
	ctx := context.Background()
	_ = s.CreateRoom(ctx, "r1")
	before := time.Now().Add(-time.Second)
	ev, err := s.Append(ctx, "r1", events.ControlRequested, actor, nil)
	if err != nil {
		t.Fatal(err)
	}
	if ev.At.Before(before) {
		t.Fatalf("At %v is not recent", ev.At)
	}
}

func TestAppendCancelledContext(t *testing.T) {
	s := New()
	_ = s.CreateRoom(context.Background(), "r1")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.Append(ctx, "r1", events.ControlRequested, actor, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}
	// A refused append must not have consumed a seq.
	ev, err := s.Append(context.Background(), "r1", events.ControlRequested, actor, nil)
	if err != nil || ev.Seq != 1 {
		t.Fatalf("seq after refused append = %d, %v; want 1", ev.Seq, err)
	}
}

func TestPayloadIsCopied(t *testing.T) {
	s := New()
	ctx := context.Background()
	_ = s.CreateRoom(ctx, "r1")
	buf := []byte(`{"text":"original"}`)
	ev, err := s.Append(ctx, "r1", events.AgentMessage, events.AgentActor, json.RawMessage(buf))
	if err != nil {
		t.Fatal(err)
	}
	// Scribbling over the caller's buffer, and over the returned event, must not
	// reach the stored log.
	copy(buf, `{"text":"CHANGED!"}`)
	copy(ev.Payload, `{"text":"MUTATED!"}`)
	got, err := s.Since(ctx, "r1", 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if string(got[0].Payload) != `{"text":"original"}` {
		t.Fatalf("stored payload was changed through an alias: %s", got[0].Payload)
	}
	copy(got[0].Payload, `{"text":"MUTATED!"}`)
	again, _ := s.Since(ctx, "r1", 0, 0)
	if string(again[0].Payload) != `{"text":"original"}` {
		t.Fatalf("stored payload was changed through Since's result: %s", again[0].Payload)
	}
}

func TestSubscriberIsUnregisteredAfterClose(t *testing.T) {
	s := New()
	_ = s.CreateRoom(context.Background(), "r1")
	ctx, cancel := context.WithCancel(context.Background())
	ch, err := s.Subscribe(ctx, "r1", 0)
	if err != nil {
		t.Fatal(err)
	}
	if n := s.subscriberCount(); n != 1 {
		t.Fatalf("subscribers = %d, want 1", n)
	}
	cancel()
	for range ch { // closed once the goroutine is done
	}
	// The pump unregisters before it closes the channel, so this is exact.
	if n := s.subscriberCount(); n != 0 {
		t.Fatalf("subscribers after close = %d, want 0 (goroutine leak)", n)
	}
}

func TestSlowSubscriberIsDropped(t *testing.T) {
	s := New()
	s.slowAfter = 100 * time.Millisecond
	ctx := context.Background()
	_ = s.CreateRoom(ctx, "r1")
	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	ch, err := s.Subscribe(subCtx, "r1", 0)
	if err != nil {
		t.Fatal(err)
	}

	// Never read: 256 fill the buffer, the 257th blocks the pump, and after
	// slowAfter the subscription is cut.
	const total = 300
	for range total {
		if _, err := s.Append(ctx, "r1", events.ControlRequested, actor, nil); err != nil {
			t.Fatal(err)
		}
	}
	deadline := time.After(5 * time.Second)
	for s.subscriberCount() != 0 {
		select {
		case <-deadline:
			t.Fatal("slow subscriber was never dropped")
		case <-time.After(10 * time.Millisecond):
		}
	}

	// What it did get is a clean prefix, so resubscribing from the last seq
	// resumes without a gap or repeat.
	var last int64
	for ev := range ch {
		if ev.Seq != last+1 {
			t.Fatalf("gap in delivered prefix: %d after %d", ev.Seq, last)
		}
		last = ev.Seq
	}
	if last != 256 {
		t.Fatalf("delivered %d events before the drop, want the full buffer of 256", last)
	}
	ch2, err := s.Subscribe(subCtx, "r1", last)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for ev := range ch2 {
		n++
		if ev.Seq != last+int64(n) {
			t.Fatalf("resumed stream has seq %d, want %d", ev.Seq, last+int64(n))
		}
		if n == total-int(last) {
			break
		}
	}
}

func TestSlowSubscriberDoesNotBlockAppend(t *testing.T) {
	s := New()
	s.slowAfter = time.Hour // so the stalled subscriber stays registered throughout
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = s.CreateRoom(ctx, "r1")
	if _, err := s.Subscribe(ctx, "r1", 0); err != nil { // never read
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range 1000 {
			if _, err := s.Append(ctx, "r1", events.ControlRequested, actor, nil); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Append blocked behind a stalled subscriber")
	}
}

func TestManyRoomsConcurrently(t *testing.T) {
	s := New()
	ctx := context.Background()
	var wg sync.WaitGroup
	for _, id := range []string{"a", "b", "c"} {
		if err := s.CreateRoom(ctx, id); err != nil {
			t.Fatal(err)
		}
		for range 5 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for range 20 {
					if _, err := s.Append(ctx, id, events.ControlRequested, actor, nil); err != nil {
						t.Error(err)
						return
					}
				}
			}()
		}
	}
	wg.Wait()
	for _, id := range []string{"a", "b", "c"} {
		evs, err := s.Since(ctx, id, 0, 1000)
		if err != nil || len(evs) != 100 || evs[99].Seq != 100 {
			t.Fatalf("room %s: %d events, last %v, err %v", id, len(evs), evs[len(evs)-1].Seq, err)
		}
	}
	ids, _ := s.RoomIDs(ctx)
	if len(ids) != 3 || ids[0] != "a" || ids[2] != "c" {
		t.Fatalf("RoomIDs = %v, want creation order a,b,c", ids)
	}
}

func TestEmptyStoreRoomIDsIsNotNil(t *testing.T) {
	ids, err := New().RoomIDs(context.Background())
	if err != nil || ids == nil || len(ids) != 0 {
		t.Fatalf("RoomIDs on empty store = %#v, %v; want empty non-nil", ids, err)
	}
}
