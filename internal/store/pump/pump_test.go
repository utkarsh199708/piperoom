package pump

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"piperoom/internal/events"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

// fakeLog is a trivially correct reader plus a way to ring the doorbell.
type fakeLog struct {
	mu   sync.Mutex
	evs  []events.Event
	wake chan struct{}
	fail atomic.Int32 // number of upcoming reads that must fail
}

func newFakeLog() *fakeLog { return &fakeLog{wake: make(chan struct{}, 1)} }

func (f *fakeLog) add(n int) {
	f.mu.Lock()
	for range n {
		f.evs = append(f.evs, events.Event{RoomID: "r", Seq: int64(len(f.evs)) + 1})
	}
	f.mu.Unlock()
	select {
	case f.wake <- struct{}{}:
	default:
	}
}

func (f *fakeLog) read(_ context.Context, after int64, limit int) ([]events.Event, error) {
	if f.fail.Load() > 0 {
		f.fail.Add(-1)
		return nil, errors.New("boom")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	out := []events.Event{}
	for i := after; i < int64(len(f.evs)) && len(out) < limit; i++ {
		out = append(out, f.evs[i])
	}
	return out, nil
}

func recvN(t *testing.T, ch <-chan events.Event, n int) []int64 {
	t.Helper()
	var got []int64
	for len(got) < n {
		select {
		case ev, ok := <-ch:
			if !ok {
				t.Fatalf("closed after %v", got)
			}
			got = append(got, ev.Seq)
		case <-time.After(5 * time.Second):
			t.Fatalf("timeout after %v", got)
		}
	}
	return got
}

func wantSeqs(t *testing.T, got []int64, from, to int64) {
	t.Helper()
	if int64(len(got)) != to-from+1 {
		t.Fatalf("got %v, want %d..%d", got, from, to)
	}
	for i, s := range got {
		if s != from+int64(i) {
			t.Fatalf("got %v, want %d..%d", got, from, to)
		}
	}
}

func TestPagesAreDrainedWithoutWaitingForAWake(t *testing.T) {
	f := newFakeLog()
	f.add(25)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Page 10 with 25 waiting events: only one wake-up is pending, yet all 25
	// must arrive, because a full page means "there is more".
	ch := Start(ctx, Config{Read: f.read, Wake: f.wake, Page: 10, Log: quiet}, 0, nil)
	wantSeqs(t, recvN(t, ch, 25), 1, 25)
}

func TestFirstBatchIsNotReadTwice(t *testing.T) {
	f := newFakeLog()
	f.add(3)
	first, _ := f.read(context.Background(), 0, 10)
	f.fail.Store(1000) // any further read would fail and stall the stream
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := Start(ctx, Config{Read: f.read, Wake: f.wake, Log: quiet}, 0, first)
	wantSeqs(t, recvN(t, ch, 3), 1, 3)
}

func TestReadFailuresAreRetriedNotFatal(t *testing.T) {
	f := newFakeLog()
	f.fail.Store(3)
	f.add(4)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// No wake-ups arrive after the first, so recovery relies on the poll.
	ch := Start(ctx, Config{Read: f.read, Wake: f.wake, Poll: 10 * time.Millisecond, Log: quiet}, 0, nil)
	wantSeqs(t, recvN(t, ch, 4), 1, 4)
}

func TestPollCoversALostWakeup(t *testing.T) {
	f := newFakeLog()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := Start(ctx, Config{Read: f.read, Wake: f.wake, Poll: 20 * time.Millisecond, Log: quiet}, 0, []events.Event{})
	f.mu.Lock() // append without ringing the doorbell
	f.evs = append(f.evs, events.Event{RoomID: "r", Seq: 1})
	f.mu.Unlock()
	wantSeqs(t, recvN(t, ch, 1), 1, 1)
}

func TestDuplicatesFromReaderAreDropped(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls := 0
	read := func(_ context.Context, after int64, _ int) ([]events.Event, error) {
		calls++
		if calls == 1 { // overlapping window: 2,3 were already delivered
			return []events.Event{{Seq: 2}, {Seq: 3}, {Seq: 4}}, nil
		}
		return []events.Event{}, nil
	}
	wake := make(chan struct{}, 1)
	ch := Start(ctx, Config{Read: read, Wake: wake, Log: quiet}, 0, []events.Event{{Seq: 1}, {Seq: 2}, {Seq: 3}})
	wantSeqs(t, recvN(t, ch, 3), 1, 3)
	wake <- struct{}{} // now the reader answers with the overlapping window
	wantSeqs(t, recvN(t, ch, 1), 4, 4)
}

func TestSlowConsumerIsDroppedWithACleanPrefix(t *testing.T) {
	f := newFakeLog()
	f.add(50)
	var exited atomic.Bool
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := Start(ctx, Config{
		Read: f.read, Wake: f.wake, Buffer: 8, SlowAfter: 50 * time.Millisecond, Log: quiet,
		OnExit: func() { exited.Store(true) },
	}, 0, nil)

	deadline := time.Now().Add(5 * time.Second)
	for !exited.Load() {
		if time.Now().After(deadline) {
			t.Fatal("slow consumer was not dropped")
		}
		time.Sleep(5 * time.Millisecond)
	}
	var got []int64
	for ev := range ch {
		got = append(got, ev.Seq)
	}
	wantSeqs(t, got, 1, 8)
}

func TestContextEndClosesChannelAndRunsOnExitFirst(t *testing.T) {
	f := newFakeLog()
	var exited atomic.Bool
	ctx, cancel := context.WithCancel(context.Background())
	ch := Start(ctx, Config{Read: f.read, Wake: f.wake, Log: quiet, OnExit: func() { exited.Store(true) }}, 0, nil)
	cancel()
	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("unexpected event")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("channel not closed after ctx ended")
	}
	if !exited.Load() {
		t.Fatal("OnExit must run before the channel is closed")
	}
}

func TestBlockedSendStopsOnContextEnd(t *testing.T) {
	f := newFakeLog()
	f.add(10)
	ctx, cancel := context.WithCancel(context.Background())
	ch := Start(ctx, Config{Read: f.read, Wake: f.wake, Buffer: 2, SlowAfter: time.Hour, Log: quiet}, 0, nil)
	time.Sleep(50 * time.Millisecond) // pump is now blocked on the third send
	cancel()
	deadline := time.After(5 * time.Second)
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				return
			}
		case <-deadline:
			t.Fatal("pump goroutine stuck in a blocked send after ctx ended")
		}
	}
}
