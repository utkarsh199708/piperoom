package secrets

import (
	"context"
	"errors"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

func TestRenewerRenewsAtTwoThirds(t *testing.T) {
	var calls atomic.Int64
	start := time.Now()
	first := make(chan time.Duration, 1)
	r := NewRenewer(func(context.Context) (time.Time, error) {
		if calls.Add(1) == 1 {
			first <- time.Since(start)
		}
		return time.Now().Add(300 * time.Millisecond), nil
	}, nil)
	r.Start(context.Background(), start.Add(300*time.Millisecond))
	defer r.Close()
	got := <-first
	// 2/3 of 300 ms = 200 ms; allow scheduling slack.
	if got < 150*time.Millisecond || got > 290*time.Millisecond {
		t.Fatalf("first renewal after %v, want about 200ms", got)
	}
}

func TestRenewerStopsOnContextCancel(t *testing.T) {
	before := runtime.NumGoroutine()
	var calls atomic.Int64
	ctx, cancel := context.WithCancel(context.Background())
	r := NewRenewer(func(context.Context) (time.Time, error) {
		calls.Add(1)
		return time.Now().Add(30 * time.Millisecond), nil
	}, nil)
	r.Start(ctx, time.Now().Add(30*time.Millisecond))
	time.Sleep(80 * time.Millisecond)
	cancel()
	<-r.done
	n := calls.Load()
	if n == 0 {
		t.Fatal("never renewed")
	}
	time.Sleep(60 * time.Millisecond)
	if calls.Load() != n {
		t.Fatal("renewed after cancel")
	}
	r.Close() // after the loop ended: must not block
	waitGoroutines(t, before)
}

func TestRenewerCloseBeforeStartAndTwice(t *testing.T) {
	r := NewRenewer(func(context.Context) (time.Time, error) { return time.Time{}, nil }, nil)
	r.Close()
	r.Start(context.Background(), time.Now()) // no-op after Close
	r.Close()
	if r.done != nil {
		t.Fatal("Start after Close started a loop")
	}
}

func TestRenewerLost(t *testing.T) {
	lost := make(chan error, 1)
	boom := errors.New("lease revoked")
	var r *Renewer
	r = NewRenewer(func(context.Context) (time.Time, error) { return time.Time{}, boom }, func(err error) {
		r.Close() // calling Close from lost must not deadlock
		lost <- err
	})
	r.Start(context.Background(), time.Now().Add(15*time.Millisecond))
	select {
	case err := <-lost:
		if !errors.Is(err, boom) {
			t.Fatalf("err = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("lost not called")
	}
}

func TestRenewerLeaseEnded(t *testing.T) {
	lost := make(chan error, 1)
	// A renewal that returns no future expiry (max TTL reached) is a loss.
	r := NewRenewer(func(context.Context) (time.Time, error) { return time.Now().Add(-time.Second), nil },
		func(err error) { lost <- err })
	r.Start(context.Background(), time.Now().Add(15*time.Millisecond))
	defer r.Close()
	select {
	case err := <-lost:
		if !errors.Is(err, errLeaseEnded) {
			t.Fatalf("err = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("lost not called")
	}
}

func waitGoroutines(t *testing.T, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for runtime.NumGoroutine() > want {
		if time.Now().After(deadline) {
			t.Fatalf("goroutines = %d, want <= %d (leak)", runtime.NumGoroutine(), want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
