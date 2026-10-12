package watch

import (
	"context"
	"runtime"
	"strings"
	"testing"
	"time"

	"piperoom/internal/owners"
)

// waitFor polls cond for up to 2 s.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func TestHangingConnectorDoesNotDelayTheOthers(t *testing.T) {
	h := newHarness(t, opts{pipes: []pipeSpec{{id: "slow"}, {id: "fast"}}, checkTimeout: 100 * time.Millisecond})
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	h.conn.mu.Lock()
	h.conn.hang["pg-slow"] = release // ignores its context, like a stuck client
	h.conn.mu.Unlock()
	h.breakConnector("fast", "boom")

	start := time.Now()
	h.w.tick(context.Background())
	if took := time.Since(start); took > time.Second {
		t.Fatalf("tick took %v with one hanging connector", took)
	}
	h.step(3 * time.Minute)
	if h.phase("fast", owners.SignalConnectorDown) != phaseFiring {
		t.Fatalf("the healthy path was held up by the hanging one\n%s", h.logs)
	}
	if h.phase("slow", owners.SignalConnectorDown) != phaseOK {
		t.Fatal("a timed-out check changed state")
	}
	if !strings.Contains(h.logs.String(), "pipeline=slow") || !strings.Contains(h.logs.String(), "deadline exceeded") {
		t.Fatalf("the timeout was not reported\n%s", h.logs)
	}
	// The slot rows of the other pipelines were still read.
	if h.src.callCount() < 2 {
		t.Fatal("source not read")
	}
}

func TestNextDelayAndJitter(t *testing.T) {
	h := newHarness(t, opts{})
	if d := h.w.nextDelay(t0, t0.Add(time.Second)); d != 59*time.Second {
		t.Fatalf("delay = %v, want 59s", d)
	}
	for range 200 {
		j := randomJitter(6 * time.Second)
		if j < -6*time.Second || j > 6*time.Second {
			t.Fatalf("jitter %v out of range", j)
		}
	}
	if randomJitter(0) != 0 {
		t.Fatal("zero max must give zero jitter")
	}
}

func TestOverrunTickIsSkippedWithAWarning(t *testing.T) {
	h := newHarness(t, opts{})
	// The tick takes 70 s of (fake) time, longer than the 60 s interval.
	h.src.hook = func(ctx context.Context) { h.clk.Advance(70 * time.Second) }

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = h.w.Run(ctx); close(done) }()
	t.Cleanup(func() { cancel(); <-done })

	waitFor(t, "the first tick and the next timer", func() bool {
		_, armed := h.clk.nextDeadline()
		return h.src.callCount() == 1 && armed
	})
	next, _ := h.clk.nextDeadline()
	// Started at t0, jitter 0: the slot at t0+60s is skipped, the next is t0+120s.
	if want := t0.Add(120 * time.Second); !next.Equal(want) {
		t.Fatalf("next tick at %v, want %v (one slot skipped)", next.Sub(t0), want.Sub(t0))
	}
	if h.logs.count("tick overran") != 1 {
		t.Fatalf("want one overrun warning\n%s", h.logs)
	}
}

func TestRunReturnsPromptlyOnCancelAndLeaksNoGoroutines(t *testing.T) {
	// A tick that is blocked in the source must not keep Run alive.
	h := newHarness(t, opts{checkTimeout: 10 * time.Second})
	h.src.hook = func(ctx context.Context) { <-ctx.Done() } // honours its context
	h.breakConnector("orders", "x")

	before := runtime.NumGoroutine()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- h.w.Run(ctx) }()
	waitFor(t, "the tick to reach the source", func() bool { return h.src.callCount() == 1 })

	start := time.Now()
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run did not return within 1 s of cancel")
	}
	if took := time.Since(start); took > time.Second {
		t.Fatalf("Run took %v to stop", took)
	}
	waitFor(t, "goroutines to return to the start count", func() bool { return runtime.NumGoroutine() <= before })
}

func TestRunWithTimerWaitingStopsAndLeaksNothing(t *testing.T) {
	h := newHarness(t, opts{})
	before := runtime.NumGoroutine()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- h.w.Run(ctx) }()
	waitFor(t, "first tick", func() bool {
		_, armed := h.clk.nextDeadline()
		return h.src.callCount() == 1 && armed
	})
	// Drive a second tick through the timer, then cancel while it waits.
	h.clk.Advance(time.Minute)
	waitFor(t, "second tick", func() bool { return h.src.callCount() == 2 })
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run did not return within 1 s of cancel")
	}
	waitFor(t, "goroutines to return to the start count", func() bool { return runtime.NumGoroutine() <= before })
}

func TestRunWithRealClockTicksAndStops(t *testing.T) {
	h := newHarness(t, opts{})
	h.w.clock = RealClock()
	h.w.cfg.Interval = 20 * time.Millisecond
	h.w.cfg.CheckTimeout = 50 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- h.w.Run(ctx) }()
	waitFor(t, "three real ticks", func() bool { return h.src.callCount() >= 3 })
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run did not stop")
	}
}
