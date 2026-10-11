package store

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func testHub(t *testing.T, dsn string) *hub {
	t.Helper()
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("ParseConfig: %v", err)
	}
	return newHub(cfg, slog.New(slog.DiscardHandler))
}

func woken(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func TestHubDispatchWakesOnlyThatRoom(t *testing.T) {
	h := testHub(t, "postgres://u:p@127.0.0.1:1/db")
	wakeA, removeA := h.add("r_a:b") // a room id may itself contain a colon
	wakeB, removeB := h.add("r_other")
	defer removeA()
	defer removeB()

	tests := []struct {
		name         string
		payload      string
		wantA, wantB bool
	}{
		{"notify for room A", "r_a:b:17", true, false},
		{"notify for room B", "r_other:3", false, true},
		{"notify for a room nobody follows", "r_zzz:9", false, false},
		{"payload without a seq wakes everyone", "garbage", true, true},
		{"empty payload wakes everyone", "", true, true},
	}
	for _, tc := range tests {
		h.dispatch(&pgconn.Notification{Channel: notifyChannel, Payload: tc.payload})
		if got := woken(wakeA); got != tc.wantA {
			t.Errorf("%s: room A woken = %v, want %v", tc.name, got, tc.wantA)
		}
		if got := woken(wakeB); got != tc.wantB {
			t.Errorf("%s: room B woken = %v, want %v", tc.name, got, tc.wantB)
		}
	}
	h.dispatch(nil) // must not panic
}

func TestHubWakeUpsCoalesce(t *testing.T) {
	h := testHub(t, "postgres://u:p@127.0.0.1:1/db")
	wake, remove := h.add("r1")
	defer remove()
	for range 50 {
		h.wakeRoom("r1") // the sender must never block on a slow subscriber
	}
	if !woken(wake) {
		t.Fatal("no wake-up pending")
	}
	if woken(wake) {
		t.Fatal("wake-ups were queued instead of merged")
	}
}

func TestHubWakeAllAndRemove(t *testing.T) {
	h := testHub(t, "postgres://u:p@127.0.0.1:1/db")
	w1, rm1 := h.add("r1")
	w2, rm2 := h.add("r1")
	w3, rm3 := h.add("r2")
	h.wakeAll()
	for i, w := range []<-chan struct{}{w1, w2, w3} {
		if !woken(w) {
			t.Errorf("subscriber %d was not woken by wakeAll", i+1)
		}
	}
	rm1()
	rm2()
	rm3()
	h.mu.Lock()
	left := len(h.subs)
	h.mu.Unlock()
	if left != 0 {
		t.Fatalf("hub still tracks %d rooms after every subscriber left", left)
	}
	h.wakeAll() // nothing registered: fine
}

// With the server unreachable the hub must keep retrying quietly and stop
// promptly when its context ends, without leaving a goroutine behind.
func TestHubRunRetriesAndStopsOnContextEnd(t *testing.T) {
	h := testHub(t, "postgres://u:p@127.0.0.1:1/db?connect_timeout=1")
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); h.run(ctx) }()

	time.Sleep(500 * time.Millisecond) // a few failed connects and backoff sleeps
	if pid := h.pid.Load(); pid != 0 {
		t.Fatalf("pid = %d while the server is unreachable", pid)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("hub.run did not stop after its context ended")
	}
}

func TestSleepStopsOnContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	if !sleep(ctx, time.Millisecond) {
		t.Fatal("a short sleep on a live context should complete")
	}
	cancel()
	start := time.Now()
	if sleep(ctx, time.Hour) {
		t.Fatal("sleep reported a full wait on a cancelled context")
	}
	if time.Since(start) > time.Second {
		t.Fatal("sleep did not return promptly")
	}
}
