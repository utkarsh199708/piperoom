package room

import (
	"context"
	"sync"
	"testing"
	"time"

	"piperoom/internal/agent"
	"piperoom/internal/config"
	"piperoom/internal/owners"
	"piperoom/internal/store/memstore"
)

type testClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *testClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *testClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

// TestAutoCloseRechecksUnderTheLock: a room the pass found due is closed only
// if it is still due when its close runs, judged by the time read under the
// lock. An extension in between (the driver
// clicked while the pass was running) keeps it open.
func TestAutoCloseRechecksUnderTheLock(t *testing.T) {
	c := &testClock{t: time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)}
	svc := New(Deps{
		Store:  memstore.NewWithClock(c.Now),
		Users:  DefaultUsers(),
		Owners: &owners.Owners{Pipelines: []owners.Pipeline{{ID: "orders", SourceTables: []string{"public.orders"}}}},
		Run: func(ctx context.Context, _ agent.Alert, _ agent.Deps, _ agent.Options) error {
			<-ctx.Done()
			return nil
		},
		Now:    c.Now,
		Limits: config.Limits{MaxOpenDays: 14},
	})
	t.Cleanup(svc.Close)
	ctx := context.Background()
	arjun, _ := svc.LookupUser("arjun")
	open := func() string {
		snap, err := svc.CreateRoom(ctx, arjun, "t", "sev2", "orders")
		if err != nil {
			t.Fatal(err)
		}
		return snap.ID
	}
	extended, plain := open(), open()
	c.Advance(13 * 24 * time.Hour)
	if res, _ := svc.EnforceOpenLimit(ctx); res.Warned != 2 {
		t.Fatalf("warnings: %+v", res)
	}
	// Warned but not yet at its close time: left open.
	if closed, err := svc.autoClose(ctx, plain); err != nil || closed {
		t.Fatalf("warned room before close_at: closed=%v err=%v, want left open", closed, err)
	}
	c.Advance(24 * time.Hour) // both due now: the pass would list both
	if _, err := svc.ExtendRoom(ctx, arjun, extended); err != nil {
		t.Fatal(err)
	}
	if closed, err := svc.autoClose(ctx, extended); err != nil || closed {
		t.Fatalf("extended room: closed=%v err=%v, want left open", closed, err)
	}
	if snap, _ := svc.GetRoom(ctx, extended); snap.Status != "open" {
		t.Fatal("extended room was closed")
	}
	if closed, err := svc.autoClose(ctx, plain); err != nil || !closed {
		t.Fatalf("due room: closed=%v err=%v, want closed", closed, err)
	}
}
