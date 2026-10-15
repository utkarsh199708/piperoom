package room

import (
	"context"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"piperoom/internal/events"
	"piperoom/internal/notify"
)

// gateNotifier holds every delivery until release is closed (or its context
// ends) and counts the ones that finished.
type gateNotifier struct {
	started atomic.Int32
	done    atomic.Int32
	release chan struct{}
}

func (g *gateNotifier) Deliver(ctx context.Context, _ notify.OwnerPaged) error {
	g.started.Add(1)
	select {
	case <-g.release:
	case <-ctx.Done():
		return ctx.Err()
	}
	g.done.Add(1)
	return nil
}

// pageN writes n pages of distinct (room, layer) pairs.
func pageN(t *testing.T, svc *Service, n int) {
	t.Helper()
	ctx := context.Background()
	u := User{ID: "priya", Name: "Priya"}
	for done := 0; done < n; {
		snap, err := svc.CreateRoom(ctx, u, "t", "sev2", "orders")
		if err != nil {
			t.Fatal(err)
		}
		for _, layer := range events.Layers {
			if done == n {
				break
			}
			if _, err := svc.PageOwners(ctx, u, snap.ID, layer, "look", ""); err != nil {
				t.Fatal(err)
			}
			done++
		}
	}
}

func waitCount(t *testing.T, c *atomic.Int32, want int32) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for c.Load() != want {
		if time.Now().After(deadline) {
			t.Fatalf("count = %d, want %d", c.Load(), want)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// QA R3: a page past the deliveries in flight waits in a queue and is
// delivered when a slot frees; it is not dropped.
func TestPageDeliveryQueuesPastInFlightLimit(t *testing.T) {
	g := &gateNotifier{release: make(chan struct{})}
	svc := pageOwnersService(t, nil, g)
	pageN(t, svc, maxPageDeliveries+3)
	waitCount(t, &g.started, maxPageDeliveries)
	time.Sleep(30 * time.Millisecond)
	if n := g.started.Load(); n != maxPageDeliveries {
		t.Fatalf("started = %d, want %d while all slots are busy", n, maxPageDeliveries)
	}
	close(g.release)
	waitCount(t, &g.done, maxPageDeliveries+3)
}

// QA R3: only when the queue itself is full is a page dropped, with a WARN.
func TestPageDeliveryQueueOverflowWarns(t *testing.T) {
	g := &gateNotifier{release: make(chan struct{})}
	svc := pageOwnersService(t, nil, g)
	buf := &syncBuf{}
	svc.log = slog.New(slog.NewTextHandler(buf, nil))
	svc.paging.queue = make(chan pageJob, 2)
	// Fill every worker first, so the queue is provably empty and all workers
	// are blocked before the next pages arrive: 2 queue, the third overflows.
	for i := 1; i <= maxPageDeliveries; i++ {
		pageN(t, svc, 1)
		waitCount(t, &g.started, int32(i))
	}
	pageN(t, svc, 2+1)
	if !strings.Contains(buf.String(), "queue is full") {
		t.Fatalf("no overflow warning in log:\n%s", buf.String())
	}
	close(g.release)
	waitCount(t, &g.done, maxPageDeliveries+2)
	time.Sleep(30 * time.Millisecond)
	if n := g.started.Load(); n != maxPageDeliveries+2 {
		t.Fatalf("started = %d, want %d (the overflowing page is not sent)", n, maxPageDeliveries+2)
	}
}
