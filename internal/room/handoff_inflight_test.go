package room_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"piperoom/internal/agent"
	"piperoom/internal/events"
	"piperoom/internal/room"
	"piperoom/internal/store/memstore"
)

// gatedWriter is a WriteHandoff that reports each call on entered and then
// waits for release (or answers err), so a test controls exactly when the
// summary finishes without sleeping.
type gatedWriter struct {
	entered chan struct{}
	release chan struct{}
	mu      sync.Mutex
	calls   int
	err     error
}

func newGatedWriter() *gatedWriter {
	return &gatedWriter{entered: make(chan struct{}, 64), release: make(chan struct{})}
}

func (g *gatedWriter) write(ctx context.Context, _ []events.Event, _ agent.Options) (events.HandoffSummary, error) {
	g.mu.Lock()
	g.calls++
	err := g.err
	g.mu.Unlock()
	g.entered <- struct{}{}
	select {
	case <-g.release:
	case <-ctx.Done():
		return events.HandoffSummary{}, ctx.Err()
	}
	if err != nil {
		return events.HandoffSummary{}, err
	}
	return events.HandoffSummary{WhatBroke: "x"}, nil
}

func (g *gatedWriter) Calls() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.calls
}

func newHandoffService(t *testing.T, w *gatedWriter) (*room.Service, *memstore.Store, *clock) {
	t.Helper()
	c := &clock{t: time.Date(2026, 10, 11, 10, 0, 0, 0, time.UTC)}
	st := memstore.NewWithClock(c.Now)
	svc := room.New(room.Deps{
		Store: st, Users: room.DefaultUsers(), Owners: testOwners(),
		Run: fakeRun, WriteHandoff: w.write, Now: c.Now,
	})
	t.Cleanup(svc.Close)
	return svc, st, c
}

func countType(t *testing.T, st *memstore.Store, roomID string, typ events.Type) int {
	t.Helper()
	n := 0
	for _, e := range allEvents(t, st, roomID) {
		if e.Type == typ {
			n++
		}
	}
	return n
}

// Concurrent hand-off requests: exactly one is written, every other one
// answers 409 handoff_in_progress, whoever sends it, while the first is in
// flight. Run with -race.
func TestHandoffConcurrentRequestsExactlyOneWins(t *testing.T) {
	w := newGatedWriter()
	svc, st, _ := newHandoffService(t, w)
	roomID := createRoom(t, svc, "arjun").ID
	arjun := mustUser(t, svc, "arjun")
	priya := mustUser(t, svc, "priya")

	const n = 16
	start := make(chan struct{})
	type result struct {
		ev  events.Event
		err error
	}
	results := make(chan result, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			u, to := arjun, "priya"
			if i%4 == 3 {
				u, to = priya, "kabir" // a non-driver
			}
			ev, err := svc.CreateHandoff(context.Background(), u, roomID, to, false)
			results <- result{ev, err}
		}(i)
	}
	close(start)

	// One request reaches the summary; it is held there.
	<-w.entered
	// Every other request finishes on its own: none of them can get in.
	busy := 0
	for i := 0; i < n-1; i++ {
		r := <-results
		var re *room.Error
		if !errors.As(r.err, &re) {
			t.Fatalf("a request got through while one was in flight: %+v, %v", r.ev, r.err)
		}
		switch re.Code {
		case "handoff_in_progress":
			busy++
		case "not_driver":
			// A non-driver that ran before the winner logged its request.
		default:
			t.Fatalf("unexpected error %q (%s)", re.Code, re.Msg)
		}
		if re.Code == "handoff_in_progress" && re.Status != 409 {
			t.Fatalf("status %d", re.Status)
		}
	}
	if busy == 0 {
		t.Fatal("no request saw handoff_in_progress")
	}
	close(w.release)
	r := <-results
	wg.Wait()
	if r.err != nil || r.ev.Type != events.HandoffCreated {
		t.Fatalf("the winner: %+v, %v", r.ev, r.err)
	}
	if got := w.Calls(); got != 1 {
		t.Fatalf("summary written %d times, want 1", got)
	}
	if got := countType(t, st, roomID, events.HandoffRequested); got != 1 {
		t.Fatalf("%d handoff.requested, want 1", got)
	}
	if got := countType(t, st, roomID, events.HandoffCreated); got != 1 {
		t.Fatalf("%d handoff.created, want 1", got)
	}

	// The created event ends the request: the next one goes through and
	// carries the request id it completes.
	req, _ := events.Decode[events.HandoffRequestedPayload](findLast(t, st, roomID, events.HandoffRequested))
	created, _ := events.Decode[events.HandoffCreatedPayload](r.ev)
	if created.RequestID == "" || created.RequestID != req.RequestID {
		t.Fatalf("created.request_id %q, requested %q", created.RequestID, req.RequestID)
	}
	if _, err := svc.CreateHandoff(context.Background(), arjun, roomID, "meera", false); err != nil {
		t.Fatalf("second hand-off after the first ended: %v", err)
	}
}

func findLast(t *testing.T, st *memstore.Store, roomID string, typ events.Type) events.Event {
	t.Helper()
	var last events.Event
	for _, e := range allEvents(t, st, roomID) {
		if e.Type == typ {
			last = e
		}
	}
	if last.Type == "" {
		t.Fatalf("no %s event", typ)
	}
	return last
}

// A failed summary ends the request with handoff.failed (a fixed reason, no
// error text) and the room takes a new request.
func TestHandoffFailedSummaryEndsRequest(t *testing.T) {
	w := newGatedWriter()
	w.err = errors.New("model said: secret row value 4111-1111")
	close(w.release)
	svc, st, _ := newHandoffService(t, w)
	roomID := createRoom(t, svc, "arjun").ID
	arjun := mustUser(t, svc, "arjun")

	if _, err := svc.CreateHandoff(context.Background(), arjun, roomID, "priya", false); err == nil {
		t.Fatal("a failed summary created a hand-off")
	}
	f := findLast(t, st, roomID, events.HandoffFailed)
	p, err := events.Decode[events.HandoffFailedPayload](f)
	if err != nil || p.Reason != events.HandoffFailedSummary || f.Actor.Kind != events.ActorSystem {
		t.Fatalf("handoff.failed = %+v (%v), actor %+v", p, err, f.Actor)
	}
	if strings.Contains(string(f.Payload), "4111") {
		t.Fatalf("failed payload leaks the error: %s", f.Payload)
	}
	w.mu.Lock()
	w.err = nil
	w.mu.Unlock()
	if _, err := svc.CreateHandoff(context.Background(), arjun, roomID, "priya", false); err != nil {
		t.Fatalf("retry after a failed request: %v", err)
	}
}

// The driver changing while the summary is written ends the request with
// handoff.failed not_driver and the route's 403.
func TestHandoffDriverChangedMidRequest(t *testing.T) {
	w := newGatedWriter()
	svc, st, c := newHandoffService(t, w)
	roomID := createRoom(t, svc, "arjun").ID
	errc := make(chan error, 1)
	go func() {
		_, err := svc.CreateHandoff(context.Background(), mustUser(t, svc, "arjun"), roomID, "priya", false)
		errc <- err
	}()
	<-w.entered
	// The lease runs out and kabir takes control while the summary is written.
	c.Advance(31 * time.Minute)
	if _, err := svc.GrantControl(context.Background(), mustUser(t, svc, "kabir"), roomID, "kabir"); err != nil {
		t.Fatalf("take control: %v", err)
	}
	close(w.release)
	wantCode(t, <-errc, "not_driver")
	p, _ := events.Decode[events.HandoffFailedPayload](findLast(t, st, roomID, events.HandoffFailed))
	if p.Reason != events.HandoffFailedNotDriver {
		t.Fatalf("reason %q", p.Reason)
	}
}

// A request whose server stopped mid-way (no created, no failed) stops
// blocking the room at its expires_at.
func TestHandoffRequestExpires(t *testing.T) {
	w := newGatedWriter()
	svc, st, c := newHandoffService(t, w)
	roomID := createRoom(t, svc, "arjun").ID
	arjun := mustUser(t, svc, "arjun")
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = svc.CreateHandoff(ctx, arjun, roomID, "priya", false)
	}()
	<-w.entered
	_, err := svc.CreateHandoff(context.Background(), arjun, roomID, "priya", false)
	wantCode(t, err, "handoff_in_progress")

	// Simulate the lost server: the room as a fresh service would rebuild it
	// from the log, where the request never ended.
	cancel()
	<-done
	evs := allEvents(t, st, roomID)
	snap := room.RoomSnapshot{}
	for _, e := range evs {
		if e.Type == events.HandoffFailed {
			continue // drop the end the live server wrote
		}
		snap = room.Apply(snap, e)
	}
	if !room.HandoffInFlightAt(snap, c.Now()) {
		t.Fatal("an unfinished request is not in flight")
	}
	if room.HandoffInFlightAt(snap, c.Now().Add(3*time.Minute)) {
		t.Fatal("a request is still in flight at its expires_at")
	}
}
