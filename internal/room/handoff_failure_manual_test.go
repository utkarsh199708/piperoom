package room_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"piperoom/internal/agent"
	"piperoom/internal/events"
	"piperoom/internal/room"
	"piperoom/internal/store/memstore"
)

// switchWriter is a WriteHandoff whose behaviour a test changes between calls.
type switchWriter struct {
	fn    atomic.Pointer[agent.HandoffWriter]
	calls atomic.Int32
}

func newSwitchWriter(w agent.HandoffWriter) *switchWriter {
	s := &switchWriter{}
	s.set(w)
	return s
}

func (s *switchWriter) set(w agent.HandoffWriter) { s.fn.Store(&w) }

func (s *switchWriter) write(ctx context.Context, evs []events.Event, o agent.Options) (events.HandoffSummary, error) {
	s.calls.Add(1)
	return (*s.fn.Load())(ctx, evs, o)
}

func okSummary(context.Context, []events.Event, agent.Options) (events.HandoffSummary, error) {
	return events.HandoffSummary{WhatBroke: "schema change", Why: "int->bigint"}, nil
}

// failingStore makes Since fail on demand, as a database that went away.
type failingStore struct {
	*memstore.Store
	failSince atomic.Bool
}

func (f *failingStore) Since(ctx context.Context, roomID string, after int64, limit int) ([]events.Event, error) {
	if f.failSince.Load() {
		return nil, errors.New("db down: connection refused")
	}
	return f.Store.Since(ctx, roomID, after, limit)
}

func newSwitchService(t *testing.T, w *switchWriter) (*room.Service, *memstore.Store, *clock) {
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

func typesOf(evs []events.Event, typ events.Type) []events.Event {
	var out []events.Event
	for _, e := range evs {
		if e.Type == typ {
			out = append(out, e)
		}
	}
	return out
}

// Every way the agent's summary can fail ends as handoff.failed
// {summary_failed} and a 503 summary_failed that tells the driver what to do;
// no cause leaks into the error or the log, and a retry goes through.
func TestHandoffSummaryFailureIs503(t *testing.T) {
	cases := []struct {
		name  string
		write agent.HandoffWriter
	}{
		{"model omitted a field", func(context.Context, []events.Event, agent.Options) (events.HandoffSummary, error) {
			return events.HandoffSummary{}, errors.New("agent: handoff: model omitted what_broke")
		}},
		{"model error quoting content", func(context.Context, []events.Event, agent.Options) (events.HandoffSummary, error) {
			return events.HandoffSummary{}, fmt.Errorf("agent: handoff request: %w", errors.New("429 rate limited: row 4111-1111"))
		}},
		{"deadline exceeded", func(context.Context, []events.Event, agent.Options) (events.HandoffSummary, error) {
			return events.HandoffSummary{}, fmt.Errorf("agent: handoff request: %w", context.DeadlineExceeded)
		}},
		{"cancelled", func(context.Context, []events.Event, agent.Options) (events.HandoffSummary, error) {
			return events.HandoffSummary{}, context.Canceled
		}},
		{"empty summary without an error", func(context.Context, []events.Event, agent.Options) (events.HandoffSummary, error) {
			return events.HandoffSummary{}, nil
		}},
		{"what_broke only blanks", func(context.Context, []events.Event, agent.Options) (events.HandoffSummary, error) {
			return events.HandoffSummary{WhatBroke: "", Why: "int->bigint", Done: []string{"x"}}, nil
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			w := newSwitchWriter(c.write)
			svc, st, _ := newSwitchService(t, w)
			roomID := createRoom(t, svc, "arjun").ID
			arjun := mustUser(t, svc, "arjun")

			_, err := svc.CreateHandoff(context.Background(), arjun, roomID, "priya", false)
			var re *room.Error
			if !errors.As(err, &re) {
				t.Fatalf("err = %v, want a room error", err)
			}
			if re.Status != 503 || re.Code != "summary_failed" {
				t.Fatalf("got %d %q, want 503 summary_failed", re.Status, re.Code)
			}
			msg := strings.ToLower(re.Msg)
			if !strings.Contains(msg, "try again") || !strings.Contains(msg, "yourself") || strings.Contains(re.Msg, "4111") {
				t.Fatalf("message = %q; want a retry / write-it-yourself hint and no cause", re.Msg)
			}

			evs := allEvents(t, st, roomID)
			if n := len(typesOf(evs, events.HandoffCreated)); n != 0 {
				t.Fatalf("%d handoff.created after a failed summary", n)
			}
			reqs, fails := typesOf(evs, events.HandoffRequested), typesOf(evs, events.HandoffFailed)
			if len(reqs) != 1 || len(fails) != 1 {
				t.Fatalf("%d requested, %d failed; want 1 and 1", len(reqs), len(fails))
			}
			fp, _ := events.Decode[events.HandoffFailedPayload](fails[0])
			rp, _ := events.Decode[events.HandoffRequestedPayload](reqs[0])
			if fp.Reason != events.HandoffFailedSummary || fp.RequestID != rp.RequestID || fails[0].Actor.Kind != events.ActorSystem {
				t.Fatalf("handoff.failed = %+v (request %q), actor %+v", fp, rp.RequestID, fails[0].Actor)
			}
			if strings.Contains(string(fails[0].Payload), "4111") {
				t.Fatalf("handoff.failed leaks the cause: %s", fails[0].Payload)
			}

			// The request ended: a retry with a working writer goes through.
			w.set(okSummary)
			if _, err := svc.CreateHandoff(context.Background(), arjun, roomID, "priya", false); err != nil {
				t.Fatalf("retry: %v", err)
			}
		})
	}
}

// A store failure is the server's fault: it stays a plain error (the route
// answers 500 internal_error) and handoff.failed says internal_error.
func TestHandoffStoreFailureStaysInternal(t *testing.T) {
	c := &clock{t: time.Date(2026, 10, 11, 10, 0, 0, 0, time.UTC)}
	mem := memstore.NewWithClock(c.Now)
	fs := &failingStore{Store: mem}
	w := newSwitchWriter(okSummary)
	svc := room.New(room.Deps{
		Store: fs, Users: room.DefaultUsers(), Owners: testOwners(),
		Run: fakeRun, WriteHandoff: w.write, Now: c.Now,
	})
	t.Cleanup(svc.Close)
	roomID := createRoom(t, svc, "arjun").ID

	fs.failSince.Store(true)
	_, err := svc.CreateHandoff(context.Background(), mustUser(t, svc, "arjun"), roomID, "priya", false)
	var re *room.Error
	if err == nil || errors.As(err, &re) {
		t.Fatalf("err = %v; a store failure must be a plain error (500), not a room error", err)
	}
	if got := w.calls.Load(); got != 0 {
		t.Fatalf("the writer ran %d times without a readable log", got)
	}
	fails := typesOf(allEvents(t, mem, roomID), events.HandoffFailed)
	if len(fails) != 1 {
		t.Fatalf("%d handoff.failed, want 1", len(fails))
	}
	if p, _ := events.Decode[events.HandoffFailedPayload](fails[0]); p.Reason != events.HandoffFailedInternal {
		t.Fatalf("reason = %q, want internal_error", p.Reason)
	}

	fs.failSince.Store(false)
	if _, err := svc.CreateHandoff(context.Background(), mustUser(t, svc, "arjun"), roomID, "priya", false); err != nil {
		t.Fatalf("retry after the store came back: %v", err)
	}
}

func manualSummary() events.HandoffSummary {
	return events.HandoffSummary{
		WhatBroke: "orders consumer stopped after the schema change",
		Why:       "amount became numeric",
		Done:      []string{"restarted the connector"},
		RuledOut:  []string{"broker health"},
		Open:      []string{"watch the lag for an hour"},
		Risks:     []string{"a second alter may break it again"},
	}
}

// A manual summary makes no model call, is stored as written, is marked
// manual and has no violations; accept works as for an agent summary.
func TestHandoffManualSummary(t *testing.T) {
	w := newSwitchWriter(func(context.Context, []events.Event, agent.Options) (events.HandoffSummary, error) {
		return events.HandoffSummary{}, errors.New("must not be called")
	})
	svc, st, _ := newSwitchService(t, w)
	roomID := createRoom(t, svc, "arjun").ID
	arjun, priya := mustUser(t, svc, "arjun"), mustUser(t, svc, "priya")

	in := manualSummary()
	in.Violations = []events.HandoffViolation{{Rule: "unknown_number", Field: "why", Detail: "spoofed"}}
	ev, err := svc.CreateHandoffManual(context.Background(), arjun, roomID, "priya", true, in)
	if err != nil {
		t.Fatalf("manual hand-off: %v", err)
	}
	if got := w.calls.Load(); got != 0 {
		t.Fatalf("model called %d times for a manual summary", got)
	}
	p, err := events.Decode[events.HandoffCreatedPayload](ev)
	if err != nil {
		t.Fatal(err)
	}
	if !p.Manual || p.Summary.Violations != nil || p.RequestID == "" || !p.StayAsWatcher {
		t.Fatalf("payload = %+v", p)
	}
	want := manualSummary()
	if p.Summary.WhatBroke != want.WhatBroke || p.Summary.Why != want.Why ||
		fmt.Sprint(p.Summary.Done, p.Summary.RuledOut, p.Summary.Open, p.Summary.Risks) !=
			fmt.Sprint(want.Done, want.RuledOut, want.Open, want.Risks) {
		t.Fatalf("summary = %+v", p.Summary)
	}
	evs := allEvents(t, st, roomID)
	if len(typesOf(evs, events.HandoffRequested)) != 1 || len(typesOf(evs, events.HandoffFailed)) != 0 {
		t.Fatal("a manual hand-off is still requested first and never failed")
	}
	// The agent's own summaries are not manual.
	w.set(okSummary)
	ev2, err := svc.CreateHandoff(context.Background(), arjun, roomID, "priya", false)
	if err != nil {
		t.Fatal(err)
	}
	if p2, _ := events.Decode[events.HandoffCreatedPayload](ev2); p2.Manual {
		t.Fatal("an agent summary is marked manual")
	}
	// The newer hand-off voided the manual one.
	_, err = svc.AcceptHandoff(context.Background(), priya, p.HandoffID)
	wantCode(t, err, "stale_handoff")
}

// A manual hand-off is accepted like any other and moves the driver role.
func TestHandoffManualSummaryAccept(t *testing.T) {
	svc, _, _ := newSwitchService(t, newSwitchWriter(okSummary))
	roomID := createRoom(t, svc, "arjun").ID
	ev, err := svc.CreateHandoffManual(context.Background(), mustUser(t, svc, "arjun"), roomID, "priya", false, manualSummary())
	if err != nil {
		t.Fatal(err)
	}
	p, _ := events.Decode[events.HandoffCreatedPayload](ev)
	if _, err := svc.AcceptHandoff(context.Background(), mustUser(t, svc, "priya"), p.HandoffID); err != nil {
		t.Fatalf("accept: %v", err)
	}
	snap, err := svc.GetRoom(context.Background(), roomID)
	if err != nil || snap.Driver == nil || snap.Driver.ID != "priya" {
		t.Fatalf("driver = %+v, %v; want priya", snap.Driver, err)
	}
}

// The G4 flow: the agent's summary fails (503), the driver sends their own.
func TestHandoffManualAfterFailedAgentSummary(t *testing.T) {
	w := newSwitchWriter(func(context.Context, []events.Event, agent.Options) (events.HandoffSummary, error) {
		return events.HandoffSummary{}, errors.New("model omitted what_broke")
	})
	svc, st, _ := newSwitchService(t, w)
	roomID := createRoom(t, svc, "arjun").ID
	arjun := mustUser(t, svc, "arjun")
	if _, err := svc.CreateHandoff(context.Background(), arjun, roomID, "priya", false); err == nil {
		t.Fatal("the failing writer produced a hand-off")
	}
	calls := w.calls.Load()
	if _, err := svc.CreateHandoffManual(context.Background(), arjun, roomID, "priya", false, manualSummary()); err != nil {
		t.Fatalf("manual after a failed attempt: %v", err)
	}
	if w.calls.Load() != calls {
		t.Fatal("the manual hand-off called the model")
	}
	evs := allEvents(t, st, roomID)
	if len(typesOf(evs, events.HandoffRequested)) != 2 || len(typesOf(evs, events.HandoffFailed)) != 1 || len(typesOf(evs, events.HandoffCreated)) != 1 {
		t.Fatalf("log = %d requested, %d failed, %d created", len(typesOf(evs, events.HandoffRequested)), len(typesOf(evs, events.HandoffFailed)), len(typesOf(evs, events.HandoffCreated)))
	}
}

// The other rules of the route apply to a manual hand-off: the in-flight rule,
// the driver rule, and (before any write) the validation.
func TestHandoffManualSummaryRules(t *testing.T) {
	t.Run("in flight", func(t *testing.T) {
		g := newGatedWriter()
		svc, st, _ := newHandoffService(t, g)
		roomID := createRoom(t, svc, "arjun").ID
		arjun := mustUser(t, svc, "arjun")
		done := make(chan error, 1)
		go func() {
			_, err := svc.CreateHandoff(context.Background(), arjun, roomID, "priya", false)
			done <- err
		}()
		<-g.entered
		_, err := svc.CreateHandoffManual(context.Background(), arjun, roomID, "meera", false, manualSummary())
		wantCode(t, err, "handoff_in_progress")
		close(g.release)
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		if n := countType(t, st, roomID, events.HandoffCreated); n != 1 {
			t.Fatalf("%d handoff.created, want 1", n)
		}
	})
	t.Run("not the driver", func(t *testing.T) {
		svc, st, _ := newSwitchService(t, newSwitchWriter(okSummary))
		roomID := createRoom(t, svc, "arjun").ID
		_, err := svc.CreateHandoffManual(context.Background(), mustUser(t, svc, "kabir"), roomID, "priya", false, manualSummary())
		wantCode(t, err, "not_driver")
		if n := countType(t, st, roomID, events.HandoffRequested); n != 0 {
			t.Fatalf("%d handoff.requested after a refused request", n)
		}
	})
	t.Run("closed room", func(t *testing.T) {
		svc, _, _ := newSwitchService(t, newSwitchWriter(okSummary))
		roomID := createRoom(t, svc, "arjun").ID
		closeRoomAs(t, svc, roomID, "arjun")
		_, err := svc.CreateHandoffManual(context.Background(), mustUser(t, svc, "arjun"), roomID, "priya", false, manualSummary())
		wantCode(t, err, "room_closed")
	})
	t.Run("invalid summary writes nothing", func(t *testing.T) {
		svc, st, _ := newSwitchService(t, newSwitchWriter(okSummary))
		roomID := createRoom(t, svc, "arjun").ID
		_, err := svc.CreateHandoffManual(context.Background(), mustUser(t, svc, "arjun"), roomID, "priya", false, events.HandoffSummary{Why: "x"})
		wantCode(t, err, "validation")
		if n := countType(t, st, roomID, events.HandoffRequested); n != 0 {
			t.Fatalf("%d handoff.requested after a 400", n)
		}
	})
}

// Validation of a manual summary: lengths in characters, list sizes, the
// typed-text rule. The message never quotes the text.
func TestHandoffManualSummaryValidation(t *testing.T) {
	rep := func(n int) string { return strings.Repeat("é", n) } // multi-byte: characters, not bytes
	items := func(n int) []string {
		out := make([]string, n)
		for i := range out {
			out[i] = fmt.Sprintf("item %d", i)
		}
		return out
	}
	with := func(f func(*events.HandoffSummary)) events.HandoffSummary {
		s := manualSummary()
		f(&s)
		return s
	}
	cases := []struct {
		name string
		in   events.HandoffSummary
		ok   bool
	}{
		{"valid", manualSummary(), true},
		{"only the required fields", events.HandoffSummary{WhatBroke: "a", Why: "b"}, true},
		{"what_broke missing", with(func(s *events.HandoffSummary) { s.WhatBroke = "" }), false},
		{"what_broke blank", with(func(s *events.HandoffSummary) { s.WhatBroke = " \n\t " }), false},
		{"why missing", with(func(s *events.HandoffSummary) { s.Why = "" }), false},
		{"why blank", with(func(s *events.HandoffSummary) { s.Why = "   " }), false},
		{"what_broke 1000 chars", with(func(s *events.HandoffSummary) { s.WhatBroke = rep(1000) }), true},
		{"what_broke 1001 chars", with(func(s *events.HandoffSummary) { s.WhatBroke = rep(1001) }), false},
		{"why 1000 chars", with(func(s *events.HandoffSummary) { s.Why = rep(1000) }), true},
		{"why 1001 chars", with(func(s *events.HandoffSummary) { s.Why = rep(1001) }), false},
		{"item 500 chars", with(func(s *events.HandoffSummary) { s.Open = []string{rep(500)} }), true},
		{"item 501 chars", with(func(s *events.HandoffSummary) { s.Open = []string{rep(501)} }), false},
		{"done item 501 chars", with(func(s *events.HandoffSummary) { s.Done = []string{rep(501)} }), false},
		{"ruled_out item 501 chars", with(func(s *events.HandoffSummary) { s.RuledOut = []string{rep(501)} }), false},
		{"risks item 501 chars", with(func(s *events.HandoffSummary) { s.Risks = []string{rep(501)} }), false},
		{"20 items", with(func(s *events.HandoffSummary) { s.Done = items(20) }), true},
		{"21 items", with(func(s *events.HandoffSummary) { s.Done = items(21) }), false},
		{"21 ruled_out", with(func(s *events.HandoffSummary) { s.RuledOut = items(21) }), false},
		{"21 open", with(func(s *events.HandoffSummary) { s.Open = items(21) }), false},
		{"21 risks", with(func(s *events.HandoffSummary) { s.Risks = items(21) }), false},
		{"blank items are dropped, not counted", with(func(s *events.HandoffSummary) { s.Open = append(items(20), "", "  ") }), true},
		{"NUL in what_broke", with(func(s *events.HandoffSummary) { s.WhatBroke = "a\x00b" }), false},
		{"invalid UTF-8 in why", with(func(s *events.HandoffSummary) { s.Why = "a\xffb" }), false},
		{"NUL in a list item", with(func(s *events.HandoffSummary) { s.Risks = []string{"fine", "bad\x00"} }), false},
		{"invalid UTF-8 in a list item", with(func(s *events.HandoffSummary) { s.Done = []string{"bad\xc3"} }), false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			svc, st, _ := newSwitchService(t, newSwitchWriter(okSummary))
			roomID := createRoom(t, svc, "arjun").ID
			ev, err := svc.CreateHandoffManual(context.Background(), mustUser(t, svc, "arjun"), roomID, "priya", false, c.in)
			if c.ok {
				if err != nil {
					t.Fatalf("refused: %v", err)
				}
				p, _ := events.Decode[events.HandoffCreatedPayload](ev)
				if p.Summary.Done == nil || p.Summary.RuledOut == nil || p.Summary.Open == nil || p.Summary.Risks == nil {
					t.Fatalf("a list is nil: %+v", p.Summary)
				}
				for _, l := range [][]string{p.Summary.Done, p.Summary.RuledOut, p.Summary.Open, p.Summary.Risks} {
					for _, it := range l {
						if strings.TrimSpace(it) == "" {
							t.Fatalf("a blank item was stored: %q", l)
						}
					}
				}
				return
			}
			wantCode(t, err, "validation")
			var re *room.Error
			_ = errors.As(err, &re)
			if re.Status != 400 || strings.Contains(re.Msg, "bad") {
				t.Fatalf("error = %d %q", re.Status, re.Msg)
			}
			if n := countType(t, st, roomID, events.HandoffRequested); n != 0 {
				t.Fatalf("%d handoff.requested after a 400", n)
			}
		})
	}
}

// The text of a manual summary is trimmed and otherwise kept as typed.
func TestHandoffManualSummaryTrimsText(t *testing.T) {
	svc, _, _ := newSwitchService(t, newSwitchWriter(okSummary))
	roomID := createRoom(t, svc, "arjun").ID
	ev, err := svc.CreateHandoffManual(context.Background(), mustUser(t, svc, "arjun"), roomID, "priya", false, events.HandoffSummary{
		WhatBroke: "  broke \n", Why: "\twhy  ", Open: []string{"  keep this  ", "", "second"},
	})
	if err != nil {
		t.Fatal(err)
	}
	p, _ := events.Decode[events.HandoffCreatedPayload](ev)
	if p.Summary.WhatBroke != "broke" || p.Summary.Why != "why" || fmt.Sprint(p.Summary.Open) != "[keep this second]" {
		t.Fatalf("summary = %+v", p.Summary)
	}
}
