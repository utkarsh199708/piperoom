package room_test

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"piperoom/internal/actions"
	"piperoom/internal/agent"
	"piperoom/internal/events"
	"piperoom/internal/room"
	"piperoom/internal/store/memstore"
)

// hypRig is a room service whose fake agent hands its Sink to the test (so the
// test can write agent.hypothesis) and records every input it receives.
type hypRig struct {
	svc  *room.Service
	st   *memstore.Store
	clk  *clock
	mu   sync.Mutex
	sink agent.Sink
	got  []agent.HumanInput
}

func newHypRig(t *testing.T, act ...actions.Action) *hypRig {
	t.Helper()
	r := &hypRig{}
	r.clk = &clock{t: time.Date(2026, 10, 11, 10, 0, 0, 0, time.UTC)}
	r.st = memstore.NewWithClock(r.clk.Now)
	reg := actions.NewRegistry()
	for _, a := range act {
		if err := reg.Register(a); err != nil {
			t.Fatal(err)
		}
	}
	run := func(ctx context.Context, alert agent.Alert, d agent.Deps, opts agent.Options) error {
		r.mu.Lock()
		r.sink = d.Sink
		r.mu.Unlock()
		for {
			if err := d.Inbox.Wait(ctx); err != nil {
				return nil
			}
			in, _ := d.Inbox.Drain(ctx)
			r.mu.Lock()
			r.got = append(r.got, in...)
			r.mu.Unlock()
		}
	}
	r.svc = room.New(room.Deps{Store: r.st, Actions: reg, Users: room.DefaultUsers(), Owners: testOwners(), Run: run, WriteHandoff: fakeWrite, Now: r.clk.Now})
	t.Cleanup(r.svc.Close)
	return r
}

func (r *hypRig) hypothesis(t *testing.T, roomID, conf string) {
	t.Helper()
	waitFor(t, 2*time.Second, func() bool { r.mu.Lock(); defer r.mu.Unlock(); return r.sink != nil })
	r.mu.Lock()
	sink := r.sink
	r.mu.Unlock()
	if err := sink.Emit(context.Background(), events.AgentHypothesis, events.AgentHypothesisPayload{Text: "ddl widened the column", Confidence: conf, Confirmed: true}); err != nil {
		t.Fatal(err)
	}
}

func (r *hypRig) snap(t *testing.T, roomID string) room.RoomSnapshot {
	t.Helper()
	s, err := r.svc.GetRoom(context.Background(), roomID)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestHypothesisVerdictFlow(t *testing.T) {
	r := newHypRig(t)
	ctx := context.Background()
	roomID := createRoom(t, r.svc, "arjun").ID
	kabir, priya := mustUser(t, r.svc, "kabir"), mustUser(t, r.svc, "priya")

	// No hypothesis yet.
	_, err := r.svc.DecideHypothesis(ctx, kabir, roomID, "confirmed", "")
	wantCode(t, err, "no_hypothesis")

	r.hypothesis(t, roomID, "medium")
	// The agent-written confirmed:true is ignored.
	if h := r.snap(t, roomID).Hypothesis; h == nil || h.Confirmed || h.Seq == 0 || len(h.Verdicts) != 0 {
		t.Fatalf("fresh hypothesis: %+v", h)
	}

	ev, err := r.svc.DecideHypothesis(ctx, kabir, roomID, "confirmed", "")
	if err != nil {
		t.Fatalf("member confirm: %v", err)
	}
	p, _ := events.Decode[events.HypothesisVerdictPayload](ev)
	h := r.snap(t, roomID).Hypothesis
	if p.HypothesisSeq != h.Seq || ev.Actor.ID != "kabir" || !h.Confirmed || len(h.Verdicts) != 1 || h.Verdicts[0].By.ID != "kabir" {
		t.Fatalf("after confirm: payload %+v hyp %+v", p, h)
	}

	// Same actor, same verdict: 409; a changed mind is a new event.
	_, err = r.svc.DecideHypothesis(ctx, kabir, roomID, "confirmed", "")
	wantCode(t, err, "already_decided")
	if _, err = r.svc.DecideHypothesis(ctx, priya, roomID, "disputed", "DDL never ran"); err != nil {
		t.Fatal(err)
	}
	h = r.snap(t, roomID).Hypothesis
	if h.Confirmed || len(h.Verdicts) != 2 {
		t.Fatalf("a dispute must flip confirmed: %+v", h)
	}
	if _, err = r.svc.DecideHypothesis(ctx, priya, roomID, "confirmed", ""); err != nil {
		t.Fatalf("changed mind: %v", err)
	}
	h = r.snap(t, roomID).Hypothesis
	if !h.Confirmed || len(h.Verdicts) != 2 || h.Verdicts[1].By.ID != "priya" || h.Verdicts[1].Verdict != "confirmed" {
		t.Fatalf("latest per person, in seq order: %+v", h.Verdicts)
	}

	// A new hypothesis starts over.
	r.hypothesis(t, roomID, "high")
	waitFor(t, 2*time.Second, func() bool { return r.snap(t, roomID).Hypothesis.Seq != h.Seq })
	if h2 := r.snap(t, roomID).Hypothesis; h2.Confirmed || len(h2.Verdicts) != 0 {
		t.Fatalf("verdicts not reset: %+v", h2)
	}
	if _, err = r.svc.DecideHypothesis(ctx, kabir, roomID, "confirmed", ""); err != nil {
		t.Fatalf("same verdict on a new hypothesis: %v", err)
	}
}

func TestHypothesisVerdictAgentInput(t *testing.T) {
	r := newHypRig(t)
	ctx := context.Background()
	roomID := createRoom(t, r.svc, "arjun").ID
	r.hypothesis(t, roomID, "medium")
	if _, err := r.svc.DecideHypothesis(ctx, mustUser(t, r.svc, "priya"), roomID, "disputed", "DDL never ran"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 2*time.Second, func() bool {
		r.mu.Lock()
		defer r.mu.Unlock()
		for _, in := range r.got {
			if in.Kind == "comment" && in.Text == "disputed the current hypothesis: DDL never ran" && in.CommentID == "" && in.StepID == "" {
				return true
			}
		}
		return false
	})
}

func TestHypothesisVerdictValidationAndStates(t *testing.T) {
	r := newHypRig(t)
	ctx := context.Background()
	roomID := createRoom(t, r.svc, "arjun").ID
	kabir := mustUser(t, r.svc, "kabir")
	r.hypothesis(t, roomID, "low")

	for name, tc := range map[string]struct{ verdict, note string }{
		"bad verdict": {"maybe", ""},
		"empty":       {"", ""},
		"long note":   {"disputed", strings.Repeat("x", 501)},
		"nul":         {"disputed", "a\x00b"},
		"bad utf8":    {"disputed", "a\xffb"},
	} {
		_, err := r.svc.DecideHypothesis(ctx, kabir, roomID, tc.verdict, tc.note)
		if err == nil {
			t.Errorf("%s: want validation error", name)
			continue
		}
		wantCode(t, err, "validation")
	}
	// 500 characters (runes) is fine.
	if _, err := r.svc.DecideHypothesis(ctx, kabir, roomID, "disputed", strings.Repeat("é", 500)); err != nil {
		t.Fatalf("500 runes: %v", err)
	}

	_, err := r.svc.DecideHypothesis(ctx, kabir, "r_missing", "confirmed", "")
	wantCode(t, err, "not_found")

	if _, err := r.svc.CloseRoom(ctx, mustUser(t, r.svc, "arjun"), roomID, "resolved"); err != nil {
		t.Fatal(err)
	}
	_, err = r.svc.DecideHypothesis(ctx, mustUser(t, r.svc, "priya"), roomID, "confirmed", "")
	wantCode(t, err, "room_closed")
}

func TestHypothesisVerdictDriverLease(t *testing.T) {
	r := newHypRig(t)
	ctx := context.Background()
	roomID := createRoom(t, r.svc, "arjun").ID
	r.hypothesis(t, roomID, "low")
	r.clk.Advance(20 * time.Minute)

	before := r.snap(t, roomID).Driver.LeaseUntil
	if _, err := r.svc.DecideHypothesis(ctx, mustUser(t, r.svc, "kabir"), roomID, "confirmed", ""); err != nil {
		t.Fatal(err)
	}
	if got := r.snap(t, roomID).Driver.LeaseUntil; !got.Equal(before) {
		t.Fatalf("a non-driver verdict must not renew the lease: %v -> %v", before, got)
	}
	if _, err := r.svc.DecideHypothesis(ctx, mustUser(t, r.svc, "arjun"), roomID, "confirmed", ""); err != nil {
		t.Fatal(err)
	}
	if got, want := r.snap(t, roomID).Driver.LeaseUntil, r.clk.Now().UTC().Add(room.DriverLease); !got.Equal(want) {
		t.Fatalf("driver verdict renews the lease: %v want %v", got, want)
	}
}
