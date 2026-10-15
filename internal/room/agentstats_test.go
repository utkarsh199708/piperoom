package room_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"piperoom/internal/events"
	"piperoom/internal/room"
	"piperoom/internal/store"
	"piperoom/internal/store/memstore"
)

type statsRoom struct {
	id        string
	shadow    bool
	openedAgo time.Duration // opened this long before "now"
	lasted    time.Duration // closed this long after opening
	firstHyp  time.Duration // 0 = no hypothesis
	conf      string
	grade     string // "", yes, partial, no (shadow.graded)
	labelled  bool   // room.labelled
	costs     []*float64
}

func seedStats(t *testing.T, st *memstore.Store, c *clock, now time.Time, r statsRoom) {
	t.Helper()
	ctx := context.Background()
	opened := now.Add(-r.openedAgo)
	c.mu.Lock()
	c.t = opened
	c.mu.Unlock()
	if err := st.CreateRoom(ctx, store.Room{ID: r.id, Title: "t", Severity: "sev2", PipelineID: "orders", Status: "open", OpenedAt: opened}); err != nil {
		t.Fatal(err)
	}
	app := func(at time.Time, typ events.Type, p any) {
		c.mu.Lock()
		c.t = at
		c.mu.Unlock()
		if _, err := st.Append(ctx, r.id, typ, events.AgentActor, p); err != nil {
			t.Fatal(err)
		}
	}
	app(opened, events.RoomCreated, events.RoomCreatedPayload{Title: "t", PipelineID: "orders", Shadow: r.shadow})
	if r.shadow {
		run := store.ShadowRun{RoomID: r.id, Status: store.ShadowNoHypothesis, Model: "m", PipelineMap: "x"}
		if r.firstHyp > 0 {
			at := opened.Add(r.firstHyp)
			run.Status, run.FirstHypothesisAt = store.ShadowHypothesised, &at
			run.FinalHypothesis = json.RawMessage(fmt.Sprintf(`{"confidence":%q}`, r.conf))
		}
		if err := st.CreateShadowRun(ctx, run); err != nil {
			t.Fatal(err)
		}
	} else {
		if r.firstHyp > 0 {
			app(opened.Add(r.firstHyp), events.AgentHypothesis, events.AgentHypothesisPayload{Text: "secret text", Confidence: r.conf})
		}
		for _, cost := range r.costs {
			app(opened.Add(time.Second), events.AgentUsage, events.AgentUsagePayload{Model: "m", Role: "main", CostUSD: cost})
		}
	}
	closed := opened.Add(r.lasted)
	app(closed, events.RoomClosed, events.RoomClosedPayload{Resolution: "fixed"})
	if err := st.CloseRoom(ctx, r.id, closed); err != nil {
		t.Fatal(err)
	}
	if r.grade != "" {
		app(closed.Add(time.Second), events.ShadowGraded, events.ShadowGradedPayload{LayerCorrect: r.grade != "no", RootCause: r.grade})
	}
	if r.labelled {
		app(closed.Add(time.Second), events.RoomLabelled, events.RoomLabelledPayload{Cause: "x"})
	}
	c.mu.Lock()
	c.t = now
	c.mu.Unlock()
}

func TestAgentStats(t *testing.T) {
	const day = 24 * time.Hour
	now := time.Date(2026, 10, 14, 12, 0, 0, 0, time.UTC)
	newSvc := func(t *testing.T, rooms ...statsRoom) (*room.Service, *clock) {
		svc, st, c := newTestService(t)
		for _, r := range rooms {
			seedStats(t, st, c, now, r)
		}
		return svc, c
	}
	f := func(v float64) *float64 { return &v }

	t.Run("empty pipeline", func(t *testing.T) {
		svc, _ := newSvc(t)
		s, err := svc.AgentStats(context.Background(), "orders", 90)
		if err != nil {
			t.Fatal(err)
		}
		if s.Rooms != 0 || s.EnoughData || s.CostAvailable || s.MedianTimeToFirstHypothesisS != -1 || s.MedianHumanMTTRS != -1 || s.ByConfidence == nil {
			t.Fatalf("bad empty stats: %+v", s)
		}
		if b, _ := json.Marshal(s); len(b) == 0 {
			t.Fatal("no json")
		}
	})

	t.Run("window edge", func(t *testing.T) {
		svc, _ := newSvc(t,
			statsRoom{id: "r_in", openedAgo: 10*day + time.Hour, lasted: time.Hour},     // closed exactly 10 d ago: in
			statsRoom{id: "r_out", openedAgo: 10*day + 3*time.Hour, lasted: time.Hour},  // closed 10 d + 2 h ago: out
			statsRoom{id: "r_edge", openedAgo: 10*day + 2*time.Hour, lasted: time.Hour}, // closed 10 d + 1 h ago: out
		)
		s, err := svc.AgentStats(context.Background(), "orders", 10)
		if err != nil {
			t.Fatal(err)
		}
		if s.Rooms != 1 {
			t.Fatalf("want 1 room in window, got %d", s.Rooms)
		}
	})

	t.Run("medians and records", func(t *testing.T) {
		svc, _ := newSvc(t,
			statsRoom{id: "r_1", openedAgo: day, lasted: 10 * time.Minute, firstHyp: 40 * time.Second, conf: "high", grade: "yes", costs: []*float64{f(0.2), f(0.2)}},
			statsRoom{id: "r_2", openedAgo: 2 * day, lasted: 20 * time.Minute, firstHyp: 60 * time.Second, conf: "high", grade: "partial", costs: []*float64{f(0.1)}},
			statsRoom{id: "r_3", openedAgo: 3 * day, lasted: 30 * time.Minute, firstHyp: 90 * time.Second, conf: "low", grade: "no", costs: []*float64{nil}},
			statsRoom{id: "r_4", openedAgo: 4 * day, lasted: 40 * time.Minute, labelled: true},
		)
		s, err := svc.AgentStats(context.Background(), "orders", 90)
		if err != nil {
			t.Fatal(err)
		}
		if s.Rooms != 4 || s.Hypothesised != 3 || s.Labelled != 1 || s.Graded != 3 || s.LayerCorrect != 2 {
			t.Fatalf("counts: %+v", s)
		}
		if s.RootCause.Yes != 1 || s.RootCause.Partial != 1 || s.RootCause.No != 1 {
			t.Fatalf("root cause: %+v", s.RootCause)
		}
		// median of 40, 60, 90 and of 600, 1200, 1800, 2400 (mean of the middle two).
		if s.MedianTimeToFirstHypothesisS != 60 || s.MedianHumanMTTRS != 1500 {
			t.Fatalf("medians: %d %d", s.MedianTimeToFirstHypothesisS, s.MedianHumanMTTRS)
		}
		// Costs 0.4 and 0.1 are priced (r_3 is unpriced): even count, mean of both.
		if !s.CostAvailable || s.MedianCostUSD < 0.249 || s.MedianCostUSD > 0.251 {
			t.Fatalf("cost: %v %v", s.CostAvailable, s.MedianCostUSD)
		}
		// partial is not correct; high 0/... r_1 yes, r_2 partial.
		want := []room.AgentStatsBucket{{Confidence: "high", Runs: 2, Correct: 1, Accuracy: 0.5}, {Confidence: "low", Runs: 1}}
		if len(s.ByConfidence) != 2 || s.ByConfidence[0] != want[0] || s.ByConfidence[1] != want[1] {
			t.Fatalf("buckets: %+v", s.ByConfidence)
		}
		if s.EnoughData {
			t.Fatal("3 graded rooms are not enough data")
		}
	})

	t.Run("one room median and unpriced", func(t *testing.T) {
		svc, _ := newSvc(t, statsRoom{id: "r_1", openedAgo: day, lasted: time.Minute, firstHyp: 7 * time.Second, conf: "medium", costs: []*float64{nil, nil}})
		s, err := svc.AgentStats(context.Background(), "orders", 90)
		if err != nil {
			t.Fatal(err)
		}
		if s.MedianTimeToFirstHypothesisS != 7 || s.MedianHumanMTTRS != 60 || s.CostAvailable || s.MedianCostUSD != -1 || len(s.ByConfidence) != 0 {
			t.Fatalf("stats: %+v", s)
		}
	})

	t.Run("enough_data at 9 vs 10", func(t *testing.T) {
		mk := func(n int) []statsRoom {
			var rs []statsRoom
			for i := 0; i < n; i++ {
				rs = append(rs, statsRoom{id: fmt.Sprintf("r_%d", i), openedAgo: day, lasted: time.Minute, firstHyp: time.Second, conf: "high", grade: "yes"})
			}
			return rs
		}
		for n, want := range map[int]bool{9: false, 10: true} {
			svc, _ := newSvc(t, mk(n)...)
			s, err := svc.AgentStats(context.Background(), "orders", 90)
			if err != nil {
				t.Fatal(err)
			}
			if s.Graded != n || s.EnoughData != want {
				t.Fatalf("n=%d: graded %d enough %v", n, s.Graded, s.EnoughData)
			}
		}
	})

	t.Run("shadow subset", func(t *testing.T) {
		svc, _ := newSvc(t,
			statsRoom{id: "r_s1", shadow: true, openedAgo: day, lasted: time.Hour, firstHyp: 30 * time.Second, conf: "high", grade: "yes"},
			statsRoom{id: "r_s2", shadow: true, openedAgo: day, lasted: time.Hour, grade: "no"},
			statsRoom{id: "r_l1", openedAgo: day, lasted: time.Hour, firstHyp: 50 * time.Second, conf: "high"},
		)
		s, err := svc.AgentStats(context.Background(), "orders", 90)
		if err != nil {
			t.Fatal(err)
		}
		if s.Rooms != 3 || s.Hypothesised != 2 || s.Graded != 2 || s.LayerCorrect != 1 {
			t.Fatalf("totals: %+v", s)
		}
		if s.Shadow != (room.AgentStatsShadow{Rooms: 2, Graded: 2, LayerCorrect: 1}) {
			t.Fatalf("shadow: %+v", s.Shadow)
		}
		if len(s.ByConfidence) != 1 || s.ByConfidence[0].Runs != 1 || s.ByConfidence[0].Correct != 1 {
			t.Fatalf("buckets: %+v", s.ByConfidence)
		}
		if s.MedianTimeToFirstHypothesisS != 40 {
			t.Fatalf("median first: %d", s.MedianTimeToFirstHypothesisS)
		}
	})

	t.Run("cache hit then miss", func(t *testing.T) {
		svc, st, c := newTestService(t)
		seedStats(t, st, c, now, statsRoom{id: "r_1", openedAgo: day, lasted: time.Minute})
		s1, _ := svc.AgentStats(context.Background(), "orders", 90)
		seedStats(t, st, c, now, statsRoom{id: "r_2", openedAgo: day, lasted: time.Minute})
		c.mu.Lock()
		c.t = now.Add(time.Minute)
		c.mu.Unlock()
		s2, _ := svc.AgentStats(context.Background(), "orders", 90)
		if s1.Rooms != 1 || s2.Rooms != 1 {
			t.Fatalf("cache miss within 2 min: %d %d", s1.Rooms, s2.Rooms)
		}
		c.mu.Lock()
		c.t = now.Add(3 * time.Minute)
		c.mu.Unlock()
		s3, _ := svc.AgentStats(context.Background(), "orders", 90)
		if s3.Rooms != 2 {
			t.Fatalf("cache not refreshed after 2 min: %d", s3.Rooms)
		}
	})

	t.Run("bad days", func(t *testing.T) {
		svc, _ := newSvc(t)
		if _, err := svc.AgentStats(context.Background(), "orders", 0); err == nil {
			t.Fatal("days 0 accepted")
		}
	})

	t.Run("reader error is wrapped", func(t *testing.T) {
		svc, st, _ := newTestService(t)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_ = st
		_, err := svc.AgentStats(ctx, "orders", 90)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("want wrapped context.Canceled, got %v", err)
		}
	})
}
