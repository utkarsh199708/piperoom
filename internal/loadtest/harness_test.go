package loadtest

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/coder/websocket"

	"piperoom/internal/events"
)

func ev(seq int64, typ events.Type, at time.Time, payload any) events.Event {
	p, _ := json.Marshal(payload)
	return events.Event{RoomID: "r_1", Seq: seq, Type: typ, Actor: events.AgentActor, Payload: p, At: at}
}

func TestBuildTimeline(t *testing.T) {
	t0 := time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC)
	at := func(ms int) time.Time { return t0.Add(time.Duration(ms) * time.Millisecond) }
	evs := []events.Event{
		ev(1, events.RoomCreated, at(0), events.RoomCreatedPayload{Title: "x", PipelineID: "pb"}),
		ev(2, events.AgentStatus, at(5), events.AgentStatusPayload{State: "queued", Detail: "waiting for an agent slot (7 ahead)"}),
		ev(3, events.AgentStatus, at(900), events.AgentStatusPayload{State: "idle"}), // before any turn: not the run's idle
		ev(4, events.AgentStatus, at(1000), events.AgentStatusPayload{State: "working", Detail: "agent slot acquired"}),
		ev(5, events.AgentStatus, at(1001), events.AgentStatusPayload{State: "working"}),
		ev(6, events.AgentStepStart, at(1300), events.AgentStepStartPayload{StepID: "s_1"}),
		ev(7, events.AgentMessage, at(1500), events.AgentMessagePayload{Text: "hi"}),
		ev(8, events.ProposalCreated, at(1600), map[string]string{"proposal_id": "p_1"}),
		ev(9, events.AgentStatus, at(1700), events.AgentStatusPayload{State: "idle"}),
		ev(10, events.AgentStatus, at(1800), events.AgentStatusPayload{State: "idle"}),
		ev(11, events.ActionExecuted, at(1900), map[string]any{"ok": true}),
	}
	tl := buildTimeline("r_1", evs)
	if tl.Pipeline != "pb" || !tl.Queued || tl.Ahead != 7 || tl.Events != 11 || tl.LastSeq != 11 || tl.Proposals != 1 || tl.Executed != 1 {
		t.Fatalf("timeline %+v", tl)
	}
	if tl.QueueWait() != time.Second || tl.TTFT() != 1300*time.Millisecond || tl.Hold() != 700*time.Millisecond {
		t.Fatalf("queue %s ttft %s hold %s", tl.QueueWait(), tl.TTFT(), tl.Hold())
	}
	// A room whose agent never ran reports missing values, never zero.
	empty := buildTimeline("r_2", evs[:1])
	if empty.QueueWait() >= 0 || empty.TTFT() >= 0 || empty.Hold() >= 0 {
		t.Fatalf("missing values must be negative: %+v", empty)
	}
	if got := durations([]timeline{tl, empty}, timeline.TTFT); len(got) != 1 {
		t.Fatalf("durations kept missing values: %v", got)
	}
}

func TestExecStatsCountExecuted(t *testing.T) {
	var es ExecStats
	es.countExecuted([]timeline{{Proposals: 1, Executed: 1}, {Proposals: 1, Executed: 3}, {Proposals: 0, Executed: 0}})
	if es.ExecutedEvents != 4 || es.DuplicateExecEvents != 2 {
		t.Fatalf("%+v", es)
	}
}

func TestParseCgroupCPU(t *testing.T) {
	tests := map[string]float64{
		"1500000000\n":          1.5,
		"usage_usec 2500000":    2.5,
		"usage_usec   10\n":     0.00001,
		"":                      -1,
		"garbage":               -1,
		"usage_usec notanumber": -1,
	}
	for in, want := range tests {
		if got := parseCgroupCPU(in); got != want {
			t.Errorf("parseCgroupCPU(%q) = %v, want %v", in, got, want)
		}
	}
	a := cpuMark{at: time.Unix(0, 0), server: 1, driver: -1, pgs: 2}
	b := cpuMark{at: time.Unix(2, 0), server: 3, driver: 5, pgs: 2.5}
	u := a.until(b)
	if u.WallS != 2 || u.ServerS != 2 || u.DriverS != -1 || u.PostgresS != 0.5 {
		t.Fatalf("until = %+v", u)
	}
	if s := u.String(); s != "server 1.00, driver n/a, postgres 0.25 cores" {
		t.Fatalf("String = %q", s)
	}
}

// wsServer serves one connection that writes frames and then closes it with
// code and reason.
func wsServer(t *testing.T, frames []any, code websocket.StatusCode, reason string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		for _, f := range frames {
			b, _ := json.Marshal(f)
			if err := c.Write(r.Context(), websocket.MessageText, b); err != nil {
				return
			}
		}
		_ = c.Close(code, reason)
	}))
}

// The subscriber's seq check and drop counters are what every fan-out and
// replay check rests on: a gap, a duplicate, a server close and the latency
// split must all be counted.
func TestSubCountsGapsDupsClosesAndLatency(t *testing.T) {
	now := time.Now().UTC()
	comment := func(seq int64, replica int) serverMsg {
		e := ev(seq, events.CommentAdded, now.Add(-10*time.Millisecond), events.CommentAddedPayload{
			CommentID: "c", Text: commentMark + "1:" + string(rune('0'+replica)) + " pad"})
		return serverMsg{Type: "event", Event: &e}
	}
	status := func(seq int64) serverMsg {
		e := ev(seq, events.AgentStatus, now, events.AgentStatusPayload{State: "idle"})
		return serverMsg{Type: "event", Event: &e}
	}
	frames := []any{
		comment(1, 0), comment(2, 1), status(2), // duplicate of 2
		map[string]any{"type": "presence", "members": []any{}},
		status(5), // seqs 3 and 4 skipped
		comment(6, 1),
	}
	srv := wsServer(t, frames, websocket.StatusPolicyViolation, "send buffer full")
	defer srv.Close()

	sink := &LatencySink{Delivery: NewRecorder(0), EndToEnd: NewRecorder(0), Same: NewRecorder(0), Cross: NewRecorder(0)}
	s := NewSub("r_1", "priya", 0, sink)
	s.Replica = 1
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	s.Run(ctx, srv.URL, srv.Client())

	if s.Received != 5 || s.Dups != 1 || s.Gaps != 2 || s.LastSeq.Load() != 6 || s.Mismatches != 0 || s.Reconnects != 0 {
		t.Fatalf("received %d dups %d gaps %d last %d mismatches %d reconnects %d", s.Received, s.Dups, s.Gaps, s.LastSeq.Load(), s.Mismatches, s.Reconnects)
	}
	if s.Closes["1008 send buffer full"] != 1 {
		t.Fatalf("closes %v", s.Closes)
	}
	// 4 events recorded (the duplicate is skipped); comments carry marks.
	if sink.Delivery.Dist().N != 4 || sink.EndToEnd.Dist().N != 3 || sink.Same.Dist().N != 2 || sink.Cross.Dist().N != 1 {
		t.Fatalf("delivery %d e2e %d same %d cross %d", sink.Delivery.Dist().N, sink.EndToEnd.Dist().N, sink.Same.Dist().N, sink.Cross.Dist().N)
	}
	tot := Totals([]*Sub{s}, map[string]int64{"r_1": 9})
	if tot.SlowClosed != 1 || tot.MissingTail != 3 || tot.Gaps != 2 || tot.Dups != 1 || tot.Received != 5 {
		t.Fatalf("totals %+v", tot)
	}
	select {
	case <-s.Done:
	default:
		t.Fatal("Done not closed after Run returned")
	}
}

func TestSubTargetAndSinkFilters(t *testing.T) {
	now := time.Now().UTC()
	var frames []any
	for i := int64(1); i <= 3; i++ {
		e := ev(i, events.AgentStatus, now, events.AgentStatusPayload{State: "idle"})
		frames = append(frames, serverMsg{Type: "event", Event: &e})
	}
	srv := wsServer(t, frames, websocket.StatusNormalClosure, "")
	defer srv.Close()

	sink := &LatencySink{Delivery: NewRecorder(0)}
	sink.From.Store(now.Add(time.Hour).UnixNano()) // every event is before From
	s := NewSub("r_1", "priya", 2, sink)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	go s.Run(ctx, srv.URL, srv.Client())
	select {
	case <-s.Done:
	case <-ctx.Done():
		t.Fatal("Done never closed at the target seq")
	}
	if sink.Delivery.Dist().N != 0 {
		t.Fatal("events before From were recorded")
	}
	sink.From.Store(0)
	sink.Off.Store(true)
	s2 := NewSub("r_1", "priya", 0, sink)
	s2.Run(ctx, srv.URL, srv.Client())
	if sink.Delivery.Dist().N != 0 || s2.Received != 3 {
		t.Fatalf("Off sink recorded %d; received %d", sink.Delivery.Dist().N, s2.Received)
	}
}
