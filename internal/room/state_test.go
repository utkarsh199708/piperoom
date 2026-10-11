package room

import (
	"encoding/json"
	"testing"
	"time"

	"piperoom/internal/events"
)

var t0 = time.Date(2026, 10, 11, 10, 0, 0, 0, time.UTC)

var (
	kabir = events.Actor{Kind: events.ActorHuman, ID: "kabir", Name: "Kabir"}
	arjun = events.Actor{Kind: events.ActorHuman, ID: "arjun", Name: "Arjun"}
)

// ev builds a test event with the next seq.
type log struct{ evs []events.Event }

func (l *log) add(typ events.Type, actor events.Actor, payload any, at time.Time) *log {
	raw, err := json.Marshal(payload)
	if err != nil {
		panic(err)
	}
	l.evs = append(l.evs, events.Event{RoomID: "r_1", Seq: int64(len(l.evs) + 1), Type: typ, Actor: actor, Payload: raw, At: at})
	return l
}

func created(l *log, actor events.Actor) *log {
	return l.add(events.RoomCreated, actor, events.RoomCreatedPayload{Title: "Orders stale", Severity: "sev2", PipelineID: "orders"}, t0)
}

func TestFoldRoomCreated(t *testing.T) {
	human := Fold("r_1", created(&log{}, kabir).evs).Snapshot()
	if human.Driver == nil || human.Driver.ID != "kabir" || !human.Driver.LeaseUntil.Equal(t0.Add(LeaseDuration)) {
		t.Fatalf("creator must be driver with a 30 min lease, got %+v", human.Driver)
	}
	if human.Status != StatusOpen || human.AgentStatus.State != "idle" || human.Title != "Orders stale" {
		t.Fatalf("snapshot = %+v", human)
	}
	sys := Fold("r_1", created(&log{}, events.Actor{Kind: events.ActorSystem, ID: "system"}).evs).Snapshot()
	if sys.Driver != nil {
		t.Fatalf("an alert-created room has no driver, got %+v", sys.Driver)
	}
}

func TestFoldSnapshotArraysAreNeverNil(t *testing.T) {
	s := Fold("r_1", created(&log{}, kabir).evs).Snapshot()
	raw, _ := json.Marshal(s)
	var m map[string]any
	_ = json.Unmarshal(raw, &m)
	for _, k := range []string{"open_proposals", "pending_suggestions", "members"} {
		if _, ok := m[k].([]any); !ok {
			t.Errorf("%s = %v, want a JSON array", k, m[k])
		}
	}
}

func TestFoldSuggestionsAndControl(t *testing.T) {
	l := created(&log{}, kabir)
	l.add(events.SteerSuggested, arjun, events.SteerSuggestedPayload{SuggestionID: "g_1", Text: "check the slot"}, t0)
	l.add(events.SteerSuggested, arjun, events.SteerSuggestedPayload{SuggestionID: "g_2", Text: "check lag"}, t0)
	if got := Fold("r_1", l.evs).Snapshot().PendingSuggestions; len(got) != 2 || got[0].From.ID != "arjun" {
		t.Fatalf("pending = %+v", got)
	}
	l.add(events.SteerSent, kabir, events.SteerSentPayload{SuggestionID: "g_1", Text: "check the slot"}, t0)
	l.add(events.ControlGranted, kabir, events.ControlGrantedPayload{To: arjun, LeaseUntil: t0.Add(time.Hour)}, t0.Add(time.Minute))
	s := Fold("r_1", l.evs).Snapshot()
	if len(s.PendingSuggestions) != 1 || s.PendingSuggestions[0].SuggestionID != "g_2" {
		t.Fatalf("sent suggestion must leave the pending list: %+v", s.PendingSuggestions)
	}
	if s.Driver.ID != "arjun" || !s.Driver.LeaseUntil.Equal(t0.Add(time.Hour)) {
		t.Fatalf("driver = %+v", s.Driver)
	}
	if s.Members[0].ID != "arjun" || s.Members[0].Role != "driver" || len(s.Members) != 2 {
		t.Fatalf("members = %+v", s.Members)
	}
}

func proposal(id string, layers ...string) events.ProposalCreatedPayload {
	return events.ProposalCreatedPayload{ProposalID: id, Title: "fix", RequiredLayers: layers,
		Steps: []events.ProposalStep{{Action: "a", Layer: layers[0], Params: json.RawMessage(`{}`)}, {Action: "b", Layer: layers[len(layers)-1], Params: json.RawMessage(`{}`)}}}
}

func TestFoldProposalLifecycle(t *testing.T) {
	cases := []struct {
		name     string
		more     func(l *log)
		wantOpen bool
		approval map[string]string
	}{
		{"created", func(l *log) {}, true, map[string]string{"kafka-connect": "pending", "warehouse": "pending"}},
		{"one approved", func(l *log) {
			l.add(events.ApprovalGiven, arjun, events.ApprovalGivenPayload{ProposalID: "p_1", Layer: "kafka-connect"}, t0)
		}, true, map[string]string{"kafka-connect": "approved", "warehouse": "pending"}},
		{"rejected closes it", func(l *log) {
			l.add(events.ApprovalRejected, arjun, events.ApprovalRejectedPayload{ProposalID: "p_1", Layer: "kafka-connect", Reason: "no"}, t0)
		}, false, nil},
		{"one step done, still open", func(l *log) {
			l.add(events.ActionExecuted, events.Actor{Kind: events.ActorSystem}, events.ActionExecutedPayload{ProposalID: "p_1", StepIndex: 0, OK: true}, t0)
		}, true, nil},
		{"all steps done", func(l *log) {
			l.add(events.ActionExecuted, events.Actor{Kind: events.ActorSystem}, events.ActionExecutedPayload{ProposalID: "p_1", StepIndex: 0, OK: true}, t0)
			l.add(events.ActionExecuted, events.Actor{Kind: events.ActorSystem}, events.ActionExecutedPayload{ProposalID: "p_1", StepIndex: 1, OK: true}, t0)
		}, false, nil},
		{"failed step ends it", func(l *log) {
			l.add(events.ActionExecuted, events.Actor{Kind: events.ActorSystem}, events.ActionExecutedPayload{ProposalID: "p_1", StepIndex: 0, OK: false, Output: "boom"}, t0)
		}, false, nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			l := created(&log{}, kabir)
			l.add(events.ProposalCreated, events.AgentActor, proposal("p_1", "kafka-connect", "warehouse"), t0)
			c.more(l)
			open := Fold("r_1", l.evs).Snapshot().OpenProposals
			if (len(open) == 1) != c.wantOpen {
				t.Fatalf("open proposals = %+v, wantOpen %v", open, c.wantOpen)
			}
			for layer, want := range c.approval {
				if got := open[0].Approvals[layer]; got != want {
					t.Errorf("approvals[%s] = %q, want %q", layer, got, want)
				}
			}
		})
	}
}

func TestFoldAgentHypothesisAndStatus(t *testing.T) {
	l := created(&log{}, kabir)
	l.add(events.AgentStatus, events.AgentActor, events.AgentStatusPayload{State: "working", Detail: "step 6"}, t0)
	l.add(events.AgentHypothesis, events.AgentActor, events.AgentHypothesisPayload{Text: "schema", Confidence: "high"}, t0)
	s := Fold("r_1", l.evs).Snapshot()
	if s.AgentStatus.State != "working" || s.AgentStatus.Detail != "step 6" {
		t.Fatalf("status = %+v", s.AgentStatus)
	}
	if s.Hypothesis == nil || s.Hypothesis.Text != "schema" || s.Hypothesis.BasedOnSteps == nil {
		t.Fatalf("hypothesis = %+v", s.Hypothesis)
	}
}

func TestFoldClose(t *testing.T) {
	l := created(&log{}, kabir)
	l.add(events.RoomClosed, kabir, events.RoomClosedPayload{Resolution: "fixed"}, t0.Add(time.Hour))
	s := Fold("r_1", l.evs).Snapshot()
	if s.Status != StatusClosed || s.Resolution != "fixed" || s.ClosedAt == nil || !s.ClosedAt.Equal(t0.Add(time.Hour)) || s.LastSeq != 2 {
		t.Fatalf("snapshot = %+v", s)
	}
}

func TestFoldIgnoresUndecodablePayload(t *testing.T) {
	l := created(&log{}, kabir)
	l.evs = append(l.evs, events.Event{RoomID: "r_1", Seq: 2, Type: events.AgentStatus, Payload: json.RawMessage(`"not an object"`), At: t0})
	if s := Fold("r_1", l.evs).Snapshot(); s.LastSeq != 2 || s.AgentStatus.State != "idle" {
		t.Fatalf("a bad payload must not break loading: %+v", s)
	}
}

func TestFoldHandoffVoidsEarlier(t *testing.T) {
	l := created(&log{}, kabir)
	l.add(events.HandoffCreated, kabir, events.HandoffCreatedPayload{HandoffID: "h_1", To: arjun, StayAsWatcher: true}, t0)
	l.add(events.HandoffCreated, kabir, events.HandoffCreatedPayload{HandoffID: "h_2", To: arjun}, t0)
	st := Fold("r_1", l.evs)
	if !st.handoffs["h_1"].voided || st.handoffs["h_2"].voided {
		t.Fatalf("the newer hand-off must void the older: %+v %+v", st.handoffs["h_1"], st.handoffs["h_2"])
	}
}

func TestNewIDShape(t *testing.T) {
	a, b := newID("r_", t0), newID("r_", t0)
	if len(a) != 2+26 || a[:2] != "r_" || a == b {
		t.Fatalf("ids = %q %q", a, b)
	}
	if later := newID("r_", t0.Add(time.Second)); later <= a {
		t.Fatalf("ids must sort by time: %q <= %q", later, a)
	}
}
