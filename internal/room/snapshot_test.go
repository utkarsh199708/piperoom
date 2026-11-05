package room_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"piperoom/internal/events"
	"piperoom/internal/room"
)

func ev(typ events.Type, seq int64, actor events.Actor, payload any) events.Event {
	raw, _ := json.Marshal(payload)
	return events.Event{RoomID: "r_1", Seq: seq, Type: typ, Actor: actor, Payload: raw, At: time.Date(2026, 10, 11, 10, 0, 0, 0, time.UTC)}
}

func human(id, name string) events.Actor {
	return events.Actor{Kind: events.ActorHuman, ID: id, Name: name}
}

func TestApplyEveryEventType(t *testing.T) {
	arjun := human("arjun", "Arjun")
	priya := human("priya", "Priya")
	lease := time.Date(2026, 10, 11, 10, 30, 0, 0, time.UTC)

	t.Run("room.created", func(t *testing.T) {
		s := room.Apply(room.NewSnapshot(), ev(events.RoomCreated, 1, arjun, events.RoomCreatedPayload{
			Title: "t", Severity: "sev2", PipelineID: "orders",
		}))
		if s.ID != "r_1" || s.Title != "t" || s.Severity != "sev2" || s.PipelineID != "orders" || s.Status != "open" || s.LastSeq != 1 {
			t.Fatalf("bad snapshot: %+v", s)
		}
		if len(s.Members) != 1 || s.Members[0].ID != "arjun" {
			t.Fatalf("creator not a member: %+v", s.Members)
		}
	})

	t.Run("agent.status", func(t *testing.T) {
		s := room.Apply(room.NewSnapshot(), ev(events.AgentStatus, 1, events.AgentActor, events.AgentStatusPayload{State: "working", Detail: "x"}))
		if s.AgentStatus.State != "working" || s.AgentStatus.Detail != "x" {
			t.Fatalf("bad agent status: %+v", s.AgentStatus)
		}
	})

	t.Run("agent.hypothesis", func(t *testing.T) {
		s := room.Apply(room.NewSnapshot(), ev(events.AgentHypothesis, 1, events.AgentActor, events.AgentHypothesisPayload{Text: "h", Confidence: "high"}))
		if s.Hypothesis == nil || s.Hypothesis.Text != "h" || s.Hypothesis.Confidence != "high" {
			t.Fatalf("bad hypothesis: %+v", s.Hypothesis)
		}
	})

	t.Run("steer.suggested then sent removes", func(t *testing.T) {
		s := room.Apply(room.NewSnapshot(), ev(events.SteerSuggested, 1, priya, events.SteerSuggestedPayload{SuggestionID: "g_1", Text: "go"}))
		if len(s.PendingSuggestions) != 1 || s.PendingSuggestions[0].From.ID != "priya" {
			t.Fatalf("bad suggestions: %+v", s.PendingSuggestions)
		}
		s = room.Apply(s, ev(events.SteerSent, 2, arjun, events.SteerSentPayload{SuggestionID: "g_1", Text: "go"}))
		if len(s.PendingSuggestions) != 0 {
			t.Fatalf("suggestion not removed: %+v", s.PendingSuggestions)
		}
	})

	t.Run("control.granted moves driver and roles", func(t *testing.T) {
		s := room.Apply(room.NewSnapshot(), ev(events.ControlGranted, 1, arjun, events.ControlGrantedPayload{To: arjun, LeaseUntil: lease}))
		if s.Driver == nil || s.Driver.ID != "arjun" || !s.Driver.LeaseUntil.Equal(lease) {
			t.Fatalf("bad driver: %+v", s.Driver)
		}
		s = room.Apply(s, ev(events.ControlGranted, 2, arjun, events.ControlGrantedPayload{To: priya, LeaseUntil: lease}))
		if s.Driver.ID != "priya" {
			t.Fatalf("driver not moved: %+v", s.Driver)
		}
		roles := map[string]string{}
		for _, m := range s.Members {
			roles[m.ID] = m.Role
		}
		if roles["priya"] != "driver" || roles["arjun"] != "watching" {
			t.Fatalf("bad roles: %+v", roles)
		}
	})

	t.Run("proposal + approvals", func(t *testing.T) {
		s := room.Apply(room.NewSnapshot(), ev(events.ProposalCreated, 1, events.AgentActor, events.ProposalCreatedPayload{
			ProposalID: "p_1", Title: "fix", Steps: []events.ProposalStep{{Action: "a", Layer: "postgres"}},
			RequiredLayers: []string{"postgres", "warehouse"},
		}))
		if len(s.OpenProposals) != 1 {
			t.Fatalf("no proposal: %+v", s.OpenProposals)
		}
		if s.OpenProposals[0].Approvals["postgres"] != "pending" || s.OpenProposals[0].Approvals["warehouse"] != "pending" {
			t.Fatalf("bad initial approvals: %+v", s.OpenProposals[0].Approvals)
		}
		s = room.Apply(s, ev(events.ApprovalGiven, 2, priya, events.ApprovalGivenPayload{ProposalID: "p_1", Layer: "postgres"}))
		if s.OpenProposals[0].Approvals["postgres"] != "approved" {
			t.Fatalf("approval not recorded: %+v", s.OpenProposals[0].Approvals)
		}
		s = room.Apply(s, ev(events.ApprovalRejected, 3, priya, events.ApprovalRejectedPayload{ProposalID: "p_1", Layer: "warehouse", Reason: "no"}))
		if len(s.OpenProposals) != 0 {
			t.Fatalf("rejected proposal should leave open_proposals: %+v", s.OpenProposals)
		}
	})

	t.Run("action.executed removes a finished proposal", func(t *testing.T) {
		s := room.Apply(room.NewSnapshot(), ev(events.ProposalCreated, 1, events.AgentActor, events.ProposalCreatedPayload{
			ProposalID: "p_1", Title: "fix",
			Steps:          []events.ProposalStep{{Action: "a", Layer: "postgres"}, {Action: "b", Layer: "warehouse"}},
			RequiredLayers: []string{"postgres", "warehouse"},
		}))
		// Step 0 succeeds: not the last step, proposal stays open.
		s = room.Apply(s, ev(events.ActionExecuted, 2, events.AgentActor, events.ActionExecutedPayload{ProposalID: "p_1", StepIndex: 0, OK: true, Output: "ok"}))
		if len(s.OpenProposals) != 1 {
			t.Fatalf("proposal should stay open after step 0: %+v", s.OpenProposals)
		}
		// Step 1 succeeds (last step): proposal leaves the open list.
		s = room.Apply(s, ev(events.ActionExecuted, 3, events.AgentActor, events.ActionExecutedPayload{ProposalID: "p_1", StepIndex: 1, OK: true, Output: "ok"}))
		if len(s.OpenProposals) != 0 {
			t.Fatalf("proposal should leave open_proposals after its last step: %+v", s.OpenProposals)
		}
	})

	t.Run("action.executed failure removes the proposal", func(t *testing.T) {
		s := room.Apply(room.NewSnapshot(), ev(events.ProposalCreated, 1, events.AgentActor, events.ProposalCreatedPayload{
			ProposalID: "p_1", Title: "fix",
			Steps:          []events.ProposalStep{{Action: "a", Layer: "postgres"}, {Action: "b", Layer: "warehouse"}},
			RequiredLayers: []string{"postgres", "warehouse"},
		}))
		s = room.Apply(s, ev(events.ActionExecuted, 2, events.AgentActor, events.ActionExecutedPayload{ProposalID: "p_1", StepIndex: 0, OK: false, Output: "boom"}))
		if len(s.OpenProposals) != 0 {
			t.Fatalf("proposal should leave open_proposals on a failed step: %+v", s.OpenProposals)
		}
	})

	t.Run("room.closed", func(t *testing.T) {
		s := room.Apply(room.NewSnapshot(), ev(events.RoomCreated, 1, arjun, events.RoomCreatedPayload{Title: "t", Severity: "sev1", PipelineID: "p"}))
		s = room.Apply(s, ev(events.RoomClosed, 2, arjun, events.RoomClosedPayload{Resolution: "done"}))
		if s.Status != "closed" {
			t.Fatalf("room not closed: %+v", s.Status)
		}
		// closed_at and resolution are part of the snapshot (review item 5).
		want := time.Date(2026, 10, 11, 10, 0, 0, 0, time.UTC)
		if s.ClosedAt == nil || !s.ClosedAt.Equal(want) {
			t.Fatalf("closed_at not set from the event time: %+v", s.ClosedAt)
		}
		if s.Resolution != "done" {
			t.Fatalf("resolution not set: %q", s.Resolution)
		}
	})

	t.Run("no-op events do not panic", func(t *testing.T) {
		s := room.NewSnapshot()
		for _, tc := range []struct {
			typ events.Type
			p   any
		}{
			{events.ControlRequested, events.ControlRequestedPayload{}},
			{events.CommentAdded, events.CommentAddedPayload{CommentID: "c_1", Text: "hi"}},
			{events.ActionExecuted, events.ActionExecutedPayload{ProposalID: "p_1", StepIndex: 0, OK: true, Output: "ok"}},
			{events.HandoffCreated, events.HandoffCreatedPayload{HandoffID: "h_1"}},
			{events.HandoffAccepted, events.HandoffAcceptedPayload{HandoffID: "h_1"}},
			{events.TracePinned, events.TracePinnedPayload{Table: "orders", Key: "1"}},
			{events.AgentStepStart, events.AgentStepStartPayload{StepID: "s_1", Title: "x"}},
			{events.AgentToolCall, events.AgentToolCallPayload{StepID: "s_1", Tool: "t"}},
			{events.AgentToolResult, events.AgentToolResultPayload{StepID: "s_1", Tool: "t"}},
			{events.AgentMessage, events.AgentMessagePayload{Text: "hi"}},
		} {
			s2 := room.Apply(s, ev(tc.typ, 1, events.AgentActor, tc.p))
			if s2.LastSeq != 1 {
				t.Fatalf("%s did not set last_seq", tc.typ)
			}
		}
	})

	t.Run("apply does not mutate its input", func(t *testing.T) {
		kabir := human("kabir", "Kabir")
		lease := time.Date(2026, 10, 11, 10, 30, 0, 0, time.UTC)
		base := room.NewSnapshot()
		base = room.Apply(base, ev(events.RoomCreated, 1, arjun, events.RoomCreatedPayload{Title: "t", Severity: "sev2", PipelineID: "orders"}))
		base = room.Apply(base, ev(events.ControlGranted, 2, arjun, events.ControlGrantedPayload{To: arjun, LeaseUntil: lease}))
		base = room.Apply(base, ev(events.ProposalCreated, 3, events.AgentActor, events.ProposalCreatedPayload{
			ProposalID: "p_1", Title: "fix", Steps: []events.ProposalStep{{Action: "a", Layer: "postgres"}},
			RequiredLayers: []string{"postgres", "warehouse"},
		}))
		base = room.Apply(base, ev(events.ApprovalGiven, 4, priya, events.ApprovalGivenPayload{ProposalID: "p_1", Layer: "postgres"}))

		beforeDriver := base.Driver.ID
		beforeRoles := map[string]string{}
		for _, m := range base.Members {
			beforeRoles[m.ID] = m.Role
		}
		beforeApproval := base.OpenProposals[0].Approvals["postgres"]
		beforeSuggestion := append([]room.Suggestion(nil), base.PendingSuggestions...)

		// Mutating applies.
		_ = room.Apply(base, ev(events.ControlGranted, 5, arjun, events.ControlGrantedPayload{To: priya, LeaseUntil: lease}))
		_ = room.Apply(base, ev(events.ApprovalGiven, 6, priya, events.ApprovalGivenPayload{ProposalID: "p_1", Layer: "warehouse"}))
		_ = room.Apply(base, ev(events.SteerSuggested, 7, kabir, events.SteerSuggestedPayload{SuggestionID: "g_1", Text: "go"}))
		_ = room.Apply(base, ev(events.SteerSent, 8, arjun, events.SteerSentPayload{SuggestionID: "g_1", Text: "go"}))

		if base.Driver.ID != beforeDriver {
			t.Fatalf("driver mutated: %+v", base.Driver)
		}
		for _, m := range base.Members {
			if beforeRoles[m.ID] != m.Role {
				t.Fatalf("member role mutated: %+v (before %+v)", base.Members, beforeRoles)
			}
		}
		if base.OpenProposals[0].Approvals["postgres"] != beforeApproval {
			t.Fatalf("approval map mutated: %+v", base.OpenProposals[0].Approvals)
		}
		if base.OpenProposals[0].Approvals["warehouse"] != "pending" {
			t.Fatalf("approval map shared: %+v", base.OpenProposals[0].Approvals)
		}
		if len(base.PendingSuggestions) != len(beforeSuggestion) {
			t.Fatalf("suggestions mutated: %+v", base.PendingSuggestions)
		}
	})

	t.Run("comment adds member", func(t *testing.T) {
		s := room.Apply(room.NewSnapshot(), ev(events.CommentAdded, 1, priya, events.CommentAddedPayload{CommentID: "c_1", Text: "hi"}))
		if len(s.Members) != 1 || s.Members[0].Role != "watching" {
			t.Fatalf("commenter not a member: %+v", s.Members)
		}
	})
}

// TestSnapshotJSONArraysNeverNull pins review item 4 / docs/api.md: the
// marshalled snapshot never contains null arrays, a new room's agent state is
// "idle", and based_on_steps/steps/required_layers are [] not null. It checks
// both a fresh snapshot and the Clone()ed snapshots the service actually
// returns to HTTP callers.
func TestSnapshotJSONArraysNeverNull(t *testing.T) {
	wants := []string{
		`"members":[]`,
		`"open_proposals":[]`,
		`"pending_suggestions":[]`,
		`"agent_status":{"state":"idle"}`,
		`"closed_at":null`,
		`"resolution":""`,
	}
	for _, tc := range []struct {
		name string
		snap room.RoomSnapshot
	}{
		{name: "fresh", snap: room.NewSnapshot()},
		{name: "clone", snap: room.NewSnapshot().Clone()},
	} {
		b, err := json.Marshal(tc.snap)
		if err != nil {
			t.Fatalf("%s: marshal: %v", tc.name, err)
		}
		for _, want := range wants {
			if !strings.Contains(string(b), want) {
				t.Fatalf("%s: snapshot JSON missing %s:\n%s", tc.name, want, b)
			}
		}
	}

	// A proposal with no explicit steps/layers and a hypothesis with no
	// based_on_steps still marshal their arrays as [].
	s := room.Apply(room.NewSnapshot(), ev(events.ProposalCreated, 1, events.AgentActor, events.ProposalCreatedPayload{
		ProposalID: "p_1", Title: "fix",
	}))
	s = room.Apply(s, ev(events.AgentHypothesis, 2, events.AgentActor, events.AgentHypothesisPayload{Text: "h"}))
	b, err := json.Marshal(s)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	for _, want := range []string{`"steps":[]`, `"required_layers":[]`, `"approvals":{}`, `"based_on_steps":[]`} {
		if !strings.Contains(string(b), want) {
			t.Fatalf("snapshot JSON missing %s:\n%s", want, b)
		}
	}

	// Folded events never turn closed_at into anything but null before close.
	s2 := room.Apply(s, ev(events.RoomCreated, 3, human("arjun", "Arjun"), events.RoomCreatedPayload{Title: "t", Severity: "sev2", PipelineID: "orders"}))
	b, err = json.Marshal(s2)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if !strings.Contains(string(b), `"closed_at":null`) {
		t.Fatalf("open snapshot closed_at must be null:\n%s", b)
	}
}

// hypothesis.verdict folds per person, derives confirmed, resets on a new
// hypothesis, ignores a verdict for a replaced hypothesis, and never mutates
// the snapshot it was given (Apply is pure).
func TestApplyHypothesisVerdicts(t *testing.T) {
	kabir, priya := human("kabir", "Kabir"), human("priya", "Priya")
	verdict := func(seq int64, a events.Actor, v string, hypSeq int64) events.Event {
		return ev(events.HypothesisVerdict, seq, a, events.HypothesisVerdictPayload{Verdict: v, HypothesisSeq: hypSeq})
	}
	s0 := room.Apply(room.NewSnapshot(), ev(events.AgentHypothesis, 5, events.AgentActor, events.AgentHypothesisPayload{Text: "h", Confidence: "low", Confirmed: true}))
	if s0.Hypothesis.Confirmed || s0.Hypothesis.Seq != 5 || s0.Hypothesis.Verdicts == nil {
		t.Fatalf("fresh: %+v", s0.Hypothesis)
	}
	s1 := room.Apply(s0, verdict(6, kabir, "confirmed", 5))
	if len(s0.Hypothesis.Verdicts) != 0 || s0.Hypothesis.Confirmed {
		t.Fatalf("Apply mutated its input: %+v", s0.Hypothesis)
	}
	if !s1.Hypothesis.Confirmed {
		t.Fatalf("confirmed not derived: %+v", s1.Hypothesis)
	}
	s2 := room.Apply(s1, verdict(7, priya, "disputed", 5))
	if s2.Hypothesis.Confirmed || len(s2.Hypothesis.Verdicts) != 2 {
		t.Fatalf("dispute: %+v", s2.Hypothesis)
	}
	// A stale verdict (older hypothesis seq) changes nothing.
	if s3 := room.Apply(s2, verdict(8, kabir, "disputed", 4)); len(s3.Hypothesis.Verdicts) != 2 || s3.Hypothesis.Verdicts[0].Verdict != "confirmed" {
		t.Fatalf("stale verdict applied: %+v", s3.Hypothesis)
	}
	// A new hypothesis resets.
	s4 := room.Apply(s2, ev(events.AgentHypothesis, 9, events.AgentActor, events.AgentHypothesisPayload{Text: "h2", Confidence: "high"}))
	if s4.Hypothesis.Confirmed || len(s4.Hypothesis.Verdicts) != 0 || s4.Hypothesis.Seq != 9 {
		t.Fatalf("reset: %+v", s4.Hypothesis)
	}
}

// open_proposals[].class is proposal.created.class (docs/api.md, X2): kept
// for recovery and performance, omitted for a fix, so a fix proposal's
// snapshot JSON is unchanged.
func TestSnapshotProposalClass(t *testing.T) {
	s := room.NewSnapshot()
	for i, class := range []string{"", events.ActionClassFix, events.ActionClassRecovery, events.ActionClassPerformance} {
		s = room.Apply(s, ev(events.ProposalCreated, int64(i+1), events.AgentActor, events.ProposalCreatedPayload{
			ProposalID: "p_" + string(rune('a'+i)), Title: "t", RequiredLayers: []string{"kafka"}, Class: class,
			Steps: []events.ProposalStep{{Action: "a", Params: json.RawMessage(`{}`)}},
		}))
	}
	var got []string
	for _, p := range s.OpenProposals {
		got = append(got, p.Class)
	}
	if strings.Join(got, ",") != ",,recovery,performance" {
		t.Fatalf("classes = %q", got)
	}
	raw, _ := json.Marshal(s.OpenProposals[1])
	if strings.Contains(string(raw), `"class"`) {
		t.Fatalf("a fix proposal marshals a class: %s", raw)
	}
	raw, _ = json.Marshal(s.OpenProposals[2])
	if !strings.Contains(string(raw), `"class":"recovery"`) {
		t.Fatalf("a recovery proposal lost its class: %s", raw)
	}
}
