package room_test

import (
	"encoding/json"
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
