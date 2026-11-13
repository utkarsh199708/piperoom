package room_test

import (
	"encoding/json"
	"strings"
	"testing"

	"piperoom/internal/events"
	"piperoom/internal/room"
)

func lastFixCreated(seq int64, id string, steps int) events.Event {
	s := make([]events.ProposalStep, steps)
	for i := range s {
		s[i] = events.ProposalStep{Action: "a", Layer: "postgres"}
	}
	return ev(events.ProposalCreated, seq, events.AgentActor, events.ProposalCreatedPayload{
		ProposalID: id, Title: "fix", Steps: s, RequiredLayers: []string{"postgres"},
	})
}

func lastFixRan(seq int64, id string, step int, ok bool) events.Event {
	return ev(events.ActionExecuted, seq, events.AgentActor, events.ActionExecutedPayload{ProposalID: id, StepIndex: step, OK: ok})
}

func lastFixVerdict(seq int64, id string, recovered bool, verdict string, repeat bool) events.Event {
	return ev(events.AgentVerification, seq, events.AgentActor, events.AgentVerificationPayload{
		ProposalID: id, Recovered: recovered, Verdict: verdict, Repeat: repeat,
	})
}

func lastFixFold(evs ...events.Event) room.RoomSnapshot {
	s := room.NewSnapshot()
	for _, e := range evs {
		s = room.Apply(s, e)
	}
	return s
}

// The room list reads last_fix, because an executed proposal has left
// open_proposals (docs/api.md, KAN-594).
func TestLastFixFold(t *testing.T) {
	t.Run("absent until a run ends", func(t *testing.T) {
		s := lastFixFold(lastFixCreated(1, "p_1", 2), lastFixRan(2, "p_1", 0, true))
		if s.LastFix != nil {
			t.Fatalf("last_fix set while the run goes on: %+v", s.LastFix)
		}
		if len(s.OpenProposals) != 1 || s.OpenProposals[0].Seq != 1 {
			t.Fatalf("open proposal seq: %+v", s.OpenProposals)
		}
		b, err := json.Marshal(s)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(b), "last_fix") {
			t.Fatalf("last_fix not omitted: %s", b)
		}
	})

	t.Run("the last step ran", func(t *testing.T) {
		s := lastFixFold(lastFixCreated(4, "p_1", 2), lastFixRan(5, "p_1", 0, true), lastFixRan(6, "p_1", 1, true))
		want := room.LastFix{ProposalID: "p_1", Seq: 4, Steps: 2}
		if s.LastFix == nil || *s.LastFix != want {
			t.Fatalf("last_fix = %+v, want %+v", s.LastFix, want)
		}
	})

	t.Run("a failed step names the step", func(t *testing.T) {
		s := lastFixFold(lastFixCreated(1, "p_1", 3), lastFixRan(2, "p_1", 0, true), lastFixRan(3, "p_1", 1, false))
		if s.LastFix == nil || s.LastFix.FailedStep != 2 || s.LastFix.Steps != 3 {
			t.Fatalf("last_fix = %+v", s.LastFix)
		}
	})

	t.Run("an unknown outcome counts as failed", func(t *testing.T) {
		s := lastFixFold(lastFixCreated(1, "p_1", 2), ev(events.ActionExecuted, 2, events.AgentActor, events.ActionExecutedPayload{
			ProposalID: "p_1", StepIndex: 0, OutcomeUnknown: true,
		}))
		if s.LastFix == nil || s.LastFix.FailedStep != 1 {
			t.Fatalf("last_fix = %+v", s.LastFix)
		}
	})

	t.Run("an action of a proposal that is not open changes nothing", func(t *testing.T) {
		s := lastFixFold(lastFixCreated(1, "p_1", 1), lastFixRan(2, "p_1", 0, true), lastFixRan(3, "p_1", 0, false), lastFixRan(4, "p_x", 0, false))
		want := room.LastFix{ProposalID: "p_1", Seq: 1, Steps: 1}
		if s.LastFix == nil || *s.LastFix != want {
			t.Fatalf("last_fix = %+v, want %+v", s.LastFix, want)
		}
	})

	t.Run("the verdict lands on the fix", func(t *testing.T) {
		s := lastFixFold(lastFixCreated(1, "p_1", 1), lastFixRan(2, "p_1", 0, true), lastFixVerdict(3, "p_1", true, "recovered: ok", false))
		if v := s.LastFix.Verification; v == nil || !v.Recovered || v.Verdict != "recovered: ok" {
			t.Fatalf("verification = %+v", v)
		}
		b, err := json.Marshal(s.LastFix)
		if err != nil {
			t.Fatal(err)
		}
		want := `{"proposal_id":"p_1","seq":1,"steps":1,"failed_step":0,"verification":{"recovered":true,"verdict":"recovered: ok"}}`
		if string(b) != want {
			t.Fatalf("json = %s, want %s", b, want)
		}
	})

	t.Run("a repeat never overwrites a full verdict", func(t *testing.T) {
		s := lastFixFold(
			lastFixCreated(1, "p_1", 1), lastFixRan(2, "p_1", 0, true),
			lastFixVerdict(3, "p_1", false, "NOT recovered: x", false),
			lastFixVerdict(4, "p_1", true, "already verified", true),
		)
		if v := s.LastFix.Verification; v.Recovered || v.Verdict != "NOT recovered: x" {
			t.Fatalf("verification = %+v", v)
		}
		// A repeat is kept when nothing better exists, and a full verdict replaces it.
		s = lastFixFold(lastFixCreated(1, "p_1", 1), lastFixRan(2, "p_1", 0, true), lastFixVerdict(3, "p_1", true, "already verified", true))
		if v := s.LastFix.Verification; v == nil || v.Verdict != "already verified" {
			t.Fatalf("verification = %+v", v)
		}
		s = room.Apply(s, lastFixVerdict(4, "p_1", false, "NOT recovered: y", false))
		if v := s.LastFix.Verification; v.Recovered || v.Verdict != "NOT recovered: y" {
			t.Fatalf("verification = %+v", v)
		}
	})

	t.Run("the verdict of another proposal is ignored", func(t *testing.T) {
		s := lastFixFold(lastFixCreated(1, "p_1", 1), lastFixRan(2, "p_1", 0, true), lastFixVerdict(3, "p_x", true, "recovered: x", false))
		if s.LastFix.Verification != nil {
			t.Fatalf("verification = %+v", s.LastFix.Verification)
		}
	})

	t.Run("a newer fix replaces it; an older one finishing later does not", func(t *testing.T) {
		s := lastFixFold(
			lastFixCreated(1, "p_old", 1), lastFixCreated(2, "p_new", 1),
			lastFixRan(3, "p_new", 0, true), lastFixVerdict(4, "p_new", true, "recovered: new", false),
			lastFixRan(5, "p_old", 0, false), lastFixVerdict(6, "p_old", false, "NOT recovered: old", false),
		)
		if s.LastFix.ProposalID != "p_new" || s.LastFix.FailedStep != 0 || s.LastFix.Verification.Verdict != "recovered: new" {
			t.Fatalf("last_fix = %+v", s.LastFix)
		}
		s = lastFixFold(lastFixCreated(1, "p_1", 1), lastFixRan(2, "p_1", 0, true), lastFixVerdict(3, "p_1", true, "recovered: 1", false),
			lastFixCreated(4, "p_2", 1), lastFixRan(5, "p_2", 0, false))
		if s.LastFix.ProposalID != "p_2" || s.LastFix.Seq != 4 || s.LastFix.FailedStep != 1 || s.LastFix.Verification != nil {
			t.Fatalf("last_fix = %+v", s.LastFix)
		}
	})

	t.Run("open proposals carry the seq of their proposal.created", func(t *testing.T) {
		s := lastFixFold(lastFixCreated(3, "p_1", 1), lastFixRan(4, "p_1", 0, true), lastFixCreated(9, "p_2", 1))
		b, err := json.Marshal(s.OpenProposals[0])
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(b), `"seq":9`) {
			t.Fatalf("open proposal: %s", b)
		}
		if s.LastFix.Seq != 3 {
			t.Fatalf("last_fix = %+v", s.LastFix)
		}
	})
}

func TestLastFixStaysPureAndClones(t *testing.T) {
	before := lastFixFold(lastFixCreated(1, "p_1", 1), lastFixRan(2, "p_1", 0, true), lastFixVerdict(3, "p_1", true, "recovered: a", false))

	// Apply is pure: a later verdict leaves the earlier snapshot as it was.
	after := room.Apply(before, lastFixVerdict(4, "p_1", false, "NOT recovered: b", false))
	if before.LastFix.Verification.Verdict != "recovered: a" || after.LastFix.Verification.Verdict != "NOT recovered: b" {
		t.Fatalf("before = %+v, after = %+v", before.LastFix.Verification, after.LastFix.Verification)
	}

	// Clone is deep: a change to the copy stays there.
	c := before.Clone()
	c.LastFix.Steps = 99
	c.LastFix.Verification.Verdict = "changed"
	if before.LastFix.Steps != 1 || before.LastFix.Verification.Verdict != "recovered: a" {
		t.Fatalf("clone shares last_fix: %+v %+v", before.LastFix, before.LastFix.Verification)
	}
}
