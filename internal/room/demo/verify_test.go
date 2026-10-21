package demo

// The scripted agent must verify a fix the way the real agent does: one
// agent.verification after the last step succeeded, and none when the fix did
// not fully run. Without it the room stays at "Fix ran · verifying" forever
// (KAN-389).

import (
	"context"
	"strings"
	"testing"

	"piperoom/internal/agent"
	"piperoom/internal/events"
)

// sinkEmit adapts the test sink to the verifier's emitter.
func sinkEmit(sink *recordingSink) func(events.Type, any) error {
	return func(typ events.Type, payload any) error {
		return sink.Emit(context.Background(), typ, payload)
	}
}

// actionResult builds the input the room delivers for one executed step.
func actionResult(proposalID string, step int, ok *bool) agent.HumanInput {
	idx := step
	return agent.HumanInput{
		Kind:       "action_result",
		ProposalID: proposalID,
		StepIndex:  &idx,
		OK:         ok,
		Text:       "step",
	}
}

func ok(v bool) *bool { return &v }

func verifications(sink *recordingSink) []events.AgentVerificationPayload {
	var out []events.AgentVerificationPayload
	sink.mu.Lock()
	defer sink.mu.Unlock()
	for _, p := range sink.pays {
		if v, is := p.(events.AgentVerificationPayload); is {
			out = append(out, v)
		}
	}
	return out
}

func lastEventType(sink *recordingSink) events.Type {
	sink.mu.Lock()
	defer sink.mu.Unlock()
	if len(sink.types) == 0 {
		return ""
	}
	return sink.types[len(sink.types)-1]
}

func TestFixVerifierWritesOneRecoveredVerdictAfterEveryStep(t *testing.T) {
	cases := []struct {
		name   string
		inputs []agent.HumanInput
		want   int
	}{
		{
			name:   "both steps succeed",
			inputs: []agent.HumanInput{actionResult("p_1", 0, ok(true)), actionResult("p_1", 1, ok(true))},
			want:   1,
		},
		{
			name:   "duplicate last-step result verifies once",
			inputs: []agent.HumanInput{actionResult("p_1", 0, ok(true)), actionResult("p_1", 1, ok(true)), actionResult("p_1", 1, ok(true))},
			want:   1,
		},
		{
			name:   "only the first step ran",
			inputs: []agent.HumanInput{actionResult("p_1", 0, ok(true))},
			want:   0,
		},
		{
			name:   "only the last step's result arrived",
			inputs: []agent.HumanInput{actionResult("p_1", 1, ok(true))},
			want:   0,
		},
		{
			name:   "a step failed",
			inputs: []agent.HumanInput{actionResult("p_1", 0, ok(true)), actionResult("p_1", 1, ok(false))},
			want:   0,
		},
		{
			name:   "a step outcome is unknown",
			inputs: []agent.HumanInput{actionResult("p_1", 0, ok(true)), actionResult("p_1", 1, nil)},
			want:   0,
		},
		{
			name:   "a result of another proposal",
			inputs: []agent.HumanInput{actionResult("p_other", 0, ok(true)), actionResult("p_other", 1, ok(true))},
			want:   0,
		},
		{
			name:   "a step index outside the proposal",
			inputs: []agent.HumanInput{actionResult("p_1", 0, ok(true)), actionResult("p_1", 2, ok(true))},
			want:   0,
		},
		{
			name:   "no action results at all (rejected before running)",
			inputs: nil,
			want:   0,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sink := &recordingSink{}
			v := newFixVerifier("p_1", 2, func() bool { return true }, sinkEmit(sink))
			for _, in := range tc.inputs {
				v.observe(in)
			}
			got := verifications(sink)
			if len(got) != tc.want {
				t.Fatalf("verifications = %d, want %d (events %v)", len(got), tc.want, sink.types)
			}
			if tc.want == 0 {
				return
			}
			p := got[0]
			if p.ProposalID != "p_1" || !p.Recovered || !strings.HasPrefix(p.Verdict, "recovered:") {
				t.Fatalf("verdict = %+v, want a recovered verdict for p_1", p)
			}
			if len(p.Checks) != 2 {
				t.Fatalf("checks = %+v, want the schema-registry and kafka-connect checks", p.Checks)
			}
			for _, c := range p.Checks {
				if !c.OK || c.Detail == "" || c.Name == "" {
					t.Fatalf("check = %+v, want a passing check with a name and detail", c)
				}
			}
			if p.DataFlow == nil || !p.DataFlow.Checked || !p.DataFlow.Fresh || len(p.DataFlow.Sinks) == 0 {
				t.Fatalf("data flow = %+v, want checked, fresh and a sink", p.DataFlow)
			}
			// The parting message comes after the verdict and, like every other
			// scripted message, never names a proposal id (F5b).
			if lastEventType(sink) != events.AgentMessage {
				t.Fatalf("last event = %s, want the recovery message after the verdict", lastEventType(sink))
			}
			sink.mu.Lock()
			text := sink.pays[len(sink.pays)-1].(events.AgentMessagePayload).Text
			sink.mu.Unlock()
			if !strings.Contains(text, "Verification passed") || strings.Contains(text, "p_1") {
				t.Fatalf("recovery message = %q", text)
			}
		})
	}
}

func TestFixVerifierPausedByClosedRoomWritesNoVerdict(t *testing.T) {
	// The pause is the window between the last step and the re-check. If the
	// room closes in that window there is nothing to report to.
	sink := &recordingSink{}
	v := newFixVerifier("p_1", 2, func() bool { return false }, sinkEmit(sink))
	v.observe(actionResult("p_1", 0, ok(true)))
	v.observe(actionResult("p_1", 1, ok(true)))
	if got := verifications(sink); len(got) != 0 {
		t.Fatalf("verifications = %d after a closed room, want 0", len(got))
	}
}
