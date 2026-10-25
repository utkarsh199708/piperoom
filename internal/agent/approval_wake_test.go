package agent

import (
	"context"
	"strings"
	"testing"

	"piperoom/internal/events"
)

// ---- approvals never start a model turn on their own ----
//
// A live schema-change room spent two Opus calls per approval.given, saying
// only what the approval bar already showed. These tests pin that an approval
// is held for the next batch, and that what follows it (the executor's step
// results, a rejection, a steer) still wakes the agent.

// idleApprovalRunner is a compat runner (proposal p_1, layers schema-registry
// and kafka-connect) whose inbox delivers each batch in turn while the agent is
// idle, then reports the room closed.
func idleApprovalRunner(t *testing.T, batches ...[]HumanInput) (*runner, *scriptedModel, *fakeSink) {
	t.Helper()
	inbox := &pushInbox{}
	for _, b := range batches {
		inbox.steps = append(inbox.steps, inbox.deliver(b...))
	}
	model := &scriptedModel{}
	r, _, sink := waitRunner(t, model, inbox)
	return r, model, sink
}

func TestApprovalDoesNotStartModelTurn(t *testing.T) {
	// The live case: kafka-connect approves while the agent is idle and
	// schema-registry is still pending (twice, as a re-sent event would, and
	// one for a proposal the agent did not create). Only the opening turn may
	// call the model.
	r, model, sink := idleApprovalRunner(t,
		[]HumanInput{approvalInput("kafka-connect", "p_1")},
		[]HumanInput{approvalInput("kafka-connect", "p_1"), approvalInput("kafka-connect", "p_driver")},
	)
	_ = r.run(context.Background())
	if len(model.calls) != 1 {
		t.Fatalf("model calls = %d, want 1: an approval alone must not start a turn", len(model.calls))
	}
	st := statusList(sink)
	// The opening turn reports working, then idle; an approval must add nothing
	// after that idle (no "working" for a turn that never comes).
	if len(st) == 0 || st[len(st)-1] != "idle/" || strings.Count(strings.Join(st, ","), "idle/") != 1 {
		t.Fatalf("the agent must stay idle through the approvals, statuses %v", st)
	}
}

func TestActionResultAfterApprovalsWakesOnceWithBoth(t *testing.T) {
	// The final approval triggers execution. It wakes the agent into its wait
	// for step results, with no model call; the results end the wait, and the
	// one turn carries the held approval, the final one and the results, in
	// that order, each once.
	r, model, sink := idleApprovalRunner(t,
		[]HumanInput{approvalInput("kafka-connect", "p_1")},
		[]HumanInput{approvalInput("schema-registry", "p_1")},
		bothResults("p_1"),
	)
	_ = r.run(context.Background())
	if len(model.calls) != 2 {
		t.Fatalf("model calls = %d, want 2: the opening turn and one turn for the step results", len(model.calls))
	}
	text := lastUserText(t, model.calls[1])
	ia := strings.Index(text, `layer "kafka-connect" approved proposal p_1`)
	ib := strings.Index(text, `layer "schema-registry" approved proposal p_1`)
	is := strings.Index(text, "proposal p_1 step 1:")
	if ia < 0 || ib < 0 || is < 0 || ia > is || ib > is {
		t.Fatalf("the results turn must carry both approvals before the step results:\n%s", text)
	}
	if strings.Count(text, "[approval result]") != 2 {
		t.Fatalf("each approval must reach the model exactly once:\n%s", text)
	}
	if !strings.Contains(text, verifyTag+"recovered:") {
		t.Fatalf("verification must still run on the results:\n%s", text)
	}
	if noteLine(text) != "" {
		t.Fatalf("the results arrived, so no pending note is expected:\n%s", text)
	}
	if !containsString(statusList(sink), "working/"+waitingDetailResults) {
		t.Fatalf("the final approval must wake the agent into its wait for step results, statuses %v", statusList(sink))
	}
}

func TestRejectionWakesAgent(t *testing.T) {
	// A rejection may make the agent re-plan: it gets a turn at once, and an
	// approval held before it rides along.
	r, model, _ := idleApprovalRunner(t,
		[]HumanInput{approvalInput("kafka-connect", "p_1")},
		[]HumanInput{rejectionInput("schema-registry", "p_1")},
	)
	_ = r.run(context.Background())
	if len(model.calls) != 2 {
		t.Fatalf("model calls = %d, want 2: the rejection must wake the agent (and the approval must not)", len(model.calls))
	}
	text := lastUserText(t, model.calls[1])
	if !strings.Contains(text, `layer "schema-registry" rejected proposal p_1`) || !strings.Contains(text, `layer "kafka-connect" approved proposal p_1`) {
		t.Fatalf("the rejection turn must carry the rejection and the held approval:\n%s", text)
	}
}

func TestSteerWakesAgentAfterHeldApproval(t *testing.T) {
	steer := HumanInput{Kind: "steer", Actor: events.Actor{Name: "Ana"}, Text: "check the slot"}
	r, model, _ := idleApprovalRunner(t,
		[]HumanInput{approvalInput("kafka-connect", "p_1")},
		[]HumanInput{steer},
	)
	_ = r.run(context.Background())
	if len(model.calls) != 2 {
		t.Fatalf("model calls = %d, want 2: only the steer may wake the agent", len(model.calls))
	}
	text := lastUserText(t, model.calls[1])
	ia, is := strings.Index(text, `layer "kafka-connect" approved proposal p_1`), strings.Index(text, "check the slot")
	if ia < 0 || is < 0 || ia > is {
		t.Fatalf("the steer turn must carry the held approval before the steer:\n%s", text)
	}
}

func TestIsQuietApprovalVerdicts(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   HumanInput
		want bool
	}{
		{"approval", approvalInput("kafka-connect", "p_1"), true},
		{"rejection", rejectionInput("kafka-connect", "p_1"), false},
		{"no verdict", HumanInput{Kind: "approval_result", Text: "layer approved"}, false},
		{"alert update", HumanInput{Kind: "alert_update"}, true},
		{"action result", actionInput("proposal p_1 step 0: ok", okPtr(true)), false},
		{"steer", steerInput(), false},
		{"comment", HumanInput{Kind: "comment", Text: "@agent?"}, false},
	} {
		if got := isQuiet(tc.in); got != tc.want {
			t.Errorf("%s: isQuiet = %v, want %v", tc.name, got, tc.want)
		}
	}
}
