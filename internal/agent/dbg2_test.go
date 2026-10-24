package agent

import (
	"context"
	"encoding/json"
	"testing"

	"piperoom/internal/events"
)

func TestDbgReplanTurns(t *testing.T) {
	fr := &fakeSetupRunner{t: t}
	m := &recordedModel{t: t}
	sink := &fakeSink{}
	m.turns = []func(string) string{
		func(string) string {
			b, _ := json.Marshal(map[string]any{"stop_reason": "tool_use", "content": []map[string]any{planToolUse("tu_1", map[string]any{"name": "stack", "steps": []map[string]any{{"op": "compose_write", "params_hint": "x", "verify": "y"}}})}})
			return string(b)
		},
		func(string) string { return toolUseMsg("tu_2", "propose_fix", proposeInput("compose_write", `{"templates":["kafka-kraft"],"project":"piperoom-orders"}`, "render")) },
		func(string) string { return toolUseMsg("tu_3", "propose_fix", proposeInput("compose_write", `{"templates":["kafka-kraft"],"project":"piperoom-orders"}`, "render")) },
		func(string) string {
			b, _ := json.Marshal(map[string]any{"stop_reason": "tool_use", "content": []map[string]any{planToolUse("tu_4", map[string]any{"name": "stack", "steps": []map[string]any{{"op": "compose_write", "params_hint": "x2", "verify": "y"}}})}})
			return string(b)
		},
		func(string) string { return toolUseMsg("tu_5", "propose_fix", proposeInput("compose_write", `{"templates":["kafka-kraft","connect"],"project":"piperoom-orders"}`, "render2")) },
		func(string) string {
			b, _ := json.Marshal(map[string]any{"stop_reason": "end_turn", "content": []map[string]any{{"type": "text", "text": "done."}}})
			return string(b)
		},
	}
	step0 := 0
	inbox := &fakeInbox{drains: [][]HumanInput{
		nil,
		nil,
		{actionInput("proposal p_1 step 0: compose_write failed: refused", okPtr(false))},
		nil,
		nil,
		{actionInput("proposal p_2 step 0: compose_write executed: wrote compose.yaml", okPtr(true))},
		{HumanInput{Kind: "action_result", ProposalID: "p_2", StepIndex: &step0, OK: okPtr(true), Text: "dup"}},
	}, waitErr: context.Canceled}
	props := &planGatedProposals{sink: sink}
	r := newSetupRunner(t, fr, m, inbox, sink, props, setupActions(t))
	err := r.run(context.Background())
	t.Logf("run err: %v", err)
	t.Logf("turns: %d", len(m.calls))
	_ = events.AgentMessagePayload{}
}
