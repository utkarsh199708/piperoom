//go:build claude

package agent

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/anthropics/anthropic-sdk-go"

	"piperoom/internal/events"
)

// The judge's tokens count against the room's budget, but an exhausted budget
// never refuses it: a verdict is one bounded call, like the hand-off itself.
func TestEndpointJudgeHandoffIsCountedButNeverRefused(t *testing.T) {
	verdict := `{\"contradictions\":[]}`
	turn := mustMessage(t, `{"stop_reason":"end_turn","content":[{"type":"text","text":"`+verdict+`"}],"usage":{"input_tokens":40,"output_tokens":10}}`)
	scripted := &scriptedModel{turns: []anthropic.BetaMessage{turn, turn}}
	ep := testEndpoint(scripted, 100)
	b := ep.budget("room-a")
	b.record(usageTokens{input: 500}, true) // the loop is already over budget

	evs := []events.Event{{RoomID: "room-a", Seq: 1, Type: events.AgentMessage, Payload: json.RawMessage(`{"text":"hi"}`)}}
	v, err := ep.JudgeHandoff(context.Background(), evs, events.HandoffSummary{WhatBroke: "x", Why: "y"}, Options{})
	if err != nil {
		t.Fatalf("JudgeHandoff over budget: %v", err)
	}
	if !v.OK() {
		t.Fatalf("verdict = %+v", v)
	}
	if used := b.Used(); used != 550 {
		t.Fatalf("used = %d, want 550: the judge must be counted", used)
	}
	if _, err := ep.JudgeHandoff(context.Background(), nil, events.HandoffSummary{}, Options{}); err == nil {
		t.Fatal("an empty room must be an error")
	}
}
