package agent

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/anthropics/anthropic-sdk-go"

	"piperoom/internal/events"
)

type handoffModel struct {
	msg anthropic.BetaMessage
}

func (m *handoffModel) Turn(_ context.Context, _ anthropic.BetaMessageNewParams) (anthropic.BetaMessage, error) {
	return m.msg, nil
}

func TestWriteHandoff(t *testing.T) {
	summaryJSON := `{"what_broke":"warehouse stopped updating","why":"amount type changed int->decimal","done":["found schema error"],"ruled_out":["kafka lag"],"open":["await approval"],"risks":["slot WAL growth"]}`
	m := &handoffModel{msg: anthropic.BetaMessage{
		StopReason: anthropic.BetaStopReasonEndTurn,
		Content: []anthropic.BetaContentBlockUnion{
			{Type: "text", Text: summaryJSON},
		},
	}}

	roomEvents := []events.Event{
		{Type: events.RoomCreated, Actor: events.Actor{Name: "system"}, Payload: json.RawMessage(`{"title":"orders stale"}`)},
		{Type: events.AgentHypothesis, Actor: events.Actor{Name: "Agent"}, Payload: json.RawMessage(`{"text":"amount changed","confirmed":false}`)},
	}

	got, err := writeHandoff(context.Background(), roomEvents, Options{}, m)
	if err != nil {
		t.Fatalf("writeHandoff: %v", err)
	}
	if got.WhatBroke != "warehouse stopped updating" {
		t.Fatalf("what_broke = %q", got.WhatBroke)
	}
	if got.Why != "amount type changed int->decimal" {
		t.Fatalf("why = %q", got.Why)
	}
	if len(got.Done) != 1 || got.Done[0] != "found schema error" {
		t.Fatalf("done = %v", got.Done)
	}
}

func TestWriteHandoffEmptyRoom(t *testing.T) {
	m := &handoffModel{}
	_, err := writeHandoff(context.Background(), nil, Options{}, m)
	if err == nil {
		t.Fatal("expected error for empty room")
	}
}

func TestWriteHandoffInvalidJSON(t *testing.T) {
	m := &handoffModel{msg: anthropic.BetaMessage{
		StopReason: anthropic.BetaStopReasonEndTurn,
		Content:    []anthropic.BetaContentBlockUnion{{Type: "text", Text: "not json"}},
	}}
	_, err := writeHandoff(context.Background(), []events.Event{{Type: events.RoomCreated}}, Options{}, m)
	if err == nil {
		t.Fatal("expected error for non-JSON output")
	}
}

func TestWriteHandoffMissingField(t *testing.T) {
	m := &handoffModel{msg: anthropic.BetaMessage{
		StopReason: anthropic.BetaStopReasonEndTurn,
		Content:    []anthropic.BetaContentBlockUnion{{Type: "text", Text: `{"what_broke":"","why":"","done":[],"ruled_out":[],"open":[],"risks":[]}`}},
	}}
	_, err := writeHandoff(context.Background(), []events.Event{{Type: events.RoomCreated}}, Options{}, m)
	if err == nil {
		t.Fatal("expected error when what_broke is empty")
	}
}
