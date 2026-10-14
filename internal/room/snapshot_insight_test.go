package room_test

import (
	"encoding/json"
	"testing"
	"time"

	"piperoom/internal/events"
	"piperoom/internal/room"
)

func insightEv(typ events.Type, seq int64, at time.Time, payload any) events.Event {
	raw, _ := json.Marshal(payload)
	return events.Event{RoomID: "r_1", Seq: seq, Type: typ, Actor: events.AgentActor, Payload: raw, At: at}
}

func f64(v float64) *float64 { return &v }

func TestInsightFold(t *testing.T) {
	t0 := time.Date(2026, 10, 14, 10, 0, 0, 0, time.UTC)
	open := func(shadow bool) room.RoomSnapshot {
		return room.Apply(room.NewSnapshot(), insightEv(events.RoomCreated, 1, t0, events.RoomCreatedPayload{Title: "t", PipelineID: "orders", Shadow: shadow}))
	}
	usage := func(cost *float64) events.AgentUsagePayload {
		return events.AgentUsagePayload{Model: "m", Role: "main", InputTokens: 100, CacheCreationInputTokens: 10, CacheReadInputTokens: 20, OutputTokens: 5, LatencyMS: 700, CostUSD: cost}
	}
	t.Run("new room", func(t *testing.T) {
		in := open(false).Insight
		if in.TimeToFirstHypothesisS != -1 || in.FirstHypothesisAt != nil || in.Usage.CostUSD != nil {
			t.Fatalf("bad empty insight: %+v", in)
		}
	})
	t.Run("counters", func(t *testing.T) {
		s := open(false)
		s = room.Apply(s, insightEv(events.AgentStepStart, 2, t0, events.AgentStepStartPayload{}))
		s = room.Apply(s, insightEv(events.AgentToolCall, 3, t0, events.AgentToolCallPayload{}))
		s = room.Apply(s, insightEv(events.AgentToolResult, 4, t0, events.AgentToolResultPayload{IsError: true}))
		s = room.Apply(s, insightEv(events.AgentToolResult, 5, t0, events.AgentToolResultPayload{}))
		if in := s.Insight; in.Steps != 1 || in.ToolCalls != 1 || in.ToolErrors != 1 {
			t.Fatalf("bad counters: %+v", in)
		}
	})
	t.Run("hypothesis keeps first, floors seconds, last confidence", func(t *testing.T) {
		s := open(false)
		s = room.Apply(s, insightEv(events.AgentHypothesis, 2, t0.Add(48900*time.Millisecond), events.AgentHypothesisPayload{Confidence: "medium"}))
		s = room.Apply(s, insightEv(events.AgentHypothesis, 3, t0.Add(5*time.Minute), events.AgentHypothesisPayload{Confidence: "high"}))
		in := s.Insight
		if in.TimeToFirstHypothesisS != 48 || in.FinalConfidence != "high" || in.FirstHypothesisAt == nil || !in.FirstHypothesisAt.Equal(t0.Add(48900*time.Millisecond)) {
			t.Fatalf("bad insight: %+v", in)
		}
	})
	t.Run("hypothesis before opened_at is zero seconds", func(t *testing.T) {
		s := room.Apply(open(false), insightEv(events.AgentHypothesis, 2, t0.Add(-time.Second), events.AgentHypothesisPayload{Confidence: "low"}))
		if s.Insight.TimeToFirstHypothesisS != 0 {
			t.Fatalf("want 0, got %d", s.Insight.TimeToFirstHypothesisS)
		}
	})
	t.Run("usage priced, unpriced, lower bound", func(t *testing.T) {
		s := open(false)
		s = room.Apply(s, insightEv(events.AgentUsage, 2, t0, usage(nil)))
		if u := s.Insight.Usage; u.Calls != 1 || u.UnpricedCalls != 1 || u.CostUSD != nil || u.Tokens != 135 || u.LatencyMSTotal != 700 {
			t.Fatalf("unpriced only: %+v", u)
		}
		s = room.Apply(s, insightEv(events.AgentUsage, 3, t0, usage(f64(0.25))))
		before := s
		s = room.Apply(s, insightEv(events.AgentUsage, 4, t0, usage(f64(0.17))))
		u := s.Insight.Usage
		if u.Calls != 3 || u.UnpricedCalls != 1 || u.CostUSD == nil || *u.CostUSD < 0.419 || *u.CostUSD > 0.421 || u.Tokens != 405 {
			t.Fatalf("lower bound: %+v", u)
		}
		if *before.Insight.Usage.CostUSD != 0.25 {
			t.Fatalf("Apply mutated its input: %v", *before.Insight.Usage.CostUSD)
		}
	})
	t.Run("shadow room stays zero", func(t *testing.T) {
		s := open(true)
		s = room.Apply(s, insightEv(events.AgentUsage, 2, t0, usage(f64(1))))
		s = room.Apply(s, insightEv(events.AgentHypothesis, 3, t0.Add(time.Minute), events.AgentHypothesisPayload{Confidence: "high"}))
		s = room.Apply(s, insightEv(events.AgentStepStart, 4, t0, events.AgentStepStartPayload{}))
		in := s.Insight
		if in.Usage.Calls != 0 || in.Steps != 0 || in.TimeToFirstHypothesisS != -1 || in.FinalConfidence != "" {
			t.Fatalf("shadow insight not zero: %+v", in)
		}
	})
	t.Run("json shape and clone", func(t *testing.T) {
		s := room.Apply(open(false), insightEv(events.AgentUsage, 2, t0, usage(f64(0.5))))
		c := s.Clone()
		*c.Insight.Usage.CostUSD = 9
		if *s.Insight.Usage.CostUSD != 0.5 {
			t.Fatal("Clone shares CostUSD")
		}
		b, _ := json.Marshal(open(false))
		var m map[string]map[string]any
		_ = json.Unmarshal(b, &m)
		if m["insight"]["time_to_first_hypothesis_s"].(float64) != -1 || m["insight"]["first_hypothesis_at"] != nil {
			t.Fatalf("bad json: %s", b)
		}
	})
}
