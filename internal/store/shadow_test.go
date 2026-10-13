package store_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"piperoom/internal/events"
	"piperoom/internal/store"
)

// AppendShadow refuses unstorable content in its pre-check, before any
// database work: a zero *PG has no pool, so only CheckAppend can answer. (On a
// real database Postgres would refuse it too and roll the seq back, so the
// integration test alone cannot tell the pre-check is there.)
func TestPGAppendShadowPreCheck(t *testing.T) {
	var s store.PG
	agent := events.Actor{Kind: events.ActorAgent, ID: "agent", Name: "Agent"}
	for name, payload := range map[string]any{
		"raw NUL":        events.AgentMessagePayload{Text: "a\x00b"},
		"NUL escape":     json.RawMessage(`{"text":"a\u0000b"}`),
		"lone surrogate": json.RawMessage(`{"text":"\ud800"}`),
	} {
		t.Run(name, func(t *testing.T) {
			// Past the pre-check a zero *PG panics (no clock, no pool); say
			// what that means instead of showing a nil-pointer trace.
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("pre-check missing: AppendShadow went past CheckAppend to the database (%v)", r)
				}
			}()
			_, err := s.AppendShadow(context.Background(), "r1", events.AgentMessage, agent, payload)
			if !errors.Is(err, store.ErrUnstorable) {
				t.Fatalf("err = %v, want ErrUnstorable from the pre-check", err)
			}
		})
	}
}
