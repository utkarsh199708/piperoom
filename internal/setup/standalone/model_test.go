package standalone

// The model host's spend plumbing (KAN-525): the hand-off's usage events reach
// the room log, and a ledger failure names its origin.

import (
	"context"
	"errors"
	"strings"
	"testing"

	"piperoom/internal/agent"
	"piperoom/internal/events"
	"piperoom/internal/room"
	"piperoom/internal/store"
	"piperoom/internal/store/memstore"
)

// TestUsageRelayRecordsHandoffUsage: an agent.usage event the endpoint reports
// outside a run (the hand-off summary) is appended to the room's log once the
// relay has its service, and is dropped without a panic before that.
func TestUsageRelayRecordsHandoffUsage(t *testing.T) {
	ctx := context.Background()
	relay := &usageRelay{log: discardLog()}
	// The start endpoint is built before the room service exists: an event in
	// that window has nowhere to go and must not crash the server.
	relay.emit(ctx, "r_none", events.AgentUsagePayload{Model: "m", Role: events.UsageRoleHandoff})

	svc := room.New(room.Deps{Store: memstore.New(), Run: noAgent, Logger: discardLog()})
	defer svc.Close()
	snap, err := svc.CreateRoom(ctx, room.User{ID: "alice"}, "orders lag", "sev2", "orders")
	if err != nil {
		t.Fatalf("CreateRoom: %v", err)
	}
	relay.set(svc)

	cost := 0.0042
	want := events.AgentUsagePayload{Model: "qwen3:32b", Role: events.UsageRoleHandoff, InputTokens: 120, OutputTokens: 30,
		LatencyMS: 7, CostUSD: &cost, Provider: "openai"}
	relay.emit(ctx, snap.ID, want)

	evs, _, err := svc.Events(ctx, snap.ID, 0, 500)
	if err != nil {
		t.Fatalf("Events: %v", err)
	}
	var got []events.AgentUsagePayload
	for _, e := range evs {
		if e.Type != events.AgentUsage {
			continue
		}
		p, err := events.Decode[events.AgentUsagePayload](e)
		if err != nil {
			t.Fatalf("decode agent.usage: %v", err)
		}
		got = append(got, p)
	}
	if len(got) != 1 {
		t.Fatalf("room log has %d agent.usage events, want exactly the hand-off's one: %+v", len(got), got)
	}
	g := got[0]
	if g.Model != want.Model || g.Role != want.Role || g.InputTokens != want.InputTokens || g.OutputTokens != want.OutputTokens ||
		g.LatencyMS != want.LatencyMS || g.Provider != want.Provider || g.CostUSD == nil || *g.CostUSD != cost {
		t.Fatalf("agent.usage in the room log = %+v, want %+v", g, want)
	}

	// An unknown room is logged and swallowed: the hand-off must go on.
	relay.emit(ctx, "r_missing", want)
}

// TestSpendLedgerWrapsStoreError: the ledger adapter keeps the store's error
// in the chain (errors.Is) and names the step, rule 3 of AGENTS.md.
func TestSpendLedgerWrapsStoreError(t *testing.T) {
	l := standaloneSpendLedger{st: memstore.New()}
	_, err := l.AddSpend(context.Background(), agent.SpendChange{RoomID: "r_missing", PipelineID: "orders", Day: "2026-10-25", Tokens: 10})
	if !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("AddSpend on an unknown room: %v, want store.ErrNotFound in the chain", err)
	}
	if !strings.HasPrefix(err.Error(), "setup: record model spend: ") {
		t.Fatalf("AddSpend error %q does not name the step", err)
	}
}
