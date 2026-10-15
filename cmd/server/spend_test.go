package main

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"piperoom/internal/agent"
	"piperoom/internal/auth"
	"piperoom/internal/config"
	"piperoom/internal/events"
	"piperoom/internal/room"
	"piperoom/internal/store"
	"piperoom/internal/store/memstore"
)

func writePrices(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "prices.json")
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestSpendSetup(t *testing.T) {
	prices := writePrices(t, `{"claude-opus-5-5":{"input":5,"output":25,"cache_write":6.25,"cache_read":0.5},
		"claude-haiku-5-5":{"input":1,"output":5,"cache_write":1.25,"cache_read":0.1}}`)
	base := config.Config{Model: "claude-opus-5-5"}

	t.Run("defaults", func(t *testing.T) {
		s, err := spendSetup(base)
		if err != nil {
			t.Fatal(err)
		}
		if s.roomLimit != agent.DefaultRoomTokenBudget || s.cost != (config.Cost{}) || s.prices != nil {
			t.Fatalf("defaults = %+v", s)
		}
	})
	t.Run("everything set", func(t *testing.T) {
		c := base
		c.TriageModel = "claude-haiku-5-5"
		c.ModelPrices, c.PipelineDailyTokenBudget, c.PipelineDailyCostUSD, c.RoomTokenBudget = prices, "5000000", "12.50", "0"
		s, err := spendSetup(c)
		if err != nil {
			t.Fatal(err)
		}
		if s.roomLimit != 0 || s.cost.PipelineDailyTokens != 5_000_000 || s.cost.PipelineDailyCostUSD != 12.5 || !s.prices.Has("claude-haiku-5-5") {
			t.Fatalf("settings = %+v", s)
		}
	})

	bad := map[string]struct {
		edit func(*config.Config)
		want string
	}{
		"cost with an exponent":       {func(c *config.Config) { c.PipelineDailyCostUSD, c.ModelPrices = "1e3", prices }, "PIPEROOM_PIPELINE_DAILY_COST_USD"},
		"negative cost":               {func(c *config.Config) { c.PipelineDailyCostUSD, c.ModelPrices = "-1", prices }, "PIPEROOM_PIPELINE_DAILY_COST_USD"},
		"cost without digits":         {func(c *config.Config) { c.PipelineDailyCostUSD, c.ModelPrices = ".5", prices }, "PIPEROOM_PIPELINE_DAILY_COST_USD"},
		"cost with a dangling .":      {func(c *config.Config) { c.PipelineDailyCostUSD, c.ModelPrices = "5.", prices }, "PIPEROOM_PIPELINE_DAILY_COST_USD"},
		"cost finer than a millionth": {func(c *config.Config) { c.PipelineDailyCostUSD, c.ModelPrices = "1.0000001", prices }, "decimal places"},
		"cost too large":              {func(c *config.Config) { c.PipelineDailyCostUSD, c.ModelPrices = "2000000000", prices }, "too large"},
		"cost without prices":         {func(c *config.Config) { c.PipelineDailyCostUSD = "10" }, "needs PIPEROOM_MODEL_PRICES"},
		"bad daily tokens":            {func(c *config.Config) { c.PipelineDailyTokenBudget = "lots" }, "PIPEROOM_PIPELINE_DAILY_TOKEN_BUDGET"},
		"negative daily tokens":       {func(c *config.Config) { c.PipelineDailyTokenBudget = "-5" }, "PIPEROOM_PIPELINE_DAILY_TOKEN_BUDGET"},
		"bad room budget":             {func(c *config.Config) { c.RoomTokenBudget = "2M" }, "PIPEROOM_ROOM_TOKEN_BUDGET"},
		"missing prices file":         {func(c *config.Config) { c.ModelPrices = filepath.Join(t.TempDir(), "none.json") }, "PIPEROOM_MODEL_PRICES"},
		"broken prices file":          {func(c *config.Config) { c.ModelPrices = writePrices(t, `{"m":{"input":1}}`) }, "PIPEROOM_MODEL_PRICES"},
		"unpriced main model":         {func(c *config.Config) { c.Model, c.PipelineDailyCostUSD, c.ModelPrices = "other", "10", prices }, `PIPEROOM_MODEL="other"`},
		"unpriced triage model":       {func(c *config.Config) { c.TriageModel, c.PipelineDailyCostUSD, c.ModelPrices = "tri", "10", prices }, `PIPEROOM_TRIAGE_MODEL="tri"`},
	}
	for name, b := range bad {
		t.Run(name, func(t *testing.T) {
			c := base
			b.edit(&c)
			_, err := spendSetup(c)
			if err == nil || !strings.Contains(err.Error(), b.want) {
				t.Fatalf("spendSetup = %v, want an error naming %s", err, b.want)
			}
		})
	}
	t.Run("six decimals", func(t *testing.T) {
		c := base
		c.PipelineDailyCostUSD, c.ModelPrices = "0.000001", prices
		if s, err := spendSetup(c); err != nil || s.cost.PipelineDailyCostUSD != 0.000001 {
			t.Fatalf("six decimals: %+v, %v", s.cost, err)
		}
	})
	t.Run("unpriced model without a cost cap", func(t *testing.T) {
		c := base
		c.Model, c.ModelPrices = "other", prices
		if _, err := spendSetup(c); err != nil {
			t.Fatalf("an unpriced model needs no price when no cost cap is set: %v", err)
		}
	})
}

func TestSpendLedgerMapsEveryField(t *testing.T) {
	ctx := context.Background()
	st := memstore.New()
	if err := st.CreateRoom(ctx, store.Room{ID: "r1", Title: "t", PipelineID: "orders", Status: "open"}); err != nil {
		t.Fatal(err)
	}
	l := spendLedger{st: st}
	if _, err := l.AddSpend(ctx, agent.SpendChange{RoomID: "r1", PipelineID: "orders", Day: "2026-10-13", Tokens: 100, CostMicros: 40}); err != nil {
		t.Fatal(err)
	}
	tot, err := l.AddSpend(ctx, agent.SpendChange{RoomID: "r1", PipelineID: "orders", Day: "2026-10-13", Tokens: 3, CostMicros: 2,
		Call: &agent.SpendCall{Model: "m", InputTokens: 1, CacheCreationInputTokens: 2, CacheReadInputTokens: 3, OutputTokens: 4, CostMicros: 5, Priced: true}})
	if err != nil {
		t.Fatal(err)
	}
	if tot != (agent.SpendTotals{RoomTokens: 103, RoomCostMicros: 42, PipelineTokens: 103, PipelineCostMicros: 42}) {
		t.Fatalf("totals = %+v", tot)
	}
	rep, err := st.RoomSpend(ctx, "r1")
	if err != nil {
		t.Fatal(err)
	}
	want := store.SpendUsage{Model: "m", Calls: 1, InputTokens: 1, CacheCreationInputTokens: 2, CacheReadInputTokens: 3, OutputTokens: 4, CostMicros: 5}
	if len(rep.ByModel) != 1 || rep.ByModel[0] != want {
		t.Fatalf("breakdown = %+v, want %+v", rep.ByModel, want)
	}
	if _, err := l.AddSpend(ctx, agent.SpendChange{RoomID: "missing", Day: "2026-10-13"}); err == nil {
		t.Fatal("the store error was not passed on")
	}
}

func TestUsageRelay(t *testing.T) {
	ctx := context.Background()
	var logs bytes.Buffer
	relay := &usageRelay{log: slog.New(slog.NewTextHandler(&logs, nil))}
	p := events.AgentUsagePayload{Model: "m", Role: events.UsageRoleHandoff, InputTokens: 4}
	relay.emit(ctx, "r1", p) // before the room service exists: dropped, no panic

	st := memstore.New()
	svc := room.New(room.Deps{Store: st, Users: room.DefaultUsers(), Run: fakeRun})
	t.Cleanup(svc.Close)
	snap, err := svc.CreateRoom(ctx, room.DefaultUsers().All()[0], "t", "sev2", "orders")
	if err != nil {
		t.Fatal(err)
	}
	relay.set(svc)
	relay.emit(ctx, snap.ID, p)
	evs, _, err := svc.Events(ctx, snap.ID, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	var found bool
	for _, e := range evs {
		if e.Type == events.AgentUsage && e.Actor == events.AgentActor {
			got, _ := events.Decode[events.AgentUsagePayload](e)
			found = got == p
		}
	}
	if !found {
		t.Fatalf("no agent.usage event in the room: %+v", evs)
	}
	relay.emit(ctx, "r_unknown", p)
	if !strings.Contains(logs.String(), "could not record the hand-off's model usage") {
		t.Fatalf("logs = %q, want a warning for an unknown room", logs.String())
	}
}

func TestNewRoomServiceWiresTheUsageRelay(t *testing.T) {
	ctx := context.Background()
	relay := &usageRelay{log: slog.New(slog.DiscardHandler)}
	svc := newRoomService(room.Deps{Store: memstore.New(), Users: room.DefaultUsers(), Run: fakeRun}, auth.Settings{}, relay)
	t.Cleanup(svc.Close)
	snap, err := svc.CreateRoom(ctx, room.DefaultUsers().All()[0], "t", "sev2", "orders")
	if err != nil {
		t.Fatal(err)
	}
	relay.emit(ctx, snap.ID, events.AgentUsagePayload{Model: "m", Role: events.UsageRoleHandoff})
	evs, _, err := svc.Events(ctx, snap.ID, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if evs[len(evs)-1].Type != events.AgentUsage {
		t.Fatalf("last event = %s, want the relayed agent.usage: the relay is not wired to the service", evs[len(evs)-1].Type)
	}
}
