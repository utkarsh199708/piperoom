package main

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"piperoom/internal/agent"
	"piperoom/internal/auth"
	"piperoom/internal/config"
	"piperoom/internal/events"
	"piperoom/internal/room"
	"piperoom/internal/store"
)

// spendSettings are the typed spend settings (docs/design/cost.md 3.1).
type spendSettings struct {
	cost      config.Cost
	prices    agent.Prices
	roomLimit int64 // PIPEROOM_ROOM_TOKEN_BUDGET; 0 = no cap
}

// spendSetup parses the spend settings and reads the price table. Every error
// refuses start: a typo must not drop a cap or a price. With a cost cap, the
// main and triage models must be priced, or their requests would not count
// toward it.
func spendSetup(cfg config.Config) (spendSettings, error) {
	cost, err := config.ParseCost(cfg)
	if err != nil {
		return spendSettings{}, fmt.Errorf("server: %w", err)
	}
	roomLimit, err := agent.ParseRoomTokenBudget(cfg.RoomTokenBudget)
	if err != nil {
		return spendSettings{}, fmt.Errorf("server: %w", err)
	}
	prices, err := agent.LoadModelPrices(cost.PricesFile)
	if err != nil {
		return spendSettings{}, fmt.Errorf("server: %w", err)
	}
	if cost.PipelineDailyCostUSD > 0 {
		for _, m := range []struct{ env, id string }{{"PIPEROOM_MODEL", cfg.Model}, {"PIPEROOM_TRIAGE_MODEL", cfg.TriageModel}} {
			if m.id != "" && !prices.Has(m.id) {
				return spendSettings{}, fmt.Errorf("server: PIPEROOM_PIPELINE_DAILY_COST_USD is set but PIPEROOM_MODEL_PRICES has no price for %s=%q", m.env, m.id)
			}
		}
	}
	return spendSettings{cost: cost, prices: prices, roomLimit: roomLimit}, nil
}

// logSettings writes the settings to the start-up log; never a price.
func (s spendSettings) logSettings(log *slog.Logger) {
	log.Info("server: model spend", "room_token_budget", s.roomLimit,
		"pipeline_daily_token_budget", s.cost.PipelineDailyTokens,
		"pipeline_daily_cost_usd", s.cost.PipelineDailyCostUSD,
		"priced_models", len(s.prices))
}

// options builds the endpoint's spend options over the room database.
func (s spendSettings) options(st store.SpendStore, relay *usageRelay) agent.SpendOptions {
	return agent.SpendOptions{
		Ledger:               spendLedger{st: st},
		PipelineDailyTokens:  s.cost.PipelineDailyTokens,
		PipelineDailyCostUSD: s.cost.PipelineDailyCostUSD,
		Prices:               s.prices,
		OnUsage:              relay.emit,
	}
}

// spendLedger adapts store.SpendStore to agent.SpendLedger; the two packages do
// not import each other.
type spendLedger struct {
	st store.SpendStore
}

func (l spendLedger) AddSpend(ctx context.Context, c agent.SpendChange) (agent.SpendTotals, error) {
	sc := store.SpendChange{
		RoomID:     c.RoomID,
		PipelineID: c.PipelineID,
		Day:        c.Day,
		Tokens:     c.Tokens,
		CostMicros: c.CostMicros,
	}
	if c.Call != nil {
		sc.Call = &store.SpendCall{
			Model:                    c.Call.Model,
			InputTokens:              c.Call.InputTokens,
			CacheCreationInputTokens: c.Call.CacheCreationInputTokens,
			CacheReadInputTokens:     c.Call.CacheReadInputTokens,
			OutputTokens:             c.Call.OutputTokens,
			CostMicros:               c.Call.CostMicros,
			Priced:                   c.Call.Priced,
		}
	}
	t, err := l.st.AddSpend(ctx, sc)
	if err != nil {
		return agent.SpendTotals{}, err
	}
	return agent.SpendTotals{
		RoomTokens:         t.RoomTokens,
		RoomCostMicros:     t.RoomCostMicros,
		PipelineTokens:     t.PipelineTokens,
		PipelineCostMicros: t.PipelineCostMicros,
	}, nil
}

// usageRelay hands the hand-off's agent.usage events to the room service,
// which is built after the endpoint (the same pattern as roomClosedHook).
type usageRelay struct {
	mu  sync.Mutex
	svc *room.Service
	log *slog.Logger
}

func (u *usageRelay) set(svc *room.Service) {
	u.mu.Lock()
	u.svc = svc
	u.mu.Unlock()
}

// emit appends the event; a failure is logged and the hand-off goes on.
func (u *usageRelay) emit(ctx context.Context, roomID string, p events.AgentUsagePayload) {
	u.mu.Lock()
	svc := u.svc
	u.mu.Unlock()
	if svc == nil {
		return
	}
	if err := svc.EmitAgentUsage(ctx, roomID, p); err != nil {
		u.log.Warn("server: could not record the hand-off's model usage", "room", roomID, "err", err)
	}
}

// newRoomService builds the room service and points the usage relay at it, so
// the hand-off's agent.usage events reach the room log. The auth settings
// are required: they give the room its session liveness rule (withRoomAuth).
func newRoomService(d room.Deps, authSettings auth.Settings, usage *usageRelay) *room.Service {
	svc := room.New(withRoomAuth(d, authSettings))
	usage.set(svc)
	return svc
}
