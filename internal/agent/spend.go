package agent

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"sync"
	"time"

	"github.com/anthropics/anthropic-sdk-go"

	"piperoom/internal/events"
)

// spendStoreTimeout bounds one spend store call. A store slower than this
// counts as unavailable.
const spendStoreTimeout = 5 * time.Second

// spendRetryTimeout bounds the one retry of a failed settle.
const spendRetryTimeout = 2 * time.Second

// DefaultEstimateFloor is the estimate of a room's first request in this
// process (a new room, or the first after a restart), when there is no
// previous request to size it by (docs/design/cost.md 3.3).
const DefaultEstimateFloor int64 = 20_000

// SpendLedger keeps model spend where it survives a restart (the room
// database; docs/design/cost.md 3.2). cmd/server adapts store.SpendStore to it.
type SpendLedger interface {
	// AddSpend adds c.Tokens and c.CostMicros to the room's total and to the
	// pipeline's total for c.Day in one atomic step, and returns both totals
	// after it. The amounts may be negative (an estimate taken back). When
	// c.Call is set, the finished request is also added to the per-model
	// breakdowns.
	AddSpend(ctx context.Context, c SpendChange) (SpendTotals, error)
}

// SpendChange is one change to a room's and a pipeline-day's spend.
type SpendChange struct {
	RoomID     string
	PipelineID string
	Day        string // UTC date, "2006-01-02"
	Tokens     int64
	CostMicros int64 // millionths of a USD
	Call       *SpendCall
}

// SpendCall is one finished model request for the per-model breakdowns.
type SpendCall struct {
	Model                    string
	InputTokens              int64
	CacheCreationInputTokens int64
	CacheReadInputTokens     int64
	OutputTokens             int64
	CostMicros               int64
	// Priced is false when the price table has no entry for Model; CostMicros
	// is then 0 and means unknown.
	Priced bool
}

// SpendTotals are the totals a SpendLedger returns after a change: what the
// caps are checked against.
type SpendTotals struct {
	RoomTokens         int64
	RoomCostMicros     int64
	PipelineTokens     int64
	PipelineCostMicros int64
}

// SpendOptions turns on durable spend accounting for an Endpoint (cmd/server).
type SpendOptions struct {
	// Ledger keeps the counts. Nil keeps the in-memory per-process count of
	// PIPEROOM_ROOM_TOKEN_BUDGET, and allows no daily cap.
	Ledger SpendLedger
	// PipelineDailyTokens caps model tokens per pipeline per UTC day; 0 = off.
	PipelineDailyTokens int64
	// PipelineDailyCostUSD caps the estimated cost per pipeline per UTC day;
	// 0 = off. It needs Prices.
	PipelineDailyCostUSD float64
	// Prices estimate cost; nil = every cost is omitted.
	Prices Prices
	// Now is the clock that picks the UTC day; nil = time.Now.
	Now func() time.Time
	// OnUsage receives the agent.usage events of requests made outside a run
	// (the hand-off), with the room they belong to. Nil drops them.
	OnUsage func(ctx context.Context, roomID string, p events.AgentUsagePayload)
	// EstimateFloor is the estimate of a room's first request in this process;
	// 0 = DefaultEstimateFloor. Without it first requests reserve nothing, and
	// many rooms arriving together could all pass a pipeline's daily cap.
	EstimateFloor int64
}

// spendSettings is what SetSpend stored. It never changes afterwards.
type spendSettings struct {
	ledger          SpendLedger
	dailyTokens     int64
	dailyCostMicros int64
	prices          Prices
	now             func() time.Time
	onUsage         func(ctx context.Context, roomID string, p events.AgentUsagePayload)
	estimateFloor   int64

	warnMu   sync.Mutex
	unpriced map[string]bool // models already warned about under a cost cap
}

// SetSpend configures spend accounting. Call it once, before the first Run.
func (e *Endpoint) SetSpend(o SpendOptions) error {
	if o.PipelineDailyTokens < 0 {
		return errors.New("agent: the pipeline daily token budget must not be negative")
	}
	c := o.PipelineDailyCostUSD
	if math.IsNaN(c) || math.IsInf(c, 0) || c < 0 || c > float64(math.MaxInt64)/1e6 {
		return errors.New("agent: the pipeline daily cost budget must be a finite amount >= 0")
	}
	if (o.PipelineDailyTokens > 0 || c > 0) && o.Ledger == nil {
		return errors.New("agent: a pipeline daily budget needs a spend store")
	}
	if c > 0 && len(o.Prices) == 0 {
		return errors.New("agent: PIPEROOM_PIPELINE_DAILY_COST_USD needs PIPEROOM_MODEL_PRICES")
	}
	if o.EstimateFloor < 0 {
		return errors.New("agent: the estimate floor must not be negative")
	}
	floor := o.EstimateFloor
	if floor == 0 {
		floor = DefaultEstimateFloor
	}
	s := &spendSettings{
		ledger:          o.Ledger,
		dailyTokens:     o.PipelineDailyTokens,
		dailyCostMicros: int64(math.Round(c * 1e6)),
		prices:          o.Prices,
		now:             o.Now,
		onUsage:         o.OnUsage,
		estimateFloor:   floor,
		unpriced:        map[string]bool{},
	}
	if s.now == nil {
		s.now = time.Now
	}
	e.mu.Lock()
	e.spend = s
	e.mu.Unlock()
	return nil
}

// spendSettings returns what SetSpend stored, or nil.
func (e *Endpoint) spendSettings() *spendSettings {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.spend
}

// guard returns the durable spend guard for one room, or nil when there is no
// ledger (the in-memory tokenBudget then decides alone).
func (e *Endpoint) guard(roomID, pipelineID string) *spendGuard {
	s := e.spendSettings()
	if s == nil || s.ledger == nil {
		return nil
	}
	return &spendGuard{s: s, roomID: roomID, pipelineID: pipelineID, log: e.log}
}

// prices returns the price table for usage events, or nil.
func (e *Endpoint) prices() Prices {
	if s := e.spendSettings(); s != nil {
		return s.prices
	}
	return nil
}

// handoffModel is the model the hand-off writes with: counted against the
// room's budget (never refused) and reported through OnUsage.
func (e *Endpoint) handoffModel(roomEvents []events.Event) model {
	roomID := roomEvents[0].RoomID
	var m model = &budgetModel{inner: e.model, budget: e.budget(roomID), guard: e.guard(roomID, pipelineOf(roomEvents))}
	s := e.spendSettings()
	if s == nil || s.onUsage == nil {
		return m
	}
	return &reportingModel{inner: m, role: events.UsageRoleHandoff, prices: s.prices, report: func(ctx context.Context, p events.AgentUsagePayload) {
		s.onUsage(ctx, roomID, p)
	}}
}

// pipelineOf reads the pipeline id from the room.created event of a room log.
func pipelineOf(roomEvents []events.Event) string {
	for _, ev := range roomEvents {
		if ev.Type == events.RoomCreated {
			if p, err := events.Decode[events.RoomCreatedPayload](ev); err == nil {
				return p.PipelineID
			}
			return ""
		}
	}
	return ""
}

// day is the UTC date the caps count the next request on.
func (s *spendSettings) day() string { return s.now().UTC().Format(time.DateOnly) }

// warnUnpriced logs once per model that a cost cap cannot count its requests.
func (s *spendSettings) warnUnpriced(log *slog.Logger, model string) {
	s.warnMu.Lock()
	first := !s.unpriced[model]
	s.unpriced[model] = true
	s.warnMu.Unlock()
	if first {
		log.Warn("agent: model has no price in PIPEROOM_MODEL_PRICES; PIPEROOM_PIPELINE_DAILY_COST_USD does not count its requests", "model", model)
	}
}

// spendGuard checks and records one room's requests against the ledger.
type spendGuard struct {
	s          *spendSettings
	roomID     string
	pipelineID string
	log        *slog.Logger
}

// spendCharge is what admit charged for one request, settled after it.
type spendCharge struct {
	day        string
	tokens     int64
	costMicros int64
}

// admit charges the request's estimate and refuses it when a cap is reached
// (docs/design/cost.md 3.3). Charging before the request is sent is what keeps
// two requests running at once from both using the same headroom: the charges
// serialize in the store, so the admitted estimates never add up past a cap.
//
// A store error stops the run when any cap applies (fail closed) and lets the
// request go ahead uncharged when none does (fail open).
func (g *spendGuard) admit(ctx context.Context, b *tokenBudget, model string, est int64) (spendCharge, error) {
	limit, runLimit := b.limits()
	capped := limit > 0 || g.s.dailyTokens > 0 || g.s.dailyCostMicros > 0
	if est <= 0 {
		// No previous request in this process to size it by.
		est = g.s.estimateFloor
	}
	ch := spendCharge{day: g.s.day(), tokens: est, costMicros: g.s.prices.estimateMicros(model, est)}

	sctx, cancel := context.WithTimeout(ctx, spendStoreTimeout)
	t, err := g.s.ledger.AddSpend(sctx, g.change(ch.day, ch.tokens, ch.costMicros, nil))
	cancel()
	if err != nil {
		if ctx.Err() != nil {
			// The run is ending (room closed); that is not a store outage.
			return spendCharge{}, fmt.Errorf("agent: charge spend: %w: %w", errNotSent, ctx.Err())
		}
		if capped {
			g.log.Error("agent: spend store unavailable; the run stops because a spend limit is set", "room", g.roomID, "pipeline", g.pipelineID, "err", err)
			return spendCharge{}, &BudgetError{Limit: limit, scope: budgetStore}
		}
		g.log.Warn("agent: spend store unavailable; no spend limit is set, so the request goes ahead", "room", g.roomID, "pipeline", g.pipelineID, "err", err)
		return spendCharge{day: ch.day}, nil
	}
	if be := g.over(t, ch, limit, runLimit); be != nil {
		g.refund(ctx, ch)
		return spendCharge{}, be
	}
	return ch, nil
}

// over returns the *BudgetError of the first cap the totals reached: the room
// cap, then the pipeline's daily tokens, then its daily cost. The totals
// include this request's charge, so "total >= cap" is today's
// "used + estimate >= limit".
func (g *spendGuard) over(t SpendTotals, ch spendCharge, limit int64, runLimit bool) *BudgetError {
	s := g.s
	switch {
	case limit > 0 && t.RoomTokens >= limit:
		return &BudgetError{Used: t.RoomTokens - ch.tokens, Limit: limit, runLimit: runLimit}
	case s.dailyTokens > 0 && t.PipelineTokens >= s.dailyTokens:
		return &BudgetError{Used: t.PipelineTokens - ch.tokens, Limit: s.dailyTokens, scope: budgetPipelineTokens, pipeline: g.pipelineID, day: ch.day}
	case s.dailyCostMicros > 0 && t.PipelineCostMicros >= s.dailyCostMicros:
		return &BudgetError{Used: t.PipelineCostMicros - ch.costMicros, Limit: s.dailyCostMicros, scope: budgetPipelineCost, pipeline: g.pipelineID, day: ch.day}
	}
	return nil
}

// refund takes back the charge of a refused request, on the day it was
// charged and on a context that outlives the run. If that fails, the estimate
// of a request that was never sent stays charged (an overcount).
func (g *spendGuard) refund(ctx context.Context, ch spendCharge) {
	if err := g.addDetached(ctx, g.change(ch.day, -ch.tokens, -ch.costMicros, nil), spendStoreTimeout); err != nil {
		g.log.Warn("agent: could not take back the estimate of a refused request; it stays charged", "room", g.roomID, "tokens", ch.tokens, "err", err)
	}
}

// settle replaces the charged estimate with what the request used and adds it
// to the per-model breakdowns, on the day it was charged. It runs even when
// the run's context has ended: the request was billed. A failure is retried
// once on a fresh short context. (If the first attempt did commit and only its
// reply was lost, the retry counts the request twice: an overcount, which
// errs on the side of the cap.) If the retry fails too, the estimate stays
// charged, usually an undercount, and an Error is logged with counts only.
func (g *spendGuard) settle(ctx context.Context, ch spendCharge, model string, msg anthropic.BetaMessage) {
	own, comp, _ := splitUsage(msg)
	used := own.plus(comp)
	cost, priced := g.s.prices.costMicros(model, used)
	if !priced && g.s.dailyCostMicros > 0 {
		g.s.warnUnpriced(g.log, model)
	}
	day := ch.day
	if day == "" {
		day = g.s.day()
	}
	call := &SpendCall{
		Model:                    model,
		InputTokens:              used.input,
		CacheCreationInputTokens: used.cacheCreation,
		CacheReadInputTokens:     used.cacheRead,
		OutputTokens:             used.output,
		CostMicros:               cost,
		Priced:                   priced,
	}
	change := g.change(day, used.total()-ch.tokens, cost-ch.costMicros, call)
	err := g.addDetached(ctx, change, spendStoreTimeout)
	if err == nil {
		return
	}
	g.log.Warn("agent: could not record a model request's spend; retrying once", "room", g.roomID, "pipeline", g.pipelineID, "err", err)
	if err = g.addDetached(ctx, change, spendRetryTimeout); err != nil {
		g.log.Error("agent: could not record a model request's spend; its estimate stays charged",
			"room", g.roomID, "pipeline", g.pipelineID, "model", model, "estimate", ch.tokens, "actual", used.total(), "err", err)
	}
}

// addDetached applies a change on a context that outlives the run, bounded by
// timeout.
func (g *spendGuard) addDetached(ctx context.Context, c SpendChange, timeout time.Duration) error {
	sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), timeout)
	defer cancel()
	_, err := g.s.ledger.AddSpend(sctx, c)
	return err
}

func (g *spendGuard) change(day string, tokens, costMicros int64, call *SpendCall) SpendChange {
	return SpendChange{RoomID: g.roomID, PipelineID: g.pipelineID, Day: day, Tokens: tokens, CostMicros: costMicros, Call: call}
}
