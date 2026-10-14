package agent

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/anthropics/anthropic-sdk-go"
)

// DefaultRoomTokenBudget is the per-room token cap used when
// PIPEROOM_ROOM_TOKEN_BUDGET is empty.
const DefaultRoomTokenBudget int64 = 2_000_000

// budgetEnv names the setting in the messages a person reads.
const budgetEnv = "PIPEROOM_ROOM_TOKEN_BUDGET"

// ParseRoomTokenBudget turns the PIPEROOM_ROOM_TOKEN_BUDGET value into a limit.
// Empty means DefaultRoomTokenBudget; exactly "0" means no cap (it returns 0);
// anything else must be a whole number of at least 1. A bad value is an error
// so a typo cannot silently remove the cost cap.
func ParseRoomTokenBudget(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return DefaultRoomTokenBudget, nil
	}
	if s == "0" {
		return 0, nil
	}
	var n int64
	for _, c := range s {
		if c < '0' || c > '9' {
			return 0, fmt.Errorf("agent: %s=%q: want a whole number >= 1, \"0\" for no cap, or empty for %d", budgetEnv, s, DefaultRoomTokenBudget)
		}
		if n > (1<<62)/10 {
			return 0, fmt.Errorf("agent: %s=%q: too large", budgetEnv, s)
		}
		n = n*10 + int64(c-'0')
	}
	if n < 1 {
		return 0, fmt.Errorf("agent: %s=%q: want a whole number >= 1, \"0\" for no cap, or empty for %d", budgetEnv, s, DefaultRoomTokenBudget)
	}
	return n, nil
}

// BudgetError reports that a spend limit stopped the run: the room's token
// budget, a pipeline's daily token or cost budget, or a spend store that cannot
// be read while a limit is set (docs/design/cost.md 3.3). The run stops on it;
// it is not a failure of the model call itself.
type BudgetError struct {
	// Used and Limit are tokens; for a pipeline's daily cost budget they are
	// millionths of a USD.
	Used  int64
	Limit int64

	// runLimit is true when the cap came from Options.TokenBudget, not from
	// PIPEROOM_ROOM_TOKEN_BUDGET, so the text must not point at that setting.
	runLimit bool
	// scope says which limit; the zero value is the room's token budget.
	scope budgetScope
	// pipeline and day name the pipeline-day of a daily budget.
	pipeline, day string
}

// budgetScope is the limit a BudgetError is about.
type budgetScope int

const (
	budgetRoom budgetScope = iota
	budgetPipelineTokens
	budgetPipelineCost
	budgetStore // the spend store failed while a limit was set
)

// Settings named in the budget texts.
const (
	pipelineTokensEnv = "PIPEROOM_PIPELINE_DAILY_TOKEN_BUDGET"
	pipelineCostEnv   = "PIPEROOM_PIPELINE_DAILY_COST_USD"
)

// storeUnavailableText is the stop text when the spend store fails while a
// spend limit is set.
const storeUnavailableText = "spend store unavailable, so the spend limits cannot be checked; the agent stopped. A person can continue manually or steer the agent to try again"

func (e *BudgetError) Error() string {
	switch e.scope {
	case budgetPipelineTokens:
		return fmt.Sprintf("pipeline daily token budget reached for %q on %s UTC (%d of %d); a person must raise %s or continue manually",
			e.pipeline, e.day, e.Used, e.Limit, pipelineTokensEnv)
	case budgetPipelineCost:
		return fmt.Sprintf("pipeline daily cost budget reached for %q on %s UTC ($%s of $%s); a person must raise %s or continue manually",
			e.pipeline, e.day, usdText(e.Used), usdText(e.Limit), pipelineCostEnv)
	case budgetStore:
		return storeUnavailableText
	}
	if e.runLimit {
		return runBudgetText(e.Used, e.Limit)
	}
	return budgetText(e.Used, e.Limit)
}

// usdText writes millionths of a USD as dollars with at least two decimals
// and no trailing zeros beyond them: 50000000 is "50.00", 1100 is "0.0011".
func usdText(micros int64) string {
	s := strconv.FormatFloat(float64(micros)/1e6, 'f', 6, 64)
	for strings.HasSuffix(s, "0") && len(s)-strings.IndexByte(s, '.') > 3 {
		s = s[:len(s)-1]
	}
	return s
}

// budgetText is the sentence a person reads, in the agent message and in the
// idle status detail.
func budgetText(used, limit int64) string {
	return fmt.Sprintf("token budget reached (%d of %d); a person must raise %s or continue manually", used, limit, budgetEnv)
}

// runBudgetText is budgetText for a cap set by the caller of this run.
func runBudgetText(used, limit int64) string {
	return fmt.Sprintf("token budget reached (%d of %d); the budget set for this run is spent", used, limit)
}

// tokenBudget counts the model tokens one room has used in this server
// process. The Endpoint owns one per room, so it survives agent restarts. On
// its own it is not persisted: a restart starts the count again at zero. With
// a SpendLedger (SetSpend, the server) the caps are checked against the
// persisted totals instead (spendGuard); tokenBudget then still holds the
// room's cap, the next estimate and the count Deps.Usage reports.
//
// The unit is raw tokens: input, cache creation, cache reads, output and
// compaction passes, each counted once at face value. Cache reads and writes
// are not weighted by price, so the cap bounds model work, not dollars.
//
// The cap is soft. It is checked before each call, so one call can run past the
// limit by its own output, and calls that run at the same time (a hand-off next
// to the loop) are not serialized.
type tokenBudget struct {
	mu sync.Mutex
	// limit is 0 for no cap. It sits under mu because a run with
	// Options.TokenBudget changes it on an entry another call (a hand-off) may
	// already be using.
	limit    int64
	runLimit bool // limit came from Options.TokenBudget (setLimit)
	used     int64
	input    int64 // input size of the last loop call, the floor for the next one
}

func newTokenBudget(limit int64) *tokenBudget { return &tokenBudget{limit: limit} }

// admit returns a *BudgetError when a call that needs at least estimate more
// tokens would reach the limit.
func (b *tokenBudget) admit(estimate int64) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.limit <= 0 {
		return nil
	}
	if b.used+estimate >= b.limit {
		return &BudgetError{Used: b.used, Limit: b.limit, runLimit: b.runLimit}
	}
	return nil
}

// nextEstimate is the least the next loop call can read: the history is
// append-only, so it is at least as long as the last request.
func (b *tokenBudget) nextEstimate() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.input
}

// record adds one call's usage. isLoop marks calls of the investigation loop,
// whose input size feeds nextEstimate.
func (b *tokenBudget) record(u usageTokens, isLoop bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.used += u.total()
	// A failed call reports no usage; keep the last real size then.
	if isLoop && u.input > 0 {
		b.input = u.input
	}
}

// setLimit replaces the cap and keeps the count, so a run that asks for a
// smaller budget is still charged for what the room already spent.
func (b *tokenBudget) setLimit(limit int64) {
	b.mu.Lock()
	b.limit = limit
	b.runLimit = true
	b.mu.Unlock()
}

// limits reports the cap and whether it came from Options.TokenBudget.
func (b *tokenBudget) limits() (limit int64, runLimit bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.limit, b.runLimit
}

// Limit reports the current cap; 0 means no cap.
func (b *tokenBudget) Limit() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.limit
}

// Used reports the tokens counted so far.
func (b *tokenBudget) Used() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.used
}

// usageTokens is what one API response cost.
type usageTokens struct {
	input  int64 // input_tokens + cache creation + cache reads
	output int64
}

func (u usageTokens) total() int64 { return u.input + u.output }

// messageUsage reads the tokens of one response. Cache reads and writes count
// as input: they are still tokens the model processed, and counting only the
// uncached tail would let a long cached history bypass the cap. A compaction
// pass is billed but is not part of the top-level usage fields, so its entries
// are added.
func messageUsage(m anthropic.BetaMessage) usageTokens {
	u := m.Usage
	out := usageTokens{
		input:  u.InputTokens + u.CacheCreationInputTokens + u.CacheReadInputTokens,
		output: u.OutputTokens,
	}
	for _, it := range u.Iterations {
		if it.Type == "compaction" {
			out.input += it.InputTokens + it.CacheCreationInputTokens + it.CacheReadInputTokens
			out.output += it.OutputTokens
		}
	}
	return out
}

// budgetModel counts every call of the wrapped model against one room's budget.
type budgetModel struct {
	inner  model
	budget *tokenBudget
	// loop is true for the investigation loop. Its calls are refused at the
	// limit. A hand-off is only counted: refusing it would leave a person who
	// takes over a stopped room without a summary, and it is one bounded call.
	loop bool
	// guard, when set, checks the caps against the persisted totals and
	// records every call there; nil = budget alone decides, in memory.
	guard *spendGuard
}

func (m *budgetModel) Turn(ctx context.Context, params anthropic.BetaMessageNewParams) (anthropic.BetaMessage, error) {
	var ch spendCharge
	if m.loop {
		est := m.budget.nextEstimate()
		if m.guard == nil {
			if err := m.budget.admit(est); err != nil {
				return anthropic.BetaMessage{}, err
			}
		} else {
			var err error
			if ch, err = m.guard.admit(ctx, m.budget, params.Model, est); err != nil {
				return anthropic.BetaMessage{}, err
			}
		}
	}
	msg, err := m.inner.Turn(ctx, params)
	// A failed stream may still have used tokens; count what the partial
	// message reports.
	m.budget.record(messageUsage(msg), m.loop)
	if m.guard != nil {
		m.guard.settle(ctx, ch, params.Model, msg)
	}
	return msg, err
}

// asBudgetError returns the *BudgetError in err's chain, if any.
func asBudgetError(err error) (*BudgetError, bool) {
	var be *BudgetError
	if errors.As(err, &be) {
		return be, true
	}
	return nil, false
}
