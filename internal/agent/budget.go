package agent

import (
	"context"
	"errors"
	"fmt"
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

// BudgetError reports that a room has used its token budget. The run stops on
// it; it is not a failure of the model call itself.
type BudgetError struct {
	Used  int64
	Limit int64

	// runLimit is true when the cap came from Options.TokenBudget, not from
	// PIPEROOM_ROOM_TOKEN_BUDGET, so the text must not point at that setting.
	runLimit bool
}

func (e *BudgetError) Error() string {
	if e.runLimit {
		return runBudgetText(e.Used, e.Limit)
	}
	return budgetText(e.Used, e.Limit)
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
// process. The Endpoint owns one per room, so it survives agent restarts. It is
// not persisted: a server restart starts the count again at zero.
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
}

func (m *budgetModel) Turn(ctx context.Context, params anthropic.BetaMessageNewParams) (anthropic.BetaMessage, error) {
	if m.loop {
		if err := m.budget.admit(m.budget.nextEstimate()); err != nil {
			return anthropic.BetaMessage{}, err
		}
	}
	msg, err := m.inner.Turn(ctx, params)
	// A failed stream may still have used tokens; count what the partial
	// message reports.
	m.budget.record(messageUsage(msg), m.loop)
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
