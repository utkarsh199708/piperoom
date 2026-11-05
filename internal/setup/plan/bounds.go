package plan

import (
	"errors"
	"fmt"
	"time"

	"piperoom/internal/events"
)

// The bounds of a setup run (design §3.1). On any of them the run emits
// setup.blocked with a question and goes idle; it does not fail.
const (
	// DefaultAttemptsPerStep is how many failed executions of one plan step
	// stop the run.
	DefaultAttemptsPerStep = 3
	// DefaultStepsPerPlan is how many executed steps (T1 and above) of one
	// plan stop the run.
	DefaultStepsPerPlan = 12
	// DefaultWallClock is the run's budget from Run's start, over every turn
	// and wait.
	DefaultWallClock = 45 * time.Minute
	// DefaultTokenBudget is the model-token budget of one setup room
	// (PIPEROOM_SETUP_TOKEN_BUDGET's default, config.DefaultSetupTokenBudget).
	DefaultTokenBudget int64 = 2_000_000
)

// ErrBounds is returned by Bounds.Validate for a negative bound.
var ErrBounds = errors.New("plan: invalid setup bounds")

// Bounds are the stops of a setup run. The zero value means every default:
// Fill (and Exhausted) replace zeros, so a caller passes only the bounds it
// changes. It mirrors agent.SetupBounds without importing the agent.
type Bounds struct {
	AttemptsPerStep int
	StepsPerPlan    int
	WallClock       time.Duration
	TokenBudget     int64
}

// DefaultBounds returns the design's bounds.
func DefaultBounds() Bounds {
	return Bounds{
		AttemptsPerStep: DefaultAttemptsPerStep,
		StepsPerPlan:    DefaultStepsPerPlan,
		WallClock:       DefaultWallClock,
		TokenBudget:     DefaultTokenBudget,
	}
}

// Fill returns b with every zero field replaced by its default. A negative
// field is left as it is, for Validate to refuse: a silently corrected
// negative would hide a configuration mistake.
func (b Bounds) Fill() Bounds {
	d := DefaultBounds()
	if b.AttemptsPerStep == 0 {
		b.AttemptsPerStep = d.AttemptsPerStep
	}
	if b.StepsPerPlan == 0 {
		b.StepsPerPlan = d.StepsPerPlan
	}
	if b.WallClock == 0 {
		b.WallClock = d.WallClock
	}
	if b.TokenBudget == 0 {
		b.TokenBudget = d.TokenBudget
	}
	return b
}

// Validate refuses a negative bound. Call it once when the bounds are built
// (PIPEROOM_SETUP_TOKEN_BUDGET is the one that comes from config).
func (b Bounds) Validate() error {
	switch {
	case b.AttemptsPerStep < 0:
		return fmt.Errorf("%w: attempts per step is %d", ErrBounds, b.AttemptsPerStep)
	case b.StepsPerPlan < 0:
		return fmt.Errorf("%w: steps per plan is %d", ErrBounds, b.StepsPerPlan)
	case b.WallClock < 0:
		return fmt.Errorf("%w: wall clock is %s", ErrBounds, b.WallClock)
	case b.TokenBudget < 0:
		return fmt.Errorf("%w: token budget is %d", ErrBounds, b.TokenBudget)
	}
	return nil
}

// Exhausted reports which bound has been reached, as one of
// events.SetupBlockedAttempts, SetupBlockedSteps, SetupBlockedWallClock or
// SetupBlockedBudget, and "" while the run may go on. The counts are for the
// whole run: failedAttempts of the step about to be retried, executedSteps
// (T1 and above) of the current plan, elapsed since Run's start and tokens
// used, cache included (counted like Options.TokenBudget). The order is the
// one the run checks them in: attempts, steps, wall clock, budget. Zero-value
// bounds mean the defaults, so call Fill (or Validate) first when a negative
// bound must be refused rather than read as reached.
func (b Bounds) Exhausted(failedAttempts, executedSteps int, elapsed time.Duration, tokens int64) string {
	b = b.Fill()
	switch {
	case failedAttempts >= b.AttemptsPerStep:
		return events.SetupBlockedAttempts
	case executedSteps >= b.StepsPerPlan:
		return events.SetupBlockedSteps
	case elapsed >= b.WallClock:
		return events.SetupBlockedWallClock
	case tokens >= b.TokenBudget:
		return events.SetupBlockedBudget
	}
	return ""
}
