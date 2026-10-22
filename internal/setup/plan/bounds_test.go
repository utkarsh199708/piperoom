package plan

import (
	"errors"
	"testing"
	"time"

	"piperoom/internal/events"
)

func TestDefaultBounds(t *testing.T) {
	b := DefaultBounds()
	if b.AttemptsPerStep != 3 || b.StepsPerPlan != 12 || b.WallClock != 45*time.Minute || b.TokenBudget != 2_000_000 {
		t.Errorf("DefaultBounds() = %+v", b)
	}
	if b.AttemptsPerStep != DefaultAttemptsPerStep || b.StepsPerPlan != DefaultStepsPerPlan ||
		b.WallClock != DefaultWallClock || b.TokenBudget != DefaultTokenBudget {
		t.Error("DefaultBounds does not use its constants")
	}
}

func TestBoundsFill(t *testing.T) {
	tests := []struct {
		name string
		in   Bounds
		want Bounds
	}{
		{name: "zero is every default", in: Bounds{}, want: DefaultBounds()},
		{
			name: "only the token budget set",
			in:   Bounds{TokenBudget: 5},
			want: Bounds{AttemptsPerStep: DefaultAttemptsPerStep, StepsPerPlan: DefaultStepsPerPlan, WallClock: DefaultWallClock, TokenBudget: 5},
		},
		{
			name: "every field set",
			in:   Bounds{AttemptsPerStep: 1, StepsPerPlan: 2, WallClock: time.Minute, TokenBudget: 10},
			want: Bounds{AttemptsPerStep: 1, StepsPerPlan: 2, WallClock: time.Minute, TokenBudget: 10},
		},
		{
			name: "a negative field is not hidden by a default",
			in:   Bounds{AttemptsPerStep: -1},
			want: Bounds{AttemptsPerStep: -1, StepsPerPlan: DefaultStepsPerPlan, WallClock: DefaultWallClock, TokenBudget: DefaultTokenBudget},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.in.Fill(); got != tt.want {
				t.Errorf("Fill() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestBoundsValidate(t *testing.T) {
	if err := DefaultBounds().Validate(); err != nil {
		t.Errorf("DefaultBounds().Validate() = %v", err)
	}
	if err := (Bounds{}).Validate(); err != nil {
		t.Errorf("zero Bounds.Validate() = %v, want nil (zeros are defaults)", err)
	}
	for name, b := range map[string]Bounds{
		"attempts":   {AttemptsPerStep: -1},
		"steps":      {StepsPerPlan: -1},
		"wall clock": {WallClock: -time.Second},
		"budget":     {TokenBudget: -1},
	} {
		if err := b.Validate(); !errors.Is(err, ErrBounds) {
			t.Errorf("%s: Validate() = %v, want ErrBounds", name, err)
		}
	}
}

func TestBoundsExhausted(t *testing.T) {
	tests := []struct {
		name     string
		bounds   Bounds
		attempts int
		steps    int
		elapsed  time.Duration
		tokens   int64
		want     string
	}{
		{name: "nothing reached", bounds: DefaultBounds(), want: ""},
		{name: "two of three attempts", bounds: DefaultBounds(), attempts: 2, want: ""},
		{name: "three of three attempts", bounds: DefaultBounds(), attempts: 3, want: events.SetupBlockedAttempts},
		{name: "more than three attempts", bounds: DefaultBounds(), attempts: 4, want: events.SetupBlockedAttempts},
		{name: "eleven of twelve steps", bounds: DefaultBounds(), steps: 11, want: ""},
		{name: "twelve of twelve steps", bounds: DefaultBounds(), steps: 12, want: events.SetupBlockedSteps},
		{name: "one second under the wall clock", bounds: DefaultBounds(), elapsed: 45*time.Minute - time.Second, want: ""},
		{name: "the wall clock exactly", bounds: DefaultBounds(), elapsed: 45 * time.Minute, want: events.SetupBlockedWallClock},
		{name: "one token under the budget", bounds: DefaultBounds(), tokens: 1_999_999, want: ""},
		{name: "the budget exactly", bounds: DefaultBounds(), tokens: 2_000_000, want: events.SetupBlockedBudget},
		{name: "zero bounds mean the defaults", bounds: Bounds{}, attempts: 3, want: events.SetupBlockedAttempts},
		{
			name:     "attempts are checked before steps",
			bounds:   Bounds{AttemptsPerStep: 1, StepsPerPlan: 1},
			attempts: 1, steps: 1, want: events.SetupBlockedAttempts,
		},
		{
			name:   "steps are checked before the wall clock",
			bounds: Bounds{StepsPerPlan: 1, WallClock: time.Second},
			steps:  1, elapsed: time.Second, want: events.SetupBlockedSteps,
		},
		{
			name:    "the wall clock is checked before the budget",
			bounds:  Bounds{WallClock: time.Second, TokenBudget: 1},
			elapsed: time.Second, tokens: 1, want: events.SetupBlockedWallClock,
		},
		{
			name:     "a custom bound",
			bounds:   Bounds{AttemptsPerStep: 5},
			attempts: 4, want: "",
		},
		{name: "negative counts reach nothing", bounds: DefaultBounds(), attempts: -1, steps: -1, tokens: -1, want: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.bounds.Exhausted(tt.attempts, tt.steps, tt.elapsed, tt.tokens)
			if got != tt.want {
				t.Errorf("Exhausted(%d, %d, %s, %d) = %q, want %q", tt.attempts, tt.steps, tt.elapsed, tt.tokens, got, tt.want)
			}
		})
	}
}
