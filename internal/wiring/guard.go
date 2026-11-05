package wiring

import (
	"fmt"

	"piperoom/internal/agent"
	"piperoom/internal/config"
)

// WithHypothesisGuard returns opts with HypothesisGuard set from
// PIPEROOM_HYPOTHESIS_GUARD (config.ParseHypothesisGuard; unset is log). Every
// process that runs the agent builds its Options through it, live and shadow
// runs alike, so they share one mode. A bad value is an error naming the
// variable: the caller stops before anything opens, as for
// PIPEROOM_WRITE_MODE, instead of guessing the guard on or off. (H1)
func WithHypothesisGuard(cfg config.Config, opts agent.Options) (agent.Options, error) {
	mode, err := config.ParseHypothesisGuard(cfg)
	if err != nil {
		return opts, fmt.Errorf("wiring: %w", err)
	}
	opts.HypothesisGuard = mode
	return opts, nil
}
