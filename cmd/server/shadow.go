package main

import (
	"math"

	"piperoom/internal/agent"
	"piperoom/internal/config"
	"piperoom/internal/owners"
)

// shadowAgentOptions builds a shadow run's agent options from the config. The
// two "0" meanings differ: PIPEROOM_SHADOW_TOKEN_BUDGET=0 means no cap, while
// agent.Options.TokenBudget 0 means "the Endpoint's default"
// (PIPEROOM_ROOM_TOKEN_BUDGET, the live budget). So a 0 here becomes
// math.MaxInt64: a limit > 0 that is never reached, which is "no cap" through
// the existing contract.
func shadowAgentOptions(cfg config.Config, sh config.Shadow) agent.Options {
	budget := sh.TokenBudget
	if budget == 0 {
		budget = math.MaxInt64
	}
	return agent.Options{
		Model:       cfg.Model,
		Effort:      cfg.Effort,
		MaxTurns:    sh.MaxTurns,
		TokenBudget: budget,
	}
}

// shadowPipelines lists the ids of the pipelines with shadow mode on.
func shadowPipelines(own *owners.Owners) []string {
	var ids []string
	for _, p := range own.Pipelines {
		if p.Shadow() {
			ids = append(ids, p.ID)
		}
	}
	return ids
}
