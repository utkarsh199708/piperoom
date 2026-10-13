package main

import (
	"piperoom/internal/agent"
	"piperoom/internal/config"
	"piperoom/internal/owners"
)

// shadowAgentOptions builds a shadow run's agent options from the config.
// sh.TokenBudget passes through unchanged: agent.Options.TokenBudget 0 means
// the room default (PIPEROOM_ROOM_TOKEN_BUDGET), never unlimited.
func shadowAgentOptions(cfg config.Config, sh config.Shadow) agent.Options {
	return agent.Options{
		Model:       cfg.Model,
		Effort:      cfg.Effort,
		MaxTurns:    sh.MaxTurns,
		TokenBudget: sh.TokenBudget,
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
