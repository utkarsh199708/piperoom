package main

import (
	"piperoom/internal/agent"
	"piperoom/internal/config"
	"piperoom/internal/owners"
)

// agentOptions builds a live run's agent options from the config. The tool
// data cap is the room's PIPEROOM_EVENT_MAX_DATA_BYTES, so the agent does not
// cut Result.Data at its own 64 KiB default when the room accepts more (#371).
func agentOptions(cfg config.Config, lim config.Limits) agent.Options {
	return agent.Options{
		Model:            cfg.Model,
		Effort:           cfg.Effort,
		MaxTurns:         40,
		MaxToolDataBytes: lim.EventMaxDataBytes,
	}
}

// shadowAgentOptions builds a shadow run's agent options from the config.
// sh.TokenBudget passes through unchanged: agent.Options.TokenBudget 0 means
// the room default (PIPEROOM_ROOM_TOKEN_BUDGET), never unlimited. The tool
// data cap is the live run's (agentOptions).
func shadowAgentOptions(cfg config.Config, sh config.Shadow, lim config.Limits) agent.Options {
	return agent.Options{
		Model:            cfg.Model,
		Effort:           cfg.Effort,
		MaxTurns:         sh.MaxTurns,
		TokenBudget:      sh.TokenBudget,
		MaxToolDataBytes: lim.EventMaxDataBytes,
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
