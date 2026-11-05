package main

import (
	"context"
	"fmt"

	"piperoom/internal/config"
	"piperoom/internal/room"
	"piperoom/internal/wiring"
	"piperoom/internal/wiring/setupwire"
)

// setupStackBuilder is room.Deps.SetupStack (U1-25): a setup room's agent
// registries over its paired runner, from wiring.OpenSetupStack with the
// setupwire hooks. It returns nil unless setup and write mode are both on,
// so a server with either off has no builder at all; OpenSetupStack checks
// both again on every call, before any remote client or setup action
// exists (docs/design/setup-agent.md §3.8).
func setupStackBuilder(cfg config.Config, setupOn, writeOn bool) room.SetupStackBuilder {
	if !setupOn || !writeOn {
		return nil
	}
	return func(ctx context.Context, in room.SetupStackInput) (room.PipelineStack, error) {
		st, err := wiring.OpenSetupStack(ctx, cfg, wiring.SetupStackDeps{
			Executor:     in.Executor,
			RoomID:       in.RoomID,
			Request:      in.Request,
			CallRoom:     room.RoomID,
			Clients:      setupwire.Clients,
			SetupActions: setupwire.RegisterActions,
		})
		if err != nil {
			return room.PipelineStack{}, fmt.Errorf("server: %w", err)
		}
		return room.PipelineStack{Tools: st.Tools, Actions: st.Actions}, nil
	}
}
