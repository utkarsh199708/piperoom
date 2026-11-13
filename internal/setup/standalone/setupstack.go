package standalone

import (
	"context"
	"fmt"

	"piperoom/internal/config"
	"piperoom/internal/room"
	"piperoom/internal/wiring"
	"piperoom/internal/wiring/setupwire"
)

// setupStackBuilder is room.Deps.SetupStack for the standalone server, the
// same builder cmd/server/setupstack.go passes (row U1-31): a setup room's
// agent registries over its paired runner, from wiring.OpenSetupStack with
// the setupwire hooks, over the room service's RunnerExecutor. It returns nil
// unless setup and write mode are both on, so a lifetime with either off has
// no builder at all; OpenSetupStack checks both again on every call, before
// any remote client or setup action exists (docs/design/setup-agent.md §3.8).
// The standalone server always runs with write mode on (quickstart refuses
// "off"), so the write gate here can only trip on an injected config.
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
			return room.PipelineStack{}, fmt.Errorf("setup: %w", err)
		}
		return room.PipelineStack{Tools: st.Tools, Actions: st.Actions}, nil
	}
}
