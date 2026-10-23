package setuptools_test

// This file is compiled from outside the package, the way internal/agent and
// internal/wiring would use it: it only sees the exported API and reaches the
// room-bound tools through Deps, never through a room import. If a room (or
// room-owned) import came back into the package, the cycle would surface here
// first, in internal/agent, not in the room.

import (
	"context"
	"encoding/json"
	"testing"

	"piperoom/internal/setup/protocol"
	setuptools "piperoom/internal/setup/tools"
	"piperoom/internal/tools"
)

// runner is the smallest protocol.RunnerExecutor: it answers one OK result.
type runner struct{}

func (runner) Execute(_ context.Context, _ string, req protocol.OpRequest) (protocol.OpResult, error) {
	return protocol.OpResult{ID: req.ID, OK: true, Summary: "ok"}, nil
}

func TestRegisterThroughThePublicAPI(t *testing.T) {
	reg := tools.NewReadOnlyRegistry()
	deps := setuptools.Deps{
		Runner: runner{},
		RoomID: "r_setup",
		// The room service's room.RoomID has this shape; passing it as a
		// function is what keeps internal/room out of the import graph.
		CallRoom: func(context.Context) string { return "r_setup" },
	}
	if err := setuptools.Register(reg, deps); err != nil {
		t.Fatalf("Register: %v", err)
	}
	tool, ok := reg.Get("env_detect")
	if !ok {
		t.Fatal("env_detect did not register")
	}
	res, err := tool.Run(context.Background(), json.RawMessage(`{}`))
	if err != nil || res.Summary == "" {
		t.Fatalf("Run = %q, %v", res.Summary, err)
	}
}
