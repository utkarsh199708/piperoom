// Package setupwire is the bridge the server uses to build a setup room's
// registries: it constructs the remote clients over the paired runner and
// registers the setup-only actions, and passes both to wiring.OpenSetupStack.
//
// It lives apart from package wiring because the packages it bridges import
// internal/room, which imports wiring through alertin and connprobe:
// internal/setup/remoteclients holds room.SetupStep (its writes run only
// under a proposal step), and internal/setup/actions refuses a write without
// one. wiring therefore takes the clients and the action registration as
// hooks (SetupStackDeps), the same shape as Wired.ProbeTools, and this
// package is the caller's side of the hooks.
package setupwire

import (
	"piperoom/internal/actions"
	"piperoom/internal/setup/actions"
	"piperoom/internal/setup/protocol"
	"piperoom/internal/setup/remoteclients"
	"piperoom/internal/wiring"
)

// Clients builds the Q registries' remote clients over the room's paired
// runner: one remoteclients.Session and its four client values, as
// wiring.SetupClients. It performs no I/O: a client sends its first op when
// a tool or an executed step calls it. It has the SetupStackDeps.Clients
// signature, so the room passes it directly.
func Clients(ex protocol.RunnerExecutor, roomID string) (wiring.SetupClients, error) {
	s := remoteclients.New(ex, roomID)
	return wiring.SetupClients{
		Connect:  s.Connect(),
		Source:   s.Source(),
		Admin:    s.Source(),
		Kafka:    s.Kafka(),
		Registry: s.Registry(),
	}, nil
}

// RegisterActions registers the setup-only actions over the room's paired
// runner (the environment ops of docs/design/setup-agent.md §2.5). It has
// the SetupStackDeps.SetupActions signature, so the room passes it directly.
func RegisterActions(areg *actions.Registry, ex protocol.RunnerExecutor, roomID string) error {
	return setupactions.Register(areg, setupactions.Deps{Runner: ex, RoomID: roomID})
}
