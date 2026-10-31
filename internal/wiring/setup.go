package wiring

import (
	"context"
	"fmt"
	"strings"

	"piperoom/internal/actions"
	"piperoom/internal/clients"
	"piperoom/internal/clients/postgres"
	"piperoom/internal/config"
	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/setup/protocol"
	setuptools "piperoom/internal/setup/tools"
	"piperoom/internal/tools"
)

// The setup stack (docs/design/setup-agent.md §3.1, §3.2): one setup room's
// agent registries over the room's paired runner. The Q read tools and the
// setup read tools are built over the remote clients, the Q actions and the
// setup-only actions over the same clients, exactly as OpenStacks builds the
// same registries over the real ones, so the Q tools and actions keep their
// own names, validations, previews and scope checks in a setup room.
//
// The remote clients themselves (internal/setup/remoteclients) and the
// setup-only actions (internal/setup/actions) both import internal/room,
// which imports this package through alertin and connprobe, so this package
// cannot import them: it builds over the clients and the action registration
// the caller passes (SetupStackDeps), the same hook shape as Wired.ProbeTools.
// internal/wiring/setupwire is the bridge the server uses.

// SetupStack is one setup room's agent registries over its paired runner.
// Built by OpenSetupStack, which is the only constructor: with setup off, or
// with write mode off, it builds nothing, so no remote writer and no setup
// action exists (the P1a-3 rule, design §3.8).
type SetupStack struct {
	// Tools are the read tools, from tools.NewReadOnlyRegistry: the Q read
	// tools of the request's source kind over the remote clients, plus the
	// setup read tools (setuptools.Register) over the same runner. The room
	// scopes each call to the provisional pipeline with tools.WithPipeline.
	Tools *tools.Registry
	// Actions are the write actions the agent may PROPOSE (never run): the Q
	// actions over the remote clients (their writes reach the runner only
	// under a proposal step, room.SetupStep) plus the setup-only actions
	// (SetupStackDeps.SetupActions).
	Actions *actions.Registry
}

// SetupStackDeps are OpenSetupStack's collaborators. The room service
// implements Executor over the runner registry and passes room.RoomID as
// CallRoom, so a read for another room never reaches this room's runner.
type SetupStackDeps struct {
	// Executor sends one op to the paired runner (protocol.RunnerExecutor).
	// Required: the stack is the runner's remote view, and nothing in it can
	// be built without one.
	Executor protocol.RunnerExecutor
	// RoomID is the room the runner is paired to; the setup read tools are
	// bound to it at construction. Required.
	RoomID string
	// Request is the room's setup request: owners.Provisional turns it into
	// the pipeline the Q tools resolve ids against (dbtools requires an
	// Owners), its source kind picks the source's tools and actions, and its
	// targets are the hosts a connector the room creates may name
	// (registerLifecycle's AllowedHosts). Required.
	Request events.SetupRequest
	// Pipeline, when set, is the room's provisional pipeline as it stands —
	// owners.Provisional of the request plus everything a driver added since
	// (setup.pipeline_extended: consumer groups, sink tables) — and the
	// stack's owners scope is built from it, so the stack and the room's own
	// scope checks agree (row U1-32). Nil keeps today's behaviour and derives
	// the pipeline from Request alone, which has no consumer-group or
	// sink-table fields. Request stays required either way: its source kind
	// and targets are read regardless.
	Pipeline *owners.Pipeline
	// Class is the class of the endpoint the setup read tools reach
	// (protocol.ClassLocal, ClassShared or ClassProduction). Empty sends
	// ClassLocal; the runner raises a read's class to its own floor for the
	// target anyway, so an under-reported class is safe and an over-reported
	// one is not.
	Class string
	// CallRoom reads the room a tool call runs for (room.RoomID). Required,
	// like RoomID: the setup read tools refuse a call for another room
	// before any op is built.
	CallRoom func(context.Context) string
	// Clients builds the Q registries' remote clients over the paired
	// runner (setupwire.Clients builds them from a remoteclients.Session).
	// OpenSetupStack calls it only after the write-mode gate, so with write
	// mode off no remote writer is constructed. Required.
	Clients func(ex protocol.RunnerExecutor, roomID string) (SetupClients, error)
	// SetupActions registers the setup-only actions over the runner
	// (setupwire.RegisterActions). Called only after the write-mode gate, so
	// with write mode off no setup action is built. Nil registers none.
	SetupActions func(areg *actions.Registry, ex protocol.RunnerExecutor, roomID string) error
}

// SetupClients are the remote clients the Q registries are built over
// (design §3.2): the values a remoteclients.Session provides. Every field
// may be nil (a harness); a nil client registers its tools and actions the
// same way a nil real client does, answering "not configured".
type SetupClients struct {
	// Connect is the Kafka Connect client, reads and writes: a write sends
	// one T2 op under the call's proposal step.
	Connect clients.ConnectClient
	// Source is the read-only source client (clients.SourceDB).
	Source clients.SourceDB
	// Admin is the source admin writer of the Q2 actions
	// (pg_set_replica_identity, source_grant_select, ...): the remote
	// source, whose writes are runner ops under the proposal step. Nil
	// registers the actions over no writer, so Execute refuses before any
	// I/O.
	Admin postgres.SourceAdminWriter
	// Kafka is the Kafka reader; the runner has no Kafka writer, so the P3
	// Kafka actions register over no writer and refuse at Execute.
	Kafka clients.KafkaAdmin
	// Registry is the Schema Registry reader. It is wrapped read-only: no Q
	// action writes the registry, and a setup writes a schema only through a
	// registered action, never through a client.
	Registry clients.SchemaRegistryReader
}

// OpenSetupStack builds a setup room's registries over its paired runner.
// It refuses to build anything unless PIPEROOM_SETUP_ENABLED is on and
// PIPEROOM_WRITE_MODE is on: config.ParseSetup already refuses the pair at
// start, and this gate keeps any caller with a room from building the
// remote writer with write mode off. It performs no I/O: the clients send
// nothing until a tool or an executed step calls them.
func OpenSetupStack(ctx context.Context, cfg config.Config, d SetupStackDeps) (*SetupStack, error) {
	_ = ctx // construction performs no I/O; ctx is for callers and future pings.
	// The write-mode gate comes first: with write mode off nothing may be
	// built, whatever ParseSetup would say (it refuses setup-on-with-write-off
	// at start; this is the gate a caller with a room hits, and it runs
	// before Clients or SetupActions are called, so no remote writer and no
	// setup action exists).
	on, err := config.ParseWriteMode(cfg)
	if err != nil {
		return nil, fmt.Errorf("wiring: setup stack: %w", err)
	}
	if !on {
		return nil, fmt.Errorf("wiring: setup stack: PIPEROOM_WRITE_MODE is off; a setup room's registries (and the remote writer they hold) are not built")
	}
	setupCfg, err := config.ParseSetup(cfg)
	if err != nil {
		return nil, fmt.Errorf("wiring: setup stack: %w", err)
	}
	if !setupCfg.Enabled {
		return nil, fmt.Errorf("wiring: setup stack: PIPEROOM_SETUP_ENABLED is off; a setup room's registries are not built")
	}
	if d.Executor == nil {
		return nil, fmt.Errorf("wiring: setup stack: an executor is required (protocol.RunnerExecutor)")
	}
	if strings.TrimSpace(d.RoomID) == "" {
		return nil, fmt.Errorf("wiring: setup stack: the room id is required")
	}
	if d.CallRoom == nil {
		return nil, fmt.Errorf("wiring: setup stack: CallRoom is required (room.RoomID)")
	}
	if d.Clients == nil {
		return nil, fmt.Errorf("wiring: setup stack: Clients is required (setupwire.Clients)")
	}
	if d.Class != "" && !validSetupClass(d.Class) {
		return nil, fmt.Errorf("wiring: setup stack: class %q is not local, shared or production", d.Class)
	}
	prov, err := owners.Provisional(d.Request)
	if err != nil {
		return nil, fmt.Errorf("wiring: setup stack: %w", err)
	}
	if d.Pipeline != nil {
		// The caller's pipeline wins: it is the provisional pipeline with the
		// driver's later extensions, which Request cannot carry. Provisional
		// still ran above, so a malformed request refuses the same way with
		// or without it.
		prov = *d.Pipeline
	}
	c, err := d.Clients(d.Executor, d.RoomID)
	if err != nil {
		return nil, fmt.Errorf("wiring: setup stack: %w", err)
	}

	// The Wired the Q registration functions take, with the remote clients
	// in the write-capable fields (a remote write needs a proposal step in
	// the call's context, so nothing can write outside one) and the
	// provisional pipeline as its Owners.
	var registry clients.SchemaRegistryClient
	if !isNilIface(c.Registry) {
		registry = readOnlyRegistry{c.Registry}
	}
	w := &Wired{
		Source:   c.Source,
		Connect:  c.Connect,
		Kafka:    c.Kafka,
		Registry: registry,
		Owners:   &owners.Owners{Pipelines: []owners.Pipeline{prov}},
		// The Q2 source admin actions write through the remote source; the
		// Close the writers field's type requires is a no-op (the session
		// owns nothing to close). The P3 source and Kafka writers stay nil:
		// no runner op backs them, so their actions refuse before any I/O.
		write:   true,
		writers: writeClients{admin: remoteSourceAdmin{c.Admin}},
		// The request's targets are the hosts a connector created in this
		// room may name (design §3.2: hosts are targets).
		lifecycleHosts: setupTargetHosts(d.Request),
	}

	reg := tools.NewReadOnlyRegistry()
	areg := actions.NewRegistry()
	if err := RegisterTools(reg, areg, w); err != nil {
		return nil, fmt.Errorf("wiring: setup stack: %w", err)
	}
	if err := setuptools.Register(reg, setuptools.Deps{
		Runner:   d.Executor,
		RoomID:   d.RoomID,
		CallRoom: d.CallRoom,
		Class:    d.Class,
	}); err != nil {
		return nil, fmt.Errorf("wiring: setup stack: setup read tools: %w", err)
	}
	if d.SetupActions != nil {
		if err := d.SetupActions(areg, d.Executor, d.RoomID); err != nil {
			return nil, fmt.Errorf("wiring: setup stack: setup actions: %w", err)
		}
	}
	return &SetupStack{Tools: reg, Actions: areg}, nil
}

// remoteSourceAdmin is the remote source client as the Q2 source admin
// actions take it: the runner-backed admin methods (postgres.SourceAdminWriter)
// plus the Close the writers field's type requires. The session owns nothing
// to close.
type remoteSourceAdmin struct {
	postgres.SourceAdminWriter
}

func (remoteSourceAdmin) Close() {}

var _ postgres.SourceAdminWriter = remoteSourceAdmin{}

// setupTargetHosts lists the request's target addresses, the hosts a
// connector created in this room may name (registerLifecycle's AllowedHosts).
func setupTargetHosts(req events.SetupRequest) []string {
	var hosts []string
	for _, t := range req.Targets {
		if h := strings.TrimSpace(t.Address); h != "" {
			hosts = append(hosts, h)
		}
	}
	return hosts
}

// validSetupClass reports whether class is one of the three classes the
// runner protocol knows.
func validSetupClass(class string) bool {
	switch class {
	case protocol.ClassLocal, protocol.ClassShared, protocol.ClassProduction:
		return true
	}
	return false
}
