package main

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"

	"piperoom/internal/agent"
	"piperoom/internal/config"
	"piperoom/internal/owners"
	"piperoom/internal/room"
	"piperoom/internal/room/demo"
	"piperoom/internal/store"
	"piperoom/internal/tools/similarincidents"
	"piperoom/internal/topology"
	"piperoom/internal/wiring"
	"piperoom/internal/wiring/netwire"
)

// fakeAgentSetup reads PIPEROOM_FAKE_AGENT (config.ParseFakeAgent) and reports
// whether the server runs the scripted demo agent on its real stack instead of
// the real agent. It is read only outside --demo and --quickstart: --demo
// already runs the scripted agent over fake clients, and --quickstart runs the
// real agent on the in-memory store, so neither consults it (and neither
// refuses or warns for a PIPEROOM_FAKE_AGENT value). A scripted request that
// would reach real users or run fixes, or a typo, refuses the start
// (config.ParseFakeAgent names the variable).
func fakeAgentSetup(cfg config.Config, demoMode, quickstart bool, log *slog.Logger) (bool, error) {
	if demoMode || quickstart {
		return false, nil
	}
	on, err := config.ParseFakeAgent(cfg)
	if err != nil {
		return false, fmt.Errorf("server: %w", err)
	}
	if on {
		log.Warn("fake scripted agent: diagnoses are not real (PIPEROOM_FAKE_AGENT=scripted)")
	}
	return on, nil
}

// agentWiring is the model path and data stack wiring runWith hands to the room
// service for a non-demo server.
type agentWiring struct {
	run           agent.Runner
	write         agent.HandoffWriter
	writePM       agent.PostmortemWriter
	triage        agent.Triager
	triageTimeout time.Duration
	stacks        *wiring.Stacks
	stackFor      func(string) room.PipelineStack
	topo          topology.Builder
}

// wireAgentInput bundles the settings wireAgent needs beyond the two injected
// funcs (openStacks, newAgentEndpoint). It is a seam of runWith so the wiring
// is unit-tested without a stack or a database.
type wireAgentInput struct {
	cfg        config.Config
	log        *slog.Logger
	own        *owners.Owners
	st         store.Store
	spendStore store.SpendStore
	spend      spendSettings
	usage      *usageRelay
	hook       *roomClosedHook
	fakeAgent  bool
	pmMode     string
	writeOn    bool
	// live is the swappable endpoint of a standalone or quickstart server
	// (models.go); nil elsewhere, where a model that does not build refuses
	// the start.
	live *liveModel
}

// wireAgent wires a non-demo server. When fakeAgent is on it replaces every
// model path with the scripted demo agent (runner, hand-off and post-mortem, no
// triage and no model endpoint) while keeping the real tools, actions, topology
// and client sets (openStacks); otherwise the real model endpoint is built
// first, so a wrong provider, region, project or token budget stops the server
// before it opens any connection.
func wireAgent(
	ctx context.Context,
	openStacks func(ctx context.Context, cfg config.Config, awsCfg *aws.Config, own *owners.Owners, opts wiring.StackOptions) (*wiring.Stacks, error),
	newAgentEndpoint func(ctx context.Context, cfg config.Config, log *slog.Logger, awsCfg *aws.Config) (*agent.Endpoint, error),
	in wireAgentInput,
) (agentWiring, error) {
	// One AWS config for the process, shared by Bedrock and MSK IAM.
	awsCfg, err := wiring.LoadAWS(ctx, in.cfg)
	if err != nil {
		return agentWiring{}, fmt.Errorf("server: %w", err)
	}
	paths := agentWiring{}
	if in.fakeAgent {
		// No model, no API key: the demo's scripted story, and no triage.
		paths.run = demo.Run
		paths.write = demo.WriteHandoff
		if in.pmMode == config.PostmortemModelAuto {
			paths.writePM = demo.WritePostmortem
		}
	} else if in.live != nil {
		if paths, err = wireLive(ctx, in.live, in, awsCfg, newAgentEndpoint); err != nil {
			return agentWiring{}, err
		}
	} else {
		ep, err := newAgentEndpoint(ctx, in.cfg, in.log, awsCfg)
		if err != nil {
			return agentWiring{}, fmt.Errorf("server: %w", err)
		}
		// Spend limits are checked against the room database, so a restart does
		// not reset them.
		if err := ep.SetSpend(in.spend.options(in.spendStore, in.usage)); err != nil {
			return agentWiring{}, fmt.Errorf("server: %w", err)
		}
		paths.run = ep.Run
		paths.write = ep.WriteHandoff
		// With PIPEROOM_TRIAGE_PASS=off the pass still writes agent.triage
		// "failed"/"off" (no model call), so the UI never waits for one.
		paths.triage = ep.Triage
		_, _, paths.triageTimeout = ep.FirstLook()
		in.hook.setForget(ep)
	}

	// One client set per pipeline with connections: in owners.yaml, plus the
	// process-wide set when a pipeline uses it (docs/design/tenancy.md section
	// 5). Each room, the topology and the watcher reach only the set of their
	// own pipeline.
	sets, err := openStacks(ctx, in.cfg, awsCfg, in.own, wiring.StackOptions{
		// Defence in depth, no change in behaviour: with write mode off the
		// executor already runs nothing; this also keeps the write clients from
		// being built.
		ReadOnly: !in.writeOn,
		// similar_incidents reads past closed rooms from the room store; each
		// set's Owners holds only its own pipelines. net_probe is registered
		// through the set's hook (netwire: wiring cannot import nettools).
		Prepare: func(wired *wiring.Wired) {
			wired.Rooms = similarincidents.FromStore(in.st)
			wired.ProbeTools = netwire.Register
		},
	})
	if err != nil {
		return agentWiring{}, fmt.Errorf("server: open the data stack: %w", err)
	}
	paths.stacks = sets
	paths.stackFor = roomStacks(sets)
	paths.topo = sets
	return paths, nil
}
