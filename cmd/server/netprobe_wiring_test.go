package main

import (
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"

	"piperoom/internal/actions"
	"piperoom/internal/agent"
	"piperoom/internal/config"
	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/store/memstore"
	"piperoom/internal/tools"
	"piperoom/internal/wiring"
)

// TestServerSetsRegisterNetProbe: the server's per-set Prepare hook wires
// net_probe (Q6b), so a CDC room's tool list has it as cmd/investigate's does.
func TestServerSetsRegisterNetProbe(t *testing.T) {
	var opts wiring.StackOptions
	stop := errors.New("stop after capturing the options")
	openStacks := func(_ context.Context, _ config.Config, _ *aws.Config, _ *owners.Owners, o wiring.StackOptions) (*wiring.Stacks, error) {
		opts = o
		return nil, stop
	}
	newEndpoint := func(context.Context, config.Config, *slog.Logger, *aws.Config, func(string) string) (*agent.Endpoint, error) {
		return nil, errors.New("must not build a model endpoint for the scripted agent")
	}
	_, err := wireAgent(context.Background(), openStacks, newEndpoint, wireAgentInput{
		cfg:        scriptedCfg(),
		log:        discardLog(),
		own:        &owners.Owners{},
		st:         memstore.New(),
		spendStore: memstore.New(),
		spend:      spendSettings{},
		usage:      &usageRelay{log: discardLog()},
		hook:       &roomClosedHook{},
		fakeAgent:  true,
		pmMode:     config.PostmortemModelAuto,
	})
	if !errors.Is(err, stop) {
		t.Fatalf("wireAgent = %v, want the stub's error", err)
	}
	if opts.Prepare == nil {
		t.Fatal("no Prepare hook")
	}
	w := &wiring.Wired{Owners: &owners.Owners{Pipelines: []owners.Pipeline{{ID: "orders", Kind: events.PipelineKindCDC}}}}
	opts.Prepare(w)
	if w.ProbeTools == nil || w.Rooms == nil {
		t.Fatalf("Prepare left ProbeTools %v, Rooms %v unset", w.ProbeTools != nil, w.Rooms != nil)
	}
	reg := tools.NewReadOnlyRegistry()
	if err := wiring.RegisterTools(reg, actions.NewRegistry(), w); err != nil {
		t.Fatalf("RegisterTools: %v", err)
	}
	if _, ok := reg.Get("net_probe"); !ok {
		t.Fatal("the server's CDC set has no net_probe")
	}
}
