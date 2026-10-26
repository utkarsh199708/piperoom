package main

import (
	"context"
	"strings"
	"sync"
	"testing"

	"piperoom/internal/config"
	"piperoom/internal/events"
	"piperoom/internal/room"
	"piperoom/internal/setup/protocol"
	setuptools "piperoom/internal/setup/tools"
)

// countingRunner is a paired runner that answers nothing; it counts the ops
// it was asked for, so a test can show building a stack sends none.
type countingRunner struct {
	mu  sync.Mutex
	ops int
}

func (c *countingRunner) Execute(context.Context, string, protocol.OpRequest) (protocol.OpResult, error) {
	c.mu.Lock()
	c.ops++
	c.mu.Unlock()
	return protocol.OpResult{}, protocol.ErrNoRunner
}

func setupStackInput(ex protocol.RunnerExecutor) room.SetupStackInput {
	return room.SetupStackInput{
		RoomID:   "r_setup",
		Executor: ex,
		Request: events.SetupRequest{
			PipelineID: "shipments", Title: "Shipments", SourceKind: "postgres", Target: "compose",
			TopicPrefix: "shipments", Tables: []string{"public.shipments"}, Connectors: []string{"shipments-src"},
			Targets: []events.SetupTarget{{Kind: "postgres", Address: "db.acme:5432", Class: "shared"}},
		},
	}
}

// The server passes a setup stack builder only with setup on and write mode
// on; with both on it builds the real stack (the setup read tools and the
// setup actions over the paired runner) and sends nothing while it does.
func TestSetupStackBuilderGate(t *testing.T) {
	cfg := config.Config{SetupEnabled: "on", WriteMode: "on", SecretsDir: t.TempDir()}
	if setupStackBuilder(cfg, false, true) != nil {
		t.Fatal("a builder with setup off")
	}
	if setupStackBuilder(cfg, true, false) != nil {
		t.Fatal("a builder with write mode off")
	}
	build := setupStackBuilder(cfg, true, true)
	if build == nil {
		t.Fatal("no builder with setup and write mode on")
	}
	ex := &countingRunner{}
	stack, err := build(context.Background(), setupStackInput(ex))
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if stack.Tools == nil || !stack.Tools.ReadOnly() {
		t.Fatal("the setup stack's tools are not a read-only registry")
	}
	for _, name := range setuptools.ReadToolNames() {
		if _, ok := stack.Tools.Get(name); !ok {
			t.Errorf("setup read tool %s missing from the stack", name)
		}
	}
	for _, name := range []string{"compose_write", "compose_up", "compose_down"} {
		if _, ok := stack.Actions.Get(name); !ok {
			t.Errorf("setup action %s missing from the stack", name)
		}
	}
	if ex.ops != 0 {
		t.Fatalf("building the stack sent %d ops to the runner", ex.ops)
	}
}

// The wiring gate holds even if a builder exists: with the config's write
// mode or setup off, OpenSetupStack refuses and no stack is returned.
func TestSetupStackBuilderRefusesOffConfig(t *testing.T) {
	for name, cfg := range map[string]config.Config{
		"write mode off": {SetupEnabled: "on", WriteMode: "off", SecretsDir: t.TempDir()},
		"setup off":      {SetupEnabled: "off", WriteMode: "on"},
	} {
		build := setupStackBuilder(cfg, true, true)
		ex := &countingRunner{}
		stack, err := build(context.Background(), setupStackInput(ex))
		if err == nil || !strings.Contains(err.Error(), "is off") {
			t.Fatalf("%s: err = %v, want the wiring gate's refusal", name, err)
		}
		if stack.Tools != nil || stack.Actions != nil || ex.ops != 0 {
			t.Fatalf("%s: a refused build returned a stack or reached the runner", name)
		}
	}
	// Bad input is refused too, before anything is built.
	cfg := config.Config{SetupEnabled: "on", WriteMode: "on", SecretsDir: t.TempDir()}
	in := setupStackInput(nil)
	if _, err := setupStackBuilder(cfg, true, true)(context.Background(), in); err == nil {
		t.Fatal("a build without an executor succeeded")
	}
}
