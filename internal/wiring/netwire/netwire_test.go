package netwire_test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"slices"
	"strings"
	"testing"

	"piperoom/internal/actions"
	"piperoom/internal/agent"
	"piperoom/internal/clients"
	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
	"piperoom/internal/wiring"
	"piperoom/internal/wiring/netwire"
)

// writingConnect is a Connect client that can write; net_probe must never
// hold it.
type writingConnect struct{ writes *int }

func (writingConnect) ListConnectors(context.Context) ([]string, error) { return nil, nil }
func (writingConnect) Status(context.Context, string) (clients.ConnectorStatus, error) {
	return clients.ConnectorStatus{}, nil
}
func (writingConnect) Config(context.Context, string) (map[string]string, error) { return nil, nil }
func (c writingConnect) RestartTask(context.Context, string, int) error {
	*c.writes++
	return nil
}
func (c writingConnect) RestartConnector(context.Context, string) error {
	*c.writes++
	return nil
}

func cdcOwners() *owners.Owners {
	return &owners.Owners{Pipelines: []owners.Pipeline{{ID: "orders", Kind: events.PipelineKindCDC}}}
}

// TestRegisterThroughHook: a CDC set with the hook gets net_probe, after the
// worker tools, and only through the hook.
func TestRegisterThroughHook(t *testing.T) {
	for _, hook := range []bool{true, false} {
		w := &wiring.Wired{Owners: cdcOwners()}
		if hook {
			w.ProbeTools = netwire.Register
		}
		reg := tools.NewReadOnlyRegistry()
		if err := wiring.RegisterTools(reg, actions.NewRegistry(), w); err != nil {
			t.Fatalf("RegisterTools: %v", err)
		}
		var names []string
		for _, tl := range reg.All() {
			names = append(names, tl.Name())
		}
		i := slices.Index(names, "net_probe")
		if (i >= 0) != hook {
			t.Fatalf("hook %v: net_probe registered = %v (%v)", hook, i >= 0, names)
		}
		if hook && (i == 0 || names[i-1] != "connect_worker_logs") {
			t.Errorf("net_probe should follow connect_worker_logs: %v", names)
		}
	}
}

// TestNetProbeIsACDCTool: the agent's kind table names net_probe, so a CDC
// room offers it and a batch room does not.
func TestNetProbeIsACDCTool(t *testing.T) {
	kinds, known := agent.ToolPipelineKinds("net_probe")
	if !known || !slices.Equal(kinds, []string{events.PipelineKindCDC}) {
		t.Fatalf("ToolPipelineKinds(net_probe) = %v, %v; want cdc, known", kinds, known)
	}
}

// TestRegisterHoldsNoWriter: net_probe gets the read-only Connect value, never
// the writing client, and a hand-built set (no connection settings) offers
// no endpoint it was not given.
func TestRegisterHoldsNoWriter(t *testing.T) {
	writes := 0
	w := &wiring.Wired{Connect: writingConnect{writes: &writes}, Owners: cdcOwners()}
	reg := tools.NewReadOnlyRegistry()
	if err := netwire.Register(reg, w); err != nil {
		t.Fatal(err)
	}
	tl, ok := reg.Get("net_probe")
	if !ok {
		t.Fatal("net_probe not registered")
	}
	d := reflect.ValueOf(tl).Elem().FieldByName("d")
	c := d.FieldByName("Connect")
	if c.IsNil() {
		t.Fatal("net_probe got no Connect client")
	}
	if got := c.Elem().Type(); got == reflect.TypeOf(writingConnect{}) {
		t.Fatalf("net_probe holds the writing Connect client (%s)", got)
	}
	if d.FieldByName("Kafka").IsValid() && !d.FieldByName("Kafka").IsNil() {
		t.Fatal("a nil Kafka admin reached net_probe as a typed value")
	}

	ctx := tools.WithPipeline(context.Background(), cdcOwners().Pipelines[0])
	_, err := tl.Run(ctx, json.RawMessage(`{"target":"warehouse"}`))
	if !errors.Is(err, tools.ErrOutOfScope) || !strings.Contains(err.Error(), "no configured target") {
		t.Fatalf("net_probe over no settings = %v, want out of scope with no target", err)
	}
	if writes != 0 {
		t.Fatalf("net_probe wrote %d times", writes)
	}
}
