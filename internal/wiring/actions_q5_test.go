package wiring

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"piperoom/internal/actions"
	"piperoom/internal/config"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
)

// lifecycleActionNames are the Q5b/Q5c actions registerLifecycle adds, in
// actions.RegisterLifecycle's order.
var lifecycleActionNames = []string{
	actions.StopConnectorName, actions.ResetSourceOffsetsName, actions.CreateConnectorName, actions.DeleteConnectorName,
}

// TestLifecycleActionsWriteModeOn (Q5d): with write mode on every set lists
// the four lifecycle actions, each holding that set's own whole Connect
// client, never another set's: a stop approved for pipeline pa must not reach
// tenant b's Connect.
func TestLifecycleActionsWriteModeOn(t *testing.T) {
	f := newFakeOpener()
	cfg := config.Config{SourceDBURL: readerDSN, SourceDBWriteURL: writerDSN, WriteMode: "on"}
	st, err := openStacks(context.Background(), cfg, nil, loadOwners(t, threeSetYAML()), StackOptions{}, f.deps(env(twoTenantEnv())))
	if err != nil {
		t.Fatalf("openStacks: %v", err)
	}
	defer st.Close()
	for _, s := range st.All() {
		own, ok := s.Clients.Connect.(*spy)
		if !ok {
			t.Fatalf("set %q: Connect = %T, want the fake", s.Name, s.Clients.Connect)
		}
		for _, n := range lifecycleActionNames {
			a, ok := s.Actions.Get(n)
			if !ok {
				t.Errorf("set %q: %s not registered with write mode on", s.Name, n)
				continue
			}
			var held []*spy
			walkReachable([]actions.Action{a}, func(v reflect.Value) {
				if v.Type() == reflect.TypeOf(&spy{}) {
					held = append(held, (*spy)(v.UnsafePointer()))
				}
			})
			if len(held) == 0 {
				t.Errorf("set %q: %s holds no Connect client", s.Name, n)
			}
			for _, h := range held {
				if h != own {
					t.Errorf("set %q: %s holds the Connect client of %q, want its own (%q)", s.Name, n, h.label, own.label)
				}
			}
		}
	}
}

// assertLifecycleExecuteRefuses runs each lifecycle action's Execute in ctx
// (the room's pipeline) on connector and wants the refusal for want of a
// lifecycle writer, which every action asserts before any Connect call.
func assertLifecycleExecuteRefuses(t *testing.T, areg *actions.Registry, ctx context.Context, connector string) {
	t.Helper()
	params := map[string]json.RawMessage{
		actions.StopConnectorName:      json.RawMessage(`{"connector":"` + connector + `"}`),
		actions.ResetSourceOffsetsName: json.RawMessage(`{"connector":"` + connector + `"}`),
		actions.CreateConnectorName:    json.RawMessage(`{"connector":"` + connector + `","config":{"connector.class":"io.debezium.connector.postgresql.PostgresConnector"}}`),
		actions.DeleteConnectorName:    json.RawMessage(`{"connector":"` + connector + `"}`),
	}
	for _, n := range lifecycleActionNames {
		a, ok := areg.Get(n)
		if !ok {
			t.Errorf("%s not registered", n)
			continue
		}
		_, err := a.Execute(ctx, params[n])
		if err == nil || !strings.Contains(err.Error(), "connect.ConnectLifecycleWriter") {
			t.Errorf("%s: Execute = %v, want the refusal for want of a lifecycle writer", n, err)
		}
	}
}

// TestRegisterLifecycle: registerLifecycle adds the four actions in both
// write modes (P0 ruling #606): over the whole client with write mode on, over
// readConnect's read-only value with it off, where they hold no lifecycle
// writer and Execute refuses before any write. A set without a Connect client
// still registers them, and they refuse instead of panicking on a nil client.
func TestRegisterLifecycle(t *testing.T) {
	sp := &writeSpy{}
	life := lifecycleSpy{sp}
	p := owners.Pipeline{ID: "orders", Connectors: []string{"orders-connector"}}
	ctx, cancel := context.WithTimeout(tools.WithPipeline(context.Background(), p), 3*time.Second)
	defer cancel()

	off := actions.NewRegistry()
	if err := registerLifecycle(off, &Wired{write: false, Connect: life}); err != nil {
		t.Fatal(err)
	}
	var offNames []string
	for _, a := range off.All() {
		offNames = append(offNames, a.Name())
	}
	if strings.Join(offNames, ",") != strings.Join(lifecycleActionNames, ",") {
		t.Fatalf("write mode off registered %v, want %v", offNames, lifecycleActionNames)
	}
	if reachableTypes(off)[reflect.TypeOf(lifecycleSpy{})] {
		t.Error("write mode off: an action holds the whole Connect client")
	}
	reached := reachableTypes(off)
	if !reached[reflect.TypeOf(readOnlyConnectQ{})] && !reached[reflect.TypeOf(readOnlyConnectTasksQ{})] &&
		!reached[reflect.TypeOf(readOnlyConnectValidateQ{})] && !reached[reflect.TypeOf(readOnlyConnectValidateTasksQ{})] {
		t.Error("write mode off: the actions do not hold readConnect's value with the offsets read")
	}
	assertNoWriterReachable(t, "write mode off", off)
	assertLifecycleExecuteRefuses(t, off, ctx, "orders-connector")
	if len(sp.writes) != 0 {
		t.Fatalf("writes reached the client with write mode off: %v", sp.writes)
	}

	on := actions.NewRegistry()
	if err := registerLifecycle(on, &Wired{write: true, Connect: life}); err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, a := range on.All() {
		names = append(names, a.Name())
		if !reachableTypes(on)[reflect.TypeOf(lifecycleSpy{})] {
			t.Errorf("%s does not hold the whole Connect client with write mode on", a.Name())
		}
	}
	if strings.Join(names, ",") != strings.Join(lifecycleActionNames, ",") {
		t.Fatalf("registered %v, want %v", names, lifecycleActionNames)
	}

	none := actions.NewRegistry()
	if err := registerLifecycle(none, &Wired{write: true}); err != nil {
		t.Fatal(err)
	}
	for _, n := range lifecycleActionNames {
		a, ok := none.Get(n)
		if !ok {
			t.Fatalf("%s not registered without a Connect client", n)
		}
		if _, err := a.Execute(ctx, json.RawMessage(`{"connector":"orders-connector"}`)); err == nil {
			t.Errorf("%s executed without a Connect client", n)
		}
	}
	if len(sp.writes) != 0 {
		t.Fatalf("writes reached the client: %v", sp.writes)
	}
}
