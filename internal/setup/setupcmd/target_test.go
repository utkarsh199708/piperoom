package setupcmd

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	"piperoom/internal/events"
	"piperoom/internal/setup/ops"
	"piperoom/internal/setup/protocol"
)

// fakeExec is the catalogue's command runner: it records the argv and answers
// an empty success, so no docker or systemctl is ever started.
type fakeExec struct{ calls [][]string }

func (f *fakeExec) exec(_ context.Context, name string, args ...string) ([]byte, int, error) {
	f.calls = append(f.calls, append([]string{name}, args...))
	return nil, 0, nil
}

// newCatalogue builds the catalogue as Run does (ops.New), with a fake exec.
func newCatalogue(t *testing.T) (*ops.Registry, *fakeExec) {
	t.Helper()
	fe := &fakeExec{}
	reg, err := ops.New(ops.Config{ProjectDir: t.TempDir(), Exec: fe.exec})
	if err != nil {
		t.Fatalf("ops.New: %v", err)
	}
	t.Cleanup(func() { _ = reg.Close() })
	return reg, fe
}

func has(names []string, want ...string) bool {
	for _, w := range want {
		if !slices.Contains(names, w) {
			return false
		}
	}
	return true
}

// Before a driver is registered the runner's catalogue is the generic ops
// only: a compose op answers unknown_op. This is the bug (KAN-617).
func TestCatalogueWithoutDriverKnowsNoDriverOps(t *testing.T) {
	reg, _ := newCatalogue(t)
	if has(reg.Names(), "compose_ps") {
		t.Fatal("ops.New alone registered a compose op")
	}
	_, err := reg.Execute(context.Background(), protocol.OpRequest{Op: "compose_ps"})
	if !errors.Is(err, ops.ErrUnknownOp) {
		t.Fatalf("err = %v, want unknown op", err)
	}
}

// A room whose target is compose gets the Compose driver: its ops are listed
// and an op is run through the driver (it does not answer unknown_op), and the
// host driver is not registered.
func TestRegisterTargetCompose(t *testing.T) {
	reg, _ := newCatalogue(t)
	if err := targetRegistrar(reg)("compose", nil); err != nil {
		t.Fatalf("register compose: %v", err)
	}
	names := reg.Names()
	if !has(names, "compose_ps", "compose_up", "compose_down", "compose_write", "docker_info") {
		t.Errorf("compose ops missing from %v", names)
	}
	if has(names, "service_status") || has(names, "server_status") {
		t.Errorf("compose registered another driver's ops: %v", names)
	}
	_, err := reg.Execute(context.Background(), protocol.OpRequest{Op: "compose_ps", Params: []byte(`{}`)})
	if errors.Is(err, ops.ErrUnknownOp) {
		t.Fatalf("compose_ps answered unknown_op after the compose driver was registered")
	}
}

// A room whose target is host gets the host driver in place of compose (the
// two are exclusive in one registry).
func TestRegisterTargetHost(t *testing.T) {
	reg, _ := newCatalogue(t)
	if err := targetRegistrar(reg)("host", nil); err != nil {
		t.Fatalf("register host: %v", err)
	}
	names := reg.Names()
	if !has(names, "service_status", "service_install", "properties_write") {
		t.Errorf("host ops missing from %v", names)
	}
	if has(names, "compose_up") || has(names, "compose_ps") {
		t.Errorf("host room also registered the compose driver: %v", names)
	}
}

// dbzserver registers the Debezium Server driver next to Compose, whose ops
// start and read the server's stack.
func TestRegisterTargetDbzServer(t *testing.T) {
	reg, _ := newCatalogue(t)
	if err := targetRegistrar(reg)("dbzserver", nil); err != nil {
		t.Fatalf("register dbzserver: %v", err)
	}
	if names := reg.Names(); !has(names, "server_status", "server_properties_write", "compose_up") {
		t.Errorf("dbzserver ops missing from %v", names)
	}
}

// A target with no driver is refused by name, not left to unknown_op.
func TestRegisterTargetUnsupported(t *testing.T) {
	for _, target := range []string{"strimzi", "msk", "confluent", "aiven", "embedded", "nonsense"} {
		reg, _ := newCatalogue(t)
		err := targetRegistrar(reg)(target, nil)
		if err == nil || !strings.Contains(err.Error(), target) {
			t.Errorf("target %q: err = %v, want a refusal naming it", target, err)
		}
	}
}

// Registering the same target again (a pairing retried after a failed
// handshake) is a no-op; a different target is refused rather than mixing
// drivers in one registry.
func TestTargetRegistrarIdempotent(t *testing.T) {
	reg, _ := newCatalogue(t)
	register := targetRegistrar(reg)
	if err := register("compose", nil); err != nil {
		t.Fatal(err)
	}
	if err := register("compose", nil); err != nil {
		t.Errorf("second registration of the same target: %v", err)
	}
	if err := register("host", nil); err == nil {
		t.Error("a different target registered into the same registry")
	}
}

// The driver's class is the highest class of the room's host targets; a class
// on the database or Kafka never raises it.
func TestTargetClass(t *testing.T) {
	tests := []struct {
		name    string
		targets []events.SetupTarget
		want    string
	}{
		{"none", nil, ""},
		{"database only", []events.SetupTarget{{Kind: "postgres", Address: "db:5432", Class: protocol.ClassProduction}}, ""},
		{"host shared", []events.SetupTarget{{Kind: "host", Address: "h", Class: protocol.ClassShared}}, protocol.ClassShared},
		{"highest host wins", []events.SetupTarget{
			{Kind: "host", Address: "a", Class: protocol.ClassProduction},
			{Kind: "host", Address: "b", Class: protocol.ClassLocal},
		}, protocol.ClassProduction},
	}
	for _, tt := range tests {
		if got := targetClass(tt.targets); got != tt.want {
			t.Errorf("%s: class = %q, want %q", tt.name, got, tt.want)
		}
	}
}
