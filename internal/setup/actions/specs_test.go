package setupactions

import (
	"errors"
	"testing"

	"piperoom/internal/actions"
	"piperoom/internal/setup/ops"
	"piperoom/internal/setup/plan"
	"piperoom/internal/setup/protocol"
	"piperoom/internal/setup/targets/compose"
)

func TestEnvironmentSpecsAreSafe(t *testing.T) {
	specs := EnvironmentSpecs()
	if len(specs) == 0 {
		t.Fatal("EnvironmentSpecs is empty")
	}
	seen := map[string]bool{}
	for _, spec := range specs {
		if seen[spec.Name] {
			t.Errorf("duplicate spec %q", spec.Name)
		}
		seen[spec.Name] = true
		if _, err := New(spec, Deps{}); err != nil {
			t.Errorf("%s: New: %v", spec.Name, err)
		}
		if rankOf(spec.Tier) < 1 {
			t.Errorf("%s: tier %q is not a write tier", spec.Name, spec.Tier)
		}
		if spec.Scope == nil {
			t.Errorf("%s: no scope rule", spec.Name)
		}
	}
	for _, name := range []string{
		"file_write", "file_delete", "compose_write", "properties_write",
		"compose_up", "compose_restart", "compose_down", "plugin_install",
	} {
		if !seen[name] {
			t.Errorf("EnvironmentSpecs is missing %s", name)
		}
	}
}

func TestRegisterAddsEveryEnvironmentAction(t *testing.T) {
	reg := actions.NewRegistry()
	if err := Register(reg, Deps{Runner: &fakeRunner{}, RoomID: "r_setup"}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	for _, spec := range EnvironmentSpecs() {
		a, ok := reg.Get(spec.Name)
		if !ok {
			t.Fatalf("%s did not register", spec.Name)
		}
		if _, ok := a.(actions.Receipted); !ok {
			t.Errorf("%s is not Receipted", spec.Name)
		}
		if _, ok := a.(actions.Scoped); !ok {
			t.Errorf("%s is not Scoped", spec.Name)
		}
		if _, ok := a.(actions.Tiered); !ok {
			t.Errorf("%s is not Tiered", spec.Name)
		}
		if _, ok := a.(actions.Preview); !ok {
			t.Errorf("%s has no Preview", spec.Name)
		}
	}
	if err := Register(reg, Deps{}); err == nil {
		t.Fatal("a second Register did not fail on the duplicates")
	}
	if err := Register(nil, Deps{}); err == nil {
		t.Fatal("Register(nil) = nil, want an error")
	}
}

func TestRegisterSpecsRefusesABadSpec(t *testing.T) {
	bad := testSpec()
	bad.Name = "Bad Name"
	err := RegisterSpecs(actions.NewRegistry(), Deps{}, []Spec{bad})
	if !errors.Is(err, ErrBadSpec) {
		t.Fatalf("RegisterSpecs = %v, want ErrBadSpec", err)
	}
}

func TestPreviewOpName(t *testing.T) {
	if got := PreviewOpName("file_write"); got != "preview_file_write" {
		t.Fatalf("PreviewOpName = %q", got)
	}
	for _, spec := range EnvironmentSpecs() {
		if spec.PreviewOp == spec.Name {
			t.Errorf("%s previews with its own write op", spec.Name)
		}
		if spec.PreviewOp == "" {
			continue
		}
		if err := checkSpec(spec); err != nil {
			t.Errorf("%s: %v", spec.Name, err)
		}
	}
}

func TestEnvironmentActionsTier(t *testing.T) {
	reg := actions.NewRegistry()
	if err := Register(reg, Deps{}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	fileWrite, _ := reg.Get("file_write")
	tiered := fileWrite.(actions.Tiered)
	params := []byte(`{"path":"notes.txt","content":"hello"}`)
	got, err := tiered.Tier(params, plan.ClassLocal)
	if err != nil || got != plan.TierT1 {
		t.Fatalf("Tier(file_write, local) = %q, %v", got, err)
	}
	got, err = tiered.Tier(params, plan.ClassProduction)
	if err != nil || got != plan.TierT3 {
		t.Fatalf("Tier(file_write, production) = %q, %v", got, err)
	}
}

// previewOpsPendingRunner lists the preview ops EnvironmentSpecs names that the
// runner catalogue does not register yet. Until a runner op exists for one,
// the runner refuses it as unknown_op and that action's Preview fails closed.
// Registering one of these ops must remove it from this list, and a spec that
// names any other unregistered preview op fails
// TestSpecOpsAreInTheRunnerCatalogue.
var previewOpsPendingRunner = map[string]bool{
	"preview_file_write":       true,
	"preview_file_delete":      true,
	"preview_compose_write":    true,
	"preview_properties_write": true,
	"preview_plugin_install":   true,
}

// runnerCatalogue builds the runner's real op catalogue the way the runner
// wiring does: ops.New plus the Compose target's ops.
func runnerCatalogue(t *testing.T) *ops.Registry {
	t.Helper()
	reg, err := ops.New(ops.Config{ProjectDir: t.TempDir()})
	if err != nil {
		t.Fatalf("ops.New: %v", err)
	}
	t.Cleanup(func() { _ = reg.Close() })
	if _, err := compose.Register(reg, compose.Options{}); err != nil {
		t.Fatalf("compose.Register: %v", err)
	}
	return reg
}

func TestSpecOpsAreInTheRunnerCatalogue(t *testing.T) {
	reg := runnerCatalogue(t)
	for _, spec := range EnvironmentSpecs() {
		floor, ok := reg.Floor(spec.Name)
		if !ok {
			t.Errorf("%s: the runner catalogue has no such op", spec.Name)
		} else if floor != spec.Tier {
			t.Errorf("%s: spec tier %q, runner floor %q", spec.Name, spec.Tier, floor)
		}
		if spec.PreviewOp == "" {
			continue
		}
		pfloor, registered := reg.Floor(spec.PreviewOp)
		switch {
		case previewOpsPendingRunner[spec.PreviewOp] && registered:
			t.Errorf("%s: preview op %q is now registered; remove it from previewOpsPendingRunner", spec.Name, spec.PreviewOp)
		case previewOpsPendingRunner[spec.PreviewOp]:
			// Known missing: the preview fails closed until the runner has it.
		case !registered:
			t.Errorf("%s: preview op %q is not in the runner catalogue nor in previewOpsPendingRunner", spec.Name, spec.PreviewOp)
		case pfloor != protocol.TierT0:
			t.Errorf("%s: preview op %q has floor %q, want T0", spec.Name, spec.PreviewOp, pfloor)
		case spec.PreviewOp != PreviewOpName(spec.Name) && spec.PreviewDecode == nil:
			// A borrowed read op answers its own Data shape, not an
			// events.PreviewResult.
			t.Errorf("%s: preview op %q needs a PreviewDecode", spec.Name, spec.PreviewOp)
		}
	}
	for op := range previewOpsPendingRunner {
		used := false
		for _, spec := range EnvironmentSpecs() {
			used = used || spec.PreviewOp == op
		}
		if !used {
			t.Errorf("previewOpsPendingRunner lists %q, which no spec names", op)
		}
	}
}
