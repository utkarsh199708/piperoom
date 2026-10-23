package setupactions

import (
	"errors"
	"testing"

	"piperoom/internal/actions"
	"piperoom/internal/setup/plan"
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
