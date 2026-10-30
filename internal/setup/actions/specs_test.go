package setupactions

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"piperoom/internal/actions"
	"piperoom/internal/setup/ops"
	"piperoom/internal/setup/plan"
	"piperoom/internal/setup/protocol"
	"piperoom/internal/setup/targets/compose"
	"piperoom/internal/setup/targets/host"
	"piperoom/internal/tools"
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
		"service_install", "service_restart",
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

// previewPendingRunner lists the actions of EnvironmentSpecs whose preview op
// (PreviewOpName of the action) the runner catalogue does not register yet.
// Their specs leave PreviewOp empty, so Preview answers "no preview" without a
// runner round trip (no unknown_op refusal, no setup.runner_refused event).
// Once the runner registers one of these ops this test fails, and the spec is
// switched to PreviewOpName and removed from this list.
var previewPendingRunner = map[string]bool{
	"file_write":       true,
	"file_delete":      true,
	"compose_write":    true,
	"properties_write": true,
	"plugin_install":   true,
	"service_install":  true,
	"service_restart":  true,
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

// hostCatalogue is the host target's runner catalogue. It is a second
// registry because compose.Register and host.Register are exclusive in one
// (both name properties_write): the runner registers the one driver the
// Welcome's targets name.
func hostCatalogue(t *testing.T) *ops.Registry {
	t.Helper()
	reg, err := ops.New(ops.Config{ProjectDir: t.TempDir()})
	if err != nil {
		t.Fatalf("ops.New: %v", err)
	}
	t.Cleanup(func() { _ = reg.Close() })
	if _, err := host.Register(reg, host.Options{}); err != nil {
		t.Fatalf("host.Register: %v", err)
	}
	return reg
}

func TestSpecOpsAreInTheRunnerCatalogue(t *testing.T) {
	composeReg := runnerCatalogue(t)
	hostReg := hostCatalogue(t)
	// floorOf looks an op up in whichever target catalogue registers it;
	// properties_write is in both with one floor.
	floorOf := func(name string) (string, bool) {
		if f, ok := composeReg.Floor(name); ok {
			return f, true
		}
		return hostReg.Floor(name)
	}
	for _, spec := range EnvironmentSpecs() {
		floor, ok := floorOf(spec.Name)
		if !ok {
			t.Errorf("%s: the runner catalogue has no such op", spec.Name)
		} else if floor != spec.Tier {
			t.Errorf("%s: spec tier %q, runner floor %q", spec.Name, spec.Tier, floor)
		}
		_, ownRegistered := floorOf(PreviewOpName(spec.Name))
		if previewPendingRunner[spec.Name] {
			if spec.PreviewOp != "" {
				t.Errorf("%s: pending a runner preview op, but names preview op %q", spec.Name, spec.PreviewOp)
			}
			if ownRegistered {
				t.Errorf("%s: the runner now registers %q; set PreviewOp and remove it from previewPendingRunner", spec.Name, PreviewOpName(spec.Name))
			}
			continue
		}
		if spec.PreviewOp == "" {
			t.Errorf("%s: no preview op and not in previewPendingRunner", spec.Name)
			continue
		}
		pfloor, registered := floorOf(spec.PreviewOp)
		switch {
		case !registered:
			t.Errorf("%s: preview op %q is not in the runner catalogue", spec.Name, spec.PreviewOp)
		case pfloor != protocol.TierT0:
			t.Errorf("%s: preview op %q has floor %q, want T0", spec.Name, spec.PreviewOp, pfloor)
		case spec.PreviewOp != PreviewOpName(spec.Name) && spec.PreviewDecode == nil:
			// A borrowed read op answers its own Data shape, not an
			// events.PreviewResult.
			t.Errorf("%s: preview op %q needs a PreviewDecode", spec.Name, spec.PreviewOp)
		}
	}
	for name := range previewPendingRunner {
		found := false
		for _, spec := range EnvironmentSpecs() {
			found = found || spec.Name == name
		}
		if !found {
			t.Errorf("previewPendingRunner lists %q, which is not an environment action", name)
		}
	}
}

func TestPendingPreviewsMakeNoRunnerCall(t *testing.T) {
	inputs := map[string]json.RawMessage{
		"file_write":       json.RawMessage(`{"path":"notes.txt","content":"hi"}`),
		"file_delete":      json.RawMessage(`{"path":"notes.txt"}`),
		"compose_write":    json.RawMessage(`{"templates":["kafka-kraft"]}`),
		"properties_write": json.RawMessage(`{"file":".env","entries":{"A":"b"}}`),
		"plugin_install":   json.RawMessage(`{"release":"2.7.0","connector":"postgres"}`),
		"service_install":  json.RawMessage(`{"name":"connect","mode":"distributed"}`),
		"service_restart":  json.RawMessage(`{"name":"connect"}`),
	}
	for _, spec := range EnvironmentSpecs() {
		if !previewPendingRunner[spec.Name] {
			continue
		}
		f := &fakeRunner{}
		a, err := New(spec, Deps{Runner: f, RoomID: "r_setup"})
		if err != nil {
			t.Fatalf("New(%s): %v", spec.Name, err)
		}
		params := inputs[spec.Name]
		ctx := tools.WithPipeline(testContext(t, params), testPipeline())
		_, err = a.Preview(ctx, params)
		if err == nil || !strings.Contains(err.Error(), "no preview") {
			t.Errorf("%s: Preview = %v, want a no-preview error", spec.Name, err)
		}
		if got := len(f.recorded()); got != 0 {
			t.Errorf("%s: Preview sent %d ops, want 0", spec.Name, got)
		}
	}
}

func TestLongOpsGetASpecTimeout(t *testing.T) {
	want := map[string]time.Duration{"compose_up": longOpTimeout, "plugin_install": longOpTimeout}
	for _, spec := range EnvironmentSpecs() {
		if spec.Timeout != want[spec.Name] {
			t.Errorf("%s: timeout %s, want %s", spec.Name, spec.Timeout, want[spec.Name])
		}
	}
	// plugin_install must outlast the runner's own download budget
	// (targets/compose downloadTimeout, 5 minutes).
	if longOpTimeout <= 5*time.Minute || longOpTimeout > maxSpecTimeout {
		t.Fatalf("longOpTimeout = %s, want above 5m and within %s", longOpTimeout, maxSpecTimeout)
	}
}

// TestHostOpParamsRoundTrip: the host write specs' example params pass both
// the spec's own validation and the host op's Validate in the runner
// catalogue, and a bad service name is refused by both (row U8f: the server's
// view and the runner compute one answer from one params set).
func TestHostOpParamsRoundTrip(t *testing.T) {
	hostReg := hostCatalogue(t)
	cases := []struct {
		op      string
		good    json.RawMessage
		bad     json.RawMessage
		badWhat string
	}{
		{"service_install", json.RawMessage(`{"name":"connect","mode":"distributed"}`), json.RawMessage(`{"name":"Not A Name"}`), "an uppercase name"},
		{"service_restart", json.RawMessage(`{"name":"connect"}`), json.RawMessage(`{"name":"-bad"}`), "a leading dash"},
	}
	for _, tc := range cases {
		var spec *Spec
		for _, s := range EnvironmentSpecs() {
			if s.Name == tc.op {
				spec = &s
				break
			}
		}
		if spec == nil {
			t.Fatalf("%s: not in EnvironmentSpecs", tc.op)
		}
		a, err := New(*spec, Deps{})
		if err != nil {
			t.Fatalf("New(%s): %v", tc.op, err)
		}
		op, ok := hostReg.Get(tc.op)
		if !ok {
			t.Fatalf("%s: not in the host catalogue", tc.op)
		}
		if err := a.Validate(context.Background(), tc.good); err != nil {
			t.Errorf("%s: spec Validate(good) = %v", tc.op, err)
		}
		if err := op.Validate(tc.good, ops.Env{}); err != nil {
			t.Errorf("%s: runner Validate(good) = %v", tc.op, err)
		}
		if err := a.Validate(context.Background(), tc.bad); err == nil {
			t.Errorf("%s: spec Validate accepted %s", tc.op, tc.badWhat)
		}
		if err := op.Validate(tc.bad, ops.Env{}); err == nil {
			t.Errorf("%s: runner Validate accepted %s", tc.op, tc.badWhat)
		}
	}
}
