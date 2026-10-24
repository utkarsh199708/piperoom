package evals

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// pendingPair were held out until U1-10 (W4s, KAN-501). U1-10 declared
// wiring.SetupStack (#1012), so their markers are gone and they run.
var pendingPair = []string{"setup/compose-postgres-clean", "setup/compose-privileged-refused"}

// pendingRows are every held-out scenario and the row it waits for:
// setup/db-unreachable-from-connect until container-resolve-op (W4c, KAN-494,
// lead ruling R10; re-keyed by the KAN-526 ruling: its cause is observable
// only once the runner op container_resolve exists).
var pendingRows = map[string]string{
	"setup/db-unreachable-from-connect": "container-resolve-op",
}

func TestPendingHarnessMarkerOnThePair(t *testing.T) {
	scs, err := LoadScenarios("scenarios")
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	for _, sc := range scs {
		if sc.Pending() {
			got[sc.Name] = sc.PendingHarness
		}
	}
	for _, name := range pendingPair {
		if got[name] != "" {
			t.Errorf("%s: pending_harness = %q, want none: U1-10 has landed", name, got[name])
		}
	}
	if len(got) != len(pendingRows) {
		t.Errorf("pending scenarios = %v, want %v", got, pendingRows)
	}
	for name, row := range pendingRows {
		if got[name] != row {
			t.Errorf("%s: pending_harness = %q, want %q", name, got[name], row)
		}
	}
	run, held := SplitPending(scs)
	if len(held) != len(pendingRows) || len(run) != len(scs)-len(pendingRows) {
		t.Errorf("SplitPending: %d run, %d held of %d", len(run), len(held), len(scs))
	}
	for _, sc := range run {
		if sc.Pending() {
			t.Errorf("%s is pending but in the run set", sc.Name)
		}
	}
}

// fixtureRoot builds a repo root whose internal/wiring holds src as wiring.go.
func fixtureRoot(t *testing.T, src string) string {
	t.Helper()
	root := t.TempDir()
	dir := filepath.Join(root, "internal", "wiring")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if src != "" {
		if err := os.WriteFile(filepath.Join(dir, "wiring.go"), []byte(src), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

func TestCheckPendingHarness(t *testing.T) {
	pending := []Scenario{{Name: "setup/x", PendingHarness: "U1-10"}, {Name: "plain"}}
	cases := []struct {
		name    string
		src     string
		test    string
		wantErr string
	}{
		{"absent: the marker is accepted", "package wiring\n\nfunc Other() {}\n", "", ""},
		{"no wiring package yet", "", "", ""},
		{"a method of that name is not the stack", "package wiring\n\ntype T struct{}\n\nfunc (T) SetupStack() {}\n", "", ""},
		{"only a test file declares it", "package wiring\n", "package wiring\n\nfunc SetupStack() {}\n", ""},
		{"func declared: refused", "package wiring\n\nfunc SetupStack() {}\n", "", "remove the marker"},
		{"type declared: refused", "package wiring\n\ntype SetupStack struct{}\n", "", "remove the marker"},
		{"var declared: refused", "package wiring\n\nvar SetupStack = 1\n", "", "remove the marker"},
	}
	for _, c := range cases {
		root := fixtureRoot(t, c.src)
		if c.test != "" {
			if err := os.WriteFile(filepath.Join(root, "internal", "wiring", "wiring_test.go"), []byte(c.test), 0o644); err != nil {
				t.Fatal(err)
			}
		}
		err := CheckPendingHarness(pending, root)
		switch {
		case c.wantErr == "" && err != nil:
			t.Errorf("%s: %v", c.name, err)
		case c.wantErr != "" && (err == nil || !strings.Contains(err.Error(), c.wantErr) || !strings.Contains(err.Error(), "setup/x")):
			t.Errorf("%s: err = %v, want it to name the scenario and %q", c.name, err, c.wantErr)
		}
	}
	// An unmarked scenario is never refused, and an unknown row is.
	if err := CheckPendingHarness([]Scenario{{Name: "plain"}}, fixtureRoot(t, "package wiring\n\nfunc SetupStack() {}\n")); err != nil {
		t.Errorf("unmarked scenario refused: %v", err)
	}
	if err := CheckPendingHarness([]Scenario{{Name: "s", PendingHarness: "Z9"}}, fixtureRoot(t, "")); err == nil || !strings.Contains(err.Error(), "no known row") {
		t.Errorf("unknown row: err = %v", err)
	}
}

// The condition is real: while the repository's own internal/wiring has no
// SetupStack the pair stays marked, and once it has one the marker must go. This
// is the check -validate makes on the checked-in scenarios.
func TestRepositoryPendingMarkersMatchTheCondition(t *testing.T) {
	scs, err := LoadScenarios("scenarios")
	if err != nil {
		t.Fatal(err)
	}
	root := FindRepoRoot(".")
	if root == "" {
		t.Fatal("no go.mod above the evals directory")
	}
	if err := CheckPendingHarness(scs, root); err != nil {
		t.Fatal(err)
	}
}

// TestContainerResolveOpRowCondition: setup/db-unreachable-from-connect's
// marker (container-resolve-op, the KAN-526 ruling) is accepted while no
// package under internal/setup declares a runner op named container_resolve,
// and refused once one does, in either way the catalogue names its ops. A
// string that is not an op's name (a tier table, a room tool's spec, a test
// file, a type without the ops.Op methods) does not land the row, and
// nettools.ConnectResolveCheck alone no longer does.
func TestContainerResolveOpRowCondition(t *testing.T) {
	pending := []Scenario{{Name: "setup/db-unreachable-from-connect", PendingHarness: "container-resolve-op"}}
	root := t.TempDir()
	write := func(rel, src string) {
		t.Helper()
		path := filepath.Join(root, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(src), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	accepted := func(stage string) {
		t.Helper()
		if err := CheckPendingHarness(pending, root); err != nil {
			t.Errorf("%s: %v", stage, err)
		}
	}
	refused := func(stage string) {
		t.Helper()
		err := CheckPendingHarness(pending, root)
		if err == nil || !strings.Contains(err.Error(), "remove the marker") || !strings.Contains(err.Error(), "runner op container_resolve") {
			t.Errorf("%s: err = %v, want the marker refused", stage, err)
		}
	}

	// No internal/setup at all.
	accepted("no internal/setup")
	if err := CheckPendingHarness(pending, ""); err != nil {
		t.Errorf("row name check: %v", err)
	}
	// The resolver check itself does not land the row.
	write("internal/tools/nettools/resolve.go", "package nettools\n\nfunc ConnectResolveCheck() {}\n")
	// Decoys: a tier table, a room tool whose spec names the op, a type with
	// Name but not the op methods, a test file, a comment.
	write("internal/setup/plan/tier.go", `package plan

// container_resolve is T0.
var floors = map[string]string{"container_resolve": "T0"}
`)
	write("internal/setup/tools/read.go", `package tools

type spec struct{ name, op string }
type readTool struct{ spec spec }

func (t *readTool) Name() string { return t.spec.name }

var specs = []spec{{name: "container_resolve", op: "container_resolve"}}
`)
	write("internal/setup/targets/compose/half.go", `package compose

type halfOp struct{}

func (halfOp) Name() string { return "container_resolve" }
`)
	write("internal/setup/targets/compose/resolve_test.go", `package compose

type testOp struct{}

func (testOp) Name() string                 { return "container_resolve" }
func (testOp) Execute()                     {}
func (testOp) Validate()                    {}
func (testOp) ParamsSchema() map[string]any { return nil }
`)
	accepted("decoys only")

	// An op whose Name returns the literal, as container_inspect does.
	lit := `package compose

type containerResolveOp struct{}

func (containerResolveOp) Name() string                 { return "container_resolve" }
func (containerResolveOp) Execute()                     {}
func (containerResolveOp) Validate()                    {}
func (containerResolveOp) ParamsSchema() map[string]any { return nil }
`
	write("internal/setup/targets/compose/resolve.go", lit)
	refused("literal Name")
	if err := os.Remove(filepath.Join(root, "internal", "setup", "targets", "compose", "resolve.go")); err != nil {
		t.Fatal(err)
	}
	accepted("literal op removed")

	// An op named by a field of a keyed literal, as compose_up is.
	write("internal/setup/targets/compose/stack.go", `package compose

type stackOp struct{ name string }

func (o *stackOp) Name() string                 { return o.name }
func (o *stackOp) Execute()                     {}
func (o *stackOp) Validate()                    {}
func (o *stackOp) ParamsSchema() map[string]any { return nil }

var all = []any{&stackOp{name: "compose_up"}}
`)
	accepted("field-named op, another name")
	write("internal/setup/targets/compose/stack2.go", `package compose

var more = []any{&stackOp{name: "container_resolve"}}
`)
	refused("field-named op")
}

// The op check finds the ops the real catalogue declares in both forms, and
// not container_resolve yet, so the row cannot pass by never matching.
func TestOpDeclaredOnTheRealCatalogue(t *testing.T) {
	root := FindRepoRoot(".")
	if root == "" {
		t.Fatal("no go.mod above the evals directory")
	}
	dir := filepath.Join(root, "internal", "setup")
	for op, want := range map[string]bool{
		"container_inspect": true,  // Name() string { return "container_inspect" }
		"compose_up":        true,  // &stackWriteOp{name: "compose_up", ...}
		"compose_restart":   true,  // the scenario's fixing op
		"no_such_op":        false, // control
	} {
		got, err := opDeclared(dir, op)
		if err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Errorf("opDeclared(%s) = %v, want %v", op, got, want)
		}
	}
}
