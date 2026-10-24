package evals

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// pendingPair are the scenarios held out until U1-10 (W4s, KAN-501).
var pendingPair = []string{"setup/compose-postgres-clean", "setup/compose-privileged-refused"}

// pendingRows are every held-out scenario and the row it waits for: the pair
// above. setup/db-unreachable-from-connect waited for W4c-resolver (W4c,
// KAN-494, lead ruling R10) until KAN-526 landed nettools.ConnectResolveCheck
// and removed its marker.
var pendingRows = map[string]string{
	"setup/compose-postgres-clean":     "U1-10",
	"setup/compose-privileged-refused": "U1-10",
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
		if got[name] != "U1-10" {
			t.Errorf("%s: pending_harness = %q, want U1-10", name, got[name])
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

// TestW4cResolverRowCondition: setup/db-unreachable-from-connect's marker
// (W4c-resolver, lead ruling R10) is accepted while internal/tools/nettools does
// not declare ConnectResolveCheck and refused once it does, so the scenario
// runs again as soon as a read can show what the Connect worker resolved.
func TestW4cResolverRowCondition(t *testing.T) {
	pending := []Scenario{{Name: "setup/db-unreachable-from-connect", PendingHarness: "W4c-resolver"}}
	root := t.TempDir()
	dir := filepath.Join(root, "internal", "tools", "nettools")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	src := filepath.Join(dir, "tool.go")
	if err := os.WriteFile(src, []byte("package nettools\n\nfunc Other() {}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := CheckPendingHarness(pending, root); err != nil {
		t.Errorf("before the resolver check lands: %v", err)
	}
	if err := CheckPendingHarness(pending, ""); err != nil {
		t.Errorf("row name check: %v", err)
	}
	if err := os.WriteFile(src, []byte("package nettools\n\nfunc ConnectResolveCheck() {}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	err := CheckPendingHarness(pending, root)
	if err == nil || !strings.Contains(err.Error(), "remove the marker") || !strings.Contains(err.Error(), "setup/db-unreachable-from-connect") {
		t.Errorf("after the resolver check lands: err = %v, want the marker refused", err)
	}
}
