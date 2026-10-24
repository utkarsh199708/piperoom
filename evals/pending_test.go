package evals

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// pendingPair are the scenarios held out until U1-10 (W4s, KAN-501).
var pendingPair = []string{"setup/compose-postgres-clean", "setup/compose-privileged-refused"}

func TestPendingHarnessMarkerOnThePair(t *testing.T) {
	scs, err := LoadScenarios("scenarios")
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, sc := range scs {
		if sc.Pending() {
			if sc.PendingHarness != "U1-10" {
				t.Errorf("%s: pending_harness = %q, want U1-10", sc.Name, sc.PendingHarness)
			}
			got = append(got, sc.Name)
		}
	}
	if strings.Join(got, ",") != strings.Join(pendingPair, ",") {
		t.Errorf("pending scenarios = %v, want %v", got, pendingPair)
	}
	run, held := SplitPending(scs)
	if len(held) != 2 || len(run) != len(scs)-2 {
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
