package evals

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// harnessCondition names what a scenario's pending_harness row waits for: a
// top-level declaration that appears in a package once the row has landed.
type harnessCondition struct {
	// Dir is the package directory under the repository root.
	Dir string
	// Symbol is the top-level func, type, var or const the row adds.
	Symbol string
}

// harnessConditions maps a pending_harness row to its landing condition. U1-10 is
// the agent's setup mode: wiring.SetupStack gives the agent the setup tools over
// the runner, which the two U1-19 compose scenarios need to read the compose file
// and the validator output. A symbol check over the source, not an import of
// internal/wiring, keeps evals free of an import cycle and of the wiring's
// dependency weight.
var harnessConditions = map[string]harnessCondition{
	"U1-10": {Dir: "internal/wiring", Symbol: "SetupStack"},
}

// Pending reports whether the scenario waits for a harness row (pending_harness).
func (s Scenario) Pending() bool { return s.PendingHarness != "" }

// SplitPending returns the scenarios a run replays and the names of those held
// out by pending_harness.
func SplitPending(scs []Scenario) (run []Scenario, pending []string) {
	for _, sc := range scs {
		if sc.Pending() {
			pending = append(pending, sc.Name)
			continue
		}
		run = append(run, sc)
	}
	return run, pending
}

// PendingNames returns the names of the scenarios that carry the marker.
func PendingNames(scs []Scenario) []string {
	var out []string
	for _, sc := range scs {
		if sc.Pending() {
			out = append(out, sc.Name)
		}
	}
	return out
}

// FindRepoRoot walks up from start to the directory holding go.mod; "" when none.
func FindRepoRoot(start string) string {
	dir, err := filepath.Abs(start)
	if err != nil {
		return ""
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return ""
		}
		dir = parent
	}
}

// HarnessLanded reports whether the row's condition holds under root: the symbol
// is declared at the top level of a non-test file of the condition's package.
func HarnessLanded(root, row string) (bool, error) {
	c, ok := harnessConditions[row]
	if !ok {
		return false, fmt.Errorf("evals: pending_harness %q names no known row (known: %s)", row, strings.Join(knownRows(), ", "))
	}
	dir := filepath.Join(root, filepath.FromSlash(c.Dir))
	ents, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, fmt.Errorf("evals: pending_harness %s: read %s: %w", row, c.Dir, err)
	}
	fset := token.NewFileSet()
	for _, e := range ents {
		n := e.Name()
		if e.IsDir() || !strings.HasSuffix(n, ".go") || strings.HasSuffix(n, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, filepath.Join(dir, n), nil, parser.SkipObjectResolution)
		if err != nil {
			return false, fmt.Errorf("evals: pending_harness %s: parse %s: %w", row, n, err)
		}
		for _, d := range f.Decls {
			if declares(d, c.Symbol) {
				return true, nil
			}
		}
	}
	return false, nil
}

func declares(d ast.Decl, sym string) bool {
	switch d := d.(type) {
	case *ast.FuncDecl:
		return d.Recv == nil && d.Name.Name == sym
	case *ast.GenDecl:
		for _, sp := range d.Specs {
			switch sp := sp.(type) {
			case *ast.TypeSpec:
				if sp.Name.Name == sym {
					return true
				}
			case *ast.ValueSpec:
				for _, n := range sp.Names {
					if n.Name == sym {
						return true
					}
				}
			}
		}
	}
	return false
}

func knownRows() []string {
	rows := make([]string, 0, len(harnessConditions))
	for r := range harnessConditions {
		rows = append(rows, r)
	}
	sort.Strings(rows)
	return rows
}

// CheckPendingHarness refuses a marker whose row has landed under root (the
// scenario must then run and be scored; remove the marker) and a marker that names
// an unknown row. An empty root checks only the row names.
func CheckPendingHarness(scs []Scenario, root string) error {
	var errs []string
	for _, sc := range scs {
		if !sc.Pending() {
			continue
		}
		if root == "" {
			if _, ok := harnessConditions[sc.PendingHarness]; !ok {
				errs = append(errs, fmt.Sprintf("%s: pending_harness %q names no known row (known: %s)", sc.Name, sc.PendingHarness, strings.Join(knownRows(), ", ")))
			}
			continue
		}
		landed, err := HarnessLanded(root, sc.PendingHarness)
		if err != nil {
			errs = append(errs, fmt.Sprintf("%s: %v", sc.Name, err))
			continue
		}
		if landed {
			c := harnessConditions[sc.PendingHarness]
			errs = append(errs, fmt.Sprintf("%s: pending_harness %s, but %s now declares %s: remove the marker so the scenario runs", sc.Name, sc.PendingHarness, c.Dir, c.Symbol))
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("evals: %s", strings.Join(errs, "; "))
	}
	return nil
}
