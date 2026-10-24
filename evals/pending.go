package evals

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// harnessCondition names what a scenario's pending_harness row waits for: a
// top-level declaration that appears in a package once the row has landed, or
// a setup runner op that appears in the op catalogue.
type harnessCondition struct {
	// Dir is the package directory under the repository root; with Op, the
	// root of the tree searched (every package under it).
	Dir string
	// Symbol is the top-level func, type, var or const the row adds.
	Symbol string
	// Op, when set instead of Symbol, is a setup runner op name: the row has
	// landed when a non-test package under Dir declares an ops.Op that names
	// itself Op (opDeclared).
	Op string
}

// harnessConditions maps a pending_harness row to its landing condition. U1-10 is
// the agent's setup mode: wiring.SetupStack gives the agent the setup tools over
// the runner, which the two U1-19 compose scenarios need to read the compose file
// and the validator output. A symbol check over the source, not an import of
// internal/wiring, keeps evals free of an import cycle and of the wiring's
// dependency weight.
//
// container-resolve-op is the follow-up to W4c (KAN-494) and KAN-526. KAN-526
// added nettools.ConnectResolveCheck, net_probe's connect_resolve check of
// what the Connect worker resolves the source host to, but it reads through a
// WorkerResolver that only the setup runner can back: Connect's REST API shows
// no resolution, and no runner op ran inside a container. Until the runner op
// container_resolve exists, a live room has no such check, so
// setup/db-unreachable-from-connect (whose cause is a stale hosts entry in the
// Connect container) would credit a capability the product lacks (lead ruling
// on #996). The row lands when an op named container_resolve is declared
// under internal/setup.
var harnessConditions = map[string]harnessCondition{
	"U1-10":                {Dir: "internal/wiring", Symbol: "SetupStack"},
	"container-resolve-op": {Dir: "internal/setup", Op: "container_resolve"},
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
// is declared at the top level of a non-test file of the condition's package,
// or, for an Op row, a package under the condition's directory declares the op.
func HarnessLanded(root, row string) (bool, error) {
	c, ok := harnessConditions[row]
	if !ok {
		return false, fmt.Errorf("evals: pending_harness %q names no known row (known: %s)", row, strings.Join(knownRows(), ", "))
	}
	dir := filepath.Join(root, filepath.FromSlash(c.Dir))
	if c.Op != "" {
		landed, err := opDeclared(dir, c.Op)
		if err != nil {
			return false, fmt.Errorf("evals: pending_harness %s: %w", row, err)
		}
		return landed, nil
	}
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

// opDeclared reports whether a non-test package under dir declares a setup
// runner op named op. An op is a type with the ops.Op method set (Name,
// Execute, Validate, ParamsSchema; a room tool or an action adapter lacks
// Execute or Validate) whose Name returns op, written either way the catalogue
// names its ops:
//
//   - Name() string { return "op" }, as container_inspect does; or
//   - Name() string { return o.field }, and a keyed composite literal of that
//     type sets field: "op", as compose_up does (&stackWriteOp{name: ...}).
//
// A string anywhere else (a tier table, a room tool's spec, a comment) does
// not count, so the row lands only when the runner can execute the op.
func opDeclared(dir, op string) (bool, error) {
	byPkg := map[string][]*ast.File{}
	fset := token.NewFileSet()
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			if os.IsNotExist(err) && path == dir {
				return filepath.SkipAll
			}
			return err
		}
		n := d.Name()
		if d.IsDir() {
			if path != dir && (strings.HasPrefix(n, ".") || strings.HasPrefix(n, "_") || n == "testdata") {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(n, ".go") || strings.HasSuffix(n, "_test.go") {
			return nil
		}
		f, err := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
		if err != nil {
			return fmt.Errorf("parse %s: %w", path, err)
		}
		pkg := filepath.Dir(path)
		byPkg[pkg] = append(byPkg[pkg], f)
		return nil
	})
	if err != nil {
		return false, err
	}
	for _, files := range byPkg {
		if pkgDeclaresOp(files, op) {
			return true, nil
		}
	}
	return false, nil
}

// opMethods is the ops.Op method set a type must have to count as an op.
var opMethods = []string{"Name", "Execute", "Validate", "ParamsSchema"}

// pkgDeclaresOp is opDeclared for one package's files.
func pkgDeclaresOp(files []*ast.File, op string) bool {
	methods := map[string]map[string]bool{}
	names := map[string]*ast.FuncDecl{}
	for _, f := range files {
		for _, d := range f.Decls {
			fd, ok := d.(*ast.FuncDecl)
			if !ok || fd.Recv == nil || len(fd.Recv.List) != 1 {
				continue
			}
			typ := recvTypeName(fd.Recv.List[0].Type)
			if typ == "" {
				continue
			}
			if methods[typ] == nil {
				methods[typ] = map[string]bool{}
			}
			methods[typ][fd.Name.Name] = true
			if fd.Name.Name == "Name" && fd.Type.Params.NumFields() == 0 && fd.Body != nil {
				names[typ] = fd
			}
		}
	}
	// fields[type] is the receiver field a Name method returns.
	fields := map[string]string{}
	for typ, fd := range names {
		if !hasAll(methods[typ], opMethods) {
			continue
		}
		res := singleReturn(fd)
		switch r := res.(type) {
		case *ast.BasicLit:
			if stringLit(r) == op {
				return true
			}
		case *ast.SelectorExpr:
			recv := fd.Recv.List[0]
			x, ok := r.X.(*ast.Ident)
			if ok && len(recv.Names) == 1 && x.Name == recv.Names[0].Name {
				fields[typ] = r.Sel.Name
			}
		}
	}
	if len(fields) == 0 {
		return false
	}
	found := false
	for _, f := range files {
		ast.Inspect(f, func(n ast.Node) bool {
			if found {
				return false
			}
			cl, ok := n.(*ast.CompositeLit)
			if !ok {
				return true
			}
			id, ok := cl.Type.(*ast.Ident)
			if !ok {
				return true
			}
			field, ok := fields[id.Name]
			if !ok {
				return true
			}
			for _, el := range cl.Elts {
				kv, ok := el.(*ast.KeyValueExpr)
				if !ok {
					continue
				}
				key, ok := kv.Key.(*ast.Ident)
				if !ok || key.Name != field {
					continue
				}
				if lit, ok := kv.Value.(*ast.BasicLit); ok && stringLit(lit) == op {
					found = true
					return false
				}
			}
			return true
		})
		if found {
			return true
		}
	}
	return false
}

// recvTypeName is the type name of a method receiver (T or *T); "" otherwise.
func recvTypeName(e ast.Expr) string {
	if s, ok := e.(*ast.StarExpr); ok {
		e = s.X
	}
	if id, ok := e.(*ast.Ident); ok {
		return id.Name
	}
	return ""
}

// singleReturn is the one result of a body that is exactly one return
// statement with one result; nil otherwise.
func singleReturn(fd *ast.FuncDecl) ast.Expr {
	if len(fd.Body.List) != 1 {
		return nil
	}
	ret, ok := fd.Body.List[0].(*ast.ReturnStmt)
	if !ok || len(ret.Results) != 1 {
		return nil
	}
	return ret.Results[0]
}

// stringLit is the value of a string literal; "" for any other literal.
func stringLit(l *ast.BasicLit) string {
	if l.Kind != token.STRING {
		return ""
	}
	v, err := strconv.Unquote(l.Value)
	if err != nil {
		return ""
	}
	return v
}

func hasAll(have map[string]bool, want []string) bool {
	for _, w := range want {
		if !have[w] {
			return false
		}
	}
	return true
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
			what := c.Symbol
			if c.Op != "" {
				what = "runner op " + c.Op
			}
			errs = append(errs, fmt.Sprintf("%s: pending_harness %s, but %s now declares %s: remove the marker so the scenario runs", sc.Name, sc.PendingHarness, c.Dir, what))
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("evals: %s", strings.Join(errs, "; "))
	}
	return nil
}
