package wiring

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// rawClientAllow lists the only places that may read Wired.Connect or
// Wired.Registry directly: the read-only adapters, the action registry
// helper, and assemble, which sets the fields. The actions.Register call in
// RegisterTools is allowed separately (see rawClientUses).
var rawClientAllow = map[string]bool{
	"readConnect": true,
	// Q1d: readConnect without the Q-wave reads, for the actions with
	// write mode off; it wraps the client exactly as readConnect did.
	"readConnectBase": true,
	"readRegistry":    true,
	"actionRegistry":  true,
	"assemble":        true,
	// X6c: hands the whole Connect client to connect_tune_throughput with
	// write mode on only (the read-only adapter otherwise), like the
	// actions.Register call in registerActions.
	"registerTuning": true,
}

// stubImporter gives every import an empty, complete package. The check only
// needs the types declared in package wiring itself (Wired and the variables
// of that type); everything imported degrades to an invalid type, whose type
// errors are ignored. It avoids compiling the module's dependencies from
// source (importer.ForCompiler with "source" does that and is slow).
type stubImporter struct{ pkgs map[string]*types.Package }

func (s stubImporter) Import(path string) (*types.Package, error) {
	if p, ok := s.pkgs[path]; ok {
		return p, nil
	}
	name := path[strings.LastIndex(path, "/")+1:]
	p := types.NewPackage(path, name)
	p.MarkComplete()
	s.pkgs[path] = p
	return p, nil
}

// rawClientUses type-checks the given package files (name -> source) and
// returns one line for every selector of the Connect or Registry field of
// Wired (a *Wired or Wired value, whatever the variable is called, including
// (*w).Connect and promoted access) outside rawClientAllow and outside a call
// to actions.Register. Composite-literal keys such as Wired{Connect: x} are
// not selectors and are not reported.
func rawClientUses(t *testing.T, files map[string]string) []string {
	t.Helper()
	fset := token.NewFileSet()
	var names []string
	for n := range files {
		names = append(names, n)
	}
	sort.Strings(names)
	var parsed []*ast.File
	for _, n := range names {
		f, err := parser.ParseFile(fset, n, files[n], 0)
		if err != nil {
			t.Fatalf("parse %s: %v", n, err)
		}
		parsed = append(parsed, f)
	}
	info := &types.Info{Selections: map[*ast.SelectorExpr]*types.Selection{}}
	conf := types.Config{
		Importer: stubImporter{pkgs: map[string]*types.Package{}},
		Error:    func(error) {}, // unresolved imports are expected
	}
	pkg, _ := conf.Check("piperoom/internal/wiring", fset, parsed, info)
	wired := pkg.Scope().Lookup("Wired")
	if wired == nil {
		t.Fatal("type Wired not found: the check would pass vacuously")
	}

	var out []string
	for _, f := range parsed {
		for _, d := range f.Decls {
			fn, ok := d.(*ast.FuncDecl)
			if !ok || rawClientAllow[fn.Name.Name] {
				continue
			}
			var inActions []*ast.CallExpr
			ast.Inspect(fn, func(n ast.Node) bool {
				if call, ok := n.(*ast.CallExpr); ok {
					if s, ok := call.Fun.(*ast.SelectorExpr); ok && s.Sel.Name == "Register" {
						if x, ok := s.X.(*ast.Ident); ok && x.Name == "actions" {
							inActions = append(inActions, call)
						}
					}
				}
				return true
			})
			ast.Inspect(fn, func(n ast.Node) bool {
				se, ok := n.(*ast.SelectorExpr)
				if !ok || (se.Sel.Name != "Connect" && se.Sel.Name != "Registry") {
					return true
				}
				sel := info.Selections[se]
				if sel == nil || sel.Kind() != types.FieldVal {
					return true
				}
				recv := sel.Recv()
				if p, ok := recv.(*types.Pointer); ok {
					recv = p.Elem()
				}
				if named, ok := recv.(*types.Named); !ok || named.Obj() != wired {
					return true
				}
				for _, c := range inActions {
					if c.Pos() <= se.Pos() && se.End() <= c.End() {
						return true
					}
				}
				out = append(out, fmt.Sprintf("%s: %s names the raw client (in %s)", fset.Position(se.Pos()), se.Sel.Name, fn.Name.Name))
				return true
			})
		}
	}
	return out
}

// realWiringSources reads every non-test .go file of package wiring.
func realWiringSources(t *testing.T) map[string]string {
	t.Helper()
	paths, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	files := map[string]string{}
	for _, p := range paths {
		if strings.HasSuffix(p, "_test.go") {
			continue
		}
		b, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		files[p] = string(b)
	}
	if len(files) == 0 {
		t.Fatal("no wiring sources found")
	}
	return files
}

// TestWiringNeverNamesRawClients covers what the reflection walk cannot see:
// dbtools keeps its Connect client in closures. Nothing in package wiring may
// read Wired.Connect or Wired.Registry except the allow-list above, so a tool
// or the topology builder cannot be handed a writer from any helper.
func TestWiringNeverNamesRawClients(t *testing.T) {
	if got := rawClientUses(t, realWiringSources(t)); len(got) > 0 {
		t.Errorf("raw client reads outside the allow-list:\n  %s", strings.Join(got, "\n  "))
	}
}

// The check must catch the ways a raw client can be reached; each mutation
// adds a file to the real sources and must be reported.
func TestRawClientCheckCatchesMutations(t *testing.T) {
	cases := map[string]string{
		"helper function": `package wiring
func dbDeps(w *Wired) dbtools.Deps { return dbtools.Deps{Connect: w.Connect} }`,
		"helper behind a rawConnect": `package wiring
func rawConnect(w *Wired) clients.ConnectClient { return w.Connect }`,
		"renamed receiver": `package wiring
func (x *Wired) leak() any { return x.Connect }`,
		"explicit deref": `package wiring
func leak(w *Wired) any { return (*w).Connect }`,
		"value, not pointer": `package wiring
func leak() any { var v Wired; return v.Registry }`,
		"registry in a closure": `package wiring
func leak(p *Wired) func() any { return func() any { return p.Registry } }`,
		"actions.Register is not a blanket pass": `package wiring
func leak(w *Wired) { _ = w.Connect; actions.Register(nil, actions.Deps{Connect: w.Connect}) }`,
	}
	for name, src := range cases {
		t.Run(name, func(t *testing.T) {
			files := realWiringSources(t)
			files["zz_mutation.go"] = src
			got := rawClientUses(t, files)
			var inMutation int
			for _, g := range got {
				if strings.Contains(g, "zz_mutation.go") {
					inMutation++
				}
			}
			if inMutation == 0 {
				t.Fatalf("mutation not reported; findings: %v", got)
			}
		})
	}

	t.Run("allowed forms pass", func(t *testing.T) {
		files := realWiringSources(t)
		files["zz_ok.go"] = `package wiring
func ok(w *Wired) { _ = Wired{Connect: nil}; actions.Register(nil, actions.Deps{Connect: w.Connect, Registry: actionRegistry(w)}) }`
		if got := rawClientUses(t, files); len(got) > 0 {
			t.Fatalf("allowed forms reported: %v", got)
		}
	})
}
