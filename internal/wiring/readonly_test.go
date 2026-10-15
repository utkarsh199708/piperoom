package wiring

import (
	"context"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"reflect"
	"testing"

	"piperoom/internal/actions"
	"piperoom/internal/clients"
	"piperoom/internal/config"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
)

func TestRegisterToolsRefusesAPlainRegistry(t *testing.T) {
	w := &Wired{Owners: &owners.Owners{}}
	err := RegisterTools(tools.NewRegistry(), actions.NewRegistry(), w)
	if !errors.Is(err, tools.ErrNotReadOnly) {
		t.Fatalf("RegisterTools(plain registry) = %v, want ErrNotReadOnly", err)
	}
}

// TestStacksBuildOnlyReadOnlyTools opens real stacks over spies, checks every
// registry is read-only with KindRead tools only, runs every tool once, and
// proves no tool reached a write method of any client.
func TestStacksBuildOnlyReadOnlyTools(t *testing.T) {
	own := loadOwners(t, twoTenantYAML(""))
	f := newFakeOpener()
	st, err := openStacks(context.Background(), config.Config{SourceDBURL: "src-process"}, nil, own, StackOptions{}, f.deps(env(twoTenantEnv())))
	if err != nil {
		t.Fatalf("openStacks: %v", err)
	}
	defer st.Close()

	stacks := append(st.All(), st.For("no-such-pipeline"))
	for _, s := range stacks {
		if !s.Tools.ReadOnly() {
			t.Errorf("stack %q: tool registry is not read-only", s.Name)
		}
		for _, tl := range s.Tools.All() {
			if tl.Kind() != tools.KindRead {
				t.Errorf("stack %q: tool %q has kind %q", s.Name, tl.Name(), tl.Kind())
			}
		}
	}
	a, b := f.spies["src-a"], f.spies["src-b"]
	runEach(t, st.For("pa").Tools, "pa", "a", a, b)
	runEach(t, st.For("pb").Tools, "pb", "b", b, a)
	for _, sp := range []*spy{a, b} {
		for _, m := range []string{"RestartTask", "RestartConnector", "SetCompatibility", "Register"} {
			if sp.saw(m) {
				t.Errorf("spy %s: a read tool called write method %s", sp.label, m)
			}
		}
	}
}

// writeSpy is a full Connect + Schema Registry client that records writes.
type writeSpy struct {
	writes []string
}

func (s *writeSpy) ListConnectors(context.Context) ([]string, error) { return []string{"c"}, nil }
func (s *writeSpy) Status(context.Context, string) (clients.ConnectorStatus, error) {
	return clients.ConnectorStatus{Name: "c"}, nil
}
func (s *writeSpy) Config(context.Context, string) (map[string]string, error) {
	return map[string]string{"k": "v"}, nil
}
func (s *writeSpy) RestartTask(context.Context, string, int) error {
	s.writes = append(s.writes, "RestartTask")
	return nil
}
func (s *writeSpy) RestartConnector(context.Context, string) error {
	s.writes = append(s.writes, "RestartConnector")
	return nil
}
func (s *writeSpy) Subjects(context.Context) ([]string, error)      { return []string{"s"}, nil }
func (s *writeSpy) Versions(context.Context, string) ([]int, error) { return []int{1}, nil }
func (s *writeSpy) Schema(context.Context, string, int) (clients.SchemaVersion, error) {
	return clients.SchemaVersion{Subject: "s"}, nil
}
func (s *writeSpy) Compatibility(context.Context, string) (string, bool, error) {
	return "BACKWARD", true, nil
}
func (s *writeSpy) SetCompatibility(context.Context, string, string) error {
	s.writes = append(s.writes, "SetCompatibility")
	return nil
}
func (s *writeSpy) Register(context.Context, string, string) (int, error) {
	s.writes = append(s.writes, "Register")
	return 1, nil
}

func TestReadOnlyClients(t *testing.T) {
	ctx := context.Background()
	sp := &writeSpy{}
	w := &Wired{Connect: sp, Registry: sp}
	c, r := readConnect(w), readRegistry(w)

	// Neither can be asserted back to the client that writes.
	if _, ok := c.(*writeSpy); ok {
		t.Fatal("readConnect returned the writing client itself")
	}
	if _, ok := r.(*writeSpy); ok {
		t.Fatal("readRegistry returned the writing client itself")
	}

	// Reads pass through.
	if got, err := c.ListConnectors(ctx); err != nil || len(got) != 1 {
		t.Fatalf("ListConnectors = %v, %v", got, err)
	}
	if st, err := c.Status(ctx, "c"); err != nil || st.Name != "c" {
		t.Fatalf("Status = %v, %v", st, err)
	}
	if cfg, err := c.Config(ctx, "c"); err != nil || cfg["k"] != "v" {
		t.Fatalf("Config = %v, %v", cfg, err)
	}
	if got, err := r.Subjects(ctx); err != nil || len(got) != 1 {
		t.Fatalf("Subjects = %v, %v", got, err)
	}
	if got, err := r.Versions(ctx, "s"); err != nil || len(got) != 1 {
		t.Fatalf("Versions = %v, %v", got, err)
	}
	if sv, err := r.Schema(ctx, "s", -1); err != nil || sv.Subject != "s" {
		t.Fatalf("Schema = %v, %v", sv, err)
	}
	if lvl, _, err := r.Compatibility(ctx, "s"); err != nil || lvl != "BACKWARD" {
		t.Fatalf("Compatibility = %v, %v", lvl, err)
	}

	// Writes refuse and never reach the client.
	for name, err := range map[string]error{
		"RestartTask":      c.RestartTask(ctx, "c", 0),
		"RestartConnector": c.RestartConnector(ctx, "c"),
		"SetCompatibility": r.SetCompatibility(ctx, "s", "NONE"),
	} {
		if !errors.Is(err, ErrReadOnly) {
			t.Errorf("%s = %v, want ErrReadOnly", name, err)
		}
	}
	if _, err := r.Register(ctx, "s", "{}"); !errors.Is(err, ErrReadOnly) {
		t.Errorf("Register = %v, want ErrReadOnly", err)
	}
	if len(sp.writes) != 0 {
		t.Fatalf("writes reached the client: %v", sp.writes)
	}

	// Not configured stays nil, so tools still say "not configured".
	if readConnect(&Wired{}) != nil || readRegistry(&Wired{}) != nil {
		t.Fatal("a missing client must stay nil")
	}
}

// TestActionsStillGetWriters guards the other half of the split: the actions
// must still be able to write after approval.
func TestActionsStillGetWriters(t *testing.T) {
	sp := &writeSpy{}
	w := &Wired{Connect: sp, Registry: sp, Owners: &owners.Owners{}}
	if got := actionRegistry(w); got != clients.SchemaRegistryClient(sp) {
		t.Fatalf("actionRegistry = %T, want the writing client", got)
	}
}

// TestRegisteredToolsHoldNoWriter registers every tool over a Wired whose
// Connect and Registry are the writing spy, then walks each tool's fields (and
// the topology builder they reach) looking for any value that can write. The
// spy is the only value that may carry a write method; readOnlyConnect and
// readOnlyRegistry are the wrappers that refuse. It fails if a call site in
// wiring.go hands the raw w.Connect / w.Registry to a tool or the builder.
//
// It cannot see a client captured by a closure (func values are opaque).
func TestRegisteredToolsHoldNoWriter(t *testing.T) {
	sp := &writeSpy{}
	w := &Wired{Connect: sp, Registry: sp, Owners: &owners.Owners{}}
	reg := tools.NewReadOnlyRegistry()
	if err := RegisterTools(reg, actions.NewRegistry(), w); err != nil {
		t.Fatalf("RegisterTools: %v", err)
	}

	connW := reflect.TypeOf((*clients.ConnectWriter)(nil)).Elem()
	regW := reflect.TypeOf((*clients.SchemaRegistryWriter)(nil)).Elem()
	allowed := map[reflect.Type]bool{
		reflect.TypeOf(readOnlyConnect{}):  true,
		reflect.TypeOf(readOnlyRegistry{}): true,
	}
	canWrite := func(ty reflect.Type) bool {
		if allowed[ty] {
			return false
		}
		if ty.Implements(connW) || ty.Implements(regW) {
			return true
		}
		for _, m := range []string{"Resume", "UpdateConfig"} {
			if _, ok := ty.MethodByName(m); ok {
				return true
			}
		}
		return false
	}

	seen := map[uintptr]bool{}
	var walk func(path string, v reflect.Value, depth int)
	walk = func(path string, v reflect.Value, depth int) {
		if !v.IsValid() || depth > 16 {
			return
		}
		if allowed[v.Type()] {
			return // the refusing wrappers hold the writer by design
		}
		// An interface-typed field is judged by the value inside it: its
		// static type (clients.ConnectClient) always has write methods.
		if v.Kind() != reflect.Interface && canWrite(v.Type()) {
			t.Errorf("%s holds %s, which can write", path, v.Type())
			return
		}
		switch v.Kind() {
		case reflect.Pointer:
			if v.IsNil() || seen[v.Pointer()] {
				return
			}
			seen[v.Pointer()] = true
			walk(path, v.Elem(), depth+1)
		case reflect.Interface:
			if !v.IsNil() {
				walk(path, v.Elem(), depth+1)
			}
		case reflect.Struct:
			for i := 0; i < v.NumField(); i++ {
				walk(path+"."+v.Type().Field(i).Name, v.Field(i), depth+1)
			}
		case reflect.Slice, reflect.Array:
			for i := 0; i < v.Len(); i++ {
				walk(path+"[]", v.Index(i), depth+1)
			}
		case reflect.Map:
			it := v.MapRange()
			for it.Next() {
				walk(path+"{}", it.Value(), depth+1)
			}
		}
	}
	all := reg.All()
	if len(all) == 0 {
		t.Fatal("no tools registered")
	}
	for _, tl := range all {
		walk("tool "+tl.Name(), reflect.ValueOf(tl), 0)
	}
	// The builder RegisterTools stored on w is what the db tools reach.
	walk("builder", reflect.ValueOf(w.Builder), 0)
}

// TestToolWiringNeverNamesRawClients covers what the reflection walk cannot see:
// dbtools keeps its Connect client in closures. RegisterTools and
// newTopologyBuilder must reach Connect and the Schema Registry only through
// readConnect and readRegistry, so neither function may mention w.Connect or
// w.Registry. (actionRegistry and the actions registration legitimately do.)
func TestToolWiringNeverNamesRawClients(t *testing.T) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "wiring.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	checked := map[string]bool{}
	for _, d := range f.Decls {
		fn, ok := d.(*ast.FuncDecl)
		if !ok || (fn.Name.Name != "RegisterTools" && fn.Name.Name != "newTopologyBuilder") {
			continue
		}
		checked[fn.Name.Name] = true
		ast.Inspect(fn, func(n ast.Node) bool {
			sel, ok := n.(*ast.SelectorExpr)
			if !ok || (sel.Sel.Name != "Connect" && sel.Sel.Name != "Registry") {
				return true
			}
			id, ok := sel.X.(*ast.Ident)
			if !ok || id.Name != "w" {
				return true
			}
			// The write actions are wired in RegisterTools too, and are the
			// one place a writer is allowed: they sit inside actions.Register.
			if fn.Name.Name == "RegisterTools" && inActionsRegister(fn, sel) {
				return true
			}
			t.Errorf("%s: %s.%s names a raw client; use readConnect/readRegistry for tools",
				fset.Position(sel.Pos()), id.Name, sel.Sel.Name)
			return true
		})
	}
	if len(checked) != 2 {
		t.Fatalf("found %v, want RegisterTools and newTopologyBuilder", checked)
	}
}

// inActionsRegister reports whether sel lies inside a call to actions.Register.
func inActionsRegister(fn *ast.FuncDecl, sel *ast.SelectorExpr) bool {
	found := false
	ast.Inspect(fn, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		if s, ok := call.Fun.(*ast.SelectorExpr); ok {
			if x, ok := s.X.(*ast.Ident); ok && x.Name == "actions" && s.Sel.Name == "Register" &&
				call.Pos() <= sel.Pos() && sel.End() <= call.End() {
				found = true
			}
		}
		return true
	})
	return found
}
