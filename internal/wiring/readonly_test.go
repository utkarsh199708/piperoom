package wiring

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"piperoom/internal/actions"
	"piperoom/internal/clients"
	"piperoom/internal/clients/connect"
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
func (s *writeSpy) ValidateConfig(context.Context, string, map[string]string) (connect.ValidateResult, error) {
	return connect.ValidateResult{}, nil
}
func (s *writeSpy) PluginConfigDef(context.Context, string) ([]connect.ConfigKeyDef, error) {
	return []connect.ConfigKeyDef{{Name: "k"}}, nil
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
func (s *writeSpy) SubjectsIncludingDeleted(context.Context) ([]string, error) {
	return []string{"s", "gone"}, nil
}
func (s *writeSpy) VersionsIncludingDeleted(context.Context, string) ([]int, error) {
	return []int{1, 2}, nil
}

// validateOnlyConnect is a Connect client with the config validate call but
// not the plugin config definitions: connect_validate_config must stay
// unregistered over it.
type validateOnlyConnect struct{ clients.ConnectClient }

func (validateOnlyConnect) ValidateConfig(context.Context, string, map[string]string) (connect.ValidateResult, error) {
	return connect.ValidateResult{}, nil
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
	// The config validation pair (Q4b) passes through: connect_validate_config
	// finds it by a type assertion on its client, so the wrapper must keep it
	// when the client has it.
	vc, ok := c.(connectConfigValidator)
	if !ok {
		t.Fatalf("readConnect(%T) = %T, which hides the config validation pair", sp, c)
	}
	if _, err := vc.ValidateConfig(ctx, "c", map[string]string{"k": "v"}); err != nil {
		t.Fatalf("ValidateConfig = %v", err)
	}
	if defs, err := vc.PluginConfigDef(ctx, "c"); err != nil || len(defs) != 1 {
		t.Fatalf("PluginConfigDef = %v, %v", defs, err)
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

	// The deleted-state reads pass through too: kafkatools' registry tools
	// find them by a type assertion on their client (KAN-407), so the
	// wrapper must keep them when the client has them, or the deleted-state
	// path is unreachable in production.
	dr, ok := r.(deletedSubjectLister)
	if !ok {
		t.Fatalf("readRegistry(%T) = %T, which hides the deleted-state reads", sp, r)
	}
	if got, err := dr.SubjectsIncludingDeleted(ctx); err != nil || len(got) != 2 {
		t.Fatalf("SubjectsIncludingDeleted = %v, %v", got, err)
	}
	if got, err := dr.VersionsIncludingDeleted(ctx, "s"); err != nil || len(got) != 2 {
		t.Fatalf("VersionsIncludingDeleted = %v, %v", got, err)
	}

	// A registry without the deleted-state reads must not grow them through
	// the wrapper: the tools' assertion must fail exactly when it would fail
	// on the client itself.
	if plain := readRegistry(&Wired{Registry: noRegistry{}}); plain != nil {
		if _, ok := plain.(deletedSubjectLister); ok {
			t.Fatalf("readRegistry(noRegistry) = %T, which fakes the deleted-state reads", plain)
		}
	}

	// The same for the config validation pair: a client with ValidateConfig
	// but no PluginConfigDef stays unregistered for connect_validate_config
	// rather than offering the model a tool that always fails.
	if plain := readConnect(&Wired{Connect: validateOnlyConnect{sp}}); plain != nil {
		if _, ok := plain.(connectConfigValidator); ok {
			t.Fatalf("readConnect(%T) = %T, which fakes PluginConfigDef", validateOnlyConnect{}, plain)
		}
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

// readOnlyAdapterMethods is every exported method a read-only adapter may
// have: the reads it forwards and the writes that refuse with ErrReadOnly.
// The no-writer walks skip descending into an adapter (it holds the raw
// client in an unexported field by design), so its method set is the only
// surface a smuggled forwarding write could ship on; assertAdapterMethodSet
// fails on any method outside this list, so adding a method to an adapter
// means consciously adding it here too.
var readOnlyAdapterMethods = map[string]bool{
	// Forwarded reads.
	"ListConnectors": true, "Status": true, "Config": true,
	"ValidateConfig": true, "PluginConfigDef": true, "WorkerTasks": true,
	"Subjects": true, "Versions": true, "Schema": true, "Compatibility": true,
	"SubjectsIncludingDeleted": true, "VersionsIncludingDeleted": true,
	"CheckCompatibility": true,
	// Writes that refuse (ErrReadOnly, or errNoRegistry on noRegistry).
	"RestartTask": true, "RestartConnector": true,
	"SetCompatibility": true, "Register": true,
}

func assertAdapterMethodSet(t *testing.T, path string, ty reflect.Type) {
	t.Helper()
	for i := 0; i < ty.NumMethod(); i++ {
		if m := ty.Method(i).Name; !readOnlyAdapterMethods[m] {
			t.Errorf("%s: read-only adapter %s has method %s outside the known reads and refusing writes; a forwarding write would ship exactly here", path, ty, m)
		}
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
	// A client that validates configs gives the model connect_validate_config
	// (Q4b); the tool is read-only, so it belongs in this registry.
	if _, ok := reg.Get("connect_validate_config"); !ok {
		t.Error("connect_validate_config is not registered over a client with the config validation pair")
	}

	connW := reflect.TypeOf((*clients.ConnectWriter)(nil)).Elem()
	regW := reflect.TypeOf((*clients.SchemaRegistryWriter)(nil)).Elem()
	fixW := reflect.TypeOf((*clients.ConnectFixWriter)(nil)).Elem()
	allowed := map[reflect.Type]bool{
		reflect.TypeOf(readOnlyConnect{}):              true,
		reflect.TypeOf(readOnlyConnectTasks{}):         true,
		reflect.TypeOf(readOnlyConnectValidate{}):      true,
		reflect.TypeOf(readOnlyConnectValidateTasks{}): true,
		reflect.TypeOf(readOnlyRegistry{}):             true,
		reflect.TypeOf(readOnlyRegistryDeleted{}):      true,
	}
	canWrite := func(ty reflect.Type) bool {
		if allowed[ty] {
			return false
		}
		if ty.Implements(connW) || ty.Implements(regW) || ty.Implements(fixW) {
			return true
		}
		// Any one write method is enough, even on a type that implements
		// none of the writer interfaces in full.
		for _, m := range []string{"Resume", "UpdateConfig", "PauseConnector", "UpdateConnectorConfigKeys"} {
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
			// The refusing wrappers hold the writer in unexported fields by
			// design, so the walk does not descend into them — which makes
			// their own method set the only surface a smuggled forwarding
			// write could ship on. Check it instead of trusting the type.
			assertAdapterMethodSet(t, path, v.Type())
			return
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

// taskSpy is the writing spy plus the worker-task read (X6b).
type taskSpy struct{ *writeSpy }

func (taskSpy) WorkerTasks(context.Context) (map[string][]clients.TaskAssignment, error) {
	return map[string][]clients.TaskAssignment{"w1:8083": {{Connector: "c", Task: 0, State: "RUNNING"}}}, nil
}

// TestReadConnectKeepsWorkerTasks: connect_task_balance finds the worker-task
// read by a type assertion, so the read-only wrapper keeps it exactly when
// the client has it, and its writes still refuse.
func TestReadConnectKeepsWorkerTasks(t *testing.T) {
	ctx := context.Background()
	sp := &writeSpy{}
	c := readConnect(&Wired{Connect: taskSpy{sp}})
	wt, ok := c.(clients.WorkerTaskReader)
	if !ok {
		t.Fatalf("readConnect(taskSpy) = %T, which hides WorkerTasks", c)
	}
	if got, err := wt.WorkerTasks(ctx); err != nil || len(got["w1:8083"]) != 1 {
		t.Fatalf("WorkerTasks = %v, %v", got, err)
	}
	if err := c.RestartTask(ctx, "c", 0); !errors.Is(err, ErrReadOnly) || len(sp.writes) != 0 {
		t.Fatalf("RestartTask = %v, writes %v", err, sp.writes)
	}
	if _, ok := readConnect(&Wired{Connect: sp}).(clients.WorkerTaskReader); ok {
		t.Fatal("readConnect grew WorkerTasks on a client without it")
	}
}
