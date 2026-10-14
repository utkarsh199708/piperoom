package wiring

import (
	"context"
	"errors"
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
