package setupcmd

import (
	"testing"

	"piperoom/internal/events"
	"piperoom/internal/setup/ops"
	"piperoom/internal/setup/protocol"
)

func newCatalogue(t *testing.T) *ops.Registry {
	t.Helper()
	cat, err := ops.New(ops.Config{ProjectDir: t.TempDir()})
	if err != nil {
		t.Fatalf("ops.New: %v", err)
	}
	t.Cleanup(func() { _ = cat.Close() })
	return cat
}

// genericOps are what ops.New registers on its own, before any target driver:
// a registrar that registers no driver must leave exactly these.
var genericOps = []string{
	"env_detect", "file_read", "dir_list", "jar_list", "java_version",
	"http_get", "net_probe", "sink_probe", "file_write", "file_delete",
	"source_publication_tables",
}

func TestRegistrarHostTarget(t *testing.T) {
	cat := newCatalogue(t)
	reg := newTargetRegistrar(cat, nil)
	err := reg.Apply([]events.SetupTarget{
		{Kind: "postgres", Address: "db.example.com:5432", Class: protocol.ClassShared},
		{Kind: "host", Address: "127.0.0.1", Class: protocol.ClassShared},
	})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	want := map[string]string{
		"service_status": protocol.TierT0, "properties_read": protocol.TierT0,
		"journal_tail": protocol.TierT0, "properties_write": protocol.TierT1,
		"service_install": protocol.TierT2, "service_restart": protocol.TierT2,
	}
	for op, tier := range want {
		got, ok := cat.Floor(op)
		if !ok {
			t.Errorf("host target: %s is not in the catalogue", op)
		} else if got != tier {
			t.Errorf("host target: %s floor = %q, want %q", op, got, tier)
		}
	}
	if _, ok := cat.Floor("compose_up"); ok {
		t.Error("host target: the compose driver registered too")
	}
}

func TestRegistrarComposeDefault(t *testing.T) {
	for name, targets := range map[string][]events.SetupTarget{
		"no targets":  nil,
		"stack hosts": {{Kind: "kafka", Address: "localhost:9092"}, {Kind: "connect", Address: "http://localhost:8083"}},
	} {
		cat := newCatalogue(t)
		if err := newTargetRegistrar(cat, nil).Apply(targets); err != nil {
			t.Fatalf("%s: Apply: %v", name, err)
		}
		if _, ok := cat.Floor("compose_up"); !ok {
			t.Errorf("%s: the compose driver did not register", name)
		}
		if _, ok := cat.Floor("service_install"); ok {
			t.Errorf("%s: the host driver registered too", name)
		}
	}
}

func TestRegistrarManagedTargetRegistersNoDriver(t *testing.T) {
	cat := newCatalogue(t)
	err := newTargetRegistrar(cat, nil).Apply([]events.SetupTarget{
		{Kind: "aws", Address: "123456789012/eu-west-1", Class: protocol.ClassProduction},
	})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	for _, op := range []string{"compose_up", "service_install", "properties_write"} {
		if _, ok := cat.Floor(op); ok {
			t.Errorf("managed target: %s registered", op)
		}
	}
	for _, op := range genericOps {
		if _, ok := cat.Floor(op); !ok {
			t.Errorf("managed target: generic op %s is missing", op)
		}
	}
}

// TestRegistrarIdempotent: a reconnect re-sends the Welcome's targets; the
// second Apply must not register twice (a duplicate op fails Register) and
// must not switch drivers.
func TestRegistrarIdempotent(t *testing.T) {
	cat := newCatalogue(t)
	reg := newTargetRegistrar(cat, nil)
	host := []events.SetupTarget{{Kind: "host", Address: "127.0.0.1"}}
	if err := reg.Apply(host); err != nil {
		t.Fatalf("first Apply: %v", err)
	}
	if err := reg.Apply(host); err != nil {
		t.Fatalf("second Apply: %v", err)
	}
	if err := reg.Apply(nil); err != nil {
		t.Fatalf("Apply after a targetless Welcome: %v", err)
	}
	if _, ok := cat.Floor("compose_up"); ok {
		t.Error("a later Welcome switched the driver to compose")
	}
}

func TestChooseTargetDriver(t *testing.T) {
	cases := []struct {
		name    string
		targets []events.SetupTarget
		driver  string
		class   string
	}{
		{"host wins over stack kinds", []events.SetupTarget{{Kind: "kafka"}, {Kind: "host", Class: protocol.ClassShared}}, "host", protocol.ClassShared},
		{"highest host class", []events.SetupTarget{{Kind: "host", Class: protocol.ClassShared}, {Kind: "host", Class: protocol.ClassProduction}}, "host", protocol.ClassProduction},
		{"host wins over managed", []events.SetupTarget{{Kind: "aws"}, {Kind: "host"}}, "host", ""},
		{"managed registers none", []events.SetupTarget{{Kind: "kubernetes"}}, "", ""},
		{"compose stays local", []events.SetupTarget{{Kind: "postgres", Class: protocol.ClassProduction}}, "compose", ""},
		{"empty means compose", nil, "compose", ""},
	}
	for _, tc := range cases {
		driver, class := chooseTargetDriver(tc.targets)
		if driver != tc.driver || class != tc.class {
			t.Errorf("%s: chooseTargetDriver = (%q, %q), want (%q, %q)", tc.name, driver, class, tc.driver, tc.class)
		}
	}
}

func TestParsePluginHosts(t *testing.T) {
	cases := []struct {
		in   string
		want []string
	}{
		{"", nil},
		{" ", nil},
		{"repo1.maven.org", []string{"repo1.maven.org"}},
		{"a.example.com, b.example.com:8443 ,", []string{"a.example.com", "b.example.com:8443"}},
	}
	for _, tc := range cases {
		got := parsePluginHosts(tc.in)
		if len(got) != len(tc.want) {
			t.Errorf("parsePluginHosts(%q) = %v, want %v", tc.in, got, tc.want)
			continue
		}
		for i := range got {
			if got[i] != tc.want[i] {
				t.Errorf("parsePluginHosts(%q)[%d] = %q, want %q", tc.in, i, got[i], tc.want[i])
			}
		}
	}
}

// With --embedded the registrar puts the catalogue in embedded mode whatever
// the targets: the embedded ops are registered, no Compose or host driver is,
// and file_write is refused.
func TestRegistrarEmbedded(t *testing.T) {
	cat := newCatalogue(t)
	reg := newTargetRegistrar(cat, nil)
	reg.embedded = true
	if err := reg.Apply([]events.SetupTarget{{Kind: "host", Address: "127.0.0.1"}}); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	for _, op := range []string{ops.OpProjectInspect, ops.OpReferenceWrite, ops.OpEmbeddedLogRead} {
		if _, ok := cat.Floor(op); !ok {
			t.Errorf("embedded: %s is not registered", op)
		}
	}
	for _, op := range []string{"compose_up", "service_install"} {
		if _, ok := cat.Floor(op); ok {
			t.Errorf("embedded: %s registered", op)
		}
	}
	if got := cat.CheckTarget(protocol.OpRequest{Op: "file_write", Params: []byte(`{"path":"a.txt","content":"x"}`)}); got != protocol.RefuseTarget {
		t.Errorf("file_write in embedded mode: CheckTarget = %q, want %q", got, protocol.RefuseTarget)
	}
}
