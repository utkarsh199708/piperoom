package main

// The compose guard of U1-15: a compose file carrying any forbidden key is
// refused before compose_up runs. compose_write renders only template output,
// so the test builds the file the way a hand-edited one arrives: a valid
// render with one denied field added, on disk, then compose_up.
//
// The fake exec records every invocation: nothing may reach Docker on a
// refusal. The final control case (the valid file) proves the fake would see
// the command, so the "no call" assertions are meaningful.

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"gopkg.in/yaml.v3"

	"piperoom/internal/setup/ops"
	"piperoom/internal/setup/targets/compose"
)

// deniedKeys are the service fields the S1 validator refuses, with the rule
// each must be refused under.
var deniedKeys = map[string]struct {
	value any
	rule  string
}{
	"privileged":          {true, compose.RulePrivilege},
	"cap_add":             {[]string{"SYS_ADMIN"}, compose.RulePrivilege},
	"devices":             {[]string{"/dev/fuse"}, compose.RulePrivilege},
	"security_opt":        {[]string{"seccomp=unconfined"}, compose.RulePrivilege},
	"pid":                 {"host", compose.RulePrivilege},
	"ipc":                 {"host", compose.RulePrivilege},
	"userns_mode":         {"host", compose.RulePrivilege},
	"device_cgroup_rules": {[]string{"c 1:3 mr"}, compose.RulePrivilege},
	"cgroup":              {"host", compose.RulePrivilege},
	"cgroup_parent":       {"parent", compose.RulePrivilege},
	"sysctls":             {map[string]string{"net.ipv4.ip_forward": "1"}, compose.RulePrivilege},
	"runtime":             {"runc", compose.RulePrivilege},
	"volumes_from":        {[]string{"other"}, compose.RulePrivilege},
	"entrypoint":          {[]string{"/bin/sh"}, compose.RulePrivilege},
	"extends":             {map[string]string{"service": "kafka"}, compose.RuleExtends},
	"build":               {".", compose.RuleBuild},
	"env_file":            {[]string{".env"}, compose.RuleMount},
	"secrets":             {[]string{"s"}, compose.RuleMount},
	"configs":             {[]string{"c"}, compose.RuleMount},
	"network_mode":        {"host", compose.RuleNetwork},
}

func TestComposeForbiddenKeysRefusedBeforeComposeUp(t *testing.T) {
	for key, tc := range deniedKeys {
		t.Run(key, func(t *testing.T) {
			p := newComposeProject(t)
			p.writeValid(t)

			// A valid file first: compose_up reaches the (fake) docker.
			if err := p.up(t); err != nil {
				t.Fatalf("compose_up on the valid file: %v", err)
			}
			if len(p.calls) != 1 {
				t.Fatalf("compose_up on the valid file ran %d docker commands, want 1", len(p.calls))
			}
			p.calls = nil

			// One denied key: refused, and no docker command ran.
			p.rewrite(t, func(services map[string]any) {
				connect := services["connect"].(map[string]any)
				connect[key] = tc.value
			})
			err := p.up(t)
			if err == nil {
				t.Fatalf("compose_up accepted the file with %s", key)
			}
			if got := compose.RuleOf(err); got != tc.rule {
				t.Fatalf("refusal rule for %s = %q, want %q (%v)", key, got, tc.rule, err)
			}
			if len(p.calls) != 0 {
				t.Fatalf("compose_up ran docker for a refused file: %v", p.calls)
			}

			// Restore, so a later case or the cleanup reads the valid file.
			p.writeValid(t)
		})
	}
}

// composeProject is a rendered stack in a temp dir with a fake docker.
type composeProject struct {
	dir   string
	env   ops.Env
	reg   *ops.Registry
	calls []string
	doc   []byte
}

func newComposeProject(t *testing.T) *composeProject {
	t.Helper()
	dir := t.TempDir()
	p := &composeProject{dir: dir}
	confine, err := ops.OpenConfiner(dir)
	if err != nil {
		t.Fatalf("OpenConfiner: %v", err)
	}
	t.Cleanup(func() { _ = confine.Close() })
	fakeExec := func(_ context.Context, name string, args ...string) ([]byte, int, error) {
		raw, _ := json.Marshal(append([]string{name}, args...))
		p.calls = append(p.calls, string(raw))
		return nil, 0, nil
	}
	p.env = ops.Env{ProjectDir: dir, Confine: confine, Exec: fakeExec, Now: time.Now}
	reg, err := ops.New(ops.Config{ProjectDir: dir, Exec: fakeExec})
	if err != nil {
		t.Fatalf("ops.New: %v", err)
	}
	t.Cleanup(func() { _ = reg.Close() })
	if _, err := compose.Register(reg, compose.Options{}); err != nil {
		t.Fatalf("compose.Register: %v", err)
	}
	p.reg = reg

	// The stack the setup driver renders for a local Postgres source: kafka,
	// connect and the .env both the pinned argv and the postgres template
	// need. The password is the dev stack's test credential, never a secret.
	p.exec(t, "compose_write", compose.WriteParams{
		Templates: []string{compose.TemplateKafka, compose.TemplateConnect},
		Project:   "it-guard",
	})
	p.exec(t, "properties_write", compose.PropertiesParams{File: compose.EnvFileName, Entries: map[string]string{
		"POSTGRES_DB": "orders", "POSTGRES_USER": "piperoom",
	}})
	p.doc = p.read(t)
	return p
}

// exec runs one registered op with the project env and fails the test on an
// error (the setup steps, not the refusal under test).
func (p *composeProject) exec(t *testing.T, name string, params any) {
	t.Helper()
	op, ok := p.reg.Get(name)
	if !ok {
		t.Fatalf("%s is not registered", name)
	}
	raw, err := json.Marshal(params)
	if err != nil {
		t.Fatalf("marshalling %s params: %v", name, err)
	}
	if _, err := op.Execute(context.Background(), raw, p.env); err != nil {
		t.Fatalf("%s: %v", name, err)
	}
}

// up runs compose_up (the op under test) and returns its error.
func (p *composeProject) up(t *testing.T) error {
	t.Helper()
	op, ok := p.reg.Get("compose_up")
	if !ok {
		t.Fatal("compose_up is not registered")
	}
	raw, err := json.Marshal(compose.StackParams{})
	if err != nil {
		t.Fatal(err)
	}
	_, err = op.Execute(context.Background(), raw, p.env)
	return err
}

// writeValid writes the render compose_write produced.
func (p *composeProject) writeValid(t *testing.T) {
	t.Helper()
	p.rewriteDoc(t, p.doc)
}

// rewrite writes the valid render with mutate applied to its services.
func (p *composeProject) rewrite(t *testing.T, mutate func(services map[string]any)) {
	t.Helper()
	var root map[string]any
	if err := yaml.Unmarshal(p.doc, &root); err != nil {
		t.Fatalf("parsing the render: %v", err)
	}
	services, ok := root["services"].(map[string]any)
	if !ok {
		t.Fatalf("the render has no services mapping: %v", root)
	}
	mutate(services)
	bad, err := yaml.Marshal(root)
	if err != nil {
		t.Fatalf("marshalling the edited render: %v", err)
	}
	p.rewriteDoc(t, bad)
}

func (p *composeProject) rewriteDoc(t *testing.T, doc []byte) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(p.dir, compose.DefaultFile), doc, 0o644); err != nil {
		t.Fatal(err)
	}
}

func (p *composeProject) read(t *testing.T) []byte {
	t.Helper()
	doc, err := os.ReadFile(filepath.Join(p.dir, compose.DefaultFile))
	if err != nil {
		t.Fatal(err)
	}
	return doc
}
