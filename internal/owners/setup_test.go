package owners

import (
	"slices"
	"strings"
	"testing"
)

// setupBlockYAML wraps one pipeline with a setup: block.
func setupBlockYAML(setup string) string {
	return "pipelines:\n  - {id: orders, topic_prefix: pg, source_tables: [public.orders], connectors: [c1], setup: " + setup + "}\nlayers: {}\n"
}

func TestSetupBlock(t *testing.T) {
	// A complete block loads and round-trips through the accessor.
	o, err := Load(writeTemp(t, setupBlockYAML("{target: compose, runner_label: laptop, room_id: r1, watched_by: local}")))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	p, _ := o.Pipeline("orders")
	s, ok := p.Setup()
	if !ok || s.Target != "compose" || s.RunnerLabel != "laptop" || s.RoomID != "r1" || s.WatchedBy != "local" {
		t.Fatalf("Setup() = %+v, %v", s, ok)
	}
	if _, ok := (Pipeline{}).Setup(); ok {
		t.Fatal("a pipeline without a setup block must answer false")
	}

	// server is the other watched_by value.
	if _, err := Load(writeTemp(t, setupBlockYAML("{target: host, watched_by: server}"))); err != nil {
		t.Fatalf("watched_by server: %v", err)
	}
	// runner_label and room_id are optional.
	if _, err := Load(writeTemp(t, setupBlockYAML("{target: embedded, watched_by: local}"))); err != nil {
		t.Fatalf("minimal setup block: %v", err)
	}
}

func TestSetupBlockRefused(t *testing.T) {
	cases := []struct {
		name    string
		setup   string
		wantErr string
	}{
		{"unknown key", "{target: compose, watched: local}", `unknown field "watched"`},
		{"bad target", "{target: laptop, watched_by: local}", "setup: target"},
		{"empty target", "{target: '', watched_by: local}", "setup: target is required"},
		{"bad watched_by", "{target: compose, watched_by: team}", "watched_by"},
		{"empty watched_by", "{target: compose}", "watched_by"},
		{"unknown unreachable layer", "{target: strimzi, watched_by: server, unreachable_layers: [nope]}", "unreachable_layers"},
		{"repeated unreachable layer", "{target: strimzi, watched_by: server, unreachable_layers: [kafka-connect, kafka-connect]}", "listed twice"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Load(writeTemp(t, setupBlockYAML(tc.setup)))
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("err = %v, want %q", err, tc.wantErr)
			}
		})
	}
}

func TestTenantAdminsAndRepos(t *testing.T) {
	o, err := Load(writeTemp(t, `
tenants:
  a:
    members: [user:ann, group:team-a]
    admins: [user:bob, group:ops]
    repos: [connect-config, ' registry-mirror ']
pipelines:
  - {id: pa, tenant: a, topic_prefix: pa, source_tables: [public.a], connectors: [ca]}
layers: {}
`))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !o.IsTenantAdmin("a", "bob", nil) {
		t.Fatal("bob is a tenant admin")
	}
	if !o.IsTenantAdmin("a", "zed", []string{"ops"}) {
		t.Fatal("group ops is a tenant admin")
	}
	if o.IsTenantAdmin("a", "ann", nil) {
		t.Fatal("a member is not an admin")
	}
	if o.IsTenantAdmin("nope", "bob", nil) || o.IsTenantAdmin("a", "", nil) {
		t.Fatal("unknown tenant / empty user")
	}
	ta := o.Tenants["a"]
	if want := []string{"connect-config", "registry-mirror"}; !slices.Equal(ta.Repos, want) {
		t.Fatalf("repos = %v, want %v (trimmed)", ta.Repos, want)
	}
	// A tenant admin is named (the login gate and solo start must see them).
	names := o.NamedPrincipals()
	var strs []string
	for _, p := range names {
		strs = append(strs, p.String())
	}
	for _, want := range []string{"user:bob", "group:ops"} {
		if !slices.Contains(strs, want) {
			t.Fatalf("NamedPrincipals = %v, missing %s", strs, want)
		}
	}
}

func TestTenantAdminsReposRefused(t *testing.T) {
	head := "pipelines:\n  - {id: p1, tenant: a, topic_prefix: pg, source_tables: [public.t], connectors: [c1]}\nlayers: {}\n"
	cases := []struct {
		name    string
		yaml    string
		wantErr string
	}{
		{"admin bad principal", "tenants:\n  a: {admins: [grp:x]}\n" + head, "bad prefix"},
		{"admin unknown field", "tenants:\n  a: {admin: [user:x]}\n" + head, `unknown field "admin"`},
		{"repos empty entry", "tenants:\n  a: {repos: ['']}\n" + head, "repos[0]: empty entry"},
		{"repos bad id", "tenants:\n  a: {repos: ['two words']}\n" + head, "not a repo id"},
		{"repos duplicate", "tenants:\n  a: {repos: [x, x]}\n" + head, "lists \"x\" twice"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Load(writeTemp(t, tc.yaml))
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("err = %v, want %q", err, tc.wantErr)
			}
		})
	}
}

func TestSetupUnreachableLayers(t *testing.T) {
	o, err := Load(writeTemp(t, setupBlockYAML("{target: strimzi, watched_by: server, unreachable_layers: [kafka-connect]}")))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	p, _ := o.Pipeline("orders")
	s, _ := p.Setup()
	if !slices.Equal(s.UnreachableLayers, []string{"kafka-connect"}) {
		t.Fatalf("UnreachableLayers = %v", s.UnreachableLayers)
	}
}
