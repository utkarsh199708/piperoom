package main

import (
	"bytes"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"piperoom/internal/actions"
	"piperoom/internal/clients"
	"piperoom/internal/config"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
	"piperoom/internal/wiring"
)

func loadTenancyOwners(t *testing.T, y string) *owners.Owners {
	t.Helper()
	path := filepath.Join(t.TempDir(), "owners.yaml")
	if err := os.WriteFile(path, []byte(y), 0o600); err != nil {
		t.Fatal(err)
	}
	o, err := owners.Load(path)
	if err != nil {
		t.Fatalf("owners.Load: %v", err)
	}
	return o
}

const singleOwners = `pipelines:
  - {id: orders, topic_prefix: pg, source_tables: [public.orders], connectors: [c1]}
layers:
  postgres: priya
`

// teamsOwners: pa overrides every layer but warehouse, which it inherits
// from layers:, so the start warns about it.
const teamsOwners = `tenants:
  a: {members: [user:ann]}
pipelines:
  - id: pa
    tenant: a
    topic_prefix: pa
    source_tables: [public.a]
    connectors: [c1]
    owners:
      postgres: {approvers: [user:priya]}
      kafka-connect: {approvers: [user:priya]}
      schema-registry: {approvers: [user:priya]}
      kafka: {approvers: [user:priya]}
      consumer: {approvers: [user:priya]}
layers:
  warehouse: meera
`

type stubSource struct{ clients.SourceDB }

type stubConnect struct{ clients.ConnectClient }

type fakeFinder map[string]*wiring.Stack

func (f fakeFinder) For(id string) *wiring.Stack {
	if s, ok := f[id]; ok {
		return s
	}
	return f[""]
}

// TestStackAdapters: the room and watcher adapters hand out exactly the set
// of the asked pipeline; the empty set gives the watcher no clients.
func TestStackAdapters(t *testing.T) {
	// Only identity is compared; no method is called.
	srcA := &stubSource{}
	connA := &stubConnect{}
	a := &wiring.Stack{Name: "pipeline pa", Tools: tools.NewRegistry(), Actions: actions.NewRegistry(),
		Clients: &wiring.Wired{Source: srcA, Connect: connA}}
	none := &wiring.Stack{Name: wiring.NoStack, Tools: tools.NewRegistry(), Actions: actions.NewRegistry()}
	f := fakeFinder{"pa": a, "": none}

	rs := roomStacks(f)
	if got := rs("pa"); got.Tools != a.Tools || got.Actions != a.Actions {
		t.Fatal("room adapter: wrong set for pa")
	}
	if got := rs("zz"); got.Tools != none.Tools || got.Actions != none.Actions {
		t.Fatal("room adapter: unknown pipeline must get the fallback set")
	}
	wc := watchClients(f)
	if got := wc("pa"); got.Set != "pipeline pa" || got.Source != srcA || got.Connect != connA {
		t.Fatalf("watch adapter pa = %+v", got)
	}
	if got := wc("zz"); got.Set != wiring.NoStack || got.Source != nil || got.Connect != nil {
		t.Fatalf("watch adapter on the empty set = %+v, want no clients", got)
	}
}

func TestTenancySetup(t *testing.T) {
	single, teams := loadTenancyOwners(t, singleOwners), loadTenancyOwners(t, teamsOwners)
	cases := []struct {
		name     string
		mode     string
		own      *owners.Owners
		slack    bool
		want     string
		wantErr  string
		wantLogs []string
		noLogs   []string
	}{
		{name: "auto without tenants", mode: "", own: single, want: config.TenancySingle, wantLogs: []string{"mode=single", "filter=\"off"}},
		{name: "auto with tenants", mode: "auto", own: teams, want: config.TenancyTeams,
			wantLogs: []string{"mode=teams", "tenants=[a]", "pipeline=pa", "layers=[warehouse]"}, noLogs: []string{"Slack"}},
		{name: "teams with tenants and slack", mode: "teams", own: teams, slack: true, want: config.TenancyTeams, wantLogs: []string{"Slack mirror posts every tenant"}},
		{name: "single without tenants", mode: "single", own: single, want: config.TenancySingle},
		{name: "teams without tenants", mode: "teams", own: single, wantErr: "PIPEROOM_TENANCY_MODE=teams but the owners file has no tenants"},
		{name: "single with tenants", mode: "single", own: teams, wantErr: "PIPEROOM_TENANCY_MODE=single but the owners file has a tenants"},
		{name: "bad value", mode: "multi", own: single, wantErr: "PIPEROOM_TENANCY_MODE"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			log := slog.New(slog.NewTextHandler(&buf, nil))
			got, err := tenancySetup(config.Config{TenancyMode: tc.mode}, tc.own, tc.slack, log)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err = %v, want %q", err, tc.wantErr)
				}
				return
			}
			if err != nil || got != tc.want {
				t.Fatalf("tenancySetup = %q, %v; want %q", got, err, tc.want)
			}
			for _, w := range tc.wantLogs {
				if !strings.Contains(buf.String(), w) {
					t.Errorf("log lacks %q:\n%s", w, buf.String())
				}
			}
			for _, w := range tc.noLogs {
				if strings.Contains(buf.String(), w) {
					t.Errorf("log has %q:\n%s", w, buf.String())
				}
			}
		})
	}
}
