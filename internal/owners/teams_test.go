package owners

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const teamsPipeline = `
pipelines:
  - id: orders
    topic_prefix: pg
    source_tables: [public.orders]
    connectors: [pg-orders]
`

func TestTeamsLoadAndTeamsFor(t *testing.T) {
	o, err := Load(writeTemp(t, `
teams:
  - id: checkout
    slack_channel: C0123ABCD
    owns: [checkout-api, orders-writer]
  - id: platform
    slack_channel: G0456EFGH
    owns: [msk]
`+teamsPipeline))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	got := o.TeamsFor("orders")
	if len(got) != 2 || got[0].ID != "checkout" || got[1].ID != "platform" || got[0].Owns[1] != "orders-writer" {
		t.Fatalf("TeamsFor(orders) = %+v", got)
	}
	got[0].Owns[0] = "changed"
	if o.Teams[0].Owns[0] != "checkout-api" {
		t.Fatal("TeamsFor must return copies")
	}
	if o.TeamsFor("unknown") != nil {
		t.Fatal("an unknown pipeline sees no team")
	}
	if (&Owners{}).TeamsFor("orders") != nil || (*Owners)(nil).TeamsFor("orders") != nil {
		t.Fatal("no teams: list means no team")
	}
}

func TestTeamsTenantFiltered(t *testing.T) {
	o, err := Load(writeTemp(t, twoTenants()+`
teams:
  - id: team-a
    slack_channel: C0123ABCD
    owns: [a-api]
    tenant: a
  - id: team-b
    slack_channel: C0456EFGH
    owns: [b-api]
    tenant: b
`))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got := o.TeamsFor("pa"); len(got) != 1 || got[0].ID != "team-a" {
		t.Fatalf("TeamsFor(pa) = %+v, want only tenant a's team", got)
	}
	if got := o.TeamsFor("pb"); len(got) != 1 || got[0].ID != "team-b" {
		t.Fatalf("TeamsFor(pb) = %+v, want only tenant b's team", got)
	}
}

func TestTeamsLoadErrors(t *testing.T) {
	for _, tc := range []struct {
		name, yaml, want string
	}{
		{"duplicate id", `
teams:
  - {id: checkout, slack_channel: C0123ABCD, owns: [a]}
  - {id: checkout, slack_channel: C0123ABCD, owns: [b]}
`, "listed twice"},
		{"service owned by two teams", `
teams:
  - {id: checkout, slack_channel: C0123ABCD, owns: [orders-writer]}
  - {id: billing, slack_channel: C0123ABCD, owns: [orders-writer]}
`, `owned by both "checkout" and "billing"`},
		{"service twice in one team", `
teams:
  - {id: checkout, slack_channel: C0123ABCD, owns: [a, a]}
`, "twice"},
		{"bad id", `
teams:
  - {id: Checkout, slack_channel: C0123ABCD, owns: [a]}
`, "must match"},
		{"channel name, not id", `
teams:
  - {id: checkout, slack_channel: "#checkout", owns: [a]}
`, "Slack channel id"},
		{"no service", `
teams:
  - {id: checkout, slack_channel: C0123ABCD, owns: []}
`, "lists no service"},
		{"service with space", `
teams:
  - {id: checkout, slack_channel: C0123ABCD, owns: [" a"]}
`, "service name"},
		{"tenant without tenants block", `
teams:
  - {id: checkout, slack_channel: C0123ABCD, owns: [a], tenant: acme}
`, "needs a tenants: block"},
		{"unknown field", `
teams:
  - {id: checkout, slack_channel: C0123ABCD, owns: [a], channel: x}
`, "channel"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Load(writeTemp(t, tc.yaml+teamsPipeline))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Load error = %v, want one containing %q", err, tc.want)
			}
		})
	}
	// With a tenants: block a team must name a declared tenant.
	for _, tc := range []struct{ team, want string }{
		{`{id: t1, slack_channel: C0123ABCD, owns: [x]}`, "tenant: is required"},
		{`{id: t1, slack_channel: C0123ABCD, owns: [x], tenant: zz}`, "not declared"},
	} {
		if _, err := Load(writeTemp(t, twoTenants()+"teams:\n  - "+tc.team+"\n")); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Fatalf("Load error = %v, want one containing %q", err, tc.want)
		}
	}
}

// A per-pipeline file adds a pipeline only: it cannot declare teams, and the
// merged set keeps the main file's teams.
func TestTeamsLoadDir(t *testing.T) {
	dir := t.TempDir()
	main := filepath.Join(dir, "owners.yaml")
	if err := os.WriteFile(main, []byte(`
teams:
  - {id: checkout, slack_channel: C0123ABCD, owns: [checkout-api]}
`+teamsPipeline), 0o600); err != nil {
		t.Fatal(err)
	}
	pdir := filepath.Join(dir, "pipelines")
	if err := os.Mkdir(pdir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(pdir, "billing.yaml"), []byte(`
pipelines:
  - id: billing
    topic_prefix: bl
    source_tables: [public.bills]
    connectors: [pg-bills]
`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(pdir, "rogue.yaml"), []byte(`
teams:
  - {id: rogue, slack_channel: C0999ZZZZ, owns: [checkout-api]}
pipelines:
  - id: rogue
    topic_prefix: rg
    source_tables: [public.r]
    connectors: [pg-r]
`), 0o600); err != nil {
		t.Fatal(err)
	}
	o, err := LoadDir(main, pdir)
	if err != nil {
		t.Fatalf("LoadDir: %v", err)
	}
	if got := o.TeamsFor("billing"); len(got) != 1 || got[0].ID != "checkout" {
		t.Fatalf("TeamsFor(billing) = %+v, want the main file's team", got)
	}
	if _, ok := o.Pipeline("rogue"); ok {
		t.Fatal("a per-pipeline file with teams: must be skipped")
	}
	if skips := o.DirSkips(); len(skips) != 1 || !strings.Contains(skips[0].Reason, "teams:") {
		t.Fatalf("DirSkips = %+v", skips)
	}
}

// A pipeline's team: (X5-0) is a free label without a teams: list, and one of
// its ids serving the pipeline's tenant with one.
func TestPipelineTeam(t *testing.T) {
	withTeam := func(team string) string {
		return strings.Replace(teamsPipeline, "    connectors: [pg-orders]\n", "    connectors: [pg-orders]\n    team: "+team+"\n", 1)
	}
	o, err := Load(writeTemp(t, withTeam("orders-core")))
	if err != nil {
		t.Fatalf("Load without teams: list: %v", err)
	}
	if p, _ := o.Pipeline("orders"); p.Team != "orders-core" {
		t.Fatalf("Team = %q", p.Team)
	}
	listed := "teams:\n  - {id: checkout, slack_channel: C0123ABCD, owns: [checkout-api]}\n"
	if _, err := Load(writeTemp(t, listed+withTeam("checkout"))); err != nil {
		t.Fatalf("Load with a listed team: %v", err)
	}
	for _, tc := range []struct{ name, yaml, want string }{
		{"bad label", withTeam(`"Orders Core"`), "must match"},
		{"not in teams:", listed + withTeam("billing"), `team "billing" is not declared in teams:`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := Load(writeTemp(t, tc.yaml)); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Load error = %v, want one containing %q", err, tc.want)
			}
		})
	}
	// With tenants, a pipeline may name only a team of its own tenant.
	tenantTeams := "teams:\n" +
		"  - {id: team-a, slack_channel: C0123ABCD, owns: [a-api], tenant: a}\n" +
		"  - {id: team-b, slack_channel: C0456EFGH, owns: [b-api], tenant: b}\n"
	ok := strings.Replace(twoTenants(), "    tenant: a\n", "    tenant: a\n    team: team-a\n", 1)
	if _, err := Load(writeTemp(t, ok+tenantTeams)); err != nil {
		t.Fatalf("Load with the tenant's own team: %v", err)
	}
	cross := strings.Replace(twoTenants(), "    tenant: a\n", "    tenant: a\n    team: team-b\n", 1)
	if _, err := Load(writeTemp(t, cross+tenantTeams)); err == nil || !strings.Contains(err.Error(), `serves tenant "b"`) {
		t.Fatalf("Load error = %v, want another tenant's team refused", err)
	}
}
