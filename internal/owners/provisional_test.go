package owners

import (
	"slices"
	"strings"
	"testing"

	"piperoom/internal/events"
)

func TestProvisional(t *testing.T) {
	req := events.SetupRequest{
		PipelineID:  "orders",
		Tenant:      "a",
		Title:       "Orders CDC",
		SourceKind:  "postgres",
		Target:      "compose",
		TopicPrefix: "pg",
		Tables:      []string{"public.orders", "public.items"},
		Connectors:  []string{"orders-connector"},
		Targets: []events.SetupTarget{
			{Kind: "postgres", Address: "db.acme:5432"},
			{Kind: "connect", Address: "connect.acme:8083"},
			{Kind: "kubernetes", Address: "ctx:ns"}, // no cdc connection key
		},
		Repos: []string{"connect-config"},
	}
	p, err := Provisional(req)
	if err != nil {
		t.Fatalf("Provisional: %v", err)
	}
	if p.ID != "orders" || p.Title != "Orders CDC" || p.Tenant != "a" {
		t.Fatalf("id/title/tenant = %q %q %q", p.ID, p.Title, p.Tenant)
	}
	if p.TenantName() != "a" {
		t.Fatalf("TenantName = %q", p.TenantName())
	}
	if p.KindOrDefault() != events.PipelineKindCDC {
		t.Fatalf("kind = %q, want cdc", p.KindOrDefault())
	}
	if p.TopicPrefix != "pg" {
		t.Fatalf("topic prefix = %q", p.TopicPrefix)
	}
	if !slices.Equal(p.SourceTables, []string{"public.orders", "public.items"}) {
		t.Fatalf("source tables = %v", p.SourceTables)
	}
	if !slices.Equal(p.Connectors, []string{"orders-connector"}) {
		t.Fatalf("connectors = %v", p.Connectors)
	}
	// Every layer unowned: no owners: block, and the cdc layers (environment
	// included) so CheckScope accepts the setup layer.
	if len(p.Owners) != 0 {
		t.Fatalf("owners = %v, want none", p.Owners)
	}
	if !slices.Contains(p.Layers(), events.LayerEnvironment) {
		t.Fatalf("layers = %v, want the environment layer", p.Layers())
	}
	// Hosts land in connections: as bare values, mapped by target kind.
	if got := p.Connections["source_db_url"]; got.Value != "db.acme:5432" {
		t.Fatalf("source_db_url = %+v", got)
	}
	if got := p.Connections["connect_url"]; got.Value != "connect.acme:8083" {
		t.Fatalf("connect_url = %+v", got)
	}
	if _, ok := p.Connections["schema_registry_url"]; ok {
		t.Fatal("no registry target, so no schema_registry_url connection")
	}
}

func TestProvisionalRefused(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(*events.SetupRequest)
		wantErr string
	}{
		{"empty topic prefix", func(r *events.SetupRequest) { r.TopicPrefix = "  " }, "topic_prefix"},
		{"empty pipeline id", func(r *events.SetupRequest) { r.PipelineID = "" }, "pipeline_id"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := events.SetupRequest{PipelineID: "orders", TopicPrefix: "pg", Tables: []string{"public.orders"}, Connectors: []string{"c1"}}
			tc.mutate(&req)
			if _, err := Provisional(req); err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("err = %v, want %q", err, tc.wantErr)
			}
		})
	}
}

// TestProvisionalCarriesNoEnvironmentOwner pins §3.6: a top-level environment:
// owner (written by discover) never approves in a setup room before
// registration, because the provisional pipeline does not carry it.
func TestProvisionalCarriesNoEnvironmentOwner(t *testing.T) {
	o, err := Load(writeTemp(t, `
pipelines:
  - {id: registered, topic_prefix: pg, source_tables: [public.orders], connectors: [c1]}
layers:
  postgres: priya
  environment: user:alice
`))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	req := events.SetupRequest{
		PipelineID:  "orders",
		TopicPrefix: "pg",
		Tables:      []string{"public.orders"},
		Connectors:  []string{"c1"},
	}
	p, err := Provisional(req)
	if err != nil {
		t.Fatalf("Provisional: %v", err)
	}
	if _, ok := p.Owners[events.LayerEnvironment]; ok {
		t.Fatal("the provisional pipeline carries an environment owner")
	}
	if len(p.Owners) != 0 {
		t.Fatalf("the provisional pipeline carries owners: %v", p.Owners)
	}
	// The discover-written environment owner still owns the registered
	// pipeline's layer, but cannot approve the provisional's.
	if g, ok := o.CanApprove("registered", events.LayerEnvironment, "alice", nil); !ok || g != "user:alice" {
		t.Fatalf("registered environment owner = %q, %v", g, ok)
	}
	if g, ok := o.CanApprove(req.PipelineID, events.LayerEnvironment, "alice", nil); ok || g != "" {
		t.Fatalf("provisional environment owner = %q, %v; want none", g, ok)
	}
}
