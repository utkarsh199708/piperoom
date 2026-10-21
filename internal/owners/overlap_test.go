package owners

import (
	"testing"

	"piperoom/internal/events"
)

// loadedOwners builds an *Owners with the given tenants and pipelines, for the
// overlap tests (no Load: the host references are literal values, which a
// provisional pipeline carries too).
func loadedOwners(tenants map[string]Tenant, pipes ...Pipeline) *Owners {
	return &Owners{Tenants: tenants, Pipelines: pipes}
}

// hasOverlap reports whether out holds an overlap of the given kind, value and
// (when wantPipeline is set) the given other pipeline id.
func hasOverlap(out []Overlap, kind, value, wantPipeline string) bool {
	for _, o := range out {
		if o.Kind == kind && o.Value == value {
			if wantPipeline == "" || o.PipelineID == wantPipeline {
				return true
			}
		}
	}
	return false
}

func TestOverlapFindsKinds(t *testing.T) {
	tenants := map[string]Tenant{
		"a": {Admins: Principals{{Kind: KindUser, Name: "ann"}}},
		"b": {Admins: Principals{{Kind: KindUser, Name: "ben"}}},
	}
	prov := events.SetupRequest{
		PipelineID:  "new",
		Tenant:      "a",
		TopicPrefix: "pg",
		Tables:      []string{"public.orders"},
		Connectors:  []string{"orders-c"},
		Targets:     []events.SetupTarget{{Kind: "postgres", Address: "db.acme:5432"}},
	}
	p, err := Provisional(prov)
	if err != nil {
		t.Fatalf("Provisional: %v", err)
	}

	t.Run("shared topic prefix", func(t *testing.T) {
		o := loadedOwners(tenants, Pipeline{ID: "qa", Tenant: "a", TopicPrefix: "pg", SourceTables: []string{"public.x"}, Connectors: []string{"x"}})
		if !hasOverlap(o.Overlap(p, nil), OverlapTopicPrefix, "pg", "qa") {
			t.Fatalf("overlaps = %+v", o.Overlap(p, nil))
		}
	})
	t.Run("shared connector", func(t *testing.T) {
		o := loadedOwners(tenants, Pipeline{ID: "qa", Tenant: "a", TopicPrefix: "other", SourceTables: []string{"public.x"}, Connectors: []string{"orders-c", "other-c"}})
		if !hasOverlap(o.Overlap(p, nil), OverlapConnector, "orders-c", "qa") {
			t.Fatalf("overlaps = %+v", o.Overlap(p, nil))
		}
	})
	t.Run("table per host", func(t *testing.T) {
		// Same table on the same source host: an overlap.
		same := Pipeline{ID: "qa", Tenant: "a", TopicPrefix: "other", SourceTables: []string{"public.orders"}, Connectors: []string{"x"},
			Connections: Connections{"source_db_url": {Value: "db.acme:5432"}}}
		if !hasOverlap(loadedOwners(tenants, same).Overlap(p, nil), OverlapTable, "public.orders", "qa") {
			t.Fatalf("same host: overlaps = %+v", loadedOwners(tenants, same).Overlap(p, nil))
		}
		// Same table on a different source host: no table overlap.
		diff := Pipeline{ID: "qa", Tenant: "a", TopicPrefix: "other", SourceTables: []string{"public.orders"}, Connectors: []string{"x"},
			Connections: Connections{"source_db_url": {Value: "other-db:5432"}}}
		if hasOverlap(loadedOwners(tenants, diff).Overlap(p, nil), OverlapTable, "public.orders", "") {
			t.Fatalf("different host: overlaps = %+v", loadedOwners(tenants, diff).Overlap(p, nil))
		}
	})
	t.Run("host across tenants", func(t *testing.T) {
		o := loadedOwners(tenants, Pipeline{ID: "qb", Tenant: "b", TopicPrefix: "other", SourceTables: []string{"public.x"}, Connectors: []string{"x"},
			Connections: Connections{"source_db_url": {Value: "DB.ACME:5432"}}})
		if !hasOverlap(o.Overlap(p, nil), OverlapHost, "db.acme:5432", "") {
			t.Fatalf("overlaps = %+v", o.Overlap(p, nil))
		}
	})
	t.Run("no overlap", func(t *testing.T) {
		o := loadedOwners(tenants, Pipeline{ID: "qa", Tenant: "a", TopicPrefix: "zz", SourceTables: []string{"public.z"}, Connectors: []string{"z"},
			Connections: Connections{"source_db_url": {Value: "elsewhere:5432"}}})
		if got := o.Overlap(p, nil); len(got) != 0 {
			t.Fatalf("overlaps = %+v, want none", got)
		}
	})
}

// TestOverlapNeverNamesAnotherTenant pins the leak rule of §3.6: a pipeline of
// another tenant appears as "a pipeline in another tenant" (empty PipelineID)
// with that tenant's admins as the contact, and its id and tenant name are
// never in the result. The requester's own tenant's pipeline is named.
func TestOverlapNeverNamesAnotherTenant(t *testing.T) {
	tenants := map[string]Tenant{
		"a": {Admins: Principals{{Kind: KindUser, Name: "ann"}}},
		"b": {Admins: Principals{{Kind: KindUser, Name: "ben"}}},
	}
	p, err := Provisional(events.SetupRequest{
		PipelineID: "new", Tenant: "a", TopicPrefix: "pg",
		Tables: []string{"public.orders"}, Connectors: []string{"orders-c"},
	})
	if err != nil {
		t.Fatalf("Provisional: %v", err)
	}
	o := loadedOwners(tenants,
		Pipeline{ID: "qa", Tenant: "a", TopicPrefix: "pg", SourceTables: []string{"public.x"}, Connectors: []string{"x"}},
		Pipeline{ID: "qb", Tenant: "b", TopicPrefix: "pg", SourceTables: []string{"public.y"}, Connectors: []string{"y"}},
	)
	out := o.Overlap(p, nil)
	if len(out) != 2 {
		t.Fatalf("overlaps = %+v, want two (one per pipeline)", out)
	}
	var own, foreign *Overlap
	for i := range out {
		switch out[i].PipelineID {
		case "qa":
			own = &out[i]
		case "":
			foreign = &out[i]
		default:
			t.Fatalf("overlap names pipeline %q (must never name another tenant's pipeline)", out[i].PipelineID)
		}
	}
	if own == nil || own.Kind != OverlapTopicPrefix || own.Value != "pg" {
		t.Fatalf("own-tenant overlap missing: %+v", out)
	}
	if foreign == nil || foreign.Kind != OverlapTopicPrefix || foreign.Value != "pg" {
		t.Fatalf("other-tenant overlap missing: %+v", out)
	}
	if got := foreign.Contact.Display(); got != "ben" {
		t.Fatalf("other-tenant contact = %q, want the other tenant's admin", got)
	}
}

// TestOverlapResolvesReferences pins the host overlap fix: a loaded pipeline
// holds an env: reference and a provisional pipeline holds a value: literal;
// only the caller's resolver knows they name the same host. With the resolver
// mapping the env: to a full DSN, the table and host overlaps must fire.
func TestOverlapResolvesReferences(t *testing.T) {
	tenants := map[string]Tenant{
		"a": {Admins: Principals{{Kind: KindUser, Name: "ann"}}},
	}
	prov, err := Provisional(events.SetupRequest{
		PipelineID:  "new",
		Tenant:      "a",
		TopicPrefix: "pg",
		Tables:      []string{"public.orders"},
		Connectors:  []string{"orders-c"},
		Targets:     []events.SetupTarget{{Kind: "postgres", Address: "db.acme:5432"}},
	})
	if err != nil {
		t.Fatalf("Provisional: %v", err)
	}
	loaded := Pipeline{ID: "qa", Tenant: "a", TopicPrefix: "other",
		SourceTables: []string{"public.orders"}, Connectors: []string{"x"},
		Connections: Connections{"source_db_url": {Env: "SRC_DB"}}}
	resolve := func(p Pipeline) map[string]string {
		if p.ID == "qa" {
			return map[string]string{"source_db_url": "postgres://user:pass@db.acme:5432/orders?sslmode=require"}
		}
		return nil
	}
	out := loadedOwners(tenants, loaded).Overlap(prov, resolve)
	if !hasOverlap(out, OverlapTable, "public.orders", "qa") {
		t.Fatalf("overlaps = %+v, want a table overlap via resolved endpoints", out)
	}
	if !hasOverlap(out, OverlapHost, "db.acme:5432", "qa") {
		t.Fatalf("overlaps = %+v, want a host overlap via resolved endpoints", out)
	}
}

// TestHostPort pins the endpoint normalization: a full DSN and a bare host:port
// naming the same host reduce to the same normalized value, and a default port
// is filled in only when the endpoint names a host without one.
func TestHostPort(t *testing.T) {
	cases := []struct {
		in      string
		defPort string
		want    string
	}{
		{"db.acme:5432", "", "db.acme:5432"},
		{"DB.ACME:5432", "", "db.acme:5432"},
		{"  db.acme:5432  ", "", "db.acme:5432"},
		{"postgres://user:pass@DB.Acme:5432/orders?sslmode=require", "", "db.acme:5432"},
		{"postgres://db.acme/orders", "", "db.acme:5432"},
		{"postgres://db.acme/orders", "5432", "db.acme:5432"},
		{"https://registry.acme", "8081", "registry.acme:443"},
		{"https://registry.acme:443", "8081", "registry.acme:443"},
		{"http://connect.acme", "8083", "connect.acme:80"},
		{"user:pass@db.acme:5432", "", "db.acme:5432"},
		{"db.acme:5432/orders", "", "db.acme:5432"},
		{"db.acme", "5432", "db.acme:5432"},
		{"db.acme", "", "db.acme"},
		{"", "", ""},
	}
	for _, tc := range cases {
		if got := hostPort(tc.in, tc.defPort); got != tc.want {
			t.Errorf("hostPort(%q, %q) = %q, want %q", tc.in, tc.defPort, got, tc.want)
		}
	}
}

// TestOverlapKafkaBrokerList pins the broker-list host overlap: kafka_brokers is
// a comma-separated list and overlap on any one broker is a host overlap, in any
// order and with or without a Kafka protocol prefix per entry.
func TestOverlapKafkaBrokerList(t *testing.T) {
	tenants := map[string]Tenant{
		"a": {Admins: Principals{{Kind: KindUser, Name: "ann"}}},
	}
	loaded := Pipeline{ID: "qa", Tenant: "a", TopicPrefix: "other", SourceTables: []string{"public.x"}, Connectors: []string{"x"},
		Connections: Connections{"kafka_brokers": {Value: "b1:9092,b2:9092,b3:9092"}}}

	cases := []struct {
		name    string
		brokers string
		want    string // the shared broker, in normalized host:port form
	}{
		{"list against a single broker", "b2:9092", "b2:9092"},
		{"same list in a different order", "b3:9092,b2:9092,b1:9092", "b1:9092"},
		{"SASL_SSL prefixes", "SASL_SSL://b1:9092,PLAINTEXT://b2:9092", "b2:9092"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := Pipeline{ID: "new", Tenant: "a", TopicPrefix: "zz", SourceTables: []string{"public.z"}, Connectors: []string{"z"},
				Connections: Connections{"kafka_brokers": {Value: tc.brokers}}}
			out := loadedOwners(tenants, loaded).Overlap(p, nil)
			if !hasOverlap(out, OverlapHost, tc.want, "qa") {
				t.Fatalf("overlaps = %+v, want a host overlap on %q", out, tc.want)
			}
		})
	}
}

// TestOverlapDefaultPort pins the default-port fill: a source DSN without a
// port matches a typed host:port for the same connection key, so the two name
// the same host and the host overlap fires.
func TestOverlapDefaultPort(t *testing.T) {
	tenants := map[string]Tenant{
		"a": {Admins: Principals{{Kind: KindUser, Name: "ann"}}},
	}
	loaded := Pipeline{ID: "qa", Tenant: "a", TopicPrefix: "other", SourceTables: []string{"public.x"}, Connectors: []string{"x"},
		Connections: Connections{"source_db_url": {Value: "postgres://db.acme/orders"}}}
	p, err := Provisional(events.SetupRequest{
		PipelineID: "new", Tenant: "a", TopicPrefix: "pg",
		Tables: []string{"public.z"}, Connectors: []string{"z"},
		Targets: []events.SetupTarget{{Kind: "postgres", Address: "db.acme:5432"}},
	})
	if err != nil {
		t.Fatalf("Provisional: %v", err)
	}
	out := loadedOwners(tenants, loaded).Overlap(p, nil)
	if !hasOverlap(out, OverlapHost, "db.acme:5432", "qa") {
		t.Fatalf("overlaps = %+v, want a host overlap on db.acme:5432 via default port", out)
	}
}

// TestOverlapSchemeDefaultPort pins the URL scheme default port fix: a URL
// endpoint without a port uses its scheme's standard port, not the connection
// key's default, so https://registry.acme and https://registry.acme:443 name the
// same host and the host overlap fires.
func TestOverlapSchemeDefaultPort(t *testing.T) {
	tenants := map[string]Tenant{
		"a": {Admins: Principals{{Kind: KindUser, Name: "ann"}}},
	}
	loaded := Pipeline{ID: "qa", Tenant: "a", TopicPrefix: "other", SourceTables: []string{"public.x"}, Connectors: []string{"x"},
		Connections: Connections{"schema_registry_url": {Value: "https://registry.acme"}}}
	p, err := Provisional(events.SetupRequest{
		PipelineID: "new", Tenant: "a", TopicPrefix: "pg",
		Tables: []string{"public.z"}, Connectors: []string{"z"},
		Targets: []events.SetupTarget{{Kind: "registry", Address: "https://registry.acme:443"}},
	})
	if err != nil {
		t.Fatalf("Provisional: %v", err)
	}
	out := loadedOwners(tenants, loaded).Overlap(p, nil)
	if !hasOverlap(out, OverlapHost, "registry.acme:443", "qa") {
		t.Fatalf("overlaps = %+v, want a host overlap on registry.acme:443 via the scheme default port", out)
	}
}
