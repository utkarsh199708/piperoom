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
		if !hasOverlap(o.Overlap(p), OverlapTopicPrefix, "pg", "qa") {
			t.Fatalf("overlaps = %+v", o.Overlap(p))
		}
	})
	t.Run("shared connector", func(t *testing.T) {
		o := loadedOwners(tenants, Pipeline{ID: "qa", Tenant: "a", TopicPrefix: "other", SourceTables: []string{"public.x"}, Connectors: []string{"orders-c", "other-c"}})
		if !hasOverlap(o.Overlap(p), OverlapConnector, "orders-c", "qa") {
			t.Fatalf("overlaps = %+v", o.Overlap(p))
		}
	})
	t.Run("table per host", func(t *testing.T) {
		// Same table on the same source host: an overlap.
		same := Pipeline{ID: "qa", Tenant: "a", TopicPrefix: "other", SourceTables: []string{"public.orders"}, Connectors: []string{"x"},
			Connections: Connections{"source_db_url": {Value: "db.acme:5432"}}}
		if !hasOverlap(loadedOwners(tenants, same).Overlap(p), OverlapTable, "public.orders", "qa") {
			t.Fatalf("same host: overlaps = %+v", loadedOwners(tenants, same).Overlap(p))
		}
		// Same table on a different source host: no table overlap.
		diff := Pipeline{ID: "qa", Tenant: "a", TopicPrefix: "other", SourceTables: []string{"public.orders"}, Connectors: []string{"x"},
			Connections: Connections{"source_db_url": {Value: "other-db:5432"}}}
		if hasOverlap(loadedOwners(tenants, diff).Overlap(p), OverlapTable, "public.orders", "") {
			t.Fatalf("different host: overlaps = %+v", loadedOwners(tenants, diff).Overlap(p))
		}
	})
	t.Run("host across tenants", func(t *testing.T) {
		o := loadedOwners(tenants, Pipeline{ID: "qb", Tenant: "b", TopicPrefix: "other", SourceTables: []string{"public.x"}, Connectors: []string{"x"},
			Connections: Connections{"source_db_url": {Value: "DB.ACME:5432"}}})
		if !hasOverlap(o.Overlap(p), OverlapHost, "db.acme:5432", "") {
			t.Fatalf("overlaps = %+v", o.Overlap(p))
		}
	})
	t.Run("no overlap", func(t *testing.T) {
		o := loadedOwners(tenants, Pipeline{ID: "qa", Tenant: "a", TopicPrefix: "zz", SourceTables: []string{"public.z"}, Connectors: []string{"z"},
			Connections: Connections{"source_db_url": {Value: "elsewhere:5432"}}})
		if got := o.Overlap(p); len(got) != 0 {
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
	out := o.Overlap(p)
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
