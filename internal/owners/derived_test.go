package owners

import (
	"os"
	"path/filepath"
	"slices"
	"testing"
)

// loadedOwners writes a minimal owners file and loads it, the way cmd/server
// does, so the derived-group store is the one Load creates.
func loadDerivedOwners(t *testing.T) *Owners {
	t.Helper()
	path := filepath.Join(t.TempDir(), "owners.yaml")
	const doc = `pipelines:
  - id: orders
    topic_prefix: pg
    source_tables: [public.orders]
    connectors: [pg-orders-connector]
    consumer_groups: [order-sync]
    sink_tables: [analytics.orders]
    sink_ts_column: updated_at
  - id: billing
    topic_prefix: bill
    source_tables: [public.invoices]
    connectors: [bill-connector]
    consumer_groups: [bill-sync]
    sink_tables: [analytics.invoices]
    sink_ts_column: updated_at
`
	if err := os.WriteFile(path, []byte(doc), 0o600); err != nil {
		t.Fatal(err)
	}
	o, err := Load(path)
	if err != nil {
		t.Fatalf("owners.Load: %v", err)
	}
	return o
}

// TestDerivedConsumerGroupsPerPipeline: a set recorded for one pipeline is not
// visible to another (the scope rule is per pipeline), duplicates are folded
// and the result is sorted.
func TestDerivedConsumerGroupsPerPipeline(t *testing.T) {
	o := loadDerivedOwners(t)
	if got := o.DerivedConsumerGroups("orders"); got != nil {
		t.Fatalf("fresh owners derived groups = %v, want none", got)
	}
	o.SetDerivedConsumerGroups("orders", []string{"piperoom-clickhouse-orders", "piperoom-clickhouse-orders"})
	o.SetDerivedConsumerGroups("billing", []string{"b"})
	if got, want := o.DerivedConsumerGroups("orders"), []string{"piperoom-clickhouse-orders"}; !slices.Equal(got, want) {
		t.Fatalf("orders derived groups = %v, want %v", got, want)
	}
	if got, want := o.DerivedConsumerGroups("billing"), []string{"b"}; !slices.Equal(got, want) {
		t.Fatalf("billing derived groups = %v, want %v", got, want)
	}
	if got := o.DerivedConsumerGroups("nope"); got != nil {
		t.Fatalf("unknown pipeline derived groups = %v, want none", got)
	}
}

// TestDerivedConsumerGroupsReplace: every set replaces the pipeline's previous
// one, so a group the engine no longer declares stops being allowed, and an
// empty set clears it.
func TestDerivedConsumerGroupsReplace(t *testing.T) {
	o := loadDerivedOwners(t)
	o.SetDerivedConsumerGroups("orders", []string{"old_group"})
	o.SetDerivedConsumerGroups("orders", []string{"new_group"})
	if got, want := o.DerivedConsumerGroups("orders"), []string{"new_group"}; !slices.Equal(got, want) {
		t.Fatalf("derived groups = %v, want %v", got, want)
	}
	o.SetDerivedConsumerGroups("orders", nil)
	if got := o.DerivedConsumerGroups("orders"); got != nil {
		t.Fatalf("derived groups after clear = %v, want none", got)
	}
	o.SetDerivedConsumerGroups("orders", []string{""})
	if got := o.DerivedConsumerGroups("orders"); got != nil {
		t.Fatalf("derived groups after blank = %v, want none", got)
	}
}

// TestDerivedConsumerGroupsCopySharesTheSet: Only copies an Owners by value; the
// copy must keep reading the same derived set (the wiring gives each client set
// such a view).
func TestDerivedConsumerGroupsCopySharesTheSet(t *testing.T) {
	o := loadDerivedOwners(t)
	o.SetDerivedConsumerGroups("orders", []string{"piperoom-clickhouse-orders"})
	view := o.Only("orders")
	if got, want := view.DerivedConsumerGroups("orders"), []string{"piperoom-clickhouse-orders"}; !slices.Equal(got, want) {
		t.Fatalf("view derived groups = %v, want %v", got, want)
	}
	if got := view.DerivedConsumerGroups("billing"); got != nil {
		t.Fatalf("view sees another pipeline's derived groups: %v", got)
	}
}

// TestDerivedConsumerGroupsHandBuiltOwners: the zero value records sets too, so
// tests and tools that build an Owners literal are not silently out of scope.
func TestDerivedConsumerGroupsHandBuiltOwners(t *testing.T) {
	o := &Owners{Pipelines: []Pipeline{{ID: "orders"}}}
	o.SetDerivedConsumerGroups("orders", []string{"g"})
	if got, want := o.DerivedConsumerGroups("orders"), []string{"g"}; !slices.Equal(got, want) {
		t.Fatalf("hand-built owners derived groups = %v, want %v", got, want)
	}
	var nilOwners *Owners
	nilOwners.SetDerivedConsumerGroups("orders", []string{"g"})
	if got := nilOwners.DerivedConsumerGroups("orders"); got != nil {
		t.Fatalf("nil owners derived groups = %v, want none", got)
	}
}

// TestDerivedConsumerGroupsRefuseDeclaredGroups: a warehouse DDL author
// controls kafka_group_name, so a derived group that any pipeline declares is
// dropped — another pipeline's group must never enter this pipeline's read
// scope, and a declared group needs no second entry. The rule must also hold
// through an Only view, whose Pipelines no longer list the other pipeline: the
// declared-group index is shared with the copy.
func TestDerivedConsumerGroupsRefuseDeclaredGroups(t *testing.T) {
	o := loadDerivedOwners(t)
	o.SetDerivedConsumerGroups("orders", []string{"piperoom-clickhouse-orders", "bill-sync", "order-sync"})
	if got, want := o.DerivedConsumerGroups("orders"), []string{"piperoom-clickhouse-orders"}; !slices.Equal(got, want) {
		t.Fatalf("derived groups = %v, want %v (only the group no pipeline declares)", got, want)
	}

	// wiring gives each pipeline stack an Only view; its builder still refuses
	// another pipeline's declared group.
	view := o.Only("orders")
	view.SetDerivedConsumerGroups("orders", []string{"piperoom-clickhouse-orders-2", "bill-sync"})
	if got, want := o.DerivedConsumerGroups("orders"), []string{"piperoom-clickhouse-orders-2"}; !slices.Equal(got, want) {
		t.Fatalf("derived groups through an Only view = %v, want %v", got, want)
	}

	// A hand-built Owners has no index; the pipelines it holds are scanned.
	h := &Owners{Pipelines: []Pipeline{
		{ID: "orders"},
		{ID: "billing", ConsumerGroups: []string{"bill-sync"}},
	}}
	h.SetDerivedConsumerGroups("orders", []string{"ch", "bill-sync"})
	if got, want := h.DerivedConsumerGroups("orders"), []string{"ch"}; !slices.Equal(got, want) {
		t.Fatalf("hand-built owners derived groups = %v, want %v", got, want)
	}
}
