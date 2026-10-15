package dbtools

import (
	"testing"

	"piperoom/internal/owners"
)

// The dev stack's owners.yaml names the slot, so pg_replication_slots works
// while Connect is down (QA round 1, S7).
func TestDevOwnersDeclaresSlot(t *testing.T) {
	o, err := owners.Load("../../../dev/owners.yaml")
	if err != nil {
		t.Fatal(err)
	}
	p, ok := o.Pipeline("orders")
	if !ok {
		t.Fatal("dev/owners.yaml has no pipeline orders")
	}
	if got := p.Watch().Slot; got != "debezium_orders" {
		t.Fatalf("watch.slot = %q, want debezium_orders", got)
	}
}
