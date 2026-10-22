package tools

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
)

const engineGroup = "piperoom-clickhouse-orders"

// TC2 (KAN-459): the read tools accept a consumer group the topology derived
// from the pipeline's own sink engine (recorded on the owners config), and
// nothing else. kafka_consumer_lag and kafka_list_groups both go through
// CheckReadGroup/FilterReadGroups, so the ClickHouse engine's group becomes
// readable without widening scope to arbitrary groups.
func TestScopeReadGroupAcceptsDerivedConsumerGroups(t *testing.T) {
	o := scopeTestOwners()
	sc, err := ScopeFor(WithPipelineID(context.Background(), "orders"), o)
	if err != nil {
		t.Fatal(err)
	}
	if err := sc.CheckReadGroup(engineGroup); !errors.Is(err, ErrOutOfScope) {
		t.Fatalf("before a topology build: err = %v, want ErrOutOfScope (fail closed)", err)
	}

	o.SetDerivedConsumerGroups("orders", []string{engineGroup})
	// The scope reads the owners config live, so one resolved before the set
	// exists still sees it: the agent's tool call order does not matter.
	if err := sc.CheckReadGroup(engineGroup); err != nil {
		t.Fatalf("derived group refused by the read scope: %v", err)
	}
	if err := sc.CheckReadGroup("order-sync"); err != nil {
		t.Fatalf("declared group refused by the read scope: %v", err)
	}
	if err := sc.CheckReadGroup("some-other-group"); !errors.Is(err, ErrOutOfScope) {
		t.Fatalf("unrelated group: err = %v, want ErrOutOfScope", err)
	}
	if got := sc.FilterReadGroups([]string{"order-sync", engineGroup, "some-other-group"}); !slices.Equal(got, []string{"order-sync", engineGroup}) {
		t.Fatalf("FilterReadGroups = %v, want the declared and derived groups only", got)
	}

	// MUST FIX (review #793): the write guard is declared-only. A group derived
	// from warehouse DDL must never reach an offset reset.
	if err := sc.CheckGroup(engineGroup); !errors.Is(err, ErrOutOfScope) {
		t.Fatalf("write guard accepted a derived group: err = %v, want ErrOutOfScope", err)
	}
	if err := sc.CheckGroup("order-sync"); err != nil {
		t.Fatalf("write guard refused a declared group: %v", err)
	}
	if got := sc.FilterGroups([]string{"order-sync", engineGroup}); !slices.Equal(got, []string{"order-sync"}) {
		t.Fatalf("FilterGroups = %v, want the declared group only (write-side listing)", got)
	}

	// A group derived for another pipeline stays out of this pipeline's scope.
	o.SetDerivedConsumerGroups("billing", []string{"billing-ch-group"})
	if err := sc.CheckReadGroup("billing-ch-group"); !errors.Is(err, ErrOutOfScope) {
		t.Fatalf("another pipeline's derived group: err = %v, want ErrOutOfScope", err)
	}
	if got := sc.FilterReadGroups([]string{"billing-ch-group"}); len(got) != 0 {
		t.Fatalf("FilterReadGroups leaked another pipeline's derived group: %v", got)
	}

	// The refusal names the allowed set, so the agent can pick the right group.
	err = sc.CheckReadGroup("wrong-group")
	if err == nil || !strings.Contains(err.Error(), engineGroup) {
		t.Fatalf("refusal = %v, want it to list the derived group", err)
	}

	// A cleared set (a failed topology read) stops allowing the group.
	o.SetDerivedConsumerGroups("orders", nil)
	if err := sc.CheckReadGroup(engineGroup); !errors.Is(err, ErrOutOfScope) {
		t.Fatalf("after a clear: err = %v, want ErrOutOfScope", err)
	}

	// Unscoped callers are unchanged.
	un, err := ScopeFor(WithUnscoped(context.Background()), o)
	if err != nil || un.CheckGroup("anything") != nil || un.CheckReadGroup("anything") != nil {
		t.Fatalf("unscoped checks = %v, %v", un, err)
	}
	if got := un.FilterReadGroups([]string{"a", "b"}); !slices.Equal(got, []string{"a", "b"}) {
		t.Fatalf("unscoped FilterReadGroups = %v", got)
	}
}

// TestScopeWriteGuardRefusesDerivedGroupEvenWithOwners: the action and the
// Writer resolve the scope from tools.WithPipeline and call CheckGroup. Even a
// Scope that can read the derived set (an Owners is passed) refuses the group
// for writes, so the rule does not depend on how the caller built its scope.
func TestScopeWriteGuardRefusesDerivedGroupEvenWithOwners(t *testing.T) {
	o := scopeTestOwners()
	p, _ := o.Pipeline("orders")
	o.SetDerivedConsumerGroups("orders", []string{engineGroup})

	for name, ctx := range map[string]context.Context{
		"whole pipeline, with owners": WithPipeline(context.Background(), p),
		"pipeline id, with owners":    WithPipelineID(context.Background(), "orders"),
	} {
		sc, err := ScopeFor(ctx, o)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if err := sc.CheckReadGroup(engineGroup); err != nil {
			t.Fatalf("%s: read scope refused the derived group: %v", name, err)
		}
		if err := sc.CheckGroup(engineGroup); !errors.Is(err, ErrOutOfScope) {
			t.Fatalf("%s: write guard err = %v, want ErrOutOfScope", name, err)
		}
	}
}
