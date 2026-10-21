package tools

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
)

// TC2 (KAN-459): CheckGroup accepts a consumer group the topology derived from
// the pipeline's own sink engine (recorded on the owners config), and nothing
// else. kafka_consumer_lag and kafka_list_groups both go through these two
// methods, so the ClickHouse engine's group becomes readable without widening
// scope to arbitrary groups.
func TestScopeAcceptsDerivedConsumerGroups(t *testing.T) {
	o := scopeTestOwners()
	sc, err := ScopeFor(WithPipelineID(context.Background(), "orders"), o)
	if err != nil {
		t.Fatal(err)
	}
	const chGroup = "piperoom-clickhouse-orders"
	if err := sc.CheckGroup(chGroup); !errors.Is(err, ErrOutOfScope) {
		t.Fatalf("before a topology build: err = %v, want ErrOutOfScope (fail closed)", err)
	}

	o.SetDerivedConsumerGroups("orders", []string{chGroup})
	// The scope reads the owners config live, so one resolved before the set
	// exists still sees it: the agent's tool call order does not matter.
	if err := sc.CheckGroup(chGroup); err != nil {
		t.Fatalf("derived group refused: %v", err)
	}
	if err := sc.CheckGroup("order-sync"); err != nil {
		t.Fatalf("declared group refused: %v", err)
	}
	if err := sc.CheckGroup("some-other-group"); !errors.Is(err, ErrOutOfScope) {
		t.Fatalf("unrelated group: err = %v, want ErrOutOfScope", err)
	}
	if got := sc.FilterGroups([]string{"order-sync", chGroup, "some-other-group"}); !slices.Equal(got, []string{"order-sync", chGroup}) {
		t.Fatalf("FilterGroups = %v, want the declared and derived groups only", got)
	}

	// A group derived for another pipeline stays out of this pipeline's scope.
	o.SetDerivedConsumerGroups("billing", []string{"billing-ch-group"})
	if err := sc.CheckGroup("billing-ch-group"); !errors.Is(err, ErrOutOfScope) {
		t.Fatalf("another pipeline's derived group: err = %v, want ErrOutOfScope", err)
	}
	if got := sc.FilterGroups([]string{"billing-ch-group"}); len(got) != 0 {
		t.Fatalf("FilterGroups leaked another pipeline's derived group: %v", got)
	}

	// The refusal names the allowed set, so the agent can pick the right group.
	err = sc.CheckGroup("wrong-group")
	if err == nil || !strings.Contains(err.Error(), chGroup) {
		t.Fatalf("refusal = %v, want it to list the derived group", err)
	}

	// A cleared set (a failed topology read) stops allowing the group.
	o.SetDerivedConsumerGroups("orders", nil)
	if err := sc.CheckGroup(chGroup); !errors.Is(err, ErrOutOfScope) {
		t.Fatalf("after a clear: err = %v, want ErrOutOfScope", err)
	}

	// Unscoped callers are unchanged.
	un, err := ScopeFor(WithUnscoped(context.Background()), o)
	if err != nil || un.CheckGroup("anything") != nil {
		t.Fatalf("unscoped CheckGroup = %v, %v", un, err)
	}
}
