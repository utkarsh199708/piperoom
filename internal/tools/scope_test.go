package tools

import (
	"context"
	"errors"
	"slices"
	"testing"

	"piperoom/internal/owners"
)

func scopeTestOwners() *owners.Owners {
	return &owners.Owners{Pipelines: []owners.Pipeline{{
		ID: "orders", TopicPrefix: "pg",
		SourceTables:   []string{"public.orders"},
		SinkTables:     []string{"analytics.orders"},
		Connectors:     []string{"pg-orders-connector"},
		ConsumerGroups: []string{"order-sync"},
	}}}
}

func TestScopeForModes(t *testing.T) {
	o := scopeTestOwners()
	p, _ := o.Pipeline("orders")

	if _, err := ScopeFor(context.Background(), o); !errors.Is(err, ErrNoScope) {
		t.Fatalf("empty ctx: err = %v, want ErrNoScope", err)
	}
	sc, err := ScopeFor(WithPipelineID(context.Background(), "orders"), o)
	if err != nil || sc.Unscoped() || sc.Pipeline().ID != "orders" {
		t.Fatalf("id: %+v, %v", sc, err)
	}
	if _, err := ScopeFor(WithPipelineID(context.Background(), "nope"), o); !errors.Is(err, ErrOutOfScope) {
		t.Fatalf("unknown id: err = %v, want ErrOutOfScope", err)
	}
	if _, err := ScopeFor(WithPipelineID(context.Background(), "orders"), nil); err == nil {
		t.Fatal("nil owners with an id must be an error")
	}
	sc, err = ScopeFor(WithPipeline(context.Background(), p), nil)
	if err != nil || sc.Pipeline().ID != "orders" {
		t.Fatalf("full pipeline: %+v, %v", sc, err)
	}
	sc, err = ScopeFor(WithUnscoped(context.Background()), o)
	if err != nil || !sc.Unscoped() {
		t.Fatalf("opt-out: %+v, %v", sc, err)
	}
	// An empty id changes nothing: still no scope.
	if _, err := ScopeFor(WithPipelineID(context.Background(), ""), o); !errors.Is(err, ErrNoScope) {
		t.Fatalf("empty id: err = %v, want ErrNoScope", err)
	}
	// The room's pipeline wins over the opt-out, in either order.
	for _, ctx := range []context.Context{
		WithUnscoped(WithPipelineID(context.Background(), "orders")),
		WithPipelineID(WithUnscoped(context.Background()), "orders"),
	} {
		sc, err := ScopeFor(ctx, o)
		if err != nil || sc.Unscoped() {
			t.Fatalf("pipeline + opt-out: %+v, %v", sc, err)
		}
	}
	if _, ok := PipelineFrom(WithPipelineID(context.Background(), "orders")); ok {
		t.Fatal("PipelineFrom must report false for an id-only context")
	}
}

func TestScopePredicates(t *testing.T) {
	o := scopeTestOwners()
	sc, err := ScopeFor(WithPipelineID(context.Background(), "orders"), o)
	if err != nil {
		t.Fatal(err)
	}
	pass := map[string]error{
		"pipeline id":   sc.CheckPipelineID("orders"),
		"empty id":      sc.CheckPipelineID(""),
		"connector":     sc.CheckConnector("pg-orders-connector"),
		"group":         sc.CheckGroup("order-sync"),
		"topic":         sc.CheckTopic("pg.public.orders"),
		"value subject": sc.CheckSubject("pg.public.orders-value"),
		"key subject":   sc.CheckSubject("pg.public.orders-key"),
		"source table":  sc.CheckSourceTable("public.orders"),
		"sink table":    sc.CheckSinkTable("analytics.orders"),
	}
	for name, err := range pass {
		if err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
	refuse := map[string]error{
		"pipeline id":          sc.CheckPipelineID("billing"),
		"connector":            sc.CheckConnector("bill-connector"),
		"empty connector":      sc.CheckConnector(""),
		"group":                sc.CheckGroup("bill-sync"),
		"topic":                sc.CheckTopic("bill.public.orders"),
		"topic prefix only":    sc.CheckTopic("pg"),
		"other table topic":    sc.CheckTopic("pg.public.customers"),
		"subject other table":  sc.CheckSubject("pg.public.customers-value"),
		"subject foreign pfx":  sc.CheckSubject("billing.public.orders-value"),
		"subject nested pfx":   sc.CheckSubject("x.pg.public.orders-value"),
		"subject no suffix":    sc.CheckSubject("pg.public.orders"),
		"subject empty":        sc.CheckSubject(""),
		"source table":         sc.CheckSourceTable("public.payments"),
		"sink table":           sc.CheckSinkTable("analytics.payments"),
		"sink table as source": sc.CheckSourceTable("analytics.orders"),
	}
	for name, err := range refuse {
		if !errors.Is(err, ErrOutOfScope) {
			t.Errorf("%s: err = %v, want ErrOutOfScope", name, err)
		}
	}
}

func TestScopeEmptyTopicPrefixMatchesNoTopic(t *testing.T) {
	sc := Scope{p: owners.Pipeline{ID: "x", SourceTables: []string{"public.orders"}}}
	if err := sc.CheckTopic(".public.orders"); err == nil {
		t.Fatal("a pipeline with no topic_prefix must have no topics")
	}
	if sc.SubjectInScope(".public.orders-value") {
		t.Fatal("a pipeline with no topic_prefix must have no subjects")
	}
}

func TestScopeFilters(t *testing.T) {
	sc, _ := ScopeFor(WithPipelineID(context.Background(), "orders"), scopeTestOwners())
	if got := sc.FilterConnectors([]string{"a", "pg-orders-connector", "b"}); !slices.Equal(got, []string{"pg-orders-connector"}) {
		t.Errorf("connectors = %v", got)
	}
	if got := sc.FilterGroups([]string{"order-sync", "bill-sync"}); !slices.Equal(got, []string{"order-sync"}) {
		t.Errorf("groups = %v", got)
	}
	if got := sc.FilterSubjects([]string{"pg.public.orders-value", "pg.public.customers-value", "orders-value"}); !slices.Equal(got, []string{"pg.public.orders-value"}) {
		t.Errorf("subjects = %v", got)
	}
	un, _ := ScopeFor(WithUnscoped(context.Background()), nil)
	if got := un.FilterGroups([]string{"a", "b"}); !slices.Equal(got, []string{"a", "b"}) {
		t.Errorf("unscoped filter = %v", got)
	}
	if un.CheckConnector("anything") != nil || un.CheckPipelineID("anything") != nil {
		t.Error("unscoped checks must pass")
	}
}

func TestScopeErrorEchoIsBounded(t *testing.T) {
	sc, _ := ScopeFor(WithPipelineID(context.Background(), "orders"), scopeTestOwners())
	long := make([]byte, 5000)
	for i := range long {
		long[i] = 'x'
	}
	err := sc.CheckConnector(string(long))
	if err == nil || len(err.Error()) > 600 {
		t.Fatalf("error length = %d, want a short message", len(err.Error()))
	}
}
