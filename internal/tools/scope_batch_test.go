package tools

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"piperoom/internal/clients"
	"piperoom/internal/owners"
)

// batchScopeOwners has one batch pipeline (the P4 example) next to the cdc
// fixture's pipeline, so a name of either is out of the other's scope.
func batchScopeOwners() *owners.Owners {
	o := scopeTestOwners()
	o.Pipelines = append(o.Pipelines, owners.Pipeline{
		ID: "daily-revenue", Kind: "batch",
		DAGs:         []string{"revenue_daily"},
		Models:       []string{"fct_revenue", "dim_customer"},
		SinkTables:   []string{"analytics.fct_revenue"},
		SinkTSColumn: "loaded_at",
	})
	return o
}

func batchScopeFor(t *testing.T, id string) Scope {
	t.Helper()
	sc, err := ScopeFor(WithPipelineID(context.Background(), id), batchScopeOwners())
	if err != nil {
		t.Fatal(err)
	}
	return sc
}

func TestBatchScope(t *testing.T) {
	batch, cdc := batchScopeFor(t, "daily-revenue"), batchScopeFor(t, "orders")
	unscoped, err := ScopeFor(WithUnscoped(context.Background()), nil)
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name    string
		err     error
		wantOK  bool
		wantMsg string // substring of the refusal
	}{
		// The batch pipeline's own names.
		{"batch dag", batch.CheckDAG("revenue_daily"), true, ""},
		{"batch model", batch.CheckModel("fct_revenue"), true, ""},
		{"batch sink table", batch.CheckSinkTable("analytics.fct_revenue"), true, ""},
		{"batch pipeline id", batch.CheckPipelineID("daily-revenue"), true, ""},
		// Out of its scope.
		{"other dag", batch.CheckDAG("kpi_weekly"), false, "allowed DAGs: revenue_daily"},
		{"dag case differs", batch.CheckDAG("Revenue_Daily"), false, "DAG"},
		{"other model", batch.CheckModel("stg_orders"), false, "allowed models: fct_revenue, dim_customer"},
		{"model unique_id is not a name", batch.CheckModel("model.analytics.fct_revenue"), false, "model"},
		{"cdc sink table", batch.CheckSinkTable("analytics.orders"), false, "allowed sink tables: analytics.fct_revenue"},
		{"cdc pipeline id", batch.CheckPipelineID("orders"), false, "not this room's pipeline"},
		// Every cdc check refuses on a batch pipeline.
		{"connector", batch.CheckConnector("pg-orders-connector"), false, "connector"},
		{"group", batch.CheckGroup("order-sync"), false, "consumer group"},
		{"topic", batch.CheckTopic("pg.public.orders"), false, "topic"},
		{"subject", batch.CheckSubject("pg.public.orders-value"), false, "subject"},
		{"source table", batch.CheckSourceTable("public.orders"), false, "table"},
		{"dlq topic", batch.CheckDLQTopic("orders-dlq"), false, "DLQ topic"},
		// And a cdc pipeline has no dags or models.
		{"cdc dag", cdc.CheckDAG("revenue_daily"), false, "this pipeline has no DAGs"},
		{"cdc model", cdc.CheckModel("fct_revenue"), false, "this pipeline has no models"},
		// Unscoped callers (CLI, dev) are not filtered.
		{"unscoped dag", unscoped.CheckDAG("anything"), true, ""},
		{"unscoped model", unscoped.CheckModel("anything"), true, ""},
	}
	for _, c := range cases {
		if c.wantOK {
			if c.err != nil {
				t.Errorf("%s: %v, want allowed", c.name, c.err)
			}
			continue
		}
		if !errors.Is(c.err, ErrOutOfScope) || !strings.Contains(c.err.Error(), c.wantMsg) {
			t.Errorf("%s: err = %v, want ErrOutOfScope containing %q", c.name, c.err, c.wantMsg)
		}
	}

	for sc, want := range map[*Scope]string{&batch: "batch", &cdc: "cdc", &unscoped: ""} {
		if got := sc.Kind(); got != want {
			t.Errorf("Kind = %q, want %q", got, want)
		}
	}
	if got := batch.Topics(); len(got) != 0 {
		t.Errorf("a batch pipeline has no topics: %v", got)
	}
	// No connector, so no slot: an empty answer, not an error and not "every slot".
	if slots, err := batch.Slots(context.Background(), nil); err != nil || len(slots) != 0 {
		t.Errorf("batch Slots = %v, %v; want none", slots, err)
	}
	if got := batch.FilterConnectors([]string{"pg-orders-connector"}); len(got) != 0 {
		t.Errorf("FilterConnectors on batch = %v", got)
	}
}

// fakeLineage answers ModelLineage from a map and counts calls.
type fakeLineage struct {
	l     map[string]clients.Lineage
	err   map[string]error
	calls int
}

func (f *fakeLineage) ModelLineage(_ context.Context, m string) (clients.Lineage, error) {
	f.calls++
	if err := f.err[m]; err != nil {
		return clients.Lineage{}, err
	}
	l, ok := f.l[m]
	if !ok {
		return clients.Lineage{}, fmt.Errorf("dbt: %q: %w", m, clients.ErrNotFound)
	}
	return l, nil
}

func TestCheckModelOrAdjacent(t *testing.T) {
	lineage := map[string]clients.Lineage{
		"fct_revenue":  {Parents: []string{"model.analytics.stg_orders", "source.analytics.raw.payments"}, Children: []string{"model.analytics.revenue_mart"}},
		"dim_customer": {Parents: []string{"seed.analytics.customers"}, Children: []string{}},
	}
	down := errors.New("dbt cloud: 503")
	cases := []struct {
		name      string
		scope     string
		model     string
		reader    func() *fakeLineage // nil = a nil LineageReader
		wantOK    bool
		wantIs    error // for a refusal or failure
		wantCalls int   // -1 = not checked
	}{
		{name: "a pipeline model needs no lineage", scope: "daily-revenue", model: "fct_revenue",
			reader: func() *fakeLineage { return &fakeLineage{l: lineage} }, wantOK: true, wantCalls: 0},
		{name: "a pipeline model with a nil reader", scope: "daily-revenue", model: "dim_customer", wantOK: true, wantCalls: -1},
		{name: "a parent", scope: "daily-revenue", model: "stg_orders",
			reader: func() *fakeLineage { return &fakeLineage{l: lineage} }, wantOK: true, wantCalls: -1},
		{name: "a child", scope: "daily-revenue", model: "revenue_mart",
			reader: func() *fakeLineage { return &fakeLineage{l: lineage} }, wantOK: true, wantCalls: -1},
		{name: "a source parent is not a model", scope: "daily-revenue", model: "payments",
			reader: func() *fakeLineage { return &fakeLineage{l: lineage} }, wantIs: ErrOutOfScope, wantCalls: 2},
		{name: "a seed parent is not a model", scope: "daily-revenue", model: "customers",
			reader: func() *fakeLineage { return &fakeLineage{l: lineage} }, wantIs: ErrOutOfScope, wantCalls: 2},
		{name: "two hops away", scope: "daily-revenue", model: "raw_orders",
			reader: func() *fakeLineage { return &fakeLineage{l: lineage} }, wantIs: ErrOutOfScope, wantCalls: 2},
		{name: "a unique_id is refused before any read", scope: "daily-revenue", model: "model.analytics.stg_orders",
			reader: func() *fakeLineage { return &fakeLineage{l: lineage} }, wantIs: ErrOutOfScope, wantCalls: 0},
		{name: "a name outside the model alphabet", scope: "daily-revenue", model: "stg orders; drop",
			reader: func() *fakeLineage { return &fakeLineage{l: lineage} }, wantIs: ErrOutOfScope, wantCalls: 0},
		{name: "a model gone from the manifest has no neighbours", scope: "daily-revenue", model: "stg_orders",
			reader: func() *fakeLineage {
				return &fakeLineage{l: lineage, err: map[string]error{"dim_customer": fmt.Errorf("dbt: %w", clients.ErrNotFound)}}
			}, wantOK: true, wantCalls: -1},
		{name: "a failing read fails closed", scope: "daily-revenue", model: "stg_orders",
			reader: func() *fakeLineage { return &fakeLineage{l: lineage, err: map[string]error{"fct_revenue": down}} },
			wantIs: down, wantCalls: 1},
		{name: "no reader", scope: "daily-revenue", model: "stg_orders", wantIs: clients.ErrNotSupported, wantCalls: -1},
		{name: "a cdc pipeline has no models", scope: "orders", model: "fct_revenue", wantIs: ErrOutOfScope, wantCalls: -1},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			sc := batchScopeFor(t, c.scope)
			var lr LineageReader
			var f *fakeLineage
			if c.reader != nil {
				f = c.reader()
				lr = f
			}
			err := sc.CheckModelOrAdjacent(context.Background(), c.model, lr)
			if c.wantOK {
				if err != nil {
					t.Fatalf("err = %v, want allowed", err)
				}
			} else if !errors.Is(err, c.wantIs) {
				t.Fatalf("err = %v, want %v", err, c.wantIs)
			}
			if c.wantIs != nil && c.wantIs != ErrOutOfScope && errors.Is(err, ErrOutOfScope) {
				t.Fatalf("a read failure must not read as a scope refusal: %v", err)
			}
			if f != nil && c.wantCalls >= 0 && f.calls != c.wantCalls {
				t.Errorf("lineage reads = %d, want %d", f.calls, c.wantCalls)
			}
		})
	}

	unscoped, err := ScopeFor(WithUnscoped(context.Background()), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := unscoped.CheckModelOrAdjacent(context.Background(), "anything", nil); err != nil {
		t.Errorf("unscoped: %v", err)
	}
	// A long name is cut before it is echoed.
	long := strings.Repeat("m", 200)
	err = batchScopeFor(t, "daily-revenue").CheckModelOrAdjacent(context.Background(), long, &fakeLineage{l: lineage})
	if !errors.Is(err, ErrOutOfScope) || strings.Contains(err.Error(), long) {
		t.Errorf("long name: %v", err)
	}
}
