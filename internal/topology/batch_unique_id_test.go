package topology

import (
	"context"
	"errors"
	"slices"
	"testing"

	"piperoom/internal/clients"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
)

// KAN-166 (P4a-5): a model listed by unique_id is drawn under its bare name
// and admitted only in its own package. The builder and tools.Scope read the
// same owners pipeline, so they agree on which unique_id model belongs.
func TestBatchModelUniqueIDAgreesWithScope(t *testing.T) {
	o := &owners.Owners{
		Pipelines: []owners.Pipeline{{
			ID: "pinned", Kind: "batch",
			Models:       []string{"model.pkg_a.orders", "dim_customer"},
			SinkTables:   []string{"analytics.orders"},
			SinkTSColumn: "loaded_at",
		}},
		Layers: owners.UserLayers(map[string]string{"transform": "tara", "warehouse": "meera"}),
	}
	tr := &fakeTransform{
		results: clients.RunResults{Results: []clients.ModelResult{
			{Model: "model.pkg_a.orders", Status: "success", Rows: 5},
			{Model: "model.analytics.dim_customer", Status: "success", Rows: 1},
		}},
		lineage: map[string]clients.Lineage{
			"model.pkg_a.orders": {Model: "model.pkg_a.orders", Parents: []string{"model.pkg_a.stg_orders"}, Children: []string{}},
			"dim_customer":       {Model: "model.analytics.dim_customer", Parents: []string{}, Children: []string{}},
		},
	}
	g, err := newBatchBuilder(o, nil, WithTransform(tr)).Build(context.Background(), "pinned")
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, n := range g.Nodes {
		if n.Kind == KindModel {
			got = append(got, n.ID)
		}
	}
	if want := []string{"model:orders", "model:dim_customer"}; !slices.Equal(got, want) {
		t.Fatalf("model nodes = %v, want %v (a unique_id entry is drawn under its bare name)", got, want)
	}

	sc, err := tools.ScopeFor(tools.WithPipeline(context.Background(), o.Pipelines[0]), o)
	if err != nil {
		t.Fatal(err)
	}
	if err := sc.CheckModel("model.pkg_a.orders"); err != nil {
		t.Errorf("scope refuses the listed unique_id: %v", err)
	}
	if err := sc.CheckModel("model.pkg_b.orders"); !errors.Is(err, tools.ErrOutOfScope) {
		t.Errorf("scope admits another package's orders: %v", err)
	}
	if err := sc.CheckModel("dim_customer"); err != nil {
		t.Errorf("scope refuses the bare-listed model: %v", err)
	}
}
