package dbt

import (
	"context"
	"errors"
	"io/fs"
	"slices"
	"strings"
	"testing"

	"piperoom/internal/clients"
	"piperoom/internal/tools"
)

func TestDirModelSQL(t *testing.T) {
	v12 := fixture(t, "manifest_v12.json")
	withMeta := []byte(`{"metadata":{"dbt_schema_version":"https://schemas.getdbt.com/dbt/manifest/v11.json"},
 "nodes":{"model.analytics.fct_revenue":{"name":"fct_revenue","resource_type":"model","original_file_path":"models/fct_revenue.sql","language":"sql","compiled_code":"select 1","raw_code":"select {{ 1 }}"}},
 "parent_map":{"model.analytics.fct_revenue":[]},"child_map":{"model.analytics.fct_revenue":[]}}`)
	tests := []struct {
		name     string
		manifest []byte
		model    string
		want     ModelSQL
		wantErr  []error
	}{
		{name: "compiled", manifest: v12, model: "fct_revenue",
			want: ModelSQL{Model: "model.analytics.fct_revenue", Compiled: true, SQL: "select order_id, sum(amount_cents) as revenue_cents from stg_orders group by 1"}},
		{name: "raw only, by unique id", manifest: v12, model: "model.analytics.stg_orders",
			want: ModelSQL{Model: "model.analytics.stg_orders", SQL: "select * from {{ source('shop', 'orders') }}"}},
		{name: "path and language", manifest: withMeta, model: "fct_revenue",
			want: ModelSQL{Model: "model.analytics.fct_revenue", Path: "models/fct_revenue.sql", Language: "sql", Compiled: true, SQL: "select 1"}},
		{name: "ambiguous", manifest: v12, model: "dim_customer", wantErr: []error{ErrAmbiguous}},
		{name: "unknown", manifest: v12, model: "fct_missing", wantErr: []error{clients.ErrNotFound}},
		{name: "source is not a model", manifest: v12, model: "source.analytics.shop.orders", wantErr: []error{clients.ErrNotFound}},
		{name: "test is not a model", manifest: v12, model: "test.analytics.not_null_fct_revenue_order_id.5c8d1e", wantErr: []error{clients.ErrNotFound}},
		{name: "empty", manifest: v12, model: " "},
		{name: "truncated", manifest: v12[:len(v12)/2], model: "fct_revenue", wantErr: []error{ErrMalformed}},
		{name: "version mismatch", manifest: []byte(strings.Replace(string(v12), "manifest/v12.json", "manifest/v8.json", 1)), model: "fct_revenue", wantErr: []error{ErrUnsupportedVersion}},
		{name: "missing file", model: "fct_revenue", wantErr: []error{ErrNoArtifact, fs.ErrNotExist}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			files := map[string][]byte{}
			if tt.manifest != nil {
				files[ManifestFile] = tt.manifest
			}
			got, err := artifactsDir(t, files).ModelSQL(context.Background(), tt.model)
			if tt.want.Model == "" {
				if err == nil {
					t.Fatalf("no error, got %+v", got)
				}
				for _, want := range tt.wantErr {
					if !errors.Is(err, want) {
						t.Errorf("err = %v, want %v", err, want)
					}
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got != tt.want {
				t.Errorf("got %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestAmbiguousErrorCandidates(t *testing.T) {
	_, err := artifactsDir(t, map[string][]byte{ManifestFile: fixture(t, "manifest_v12.json")}).ModelLineage(context.Background(), "dim_customer")
	var ae *AmbiguousError
	if !errors.As(err, &ae) || !errors.Is(err, ErrAmbiguous) {
		t.Fatalf("err = %v, want *AmbiguousError", err)
	}
	want := []string{"model.analytics.dim_customer", "model.marketing.dim_customer"}
	if ae.Name != "dim_customer" || !slices.Equal(ae.Candidates, want) {
		t.Errorf("got %+v, want candidates %v", ae, want)
	}
	if !strings.Contains(err.Error(), "matches 2 nodes") {
		t.Errorf("text = %q", err.Error())
	}
}

func TestDirRecordsProvenance(t *testing.T) {
	d := artifactsDir(t, map[string][]byte{ManifestFile: fixture(t, "manifest_v12.json")})
	rec := tools.NewRecorder()
	ctx := tools.WithRecorder(context.Background(), rec)
	if _, err := d.ModelSQL(ctx, "fct_revenue"); err != nil {
		t.Fatal(err)
	}
	if _, err := d.RunResults(ctx); err == nil {
		t.Fatal("missing run_results.json read")
	}
	calls := rec.Calls()
	if len(calls) != 2 {
		t.Fatalf("calls = %+v (want one per read, failed reads included)", calls)
	}
	if calls[0].Kind != CallFile || calls[0].Target != d.dir || calls[0].Text != "read manifest.json" || calls[1].Text != "read run_results.json" {
		t.Errorf("calls = %+v", calls)
	}
}

func TestCloudModelSQL(t *testing.T) {
	c := cloudClient(t, fakeCloud(t, runsBody, map[string][]byte{ManifestFile: fixture(t, "manifest_v12.json")}))
	got, err := c.ModelSQL(context.Background(), "fct_revenue")
	if err != nil {
		t.Fatal(err)
	}
	if got.Model != "model.analytics.fct_revenue" || !got.Compiled || !strings.Contains(got.SQL, "amount_cents") {
		t.Errorf("sql = %+v", got)
	}
	if _, err := c.ModelSQL(context.Background(), "nope"); !errors.Is(err, clients.ErrNotFound) {
		t.Errorf("unknown model err = %v", err)
	}
	none := cloudClient(t, fakeCloud(t, runsBody, nil))
	if _, err := none.ModelSQL(context.Background(), "fct_revenue"); !errors.Is(err, ErrNoArtifact) {
		t.Errorf("no manifest err = %v", err)
	}
}
