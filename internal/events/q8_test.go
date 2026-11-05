package events

import (
	"slices"
	"testing"
)

func TestSourceLayer(t *testing.T) {
	cases := []struct {
		kind, want string
	}{
		{"", "postgres"},
		{SourceKindPostgres, "postgres"},
		{SourceKindMySQL, LayerMySQL},
		{"sqlserver", ""},
		{"MySQL", ""},
		{"oracle", ""},
	}
	for _, c := range cases {
		if got := SourceLayer(c.kind); got != c.want {
			t.Errorf("SourceLayer(%q) = %q, want %q", c.kind, got, c.want)
		}
	}
}

func TestLayersOfSource(t *testing.T) {
	cdcMySQL := []string{"mysql", "kafka-connect", "schema-registry", "kafka", "consumer", "warehouse", LayerEnvironment}
	cases := []struct {
		name, kind, source string
		want               []string
	}{
		{"default kind, default source", "", "", LayersOf(PipelineKindCDC)},
		{"cdc postgres", PipelineKindCDC, SourceKindPostgres, LayersOf(PipelineKindCDC)},
		{"cdc mysql", PipelineKindCDC, SourceKindMySQL, cdcMySQL},
		{"default kind mysql", "", SourceKindMySQL, cdcMySQL},
		{"cdc unknown source fails closed", PipelineKindCDC, "sqlserver", nil},
		{"batch ignores source", PipelineKindBatch, SourceKindMySQL, LayersOf(PipelineKindBatch)},
		{"batch ignores unknown source", PipelineKindBatch, "nope", LayersOf(PipelineKindBatch)},
		{"unknown kind", "stream", "", nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := LayersOfSource(c.kind, c.source); !slices.Equal(got, c.want) {
				t.Errorf("LayersOfSource(%q, %q) = %v, want %v", c.kind, c.source, got, c.want)
			}
		})
	}
}

func TestLayersOfSourceNewSlice(t *testing.T) {
	a := LayersOfSource("", SourceKindMySQL)
	a[0] = "changed"
	if b := LayersOfSource("", SourceKindMySQL); b[0] != LayerMySQL {
		t.Fatalf("LayersOfSource shares its slice: got %q", b[0])
	}
	if l := LayersOf(""); l[0] != "postgres" {
		t.Fatalf("LayersOfSource changed LayersOf: got %q", l[0])
	}
}

func TestLayerMySQLAppended(t *testing.T) {
	if Layers[len(Layers)-1] != LayerMySQL {
		t.Fatalf("Layers must end with %q, got %v", LayerMySQL, Layers)
	}
	// Every earlier position is unchanged (U: environment stays at index 8).
	if Layers[0] != "postgres" || Layers[8] != LayerEnvironment {
		t.Fatalf("Layers positions moved: %v", Layers)
	}
	if !slices.Contains(IncidentLayers(), LayerMySQL) {
		t.Fatalf("IncidentLayers() must contain %q", LayerMySQL)
	}
	for _, k := range []string{SourceKindPostgres, SourceKindMySQL} {
		if !slices.Contains(Layers, SourceLayer(k)) {
			t.Errorf("SourceLayer(%q) not in Layers", k)
		}
	}
}
