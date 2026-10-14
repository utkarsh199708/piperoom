package topology

import (
	"encoding/json"
	"slices"
	"strings"
	"testing"
)

// A client written before edge kinds (it knows only from/to) must still
// decode a new graph, and a proven data edge must keep its old JSON shape.
func TestGraphJSONOldClientDecodesNewGraph(t *testing.T) {
	_, g := goldenGraph(t)
	raw, err := json.Marshal(g)
	if err != nil {
		t.Fatal(err)
	}
	var old struct {
		PipelineID string `json:"pipeline_id"`
		Nodes      []struct {
			ID   string `json:"id"`
			Kind string `json:"kind"`
		} `json:"nodes"`
		Edges []struct {
			From string `json:"from"`
			To   string `json:"to"`
		} `json:"edges"`
	}
	if err := json.Unmarshal(raw, &old); err != nil {
		t.Fatalf("old client cannot decode a new graph: %v", err)
	}
	if old.PipelineID != "orders" || len(old.Nodes) != len(g.Nodes) || len(old.Edges) != len(g.Edges) {
		t.Fatalf("old decode lost data: %d nodes, %d edges", len(old.Nodes), len(old.Edges))
	}

	proven, err := json.Marshal(Edge{From: "pg:public.orders", To: "connect:c"})
	if err != nil {
		t.Fatal(err)
	}
	if string(proven) != `{"from":"pg:public.orders","to":"connect:c"}` {
		t.Fatalf("proven data edge JSON = %s, want the pre-kind shape", proven)
	}
	dep, _ := json.Marshal(Edge{From: "group:g", To: "sr:s", Kind: EdgeDependsOn, Mapping: MappingInferred})
	if string(dep) != `{"from":"group:g","to":"sr:s","kind":"depends_on","mapping":"inferred"}` {
		t.Fatalf("depends_on edge JSON = %s", dep)
	}
	if !strings.Contains(string(raw), `"kind":"schema_subject"`) {
		t.Fatalf("graph JSON has no schema_subject node: %s", raw)
	}
}

// A graph from before edge kinds (no kind, no mapping) reads as proven data
// flow; the explicit default values read the same way.
func TestGraphJSONOmittedFieldsDefault(t *testing.T) {
	const old = `{"pipeline_id":"orders","nodes":[],"edges":[
		{"from":"a","to":"b"},
		{"from":"b","to":"c","kind":"data","mapping":"mapped"},
		{"from":"c","to":"sr:s","kind":"depends_on"},
		{"from":"c","to":"d","mapping":"inferred"}]}`
	var g Graph
	if err := json.Unmarshal([]byte(old), &g); err != nil {
		t.Fatal(err)
	}
	type view struct{ dep, inferred bool }
	var got []view
	for _, e := range g.Edges {
		got = append(got, view{e.IsDependency(), e.IsInferred()})
	}
	want := []view{{false, false}, {false, false}, {true, false}, {false, true}}
	if !slices.Equal(got, want) {
		t.Fatalf("edge views = %v, want %v", got, want)
	}
	// And Render treats them that way: only the inferred one is unconfirmed.
	g.Nodes = []Node{{ID: "a"}, {ID: "b"}, {ID: "c"}, {ID: "d"}, {ID: "sr:s"}}
	out := NewBuilder(testOwners(), nil, nil, nil, nil).Render(g)
	for _, line := range []string{"  from: a\n", "  from: b\n", "  depends on: sr:s\n", "  from: c (unconfirmed)\n"} {
		if !strings.Contains(out, line) {
			t.Fatalf("render missing %q:\n%s", line, out)
		}
	}
}
