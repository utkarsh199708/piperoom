package demo

// The demo topology (KAN-575): daily-revenue draws its own batch graph, and
// the orders graph stops showing the failure once a room's fix has run.

import (
	"context"
	"strings"
	"testing"

	"piperoom/internal/room"
	"piperoom/internal/topology"
)

func buildGraph(t *testing.T, b Builder, pipelineID string) topology.Graph {
	t.Helper()
	g, err := b.Build(context.Background(), pipelineID)
	if err != nil {
		t.Fatalf("Build(%q): %v", pipelineID, err)
	}
	return g
}

func nodeByID(t *testing.T, g topology.Graph, id string) topology.Node {
	t.Helper()
	for _, n := range g.Nodes {
		if n.ID == id {
			return n
		}
	}
	t.Fatalf("graph %q has no node %q", g.PipelineID, id)
	return topology.Node{}
}

func TestBuilderBatchPipelineHasItsOwnNodes(t *testing.T) {
	g := buildGraph(t, Builder{Fakes: NewFakes()}, "daily-revenue")
	if g.PipelineID != "daily-revenue" {
		t.Errorf("PipelineID = %q, want daily-revenue", g.PipelineID)
	}
	want := map[string]topology.NodeKind{
		"dag:revenue_daily":        topology.KindDAG,
		"model:stg_orders":         topology.KindModel,
		"model:fct_revenue":        topology.KindModel,
		"wh:analytics.fct_revenue": topology.KindSinkTable,
	}
	if len(g.Nodes) != len(want) {
		t.Fatalf("got %d nodes, want %d: %+v", len(g.Nodes), len(want), g.Nodes)
	}
	for id, kind := range want {
		if n := nodeByID(t, g, id); n.Kind != kind {
			t.Errorf("node %q kind = %q, want %q", id, n.Kind, kind)
		}
	}
	for _, n := range g.Nodes {
		if n.Kind == topology.KindConnector || n.Kind == topology.KindTopic || n.Kind == topology.KindConsumerGroup {
			t.Errorf("batch graph carries CDC node %q (%s)", n.ID, n.Kind)
		}
		if strings.Contains(n.ID, "orders") && n.ID != "model:stg_orders" {
			t.Errorf("batch graph carries orders CDC node %q", n.ID)
		}
	}
	ids := map[string]bool{}
	for _, n := range g.Nodes {
		ids[n.ID] = true
	}
	for _, e := range g.Edges {
		if !ids[e.From] || !ids[e.To] {
			t.Errorf("edge %s -> %s names a node outside the graph", e.From, e.To)
		}
	}
}

func TestBuilderOrdersShowsFailureBeforeAnyFix(t *testing.T) {
	g := buildGraph(t, Builder{Fakes: NewFakes()}, "orders")
	if n := nodeByID(t, g, "connect:pg-orders-connector"); n.Health != topology.HealthFail || n.Facts["task_0"] != "FAILED" {
		t.Errorf("connector = %s / %v, want fail / task_0 FAILED", n.Health, n.Facts)
	}
	if n := nodeByID(t, g, "wh:analytics.orders"); n.Health != topology.HealthFail || n.Facts["freshness"] != "stale" {
		t.Errorf("sink = %s / %v, want fail / stale", n.Health, n.Facts)
	}
	// A zero Builder with no controls behind it is the unrecovered story.
	if g := buildGraph(t, Builder{}, "orders"); nodeByID(t, g, "connect:pg-orders-connector").Health != topology.HealthFail {
		t.Error("zero Builder: connector should still show the failure")
	}
}

func TestBuilderOrdersRecoversAfterFix(t *testing.T) {
	fakes := NewFakes()
	b := Builder{Fakes: fakes}
	ctx := room.WithRoomID(context.Background(), "r_1")
	if err := fakes.Connect.RestartTask(ctx, connectorName, 0); err != nil {
		t.Fatalf("RestartTask: %v", err)
	}
	g := buildGraph(t, b, "orders")
	for _, n := range g.Nodes {
		if n.Health != topology.HealthOK {
			t.Errorf("node %q health = %s after the fix, want ok", n.ID, n.Health)
		}
	}
	if got := nodeByID(t, g, "connect:pg-orders-connector").Facts["task_0"]; got != "RUNNING" {
		t.Errorf("task_0 = %q, want RUNNING", got)
	}
	if got := nodeByID(t, g, "wh:analytics.orders").Facts["freshness"]; got != "fresh" {
		t.Errorf("freshness = %q, want fresh", got)
	}
	// The batch graph does not change with the orders story.
	if buildGraph(t, b, "daily-revenue").Nodes[0].Health != topology.HealthOK {
		t.Error("batch graph should stay healthy")
	}
	// A reset puts the failure back.
	fakes.Reset("r_1")
	if nodeByID(t, buildGraph(t, b, "orders"), "connect:pg-orders-connector").Health != topology.HealthFail {
		t.Error("after reset the connector should show the failure again")
	}
}

// The --demo server builds a zero Builder{} and NewControls over one pair of
// fakes; the Builder must find that pair without being handed it.
func TestZeroBuilderFollowsControlsFakes(t *testing.T) {
	t.Cleanup(func() { defaultFakes.Store(nil) })
	fakes := NewFakes()
	NewControls(fakes)
	if err := fakes.Connect.RestartTask(room.WithRoomID(context.Background(), "r_1"), connectorName, 0); err != nil {
		t.Fatalf("RestartTask: %v", err)
	}
	g := buildGraph(t, Builder{}, "orders")
	if n := nodeByID(t, g, "connect:pg-orders-connector"); n.Health != topology.HealthOK {
		t.Errorf("zero Builder after the fix: connector = %s, want ok", n.Health)
	}
}
