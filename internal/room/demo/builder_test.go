package demo

// The demo topology (KAN-575): daily-revenue draws its own batch graph, and
// the orders graph stops showing the failure once a room's fix has run.

import (
	"context"
	"strings"
	"testing"
	"time"

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
	check := func(label string, g topology.Graph) {
		t.Helper()
		if n := nodeByID(t, g, "connect:pg-orders-connector"); n.Health != topology.HealthFail || n.Facts["task_0"] != "FAILED" {
			t.Errorf("%s: connector = %s / %v, want fail / task_0 FAILED", label, n.Health, n.Facts)
		}
		if n := nodeByID(t, g, "wh:analytics.orders"); n.Health != topology.HealthFail || n.Facts["freshness"] != "stale" {
			t.Errorf("%s: sink = %s / %v, want fail / stale", label, n.Health, n.Facts)
		}
	}
	check("fresh fakes", buildGraph(t, Builder{Fakes: NewFakes()}, "orders"))
	// In the real demo the agent calls Status first, which creates a room
	// state with task 0 FAILED. That touched-but-unfixed room must not count
	// as recovered.
	// The room is also the one the map follows (as when Controls.Run started it).
	fakes := NewFakes()
	fakes.Connect.markRoom("r_1")
	if _, err := fakes.Connect.Status(room.WithRoomID(context.Background(), "r_1"), connectorName); err != nil {
		t.Fatalf("Status: %v", err)
	}
	check("after Status", buildGraph(t, Builder{Fakes: fakes}, "orders"))
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

func connectorHealth(t *testing.T, b Builder) topology.Health {
	t.Helper()
	return nodeByID(t, buildGraph(t, b, "orders"), "connect:pg-orders-connector").Health
}

// The topology endpoint has no room id, so the map follows the room that last
// started the story, was reset, or ran its fix.
func TestBuilderFollowsLastChangedRoom(t *testing.T) {
	fakes := NewFakes()
	b := Builder{Fakes: fakes}
	ctxA := room.WithRoomID(context.Background(), "r_a")
	ctxB := room.WithRoomID(context.Background(), "r_b")

	fakes.Connect.markRoom("r_a")
	if got := connectorHealth(t, b); got != topology.HealthFail {
		t.Fatalf("room A before its fix: %s, want fail", got)
	}
	if err := fakes.Connect.RestartTask(ctxA, connectorName, 0); err != nil {
		t.Fatal(err)
	}
	if got := connectorHealth(t, b); got != topology.HealthOK {
		t.Fatalf("after room A's fix: %s, want ok", got)
	}

	// A new room B shows the failure, even though A is recovered.
	fakes.Connect.markRoom("r_b")
	if got := connectorHealth(t, b); got != topology.HealthFail {
		t.Fatalf("new room B: %s, want fail", got)
	}
	// Reads in A do not move the map.
	if _, err := fakes.Connect.Status(ctxA, connectorName); err != nil {
		t.Fatal(err)
	}
	if _, err := fakes.Connect.Config(ctxA, connectorName); err != nil {
		t.Fatal(err)
	}
	if got := connectorHealth(t, b); got != topology.HealthFail {
		t.Fatalf("after reads in room A: %s, want fail", got)
	}
	if err := fakes.Connect.Resume(ctxB, connectorName); err != nil {
		t.Fatal(err)
	}
	if got := connectorHealth(t, b); got != topology.HealthOK {
		t.Fatalf("after room B's fix: %s, want ok", got)
	}

	// Reset of the recovered room shows the failure again.
	fakes.Reset("r_b")
	if got := connectorHealth(t, b); got != topology.HealthFail {
		t.Fatalf("after reset of room B: %s, want fail", got)
	}
	// Reset of an older room makes the map follow that room: A is recovered
	// again after its fix, B reset last, then A reset.
	if err := fakes.Connect.RestartTask(ctxA, connectorName, 0); err != nil {
		t.Fatal(err)
	}
	if err := fakes.Connect.RestartTask(ctxB, connectorName, 0); err != nil {
		t.Fatal(err)
	}
	fakes.Reset("r_a")
	if got := connectorHealth(t, b); got != topology.HealthFail {
		t.Fatalf("after reset of older room A (B still recovered): %s, want fail", got)
	}
}

func TestControlsRunMarksRoom(t *testing.T) {
	fakes := NewFakes()
	c := NewControls(fakes)
	t.Cleanup(func() { defaultFakes.Store(nil) })
	b := Builder{Fakes: fakes}
	ctxA := room.WithRoomID(context.Background(), "r_a")
	if err := fakes.Connect.RestartTask(ctxA, connectorName, 0); err != nil {
		t.Fatal(err)
	}
	if got := connectorHealth(t, b); got != topology.HealthOK {
		t.Fatalf("room A recovered: %s, want ok", got)
	}
	cancel, done := startRun(t, c, "r_new", &recordingSink{}, &countProposals{})
	defer stopRun(t, cancel, done)
	waitFor(t, 2*time.Second, "the map to follow the new room", func() bool {
		return connectorHealth(t, b) == topology.HealthFail
	})
}

// Each fix call must make its own room the one the map follows.
func TestEachFixCallMovesTheMapToItsRoom(t *testing.T) {
	fixes := map[string]func(*FakeConnect, context.Context) error{
		"RestartTask": func(c *FakeConnect, ctx context.Context) error { return c.RestartTask(ctx, connectorName, 0) },
		"Resume":      func(c *FakeConnect, ctx context.Context) error { return c.Resume(ctx, connectorName) },
		"RestartConnector": func(c *FakeConnect, ctx context.Context) error {
			return c.RestartConnector(ctx, connectorName)
		},
	}
	for name, fix := range fixes {
		t.Run(name, func(t *testing.T) {
			fakes := NewFakes()
			b := Builder{Fakes: fakes}
			ctxA := room.WithRoomID(context.Background(), "r_a")
			// Room A has state; then room B becomes the room the map follows.
			if _, err := fakes.Connect.Status(ctxA, connectorName); err != nil {
				t.Fatal(err)
			}
			fakes.Connect.markRoom("r_b")
			if got := connectorHealth(t, b); got != topology.HealthFail {
				t.Fatalf("room B before any fix: %s, want fail", got)
			}
			if err := fix(fakes.Connect, ctxA); err != nil {
				t.Fatal(err)
			}
			if got := connectorHealth(t, b); got != topology.HealthOK {
				t.Fatalf("after %s in room A: %s, want ok", name, got)
			}
		})
	}
}
