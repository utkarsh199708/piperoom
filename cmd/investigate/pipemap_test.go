package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"

	"piperoom/internal/events"
	"piperoom/internal/topology"
)

type fakeTopo struct {
	render string
	err    error
}

func (f fakeTopo) Build(context.Context, string) (topology.Graph, error) {
	return topology.Graph{}, f.err
}
func (f fakeTopo) Render(topology.Graph) string { return f.render }
func (f fakeTopo) Trace(context.Context, string, string, string) ([]events.TraceStage, error) {
	return nil, nil
}
func (f fakeTopo) LayerOwner(string, string) (string, bool) { return "", false }

func TestPipelineMapFor(t *testing.T) {
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	cases := []struct {
		name string
		topo topology.Builder
		want string
	}{
		{"live graph", fakeTopo{render: "LIVE MAP payments"}, "LIVE MAP payments"},
		{"build fails", fakeTopo{err: errors.New("connect down")}, "connect: pg-orders-connector"},
		{"empty render", fakeTopo{}, "connect: pg-orders-connector"},
		{"fake tools", nil, "connect: pg-orders-connector"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := pipelineMapFor(context.Background(), c.topo, "payments", quiet)
			if !strings.Contains(got, c.want) {
				t.Fatalf("got %q, want it to contain %q", got, c.want)
			}
		})
	}
}
