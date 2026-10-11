// Package topology builds the pipeline map: which tables, connectors, topics,
// consumer groups and sink tables form a pipeline, who owns each, and how
// healthy each is.
//
// CONTRACT FILE — owned by the reviewer. Do not edit; request changes in your PR.
// The implementation is owned by A3.
package topology

import (
	"context"

	"piperoom/internal/events"
)

// NodeKind is the kind of a pipeline node.
type NodeKind string

const (
	KindSourceTable   NodeKind = "source_table"
	KindConnector     NodeKind = "connector"
	KindTopic         NodeKind = "topic"
	KindConsumerGroup NodeKind = "consumer_group"
	KindSinkTable     NodeKind = "sink_table"
)

// Health is a node's state.
type Health string

const (
	HealthOK      Health = "ok"
	HealthWarn    Health = "warn"
	HealthFail    Health = "fail"
	HealthUnknown Health = "unknown"
)

// Node is one part of a pipeline. ID is "<prefix>:<name>" with prefixes
// pg, connect, topic, group, wh (e.g. "connect:pg-orders-connector").
type Node struct {
	ID     string            `json:"id"`
	Kind   NodeKind          `json:"kind"`
	Name   string            `json:"name"`
	Layer  string            `json:"layer"` // one of events.Layers
	Owner  string            `json:"owner"` // user id from owners.yaml
	Health Health            `json:"health"`
	Facts  map[string]string `json:"facts,omitempty"` // short display facts, e.g. "lag": "0"
}

// Edge is data flow from one node to the next.
type Edge struct {
	From string `json:"from"`
	To   string `json:"to"`
}

// Graph is one pipeline (JSON shape matches docs/api.md "Topology").
type Graph struct {
	PipelineID string `json:"pipeline_id"`
	Nodes      []Node `json:"nodes"`
	Edges      []Edge `json:"edges"`
}

// Builder builds graphs and traces records.
type Builder interface {
	// Build returns the current graph with live health. Unreachable systems
	// give HealthUnknown on their nodes, not an error.
	Build(ctx context.Context, pipelineID string) (Graph, error)
	// Render returns a compact text form of g for the model's first prompt.
	Render(g Graph) string
	// Trace follows one source row (by primary key) through every stage.
	Trace(ctx context.Context, pipelineID, table, key string) ([]events.TraceStage, error)
	// LayerOwner returns the user id that owns a layer of a pipeline.
	LayerOwner(pipelineID, layer string) (string, bool)
}
