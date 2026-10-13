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
	// KindDLQTopic is a dead-letter topic written by one of the pipeline's
	// sink connectors (errors.deadletterqueue.topic.name) or declared in
	// owners.yaml (watch.dlq_topics). It is a side node: edges lead into it,
	// never out. Its layer and owner are the writer's.
	KindDLQTopic NodeKind = "dlq_topic"
	// KindSchemaSubject is a Schema Registry subject (id "sr:<subject>", layer
	// "schema-registry") that the pipeline's connectors serialize with or its
	// consumers deserialize with. It is not a data stage: only depends_on
	// edges point at it. Facts: compatibility, latest_version and
	// registered_recently (true | false | unknown); new_version_seen (age,
	// health warn) when registered_recently is true; version_deleted_seen
	// (age, health warn) when the latest version went down, which is not a
	// registration; exists=false (health warn) when a converter config names
	// it but nothing is registered; error=unreachable (health unknown).
	KindSchemaSubject NodeKind = "schema_subject"
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
// pg, connect, topic, dlq, group, wh, sr (e.g. "connect:pg-orders-connector",
// "dlq:orders-dlq", "sr:pg.public.orders-value").
type Node struct {
	ID     string            `json:"id"`
	Kind   NodeKind          `json:"kind"`
	Name   string            `json:"name"`
	Layer  string            `json:"layer"` // one of events.Layers
	Owner  string            `json:"owner"` // user id from owners.yaml
	Health Health            `json:"health"`
	Facts  map[string]string `json:"facts,omitempty"` // short display facts, e.g. "lag": "0"
}

// EdgeKind says what an edge means. The empty value means EdgeData, so a
// graph or client from before edge kinds reads every edge as data flow.
type EdgeKind string

const (
	// EdgeData is data flow: records go from From to To.
	EdgeData EdgeKind = "data"
	// EdgeDependsOn means From needs To to work but no records flow between
	// them, e.g. a connector or consumer group that serializes or
	// deserializes with a schema subject. To is upstream of From when looking
	// for a root cause.
	EdgeDependsOn EdgeKind = "depends_on"
)

// EdgeMapping says how the builder knows an edge exists. The empty value
// means MappingMapped, so a graph from before mappings reads as proven.
type EdgeMapping string

const (
	// MappingMapped is proven from live config or metadata (connector config,
	// committed group offsets, Schema Registry subjects) or declared
	// explicitly in owners.yaml.
	MappingMapped EdgeMapping = "mapped"
	// MappingInferred is not proven; it is implied by owners.yaml (both ends
	// in the same pipeline) or by an earlier reading. Render marks it
	// "(unconfirmed)"; the agent treats it as a hypothesis to check.
	MappingInferred EdgeMapping = "inferred"
)

// Edge joins two nodes of a pipeline. Kind and Mapping are left empty for
// proven data flow, so that common edge keeps its original JSON shape.
type Edge struct {
	From    string      `json:"from"`
	To      string      `json:"to"`
	Kind    EdgeKind    `json:"kind,omitempty"`    // "" means EdgeData
	Mapping EdgeMapping `json:"mapping,omitempty"` // "" means MappingMapped
}

// IsDependency reports whether e is a depends_on edge.
func (e Edge) IsDependency() bool { return e.Kind == EdgeDependsOn }

// IsInferred reports whether e is assumed, not proven.
func (e Edge) IsInferred() bool { return e.Mapping == MappingInferred }

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
	// Render returns a compact, deterministic text form of g for the model's
	// first prompt: one line per node in data-flow order (upstream first),
	// each followed by its "from:" (data inputs) and "depends on:" lines,
	// inferred edges marked "(unconfirmed)", under a legend line saying the
	// first failing component and the root cause can differ.
	Render(g Graph) string
	// Trace follows one source row (by primary key) through every stage.
	Trace(ctx context.Context, pipelineID, table, key string) ([]events.TraceStage, error)
	// LayerOwner returns the user id that owns a layer of a pipeline.
	LayerOwner(pipelineID, layer string) (string, bool)
}
