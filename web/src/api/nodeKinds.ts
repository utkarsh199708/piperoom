// The node kinds the server's topology builder can emit, in the same order as
// the Kind… constants in internal/topology/graph.go. This list is the web's
// single source of truth for NodeKind: PipelineMap's KIND_LABEL is a
// Record<NodeKind, string>, so a kind added in Go without a label here fails
// `tsc -b`, and nodeKinds.test.ts reads graph.go and asserts this list equals
// every `NodeKind = "…"` constant, so a kind missing here fails the test.
export const NODE_KINDS = [
  'source_table',
  'connector',
  'topic',
  'consumer_group',
  'sink_table',
  'dlq_topic',
  'schema_subject',
  'dag',
  'task',
  'model',
  'kafka_engine',
  'materialized_view',
] as const

export type NodeKind = (typeof NODE_KINDS)[number]
