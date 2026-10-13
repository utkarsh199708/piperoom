// Pipeline layers and the tools that belong to each. The names match the layer
// keys used for approvals (`required_layers`) and the --layer-* colour tokens.

export type Layer =
  | 'postgres'
  | 'kafka-connect'
  | 'schema-registry'
  | 'kafka'
  | 'consumer'
  | 'warehouse'

export const LAYER_LABEL: Record<Layer, string> = {
  postgres: 'Postgres',
  'kafka-connect': 'Kafka Connect',
  'schema-registry': 'Schema Registry',
  kafka: 'Kafka',
  consumer: 'Consumer',
  warehouse: 'Warehouse',
}

// Longest prefix first: `kafka_consumer_` must win over `kafka_`.
const PREFIXES: Array<[string, Layer]> = [
  ['schema_registry_', 'schema-registry'],
  ['kafka_consumer_', 'consumer'],
  ['warehouse_', 'warehouse'],
  ['connect_', 'kafka-connect'],
  ['kafka_', 'kafka'],
  ['pg_', 'postgres'],
]

/** The layer a tool name belongs to, by prefix, or null for a tool we do not know. */
export function layerForTool(tool: string): Layer | null {
  for (const [prefix, layer] of PREFIXES) {
    if (tool.startsWith(prefix)) return layer
  }
  return null
}
