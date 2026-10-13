// TEMP until P1 merges (spec section 6): P1 ships this as lib/layers.ts with the same
// export. When it lands, delete this file and import from './layers'.
export type Layer =
  | 'postgres'
  | 'kafka-connect'
  | 'schema-registry'
  | 'kafka'
  | 'consumer'
  | 'warehouse'

/** Which pipeline layer a tool belongs to, by name prefix. Null for an unknown tool. */
export function layerForTool(tool: string): Layer | null {
  if (tool.startsWith('pg_')) return 'postgres'
  if (tool.startsWith('connect_')) return 'kafka-connect'
  if (tool.startsWith('schema_registry_')) return 'schema-registry'
  if (tool.startsWith('kafka_consumer_')) return 'consumer'
  if (tool.startsWith('kafka_')) return 'kafka'
  if (tool.startsWith('warehouse_')) return 'warehouse'
  return null
}
