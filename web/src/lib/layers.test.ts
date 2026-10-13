import { describe, expect, it } from 'vitest'
import { LAYER_LABEL, layerForTool } from './layers'

describe('layerForTool', () => {
  it.each([
    ['pg_recent_ddl', 'postgres'],
    ['pg_replication_slots', 'postgres'],
    ['connect_status', 'kafka-connect'],
    ['connect_restart_task', 'kafka-connect'],
    ['schema_registry_diff', 'schema-registry'],
    ['kafka_topic_offsets', 'kafka'],
    ['kafka_list_groups', 'kafka'],
    ['kafka_consumer_lag', 'consumer'],
    ['warehouse_freshness', 'warehouse'],
  ])('%s belongs to %s', (tool, layer) => {
    expect(layerForTool(tool)).toBe(layer)
  })

  it('returns null for an unknown tool', () => {
    expect(layerForTool('mystery_tool')).toBeNull()
    expect(layerForTool('')).toBeNull()
  })

  it('does not match a layer name inside a tool name', () => {
    expect(layerForTool('my_pg_thing')).toBeNull()
    expect(layerForTool('connector_status')).toBeNull()
  })

  it('has a label for every layer', () => {
    for (const layer of Object.keys(LAYER_LABEL)) {
      expect(LAYER_LABEL[layer as keyof typeof LAYER_LABEL]).toBeTruthy()
    }
    expect(Object.keys(LAYER_LABEL)).toHaveLength(6)
  })
})
