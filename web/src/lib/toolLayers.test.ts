import { describe, expect, it } from 'vitest'
import { argTags, formatMs, toolLayer } from './toolLayers'

describe('toolLayer', () => {
  it('maps a tool name prefix to its layer', () => {
    expect(toolLayer('connect_status')).toBe('kafka-connect')
    expect(toolLayer('schema_registry_diff')).toBe('schema-registry')
    expect(toolLayer('pg_recent_ddl')).toBe('postgres')
    expect(toolLayer('kafka_consumer_lag')).toBe('kafka')
    expect(toolLayer('warehouse_freshness')).toBe('warehouse')
    expect(toolLayer('wh_backfill')).toBe('warehouse')
  })
  it('gives an empty layer for an unknown tool', () => {
    expect(toolLayer('mystery')).toBe('')
  })
})

describe('argTags', () => {
  it('turns an object input into key=value tags', () => {
    expect(argTags({ connector: 'pg-orders-connector', task: 0 })).toEqual(['connector=pg-orders-connector', 'task=0'])
  })
  it('cuts long values and flattens whitespace', () => {
    const [tag] = argTags({ schema: `{"a":\n  "${'x'.repeat(80)}"}` }, 20)
    expect(tag.length).toBeLessThanOrEqual('schema='.length + 20)
    expect(tag.endsWith('…')).toBe(true)
    expect(tag).not.toContain('\n')
  })
  it('handles no input and a bare value', () => {
    expect(argTags(null)).toEqual([])
    expect(argTags('abc')).toEqual(['abc'])
  })
})

describe('formatMs', () => {
  it('uses ms under a second and seconds above', () => {
    expect(formatMs(412)).toBe('412 ms')
    expect(formatMs(1234)).toBe('1.2 s')
  })
})
