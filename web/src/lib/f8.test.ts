import { describe, expect, it } from 'vitest'
import { readFileSync } from 'node:fs'
import { resolve } from 'node:path'
import { changeLayer, parseChanges } from './f8'

// The --demo agent records its what_changed result in
// internal/room/demo/testdata/what_changed.json; the Go demo test asserts the
// emitted event carries exactly this, so parsing it here proves the lane can
// show --demo data (KAN-399 TC4). Vitest runs with web/ as its cwd.
const demoWhatChanged = (): unknown =>
  JSON.parse(readFileSync(resolve(process.cwd(), '../internal/room/demo/testdata/what_changed.json'), 'utf8'))

describe('changeLayer', () => {
  const table: [string, string][] = [
    // CDC kinds, unchanged (F8).
    ['ddl', 'postgres'],
    ['connector_config', 'kafka-connect'],
    ['connector_state', 'kafka-connect'],
    ['schema_version', 'schema-registry'],
    // Batch kinds (P4f-3): a DAG change lives on the orchestrator layer, a model change on transform.
    ['dag', 'orchestrator'],
    ['model', 'transform'],
  ]

  it('maps each change kind to its layer', () => {
    for (const [kind, layer] of table) {
      expect(changeLayer(kind)).toBe(layer)
    }
  })

  it('falls back to a neutral layer for an unknown kind, never throwing', () => {
    expect(() => changeLayer('oracle_patch')).not.toThrow()
    expect(changeLayer('oracle_patch')).toBe('')
    expect(changeLayer('')).toBe('')
  })
})

describe('the --demo what_changed result', () => {
  it('parses into the rows the lane shows, newest first', () => {
    const rows = parseChanges(demoWhatChanged())
    expect(rows.map((r) => r.kind)).toEqual(['ddl', 'connector_config', 'schema_version'])
    expect(rows.map((r) => r.object)).toEqual(['public.orders', 'pg-orders-connector', 'pg.public.orders-value'])
    expect(rows[1].before).toEqual({ 'heartbeat.interval.ms': '10000' })
    expect(rows[1].after).toEqual({ 'heartbeat.interval.ms': '5000' })
    expect(rows[2].before).toEqual({ version: '0' })
    expect(rows[2].after).toEqual({ version: '1' })
  })
})
