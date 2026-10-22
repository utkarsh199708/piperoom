import { describe, expect, it } from 'vitest'
import { changeLayer } from './f8'

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
