import { describe, expect, it } from 'vitest'
import type { TraceState } from '../state/types'
import { traceVerdict } from './trace'

const t = (stages: TraceState['stages']): TraceState => ({ table: 'public.orders', key: '10042', stages })

describe('traceVerdict', () => {
  it('names the stopped stage with its first fact', () => {
    expect(
      traceVerdict(
        t([
          { layer: 'kafka', name: 'pg.public.orders', status: 'seen' },
          { layer: 'consumer', name: 'order-sync', status: 'stopped_here', facts: { committed: '48212', lag: '9' } },
        ]),
      ),
    ).toEqual({ tone: 'fail', text: 'Stopped at consumer order-sync — committed=48212' })
  })
  it('omits the dash without facts', () => {
    expect(traceVerdict(t([{ layer: 'warehouse', name: 'a.b', status: 'stopped_here' }])).text).toBe('Stopped at warehouse a.b')
  })
  it('reaches the end when the last stage saw the key', () => {
    expect(traceVerdict(t([{ layer: 'postgres', name: 'x', status: 'seen' }, { layer: 'warehouse', name: 'w', status: 'seen' }]))).toEqual({
      tone: 'ok',
      text: 'Reached w — every stage saw key 10042',
    })
  })
  it('warns when nothing saw the key, also for no stages', () => {
    expect(traceVerdict(t([{ layer: 'postgres', name: 'x', status: 'not_seen' }])).tone).toBe('warn')
    expect(traceVerdict(t([])).text).toBe('Not seen anywhere — no stage holds key 10042')
  })
  it('is muted when stages could not be read', () => {
    expect(
      traceVerdict(t([{ layer: 'postgres', name: 'x', status: 'seen' }, { layer: 'kafka', name: 'k', status: 'unknown' }])),
    ).toEqual({ tone: 'muted', text: 'Partly traced — 1 stage could not be read' })
  })
  it('clips a long fact to 64 characters', () => {
    const v = traceVerdict(t([{ layer: 'kafka', name: 'k', status: 'stopped_here', facts: { v: 'x'.repeat(200) } }]))
    expect(v.text.split(' — ')[1]).toHaveLength(64)
  })
})
