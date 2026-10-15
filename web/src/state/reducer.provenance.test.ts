import { describe, expect, it } from 'vitest'
import { applyEvent } from './reducer'
import { emptyRoomState, fromSnapshot } from './types'
import type { Event } from '../api/types'

const ev = (seq: number, type: string, payload: unknown): Event =>
  ({ room_id: 'r', seq, type, actor: { kind: 'agent', id: 'agent', name: 'Agent' }, payload, at: '2026-10-14T03:00:00Z' }) as unknown as Event

const call = { kind: 'http', target: 'http://connect:8083', text: 'GET /connectors/c1/status', at: '2026-10-14T03:00:00Z', took_ms: 9 }

function withStep() {
  let s = emptyRoomState('r')
  s = applyEvent(s, ev(1, 'agent.step.started', { step_id: 's1', title: 't' }))
  s = applyEvent(s, ev(2, 'agent.tool_call', { step_id: 's1', tool_use_id: 'u1', tool: 'connect_status', input: {} }))
  return s
}

describe('reducer F3 fields', () => {
  it('agent.tool_result copies provenance', () => {
    const s = applyEvent(
      withStep(),
      ev(3, 'agent.tool_result', { step_id: 's1', tool_use_id: 'u1', tool: 'connect_status', summary: 'ok', is_error: false, took_ms: 9, provenance: [call] }),
    )
    expect(s.steps[0].tool_calls[0].provenance).toEqual([call])
  })

  it('a tool_result without provenance leaves it undefined', () => {
    const s = applyEvent(
      withStep(),
      ev(3, 'agent.tool_result', { step_id: 's1', tool_use_id: 'u1', tool: 'connect_status', summary: 'ok', is_error: false, took_ms: 9 }),
    )
    expect(s.steps[0].tool_calls[0].provenance).toBeUndefined()
  })

  it('agent.hypothesis copies unbacked_terms, defaulting to []', () => {
    let s = applyEvent(emptyRoomState('r'), ev(1, 'agent.hypothesis', { text: 'x', based_on_steps: [], confidence: 'low', confirmed: false, unbacked_terms: ['a_b'] }))
    expect(s.hypothesis?.unbacked_terms).toEqual(['a_b'])
    s = applyEvent(s, ev(2, 'agent.hypothesis', { text: 'y', based_on_steps: [], confidence: 'low', confirmed: false }))
    expect(s.hypothesis?.unbacked_terms).toEqual([])
  })

  it('fromSnapshot reads unbacked_terms and defaults to []', () => {
    const base = {
      id: 'r', title: 't', severity: 'sev2', pipeline_id: 'p', status: 'open', opened_at: '2026-10-14T03:00:00Z', last_seq: 0,
      driver: null, agent_status: { state: 'idle' }, open_proposals: [], pending_suggestions: [], members: [],
    }
    const hyp = { text: 'x', based_on_steps: [], confidence: 'low', confirmed: false, seq: 2 }
    expect(fromSnapshot({ ...base, hypothesis: { ...hyp, unbacked_terms: ['q'] } } as never).hypothesis?.unbacked_terms).toEqual(['q'])
    expect(fromSnapshot({ ...base, hypothesis: hyp } as never).hypothesis?.unbacked_terms).toEqual([])
  })
})
