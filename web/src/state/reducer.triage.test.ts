import { describe, expect, it } from 'vitest'
import type { Event, RoomSnapshot } from '../api/types'
import { applyEvent } from './reducer'
import { describeEvent } from './describe'
import { emptyRoomState, fromSnapshot } from './types'

const agent = { kind: 'agent' as const, id: 'agent', name: 'Agent' }

function ev(seq: number, type: Event['type'], payload: unknown): Event {
  return { room_id: 'r1', seq, type, actor: agent, payload, at: `2026-10-14T03:12:${String(seq).padStart(2, '0')}Z` }
}

const ok = {
  status: 'ok',
  headline: 'task 0 FAILED',
  suspected_layer: 'kafka-connect',
  confidence: 'medium',
  evidence_refs: ['s_1'],
  next_check: 'diff',
  model: 'claude-haiku-5-5',
  took_ms: 900,
}

describe('agent.triage', () => {
  it('folds into state.triage, last one wins, and a hypothesis keeps it', () => {
    let s = applyEvent(emptyRoomState('r1'), ev(1, 'agent.triage', ok))
    expect(s.triage).toEqual({
      status: 'ok',
      reason: undefined,
      headline: 'task 0 FAILED',
      suspected_layer: 'kafka-connect',
      confidence: 'medium',
      evidence_refs: ['s_1'],
      next_check: 'diff',
      at: '2026-10-14T03:12:01Z',
    })
    s = applyEvent(s, ev(2, 'agent.hypothesis', { text: 'h', based_on_steps: [], confidence: 'low', confirmed: false }))
    expect(s.triage?.headline).toBe('task 0 FAILED')
    s = applyEvent(s, ev(3, 'agent.triage', { status: 'failed', reason: 'timeout', model: 'm', took_ms: 1 }))
    expect(s.triage?.status).toBe('failed')
    expect(s.triage?.evidence_refs).toEqual([])
  })

  it('reads the snapshot triage, and an older server without one', () => {
    const snap = {
      id: 'r1',
      title: 't',
      severity: 'sev2',
      pipeline_id: 'orders',
      status: 'open',
      opened_at: '',
      last_seq: 3,
      driver: null,
      agent_status: { state: 'queued' },
      hypothesis: null,
      open_proposals: [],
      pending_suggestions: [],
      members: [],
    } as RoomSnapshot
    expect(fromSnapshot(snap).triage).toBeNull()
    const withTriage = fromSnapshot({ ...snap, triage: { status: 'ok', headline: 'h', evidence_refs: [], at: 'x' } })
    expect(withTriage.triage?.headline).toBe('h')
  })

  it('describes the first look in the activity feed', () => {
    expect(describeEvent(ev(1, 'agent.triage', ok)).text).toBe('First look: task 0 FAILED')
    expect(describeEvent(ev(2, 'agent.triage', { status: 'failed', reason: 'budget' })).text).toBe('First look failed (budget)')
  })
})
