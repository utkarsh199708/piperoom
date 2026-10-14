import { describe, expect, it } from 'vitest'
import type { Event, RoomSnapshot } from '../api/types'
import { describeEvent } from './describe'
import { applyEvent } from './reducer'
import { emptyRoomState, fromSnapshot } from './types'

const human = { kind: 'human' as const, id: 'arjun', name: 'Arjun' }
const system = { kind: 'system' as const, id: 'system', name: 'system' }

function ev(seq: number, type: Event['type'], payload: unknown, actor: Event['actor']): Event {
  return { room_id: 'r1', seq, type, actor, payload, at: `2026-10-14T10:00:${String(seq).padStart(2, '0')}Z` }
}

describe('shadow rooms in the reducer', () => {
  it('room.created carries the shadow flag; absent means live', () => {
    const base = emptyRoomState('r1')
    const shadow = applyEvent(base, ev(1, 'room.created', { title: 'T', severity: 'sev2', pipeline_id: 'orders', shadow: true }, system))
    const live = applyEvent(base, ev(1, 'room.created', { title: 'T', severity: 'sev2', pipeline_id: 'orders' }, system))
    expect(shadow.shadow).toBe(true)
    expect(live.shadow).toBe(false)
  })

  it('shadow.revealed marks the run as revealed and adds a content-free activity line', () => {
    const payload = {
      status: 'hypothesised',
      time_to_first_hypothesis_s: 60,
      time_to_final_hypothesis_s: 120,
      human_mttr_s: 3000,
      finished_after_close: false,
      tool_calls: 4,
      tokens: 0,
    }
    const next = applyEvent({ ...emptyRoomState('r1'), shadow: true, last_seq: 5 }, ev(6, 'shadow.revealed', payload, system))
    expect(next.shadow_revealed).toBe(true)
    expect(next.last_seq).toBe(6)
    expect(next.activity[0].text).toMatch(/Shadow run finished/)
  })

  it('shadow.labelled and shadow.graded describe the actor, not the content', () => {
    expect(describeEvent(ev(7, 'shadow.labelled', { actual_layer: 'kafka', root_cause_terms: ['secret term'] }, human))).toBe(
      'Arjun recorded a blind label',
    )
    expect(describeEvent(ev(8, 'shadow.graded', { layer_correct: true, root_cause: 'yes', notes: 'n' }, human))).toBe(
      'Arjun graded the shadow run',
    )
  })

  it('fromSnapshot reads shadow, and an older server (no field) means live', () => {
    const snap = {
      id: 'r1',
      title: 'T',
      severity: 'sev2',
      pipeline_id: 'orders',
      status: 'closed',
      opened_at: '2026-10-14T10:00:00Z',
      last_seq: 3,
      driver: null,
      agent_status: { state: 'idle' },
      hypothesis: null,
      open_proposals: [],
      pending_suggestions: [],
      members: [],
    } as RoomSnapshot
    expect(fromSnapshot({ ...snap, shadow: true }).shadow).toBe(true)
    expect(fromSnapshot(snap).shadow).toBe(false)
  })
})
