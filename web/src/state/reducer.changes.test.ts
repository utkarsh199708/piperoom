import { describe, expect, it } from 'vitest'
import { applyEvent } from './reducer'
import { checklistKey, emptyRoomState } from './types'
import type { Event } from '../api/types'

let seq = 0
const ev = (type: Event['type'], payload: unknown, actor = { kind: 'human', id: 'kabir', name: 'Kabir' }): Event =>
  ({ room_id: 'r1', seq: ++seq, type, actor, payload, at: '2026-10-14T03:00:00Z' }) as Event

const tick = (item: number, done: boolean) => ev('checklist.step_done', { step_id: 's1', tool_use_id: 'tu1', hit: 0, item, done })

describe('F8 events', () => {
  it('folds room.labelled; the last label wins', () => {
    seq = 0
    let s = applyEvent(emptyRoomState('r1'), ev('room.labelled', { cause: 'first' }))
    expect(s.labelled).toEqual({ cause: 'first' })
    s = applyEvent(s, ev('room.labelled', { cause: 'second', fix_that_worked: 'resume' }))
    expect(s.labelled?.cause).toBe('second')
  })

  it('folds checklist ticks by key; the last event wins and an untick removes the key', () => {
    seq = 0
    let s = applyEvent(emptyRoomState('r1'), tick(2, true))
    expect(s.checklist_done[checklistKey('tu1', 0, 2)]?.by.name).toBe('Kabir')
    s = applyEvent(s, ev('checklist.step_done', { step_id: 's1', tool_use_id: 'tu1', hit: 0, item: 2, done: true }, { kind: 'human', id: 'priya', name: 'Priya' }))
    expect(s.checklist_done[checklistKey('tu1', 0, 2)]?.by.name).toBe('Priya')
    s = applyEvent(s, tick(2, false))
    expect(s.checklist_done).toEqual({})
  })

  it('does not mutate the previous state', () => {
    seq = 0
    const before = emptyRoomState('r1')
    applyEvent(before, tick(0, true))
    expect(before.checklist_done).toEqual({})
  })

  it('describes a tick with the item text and section from the runbook result', () => {
    seq = 0
    let s = emptyRoomState('r1')
    s = applyEvent(s, ev('agent.step.started', { step_id: 's1', title: 'runbook' }, { kind: 'agent', id: 'agent', name: 'Agent' }))
    s = applyEvent(s, ev('agent.tool_call', { step_id: 's1', tool_use_id: 'tu1', tool: 'runbook_lookup', input: {} }, { kind: 'agent', id: 'agent', name: 'Agent' }))
    s = applyEvent(s, ev('agent.tool_result', { step_id: 's1', tool_use_id: 'tu1', tool: 'runbook_lookup', summary: 'x', is_error: false, took_ms: 1, data: { hits: [{ section: 'Failed connector', items: ['a', 'b', 'Resume the connector'] }] } }, { kind: 'agent', id: 'agent', name: 'Agent' }))
    s = applyEvent(s, tick(2, true))
    expect(s.activity[0].text).toBe('Kabir ticked "Resume the connector" (step 3 of Failed connector)')
    s = applyEvent(s, ev('room.labelled', { cause: 'slot lag', fix_that_worked: 'resume' }))
    expect(s.activity[0].text).toBe('Kabir labelled the room: cause slot lag · fix resume')
  })
})
