import { describe, expect, it } from 'vitest'
import { humanStatus } from './status'
import { isolate, stripIsolates } from './bidi'
import { describeEvent } from '../state/describe'
import type { Event } from '../api/types'

describe('humanStatus', () => {
  it.each([
    ['waiting_approval', 'waiting for approval'],
    ['working', 'working'],
    ['some_new_state', 'some new state'],
  ])('%s → %s', (raw, want) => {
    expect(humanStatus(raw)).toBe(want)
  })
})

describe('isolate', () => {
  it('closes an override inside the text with a PDI', () => {
    const s = isolate('Kabir\u202E')
    expect(s.startsWith('\u2068')).toBe(true)
    expect(s.endsWith('\u2069')).toBe(true)
    expect(stripIsolates(s)).toBe('Kabir\u202E')
  })
})

describe('activity sentences', () => {
  const ev = (type: Event['type'], payload: unknown, name = 'Arjun'): Event => ({
    room_id: 'r1',
    seq: 1,
    type,
    actor: { kind: 'human', id: 'u', name },
    payload,
    at: '2026-10-14T10:00:00Z',
  })

  it('say the agent status in words', () => {
    expect(describeEvent({ ...ev('agent.status', { state: 'waiting_approval' }), actor: { kind: 'agent', id: 'agent', name: 'Agent' } })).toBe(
      'Agent is waiting for approval',
    )
  })

  it('isolate a name and text with a bidi override, so the words after them keep their order', () => {
    const s = describeEvent(ev('approval.rejected', { proposal_id: 'p', layer: 'kafka', reason: 'no\u202E' }, 'Eve\u202E'))
    expect(s).toBe('\u2068Eve\u202E\u2069 rejected kafka: \u2068no\u202E\u2069')
  })
})
