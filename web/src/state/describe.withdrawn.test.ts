import { describe, expect, it } from 'vitest'
import type { Event } from '../api/types'
import { describeEvent } from './describe'

const ev = (actor: Event['actor'], payload: unknown): Event => ({
  room_id: 'r1',
  seq: 5,
  type: 'proposal.withdrawn',
  actor,
  payload,
  at: '2026-10-15T10:00:00Z',
})
const ctx = { driverBefore: null, stepTitles: {}, stepIndex: {}, proposalTitles: { p1: 'Restart the connector task' } }

describe('describeEvent: proposal.withdrawn (Q0)', () => {
  it('names the person or the agent and the fix, and keeps the reason as the detail', () => {
    expect(describeEvent(ev({ kind: 'human', id: 'kabir', name: 'Kabir' }, { proposal_id: 'p1', reason: 'wrong connector' }), ctx)).toEqual({
      text: 'Kabir withdrew a fix · Restart the connector task',
      detail: 'wrong connector',
      kind: 'decision',
    })
    expect(describeEvent(ev({ kind: 'agent', id: 'agent', name: 'Agent' }, { proposal_id: 'p1', reason: '' }), ctx).text).toBe(
      'Agent withdrew a fix · Restart the connector task',
    )
  })

  it('the system actor\'s withdrawal at room close is not "withdrew"', () => {
    const d = describeEvent(ev({ kind: 'system', id: 'system', name: 'System' }, { proposal_id: 'p1', reason: 'room closed' }), ctx)
    expect(d.text).toBe('Fix withdrawn · Restart the connector task')
    expect(d.detail).toBe('room closed')
  })

  it('works for a proposal the feed has not seen, and clips a long title', () => {
    expect(describeEvent(ev({ kind: 'human', id: 'k', name: 'Kabir' }, { proposal_id: 'p_nope', reason: '' })).text).toBe('Kabir withdrew a fix')
    const long = describeEvent(ev({ kind: 'human', id: 'k', name: 'Kabir' }, { proposal_id: 'p1', reason: '' }), {
      ...ctx,
      proposalTitles: { p1: 'x'.repeat(300) },
    })
    expect(long.text.length).toBeLessThan(120)
  })
})
