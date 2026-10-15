// QA R14 (G6): a repeat agent.verification is "already verified", not a second verdict in the activity feed.
import { describe, expect, it } from 'vitest'
import type { Event } from '../api/types'
import { EMPTY_CONTEXT, describeEvent } from './describe'

const agent = { kind: 'agent' as const, id: 'agent', name: 'Agent' }
const verification = (payload: unknown): Event => ({
  room_id: 'r1',
  seq: 9,
  type: 'agent.verification',
  actor: agent,
  payload,
  at: '2026-10-15T10:00:00Z',
})

describe('describeEvent: agent.verification repeat', () => {
  it('a repeat is a muted system row, not a second recovery announcement', () => {
    const d = describeEvent(
      verification({ proposal_id: 'p_1', checks: [], recovered: true, verdict: 'already verified', repeat: true }),
      EMPTY_CONTEXT,
    )
    expect(d.kind).toBe('system')
    expect(d.text).toBe('Verification already recorded')
  })

  it('a repeat that says not recovered is still muted', () => {
    const d = describeEvent(
      verification({ proposal_id: 'p_1', checks: [], recovered: false, verdict: 'already verified', repeat: true }),
      EMPTY_CONTEXT,
    )
    expect(d.kind).toBe('system')
    expect(d.text).not.toMatch(/recovered/)
  })

  it('the first verdict is still announced as a decision', () => {
    const d = describeEvent(verification({ proposal_id: 'p_1', checks: [], recovered: true, verdict: 'recovered' }), EMPTY_CONTEXT)
    expect(d).toMatchObject({ text: 'Verification: recovered', kind: 'decision' })
  })
})
