import { describe, expect, it } from 'vitest'
import { applyEvent } from './reducer'
import { emptyRoomState } from './types'
import type { RoomState } from './types'
import type { Actor, Event } from '../api/types'

// P2b proposal.scheduled / proposal.cancelled in the web's fold, and their feed lines (P2c-3).

const system: Actor = { kind: 'system', id: 'system', name: 'system' }
const priya: Actor = { kind: 'human', id: 'priya', name: 'Priya' }
const RUN_AT = '2026-10-19T10:01:00Z'

function ev(seq: number, type: Event['type'], payload: unknown, actor: Actor = system): Event {
  return { room_id: 'r1', seq, type, actor, payload, at: `2026-10-19T10:00:${String(seq).padStart(2, '0')}Z` }
}
const created = ev(1, 'proposal.created', {
  proposal_id: 'p1',
  title: 'Restart the connector task',
  steps: [{ action: 'connect_restart_task', layer: 'kafka-connect', params: {}, description: '' }],
  risk: 'low',
  rollback: '',
  required_layers: ['kafka-connect', 'consumer'],
  params_hash: 'h',
})
const scheduled = ev(2, 'proposal.scheduled', { proposal_id: 'p1', run_at: RUN_AT, policy: 'auto_approve', cancel_until: RUN_AT })

function fold(...events: Event[]): RoomState {
  return events.reduce(applyEvent, { ...emptyRoomState('r1'), status: 'open' as const })
}

describe('reducer: the policy countdown', () => {
  it('proposal.scheduled sets it on the open proposal', () => {
    const s = fold(created, scheduled)
    expect(s.proposals[0].scheduled).toEqual({ run_at: RUN_AT, cancel_until: RUN_AT, policy: 'auto_approve' })
    expect(s.proposals[0].status).toBe('open')
  })

  it('proposal.cancelled clears it and the proposal stays open', () => {
    const s = fold(created, scheduled, ev(3, 'proposal.cancelled', { proposal_id: 'p1', by: priya, reason: '' }, priya))
    expect(s.proposals[0].scheduled).toBeUndefined()
    expect(s.proposals[0].status).toBe('open')
    expect(s.proposals[0].approvals).toEqual({ 'kafka-connect': 'pending', consumer: 'pending' })
  })

  it("the policy's first approval and a person's approval both end it", () => {
    const policy = fold(
      created,
      scheduled,
      ev(3, 'approval.given', { proposal_id: 'p1', layer: 'kafka-connect', grant: 'policy', policy: 'auto_approve' }),
    )
    expect(policy.proposals[0].scheduled).toBeUndefined()
    const person = fold(created, scheduled, ev(3, 'approval.given', { proposal_id: 'p1', layer: 'consumer', grant: 'owner' }, priya))
    expect(person.proposals[0].scheduled).toBeUndefined()
  })

  it('a rejection ends it', () => {
    const s = fold(created, scheduled, ev(3, 'approval.rejected', { proposal_id: 'p1', layer: 'consumer', reason: 'no' }, priya))
    expect(s.proposals[0].scheduled).toBeUndefined()
    expect(s.proposals[0].status).toBe('rejected')
  })

  it('a schedule for another or an unknown proposal changes nothing; a missing cancel_until reads as run_at', () => {
    const s = fold(created, ev(2, 'proposal.scheduled', { proposal_id: 'p9', run_at: RUN_AT, policy: 'auto_approve', cancel_until: RUN_AT }))
    expect(s.proposals[0].scheduled).toBeUndefined()
    const t = fold(created, ev(2, 'proposal.scheduled', { proposal_id: 'p1', run_at: RUN_AT, policy: 'auto_approve' }))
    expect(t.proposals[0].scheduled?.cancel_until).toBe(RUN_AT)
  })
})

describe('feed lines', () => {
  it('scheduled: one line that says "may run from", never a deadline', () => {
    const s = fold(created, scheduled)
    const line = s.activity[0]
    expect(line.kind).toBe('decision')
    expect(line.text).toMatch(/^Policy auto_approve may run the fix from .+ unless cancelled$/)
    expect(line.text).not.toMatch(/until|deadline|cancel by/i)
    expect(line.detail).toBe('Restart the connector task')
  })

  it('cancelled: who cancelled, with their reason', () => {
    const s = fold(created, scheduled, ev(3, 'proposal.cancelled', { proposal_id: 'p1', by: priya, reason: 'backfill running' }, priya))
    expect(s.activity[0].text).toBe('Priya cancelled the policy countdown · the fix waits for approval')
    expect(s.activity[0].detail).toBe('backfill running')
  })

  it("the policy's approval names the policy, not the system actor", () => {
    const s = fold(
      created,
      scheduled,
      ev(3, 'approval.given', { proposal_id: 'p1', layer: 'kafka-connect', grant: 'policy', policy: 'auto_approve' }),
    )
    expect(s.activity[0].text).toBe('Policy auto_approve approved kafka-connect')
  })
})
