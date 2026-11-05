import { describe, expect, it } from 'vitest'
import type { Event } from '../api/types'
import { applyEvent } from './reducer'
import { emptyRoomState } from './types'
import { describeEvent, EMPTY_CONTEXT } from './describe'

// P2b: proposal.created.fallback_layers and control.granted.reason fold into the room state.

const agent = { kind: 'agent' as const, id: 'agent', name: 'Agent' }
const system = { kind: 'system' as const, id: 'system', name: 'System' }
const kabir = { kind: 'human' as const, id: 'kabir', name: 'Kabir' }

function ev(seq: number, type: Event['type'], payload: unknown, actor: Event['actor'] = agent): Event {
  return { room_id: 'r1', seq, type, actor, payload, at: `2026-10-18T10:00:${String(seq).padStart(2, '0')}Z` }
}

const created = {
  proposal_id: 'p1',
  title: 'Restart the task',
  steps: [],
  risk: 'low',
  rollback: '',
  required_layers: ['kafka-connect', 'consumer'],
  params_hash: 'abc',
}

describe('P2b events in the reducer', () => {
  it('proposal.created keeps fallback_layers', () => {
    const next = applyEvent(emptyRoomState('r1'), ev(1, 'proposal.created', { ...created, fallback_layers: ['consumer'] }))
    expect(next.proposals[0].fallback_layers).toEqual(['consumer'])
  })

  it('a proposal without fallback_layers folds as before (no field)', () => {
    const next = applyEvent(emptyRoomState('r1'), ev(1, 'proposal.created', created))
    expect('fallback_layers' in next.proposals[0]).toBe(false)
    const empty = applyEvent(emptyRoomState('r1'), ev(1, 'proposal.created', { ...created, fallback_layers: [] }))
    expect('fallback_layers' in empty.proposals[0]).toBe(false)
  })

  it('control.granted keeps reason sole_member on the driver; the next grant drops it', () => {
    const lease = '2026-10-18T10:30:00Z'
    const s1 = applyEvent(emptyRoomState('r1'), ev(1, 'control.granted', { to: kabir, lease_until: lease, reason: 'sole_member' }, system))
    expect(s1.driver).toMatchObject({ id: 'kabir', reason: 'sole_member' })
    const s2 = applyEvent(s1, ev(2, 'control.granted', { to: kabir, lease_until: lease }, kabir))
    expect(s2.driver?.reason).toBeUndefined()
  })

  it('the feed says the only person here became the driver', () => {
    const e = ev(1, 'control.granted', { to: kabir, lease_until: '2026-10-18T10:30:00Z', reason: 'sole_member' }, system)
    expect(describeEvent(e, EMPTY_CONTEXT).text).toBe('Kabir is the driver · the only one here')
  })
})
