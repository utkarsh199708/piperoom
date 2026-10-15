import { describe, expect, it } from 'vitest'
import type { Event } from '../api/types'
import { applyEvent } from './reducer'
import { emptyRoomState } from './types'
import type { RoomState } from './types'

// Q0 (QA G8/G9): proposal.withdrawn, and a closed room that no longer waits on anyone.
const human = { kind: 'human' as const, id: 'kabir', name: 'Kabir' }
const agent = { kind: 'agent' as const, id: 'agent', name: 'Agent' }
const system = { kind: 'system' as const, id: 'system', name: 'System' }

function ev(seq: number, type: Event['type'], payload: unknown, actor: Event['actor'] = agent): Event {
  return { room_id: 'r1', seq, type, actor, payload, at: `2026-10-15T10:00:${String(seq).padStart(2, '0')}Z` }
}

const created = (id: string, extra: Record<string, unknown> = {}) => ({
  proposal_id: id,
  title: `fix ${id}`,
  steps: [],
  risk: 'restart interrupts CDC for a few seconds',
  rollback: 'undo',
  required_layers: ['kafka-connect', 'consumer'],
  ...extra,
})

function fold(events: Event[], from: RoomState = emptyRoomState('r1')): RoomState {
  return events.reduce(applyEvent, from)
}

describe('proposal.withdrawn', () => {
  it('ends an open proposal: it takes the withdrawn status, the reason and who wrote it', () => {
    const s = fold([ev(1, 'proposal.created', created('p1')), ev(2, 'proposal.withdrawn', { proposal_id: 'p1', reason: 'wrong connector' })])
    const p = s.proposals[0]
    expect(p.status).toBe('withdrawn')
    expect(p.withdrawn).toEqual({ reason: 'wrong connector', by: agent, at: '2026-10-15T10:00:02Z' })
    // The layers it was waiting on are untouched: nobody approved or rejected them.
    expect(p.approvals).toEqual({ 'kafka-connect': 'pending', consumer: 'pending' })
    expect(s.proposals.filter((x) => x.status === 'open')).toHaveLength(0)
  })

  it('keeps a driver withdrawal with no reason', () => {
    const s = fold([ev(1, 'proposal.created', created('p1')), ev(2, 'proposal.withdrawn', { proposal_id: 'p1', reason: '' }, human)])
    expect(s.proposals[0].status).toBe('withdrawn')
    expect(s.proposals[0].withdrawn).toMatchObject({ reason: '', by: human })
  })

  it('only that proposal is withdrawn', () => {
    const s = fold([
      ev(1, 'proposal.created', created('p1')),
      ev(2, 'proposal.created', created('p2')),
      ev(3, 'proposal.withdrawn', { proposal_id: 'p1', reason: 'x' }),
    ])
    expect(s.proposals.map((p) => p.status)).toEqual(['withdrawn', 'open'])
  })

  it('an unknown proposal is ignored; the event still advances last_seq', () => {
    const before = fold([ev(1, 'proposal.created', created('p1'))])
    const after = applyEvent(before, ev(2, 'proposal.withdrawn', { proposal_id: 'p_nope', reason: 'x' }))
    expect(after.proposals).toEqual(before.proposals)
    expect(after.last_seq).toBe(2)
  })

  it('a rejected proposal keeps its outcome; a fully approved one that never ran is withdrawn (QA F7)', () => {
    const approved = fold([
      ev(1, 'proposal.created', created('p1', { required_layers: ['kafka-connect'] })),
      ev(2, 'approval.given', { proposal_id: 'p1', layer: 'kafka-connect' }, human),
      ev(3, 'proposal.withdrawn', { proposal_id: 'p1', reason: 'late' }),
    ])
    expect(approved.proposals[0].status).toBe('withdrawn')
    const rejected = fold([
      ev(1, 'proposal.created', created('p1', { required_layers: ['kafka-connect'] })),
      ev(2, 'approval.rejected', { proposal_id: 'p1', layer: 'kafka-connect', reason: 'no' }, human),
      ev(3, 'proposal.withdrawn', { proposal_id: 'p1', reason: 'late' }),
    ])
    expect(rejected.proposals[0].status).toBe('rejected')
  })

  it('shows up in the activity feed with the proposal title, the reason as detail', () => {
    const s = fold([ev(1, 'proposal.created', created('p1')), ev(2, 'proposal.withdrawn', { proposal_id: 'p1', reason: 'wrong connector' })])
    expect(s.activity[0]).toMatchObject({ text: 'Agent withdrew a fix · fix p1', detail: 'wrong connector', kind: 'decision' })
  })
})

describe('a closed room waits on nobody', () => {
  const open = [ev(1, 'proposal.created', created('p1'))]

  it('withdraws an open proposal at room.closed even when the log has no proposal.withdrawn (a log from before Q0)', () => {
    const s = fold([...open, ev(2, 'room.closed', { resolution: 'fixed by hand' }, human)])
    expect(s.status).toBe('closed')
    expect(s.proposals[0].status).toBe('withdrawn')
    expect(s.proposals[0].withdrawn).toEqual({ reason: 'room closed', at: '2026-10-15T10:00:02Z' })
    expect(s.proposals.filter((p) => p.status === 'open')).toHaveLength(0)
  })

  it('the server\'s own proposal.withdrawn after room.closed adds who wrote it and keeps one withdrawal', () => {
    const s = fold([
      ...open,
      ev(2, 'room.closed', { resolution: 'fixed' }, human),
      ev(3, 'proposal.withdrawn', { proposal_id: 'p1', reason: 'room closed' }, system),
    ])
    expect(s.proposals).toHaveLength(1)
    expect(s.proposals[0].status).toBe('withdrawn')
    expect(s.proposals[0].withdrawn).toEqual({ reason: 'room closed', by: system, at: '2026-10-15T10:00:03Z' })
    expect(s.activity[0].text).toBe('Fix withdrawn · fix p1')
  })

  it('a proposal the driver already withdrew keeps the driver\'s reason when the room closes', () => {
    const s = fold([
      ...open,
      ev(2, 'proposal.withdrawn', { proposal_id: 'p1', reason: 'wrong connector' }, human),
      ev(3, 'room.closed', { resolution: 'fixed' }, human),
    ])
    expect(s.proposals[0].withdrawn).toMatchObject({ reason: 'wrong connector', by: human })
  })

  it('leaves approved and rejected proposals as they are', () => {
    const s = fold([
      ev(1, 'proposal.created', created('p1', { required_layers: ['kafka-connect'] })),
      ev(2, 'approval.given', { proposal_id: 'p1', layer: 'kafka-connect' }, human),
      ev(3, 'proposal.created', created('p2', { required_layers: ['consumer'] })),
      ev(4, 'approval.rejected', { proposal_id: 'p2', layer: 'consumer', reason: 'no' }, human),
      ev(5, 'proposal.created', created('p3')),
      ev(6, 'room.closed', { resolution: 'done' }, human),
    ])
    expect(s.proposals.map((p) => p.status)).toEqual(['approved', 'rejected', 'withdrawn'])
  })
})

describe('withdraw racing the last approval (QA F7)', () => {
  const one = created('p1', { required_layers: ['kafka-connect'] })
  const executed = { proposal_id: 'p1', step: 0, tool: 'connect_restart_task', ok: true }
  it('approval.given then proposal.withdrawn: withdrawn', () => {
    const s = fold([ev(1, 'proposal.created', one), ev(2, 'approval.given', { proposal_id: 'p1', layer: 'kafka-connect' }, human), ev(3, 'proposal.withdrawn', { proposal_id: 'p1', reason: 'x' })])
    expect(s.proposals[0].status).toBe('withdrawn')
    expect(s.proposals[0].withdrawn).toBeDefined()
  })
  it('proposal.withdrawn then approval.given: stays withdrawn', () => {
    const s = fold([ev(1, 'proposal.created', one), ev(2, 'proposal.withdrawn', { proposal_id: 'p1', reason: 'x' }), ev(3, 'approval.given', { proposal_id: 'p1', layer: 'kafka-connect' }, human)])
    expect(s.proposals[0].status).toBe('withdrawn')
  })
  it('action.executed after proposal.withdrawn: it ran, never both', () => {
    const s = fold([ev(1, 'proposal.created', one), ev(2, 'approval.given', { proposal_id: 'p1', layer: 'kafka-connect' }, human), ev(3, 'proposal.withdrawn', { proposal_id: 'p1', reason: 'x' }), ev(4, 'action.executed', executed)])
    expect(s.proposals[0].status).toBe('approved')
    expect(s.proposals[0].withdrawn).toBeUndefined()
    expect(s.proposals[0].executed).toHaveLength(1)
  })
  it('withdrawal after a step ran does not undo it', () => {
    const s = fold([ev(1, 'proposal.created', one), ev(2, 'approval.given', { proposal_id: 'p1', layer: 'kafka-connect' }, human), ev(3, 'action.executed', executed), ev(4, 'proposal.withdrawn', { proposal_id: 'p1', reason: 'x' })])
    expect(s.proposals[0].status).toBe('approved')
    expect(s.proposals[0].withdrawn).toBeUndefined()
  })
})

describe('proposal.created: impact (QA G5)', () => {
  it('copies impacted_layers, risk_level and impact_reasons', () => {
    const s = fold([
      ev(
        1,
        'proposal.created',
        created('p1', {
          required_layers: ['kafka-connect', 'consumer', 'warehouse'],
          impacted_layers: ['consumer', 'warehouse'],
          risk_level: 'high',
          impact_reasons: ['decimal.handling.mode=double turns NUMERIC columns into doubles'],
        }),
      ),
    ])
    expect(s.proposals[0]).toMatchObject({
      impacted_layers: ['consumer', 'warehouse'],
      risk_level: 'high',
      impact_reasons: ['decimal.handling.mode=double turns NUMERIC columns into doubles'],
    })
    expect(s.proposals[0].approvals).toEqual({ 'kafka-connect': 'pending', consumer: 'pending', warehouse: 'pending' })
  })

  it('leaves the fields off a proposal from before Q0', () => {
    const p = fold([ev(1, 'proposal.created', created('p1'))]).proposals[0]
    expect(p).not.toHaveProperty('impacted_layers')
    expect(p).not.toHaveProperty('risk_level')
    expect(p).not.toHaveProperty('impact_reasons')
  })
})
