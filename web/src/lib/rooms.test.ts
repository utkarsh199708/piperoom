import { describe, expect, it } from 'vitest'
import type { RoomSnapshot } from '../api/types'
import type { CurrentUser } from '../state/permissions'
import { compareRooms, needsYou, pendingLayersForUser, roomLiveBadge, roomStats, sortRoomsForList } from './rooms'

function room(id: string, status: RoomSnapshot['status'], opened_at: string): RoomSnapshot {
  return {
    id,
    title: id,
    severity: 'sev3',
    pipeline_id: 'orders',
    status,
    opened_at,
    last_seq: 0,
    driver: null,
    agent_status: { state: 'idle' },
    hypothesis: null,
    open_proposals: [],
    pending_suggestions: [],
    members: [],
  }
}

describe('sortRoomsForList', () => {
  it('puts open rooms before closed ones regardless of opened time', () => {
    const closedNew = room('c_new', 'closed', '2026-10-11T12:00:00Z')
    const openOld = room('o_old', 'open', '2026-10-11T09:00:00Z')
    expect(sortRoomsForList([closedNew, openOld]).map((r) => r.id)).toEqual(['o_old', 'c_new'])
    expect(compareRooms(openOld, closedNew)).toBeLessThan(0)
  })

  it('sorts newest opened first within each group', () => {
    const rooms = [
      room('o_old', 'open', '2026-10-11T09:00:00Z'),
      room('c_old', 'closed', '2026-10-10T09:00:00Z'),
      room('o_new', 'open', '2026-10-11T11:00:00Z'),
      room('c_new', 'closed', '2026-10-11T08:00:00Z'),
    ]
    expect(sortRoomsForList(rooms).map((r) => r.id)).toEqual([
      'o_new',
      'o_old',
      'c_new',
      'c_old',
    ])
  })

  it('does not mutate its input and keeps input order for ties', () => {
    const first = room('b', 'open', '2026-10-11T10:00:00Z')
    const second = room('a', 'open', '2026-10-11T10:00:00Z')
    const third = room('c', 'open', '2026-10-11T10:00:00Z')
    const input = [first, second, third]
    expect(sortRoomsForList(input).map((r) => r.id)).toEqual(['b', 'a', 'c'])
    expect(input.map((r) => r.id)).toEqual(['b', 'a', 'c'])
  })

  it('treats an unparsable opened_at as oldest', () => {
    const bad = room('bad', 'open', 'not-a-date')
    const good = room('good', 'open', '2026-10-11T10:00:00Z')
    expect(sortRoomsForList([bad, good]).map((r) => r.id)).toEqual(['good', 'bad'])
  })
})

describe('needsYou', () => {
  const arjun: CurrentUser = { id: 'arjun', name: 'Arjun', layers: ['kafka-connect'] }
  const viewer: CurrentUser = { ...arjun, role: 'viewer' }
  const withProposal = (over: Partial<RoomSnapshot> = {}): RoomSnapshot => ({
    ...room('r', 'open', '2026-10-11T10:00:00Z'),
    driver: { id: 'kabir', name: 'Kabir' },
    open_proposals: [
      {
        proposal_id: 'p',
        title: 't',
        steps: [],
        risk: 'low',
        rollback: '',
        required_layers: ['kafka-connect', 'warehouse'],
        approvals: { 'kafka-connect': 'pending', warehouse: 'pending' },
      },
    ],
    ...over,
  })

  it('names the pending layers I own', () => {
    expect(needsYou(withProposal(), arjun)).toEqual({ reason: 'approval', layers: ['kafka-connect'] })
    expect(pendingLayersForUser(withProposal(), arjun)).toEqual(['kafka-connect'])
  })

  it('ignores layers that are decided or belong to someone else', () => {
    const decided = withProposal({
      open_proposals: [
        {
          proposal_id: 'p',
          title: 't',
          steps: [],
          risk: 'low',
          rollback: '',
          required_layers: ['kafka-connect'],
          approvals: { 'kafka-connect': 'approved' },
        },
      ],
    })
    expect(needsYou(decided, arjun)).toBeNull()
    expect(needsYou(withProposal(), { ...arjun, layers: ['consumer'] })).toBeNull()
  })

  it('says "no driver" for an open room without one, and prefers a pending approval over it', () => {
    expect(needsYou(withProposal({ open_proposals: [], driver: null }), arjun)).toEqual({ reason: 'no_driver' })
    expect(needsYou(withProposal({ driver: null }), arjun)).toEqual({ reason: 'approval', layers: ['kafka-connect'] })
  })

  it('is null for a viewer and for a closed room', () => {
    expect(needsYou(withProposal({ driver: null }), viewer)).toBeNull()
    expect(needsYou(withProposal({ status: 'closed', driver: null }), arjun)).toBeNull()
  })
})

describe('roomLiveBadge', () => {
  const badge = (state: string, detail?: string, status: RoomSnapshot['status'] = 'open') =>
    roomLiveBadge({ ...room('r', status, '2026-10-11T10:00:00Z'), agent_status: { state, detail } })

  it('maps the agent state to a badge kind and label', () => {
    expect(badge('working', 'step 3')).toEqual({ kind: 'working', label: 'working · step 3' })
    expect(badge('working')).toEqual({ kind: 'working', label: 'working' })
    expect(badge('waiting_approval')).toEqual({ kind: 'attn', label: 'needs approval' })
    expect(badge('done')).toEqual({ kind: 'done', label: 'resolved' })
    expect(badge('idle')).toEqual({ kind: 'idle', label: 'idle' })
    expect(badge('error')).toEqual({ kind: 'error', label: 'error' })
    expect(badge('paused', 'by driver')).toEqual({ kind: 'idle', label: 'paused · by driver' })
  })

  it('never shows a live agent in a closed room', () => {
    expect(badge('working', 'step 3', 'closed')).toEqual({ kind: 'done', label: 'closed' })
  })
})

describe('roomStats', () => {
  it('counts open and closed rooms, no-driver rooms and working agents', () => {
    const user: CurrentUser = { id: 'arjun', name: 'Arjun', layers: [] }
    const rooms = [
      room('a', 'open', '2026-10-11T10:00:00Z'),
      { ...room('b', 'open', '2026-10-11T10:00:00Z'), driver: { id: 'k', name: 'K' }, agent_status: { state: 'working' } },
      { ...room('c', 'closed', '2026-10-11T10:00:00Z'), agent_status: { state: 'working' } },
    ]
    const s = roomStats(rooms, user)
    expect([s.open, s.closed]).toEqual([2, 1])
    expect(s.noDriver.map((r) => r.id)).toEqual(['a'])
    expect(s.working.map((r) => r.id)).toEqual(['b'])
    expect(s.needsYou).toEqual([])
  })
})
