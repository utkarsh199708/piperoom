import { describe, expect, it } from 'vitest'
import type { RoomSnapshot } from '../api/types'
import {
  agentCell,
  canTakeControlOf,
  hasLiveDriver,
  isMine,
  layersNeedingYou,
  needsApproval,
  pipelineIds,
  roomStats,
  roomStatusCell,
  roomsForPipeline,
  roomsForTab,
  compareRooms,
  sortRoomsForList,
} from './rooms'
import type { CurrentUser } from '../state/permissions'

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

const arjun: CurrentUser = { id: 'arjun', name: 'Arjun', layers: ['kafka-connect'] }
const vera: CurrentUser = { id: 'vera', name: 'Vera', layers: ['kafka-connect'], role: 'viewer' }

function withProposal(r: RoomSnapshot, approvals: Record<string, 'pending' | 'approved'>): RoomSnapshot {
  return {
    ...r,
    open_proposals: [
      {
        proposal_id: 'p1',
        title: 't',
        steps: [],
        risk: 'low',
        rollback: 'none',
        required_layers: Object.keys(approvals),
        approvals,
      },
    ],
  }
}

describe('needs you', () => {
  const base = room('r', 'open', '2026-10-11T10:00:00Z')

  it('is only about pending layers the person owns', () => {
    const r = withProposal(base, { 'kafka-connect': 'pending', warehouse: 'pending' })
    expect(layersNeedingYou(r, arjun)).toEqual(['kafka-connect'])
    expect(needsApproval(r)).toBe(true)
    expect(layersNeedingYou(withProposal(base, { warehouse: 'pending' }), arjun)).toEqual([])
    expect(layersNeedingYou(withProposal(base, { 'kafka-connect': 'approved' }), arjun)).toEqual([])
  })

  it('never for a viewer or a closed room', () => {
    expect(layersNeedingYou(withProposal(base, { 'kafka-connect': 'pending' }), vera)).toEqual([])
    expect(needsApproval(withProposal({ ...base, status: 'closed' }, { 'kafka-connect': 'pending' }))).toBe(false)
  })
})

describe('canTakeControlOf', () => {
  const open = room('r', 'open', '2026-10-11T10:00:00Z')
  it('needs an open room with no driver or an expired lease', () => {
    expect(canTakeControlOf(open, arjun)).toBe(true)
    expect(canTakeControlOf({ ...open, status: 'closed' }, arjun)).toBe(false)
    const live = { ...open, driver: { id: 'k', name: 'K', lease_until: '2999-01-01T00:00:00Z' } }
    expect(canTakeControlOf(live, arjun)).toBe(false)
    expect(canTakeControlOf({ ...live, driver: { id: 'k', name: 'K', lease_until: '2020-01-01T00:00:00Z' } }, arjun)).toBe(true)
    // A driver with no lease time is a live driver.
    expect(canTakeControlOf({ ...open, driver: { id: 'k', name: 'K' } }, arjun)).toBe(false)
    expect(canTakeControlOf({ ...open, driver: { id: 'arjun', name: 'Arjun' } }, arjun)).toBe(false)
  })
  it('never for a viewer', () => {
    expect(canTakeControlOf(open, vera)).toBe(false)
  })
})

describe('tabs and filters', () => {
  const o = room('o', 'open', '2026-10-11T10:00:00Z')
  const mine = { ...room('m', 'open', '2026-10-11T10:00:00Z'), driver: { id: 'arjun', name: 'Arjun' } }
  const c = { ...room('c', 'closed', '2026-10-10T10:00:00Z'), pipeline_id: 'payments' }
  const all = [o, mine, c]

  it('splits rooms by tab', () => {
    expect(roomsForTab(all, 'open', arjun).map((r) => r.id)).toEqual(['o', 'm'])
    expect(roomsForTab(all, 'mine', arjun).map((r) => r.id)).toEqual(['m'])
    expect(roomsForTab(all, 'resolved', arjun).map((r) => r.id)).toEqual(['c'])
    expect(roomsForTab(all, 'all', arjun)).toHaveLength(3)
    expect(isMine(c, arjun)).toBe(false)
  })

  it('filters by pipeline and lists the pipelines', () => {
    expect(roomsForPipeline(all, 'payments').map((r) => r.id)).toEqual(['c'])
    expect(roomsForPipeline(all, null)).toHaveLength(3)
    expect(pipelineIds(all)).toEqual(['orders', 'payments'])
  })
})

describe('roomStats', () => {
  const now = Date.parse('2026-10-11T18:00:00')
  it('counts open, needs you, agent working and resolved today with the median', () => {
    const rooms: RoomSnapshot[] = [
      { ...withProposal(room('a', 'open', '2026-10-11T10:00:00'), { 'kafka-connect': 'pending' }), driver: { id: 'k', name: 'K' } },
      { ...room('b', 'open', '2026-10-11T10:00:00'), agent_status: { state: 'working', detail: 'step 2' }, pipeline_id: 'customers' },
      { ...room('c', 'closed', '2026-10-11T08:00:00'), closed_at: '2026-10-11T09:00:00' },
      { ...room('d', 'closed', '2026-10-11T08:00:00'), closed_at: '2026-10-11T08:20:00' },
      { ...room('e', 'closed', '2026-10-09T08:00:00'), closed_at: '2026-10-09T09:00:00' },
    ]
    const s = roomStats(rooms, arjun, now)
    expect(s.open).toBe(2)
    expect(s.openWithDriver).toBe(1)
    expect(s.needsYou).toBe(1)
    expect(s.needsYouLayers).toEqual(['kafka-connect'])
    expect(s.agentWorking).toBe(1)
    expect(s.agentCaption).toBe('customers · step 2')
    expect(s.resolvedToday).toBe(2)
    expect(s.medianMinutes).toBe(40) // (20 + 60) / 2
  })

  it('does not count a driver whose lease has expired as "with a driver"', () => {
    const rooms: RoomSnapshot[] = [
      { ...room('live', 'open', '2026-10-11T10:00:00'), driver: { id: 'k', name: 'K', lease_until: '2026-10-11T19:00:00' } },
      { ...room('old', 'open', '2026-10-11T10:00:00'), driver: { id: 'k', name: 'K', lease_until: '2026-10-11T17:00:00' } },
      { ...room('none', 'open', '2026-10-11T10:00:00'), driver: { id: 'k', name: 'K' } },
    ]
    expect(roomStats(rooms, arjun, now).openWithDriver).toBe(2)
    expect(hasLiveDriver(rooms[1], now)).toBe(false)
  })

  it('has quiet defaults with no rooms', () => {
    const s = roomStats([], arjun, now)
    expect(s).toMatchObject({ open: 0, needsYou: 0, agentWorking: 0, agentCaption: 'idle', resolvedToday: 0, medianMinutes: null })
  })
})

describe('status and agent cells', () => {
  const r = room('r', 'open', '2026-10-11T10:00:00Z')
  it('status words carry the meaning', () => {
    expect(roomStatusCell({ ...r, status: 'closed', resolution: 'fixed' })).toMatchObject({ dot: 'ok', label: 'Resolved' })
    expect(roomStatusCell({ ...r, status: 'closed' })).toMatchObject({ dot: 'idle', label: 'Closed' })
    expect(roomStatusCell(withProposal(r, { warehouse: 'pending' })).label).toMatch(/^Needs approval · /)
    const driven = { ...r, driver: { id: 'k', name: 'K' } }
    expect(roomStatusCell({ ...driven, agent_status: { state: 'working' } })).toMatchObject({ dot: 'agent', label: 'Investigating' })
    expect(roomStatusCell(driven).label).toBe('Idle · waiting for a steer')
    const verification = { recovered: true, verdict: 'recovered: all good' }
    expect(roomStatusCell({ ...driven, last_fix: { proposal_id: 'p', seq: 5, steps: 1, failed_step: 0, verification } })).toMatchObject({
      dot: 'ok',
      label: 'Fix ran · recovered',
    })
    expect(roomStatusCell(r).label).toMatch(/^No driver · /)
  })
  it('agent cell', () => {
    expect(agentCell(r)).toMatchObject({ label: 'idle', agent: false })
    expect(agentCell({ ...r, agent_status: { state: 'working', detail: 'step 2' } })).toMatchObject({
      label: 'working · step 2',
      agent: true,
    })
    const waiting = withProposal({ ...r, agent_status: { state: 'waiting_approval' } }, { a: 'approved', b: 'pending' })
    expect(agentCell(waiting).label).toBe('waiting · 1 of 2')
    expect(agentCell({ ...r, status: 'closed', agent_status: { state: 'done' } }).label).toBe('resolved')
    expect(agentCell({ ...r, status: 'closed', agent_status: { state: 'waiting_approval' } })).toMatchObject({
      dot: 'ok',
      label: 'resolved',
    })
    expect(agentCell({ ...r, agent_status: { state: 'error' } })).toMatchObject({ dot: 'fail', label: 'error' })
  })
})
