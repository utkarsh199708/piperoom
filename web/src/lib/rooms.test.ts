import { describe, expect, it } from 'vitest'
import type { RoomSnapshot } from '../api/types'
import { compareRooms, sortRoomsForList } from './rooms'

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
