import { afterEach, describe, expect, it, vi } from 'vitest'
import type { PageSummary, RoomSnapshot } from '../api/types'
import { emptyRoomState } from '../state/types'
import type { RoomState } from '../state/types'
import {
  canPageOwner,
  markSeen,
  ownerNames,
  pageKey,
  pagedBy,
  pagesFor,
  readSeen,
  suggestedPageLayer,
  unseenPages,
  writeSeen,
} from './pages'

const agent = { kind: 'agent' as const, id: 'agent', name: 'Agent' }

const page = (over: Partial<PageSummary> = {}): PageSummary => ({
  layer: 'consumer',
  owners: ['kabir'],
  reason: 'lag',
  by: agent,
  at: '2026-10-15T04:10:00Z',
  ...over,
})

const room = (id: string, pages?: PageSummary[], over: Partial<RoomSnapshot> = {}) =>
  ({ id, title: id, status: 'open', pipeline_id: 'orders', pages, ...over }) as RoomSnapshot

afterEach(() => {
  vi.unstubAllGlobals()
  localStorage.clear()
})

describe('pagesFor', () => {
  it('lists the pages of open rooms that name the user, newest first', () => {
    const rooms = [
      room('a', [page({ at: '2026-10-15T04:00:00Z' })]),
      room('b', [page({ layer: 'kafka', at: '2026-10-15T05:00:00Z' }), page({ layer: 'warehouse', owners: ['meera'] })]),
      room('c', [page()], { status: 'closed' }),
      room('d'), // an older server sends no pages
    ]
    expect(pagesFor(rooms, 'kabir').map((p) => p.key)).toEqual(['b|kafka', 'a|consumer'])
    expect(pagesFor(rooms, 'nobody')).toEqual([])
  })

  it('matches group:<name> owners against the groups of the user', () => {
    const rooms = [room('a', [page({ owners: ['group:dataeng-oncall'] })]), room('b', [page({ owners: ['group:other', 'meera'] })])]
    expect(pagesFor(rooms, 'kabir', ['dataeng-oncall']).map((p) => p.key)).toEqual(['a|consumer'])
    expect(pagesFor(rooms, 'kabir')).toEqual([])
    expect(pagesFor(rooms, 'kabir', ['dataeng'])).toEqual([])
    expect(unseenPages(rooms, 'kabir', {}, ['dataeng-oncall']).map((p) => p.key)).toEqual(['a|consumer'])
  })

  it('matches a user:<id> owner', () => {
    expect(pagesFor([room('a', [page({ owners: ['user:kabir'] })])], 'kabir').length).toBe(1)
  })

  it('survives a page without owners', () => {
    expect(pagesFor([room('a', [{ ...page(), owners: undefined as unknown as string[] }])], 'kabir')).toEqual([])
  })
})

describe('unseenPages and markSeen', () => {
  const rooms = [room('a', [page({ at: '2026-10-15T04:10:00Z' })])]

  it('everything is unseen at first', () => {
    expect(unseenPages(rooms, 'kabir', {})).toHaveLength(1)
  })

  it('a page is seen once marked, and a newer page of the same room and layer is unseen again', () => {
    const items = pagesFor(rooms, 'kabir')
    const seen = markSeen({}, items)
    expect(seen).toEqual({ 'a|consumer': '2026-10-15T04:10:00Z' })
    expect(unseenPages(rooms, 'kabir', seen)).toEqual([])
    const later = [room('a', [page({ at: '2026-10-15T04:25:00Z' })])]
    expect(unseenPages(later, 'kabir', seen)).toHaveLength(1)
  })

  it('another layer of the same room is its own slot', () => {
    const two = [room('a', [page({ at: '2026-10-15T04:10:00Z' }), page({ layer: 'kafka', at: '2026-10-15T04:11:00Z' })])]
    const seen = markSeen({}, pagesFor(two, 'kabir').filter((p) => p.page.layer === 'consumer'))
    expect(unseenPages(two, 'kabir', seen).map((p) => p.page.layer)).toEqual(['kafka'])
  })

  it('never moves a slot back to an older time, and returns the same object when nothing changes', () => {
    const seen = { 'a|consumer': '2026-10-15T09:00:00Z' }
    expect(markSeen(seen, pagesFor(rooms, 'kabir'))).toBe(seen)
  })

  it('an unreadable time is unseen until the very same text is marked', () => {
    const odd = [room('a', [page({ at: 'soon' })])]
    const seen = markSeen({}, pagesFor(odd, 'kabir'))
    expect(unseenPages(odd, 'kabir', seen)).toEqual([])
    expect(unseenPages([room('a', [page({ at: 'later' })])], 'kabir', seen)).toHaveLength(1)
  })

  it('keeps at most 200 entries, dropping the oldest', () => {
    let seen = {}
    for (let i = 0; i < 205; i++) {
      const at = new Date(Date.UTC(2026, 9, 1, 0, i)).toISOString()
      seen = markSeen(seen, [{ room: room(`r${i}`), page: page({ at }), key: pageKey(`r${i}`, 'consumer') }])
    }
    const keys = Object.keys(seen)
    expect(keys).toHaveLength(200)
    expect(keys).not.toContain('r0|consumer')
    expect(keys).toContain('r204|consumer')
  })
})

describe('the seen store', () => {
  it('round-trips per user', () => {
    writeSeen('kabir', { 'a|consumer': 'x' })
    expect(readSeen('kabir')).toEqual({ 'a|consumer': 'x' })
    expect(readSeen('meera')).toEqual({})
  })

  it('reads {} for damaged content', () => {
    localStorage.setItem('piperoom.pages.seen.kabir', '{not json')
    expect(readSeen('kabir')).toEqual({})
    localStorage.setItem('piperoom.pages.seen.kabir', '[1,2]')
    expect(readSeen('kabir')).toEqual({})
    localStorage.setItem('piperoom.pages.seen.kabir', JSON.stringify({ a: 'x', b: 7 }))
    expect(readSeen('kabir')).toEqual({ a: 'x' })
  })

  it('does not throw when storage is blocked', () => {
    vi.stubGlobal('localStorage', {
      getItem: () => {
        throw new Error('blocked')
      },
      setItem: () => {
        throw new Error('blocked')
      },
    })
    expect(readSeen('kabir')).toEqual({})
    expect(() => writeSeen('kabir', { a: 'x' })).not.toThrow()
  })
})

describe('canPageOwner', () => {
  const open: RoomState = emptyRoomState('r1')
  it('members page open rooms; viewers and closed rooms cannot', () => {
    const member = { id: 'arjun', name: 'Arjun', layers: [] }
    expect(canPageOwner(open, member)).toBe(true)
    expect(canPageOwner(open, { ...member, role: 'viewer' })).toBe(false)
    expect(canPageOwner({ ...open, status: 'closed' }, member)).toBe(false)
  })
})

describe('suggestedPageLayer', () => {
  const base = emptyRoomState('r1')
  const proposal = (approvals: Record<string, 'pending' | 'approved'>, status: 'open' | 'approved' = 'open') => ({
    proposal_id: 'p1',
    title: 'fix',
    steps: [],
    risk: '',
    rollback: '',
    required_layers: Object.keys(approvals),
    approvals,
    decisions: {},
    params_hash: '',
    status,
    executed: [],
  })

  it('starts from the first look suspect, else a layer a fix waits on, else nothing', () => {
    const triage = { status: 'ok' as const, suspected_layer: 'schema-registry', evidence_refs: [], at: 'x' }
    expect(suggestedPageLayer({ ...base, triage })).toBe('schema-registry')
    expect(suggestedPageLayer({ ...base, proposals: [proposal({ postgres: 'approved', warehouse: 'pending' })] })).toBe('warehouse')
    expect(suggestedPageLayer({ ...base, proposals: [proposal({ warehouse: 'pending' }, 'approved')] })).toBe('')
    expect(suggestedPageLayer(base)).toBe('')
  })

  it('ignores a suspect that is not a layer, and a failed first look', () => {
    expect(suggestedPageLayer({ ...base, triage: { status: 'ok', suspected_layer: 'the moon', evidence_refs: [], at: 'x' } })).toBe('')
    expect(suggestedPageLayer({ ...base, triage: { status: 'failed', suspected_layer: 'kafka', evidence_refs: [], at: 'x' } })).toBe('')
  })
})

describe('names', () => {
  it('ownerNames uses a known name, else the id, as plain text', () => {
    expect(ownerNames(['kabir', 'meera'], { kabir: 'Kabir' })).toBe('Kabir, meera')
    expect(ownerNames([])).toBe('')
  })

  it('pagedBy: the agent, a person, or the server for an automatic page', () => {
    expect(pagedBy({ by: agent, auto: false })).toBe('Agent')
    expect(pagedBy({ by: { kind: 'human', id: 'arjun', name: 'Arjun' }, auto: false })).toBe('Arjun')
    expect(pagedBy({ by: { kind: 'system', id: 'server', name: 'x' }, auto: true })).toBe('Piperoom (auto)')
  })
})
