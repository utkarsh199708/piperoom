import { describe, expect, it } from 'vitest'
import { emptyRoomState } from '../state/types'
import type { ProposalState } from '../state/types'
import { attentionItems } from './attention'
import { withBadge } from './notify'

const meera = { id: 'meera', name: 'Meera', layers: ['warehouse'] }
const arjun = { id: 'arjun', name: 'Arjun', layers: ['kafka'] }
const actor = (id: string, name: string) => ({ kind: 'human' as const, id, name })

const proposal = (layers: string[]): ProposalState => ({
  proposal_id: 'p1',
  title: 'Reset offsets',
  steps: [],
  risk: '',
  rollback: '',
  required_layers: layers,
  approvals: Object.fromEntries(layers.map((l) => [l, 'pending' as const])),
  decisions: {},
  params_hash: '',
  status: 'open',
  executed: [],
})

describe('attentionItems', () => {
  const prev = { ...emptyRoomState('r1'), title: 'Orders stale', driver: { id: 'arjun', name: 'Arjun' } }

  it('a new proposal is "approval needed" for an owner of a pending layer, "proposed" for others', () => {
    const next = { ...prev, proposals: [proposal(['warehouse'])] }
    expect(attentionItems(prev, next, meera)[0]).toMatchObject({ kind: 'approval_needed', body: 'Reset offsets' })
    expect(attentionItems(prev, next, arjun)[0].kind).toBe('proposal')
  })

  it('a known proposal does not notify twice', () => {
    const withP = { ...prev, proposals: [proposal(['warehouse'])] }
    expect(attentionItems(withP, { ...withP }, meera)).toEqual([])
  })

  it('a hand-off to me notifies me; one I wrote as driver does not', () => {
    const h = {
      handoff_id: 'h1',
      to: actor('meera', 'Meera'),
      summary: { what_broke: '', why: '', done: [], ruled_out: [], open: [], risks: [] },
      stay_as_watcher: false,
      status: 'pending' as const,
      from: { kind: 'human' as const, id: 'arjun', name: 'Arjun' }, created_at: '2026-10-14T10:05:00Z',
    }
    const next = { ...prev, handoffs: [h] }
    expect(attentionItems(prev, next, meera)[0]).toMatchObject({ kind: 'handoff' })
    expect(attentionItems(prev, next, meera)[0].title).toMatch(/for you to accept/)
    expect(attentionItems(prev, next, arjun)).toEqual([])
  })

  it('a driver change notifies others but not the new driver, and a lease renewal is no change', () => {
    const next = { ...prev, driver: { id: 'kabir', name: 'Kabir' } }
    expect(attentionItems(prev, next, meera)[0]).toMatchObject({ kind: 'driver_changed', body: 'Kabir is driving' })
    // The new driver is told they have control, not that someone else drives.
    expect(attentionItems(prev, next, actor('kabir', 'Kabir') as never).map((i) => i.kind)).toEqual(['control_granted'])
    const renewed = { ...prev, driver: { id: 'arjun', name: 'Arjun', lease_until: '2026-10-11T12:00:00Z' } }
    expect(attentionItems(prev, renewed, meera)).toEqual([])
  })
})

describe('new kinds', () => {
  const base = { ...emptyRoomState('r1'), title: 'Orders stale', driver: { id: 'arjun', name: 'Arjun' } }

  it('control granted to me: only when I was not the driver before', () => {
    const next = { ...base, driver: { id: 'meera', name: 'Meera' } }
    expect(attentionItems(base, next, meera)).toMatchObject([{ kind: 'control_granted', title: 'You now drive "Orders stale"' }])
    expect(attentionItems(next, { ...next }, meera)).toEqual([])
  })

  it('agent_error body never carries the status detail', () => {
    const next = { ...base, agent_status: { state: 'error', detail: 'password=hunter2 row {"id":4}' } }
    const [item] = attentionItems(base, next, meera)
    expect(item.kind).toBe('agent_error')
    expect(JSON.stringify(item)).not.toMatch(/hunter2|password|row/)
    expect(item.body).toBe('The agent stopped. Open the room to steer it.')
    expect(attentionItems(next, { ...next }, meera)).toEqual([])
  })

  it('resolved when the room turns closed, naming who closed it from the activity feed', () => {
    const next = {
      ...base,
      status: 'closed' as const,
      activity: [{ seq: 1, at: '', actor: { kind: 'human' as const, id: 'k', name: 'Kabir' }, text: 'closed the room', kind: 'decision' as const }],
    }
    expect(attentionItems(base, next, meera)).toMatchObject([{ kind: 'resolved', body: 'Closed by Kabir' }])
  })

  it('approval_needed carries the layer and a cleaned, capped title', () => {
    const long = 'x'.repeat(200)
    const next = { ...base, proposals: [{ ...proposal(['warehouse']), title: long }] }
    const [item] = attentionItems(base, next, meera)
    expect(item).toMatchObject({ kind: 'approval_needed', layer: 'warehouse' })
    expect(item.body.length).toBeLessThanOrEqual(120)
  })
})

describe('withBadge', () => {
  it('adds, replaces and removes the count', () => {
    expect(withBadge('Piperoom', 3)).toBe('(3) Piperoom')
    expect(withBadge('(3) Piperoom', 4)).toBe('(4) Piperoom')
    expect(withBadge('(3) Piperoom', 0)).toBe('Piperoom')
  })
})
