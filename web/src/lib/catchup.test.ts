import { describe, expect, it } from 'vitest'
import { catchUpSummary, awayLabel } from './catchup'
import type { CurrentUser } from '../state/permissions'
import { emptyRoomState } from '../state/types'
import type { ProposalState, RoomState } from '../state/types'

const me: CurrentUser = { id: 'arjun', name: 'Arjun', layers: ['kafka-connect'] }
const a = { kind: 'human' as const, id: 'kabir', name: 'Kabir' }
const prev = (): RoomState => ({ ...emptyRoomState('r1'), driver: { id: 'arjun', name: 'Arjun' } })
const step = (id: string) => ({ id, title: id, tool_calls: [], messages: [] })
const proposal = (layers: string[]): ProposalState => ({
  proposal_id: 'p1', title: 't', steps: [], risk: 'low', rollback: 'r', required_layers: layers,
  approvals: Object.fromEntries(layers.map((l) => [l, 'pending'])) as ProposalState['approvals'],
  decisions: {}, params_hash: 'h', status: 'open', executed: [],
})
const hyp = (confidence: string, text = 'h') => ({ text, based_on_steps: [], confidence, confirmed: false })

describe('catchUpSummary', () => {
  it('is null when nothing happened', () => {
    expect(catchUpSummary(prev(), prev(), me)).toBeNull()
  })

  it('reads exactly like the strip: steps, hypothesis, a proposal for you', () => {
    const next = {
      ...prev(),
      steps: [step('a'), step('b'), step('c')],
      hypothesis: hyp('high'),
      proposals: [proposal(['kafka-connect'])],
    }
    const s = catchUpSummary(prev(), next, me)
    expect(s?.items).toEqual(['3 steps', 'hypothesis → high', 'a proposal needs you'])
    expect(s?.target).toBe('proposal')
  })

  it('counts one step in the singular', () => {
    expect(catchUpSummary(prev(), { ...prev(), steps: [step('a')] }, me)).toEqual({ items: ['1 step'], target: 'steps', lastStepId: 'a' })
  })

  it('tells a changed hypothesis from a new confidence', () => {
    const before = { ...prev(), hypothesis: hyp('medium', 'old') }
    expect(catchUpSummary(before, { ...before, hypothesis: hyp('medium', 'new') }, me)?.items).toEqual(['hypothesis changed'])
    expect(catchUpSummary(before, { ...before, hypothesis: hyp('high', 'old') }, me)?.items).toEqual(['hypothesis → high'])
    expect(catchUpSummary(before, before, me)).toBeNull()
  })

  it('says "a fix was proposed" when no pending layer is yours', () => {
    const s = catchUpSummary(prev(), { ...prev(), proposals: [proposal(['postgres'])] }, me)
    expect(s?.items).toEqual(['a fix was proposed'])
  })

  it('points the driver at a proposal whose only pending layer is a fallback layer, in an open room only', () => {
    // Arjun drives but does not own "consumer"; nobody does, so the driver decides it (P2b fallback_layers).
    const p = { ...proposal(['consumer']), fallback_layers: ['consumer'] }
    const s = catchUpSummary(prev(), { ...prev(), proposals: [p] }, me)
    expect(s?.items).toEqual(['a proposal needs you'])
    expect(s?.target).toBe('proposal')
    const kabir: CurrentUser = { id: 'kabir', name: 'Kabir', layers: ['postgres'] }
    expect(catchUpSummary(prev(), { ...prev(), proposals: [p] }, kabir)?.items).toEqual(['a fix was proposed'])
    const closed = { ...prev(), status: 'closed' as const }
    expect(catchUpSummary(closed, { ...closed, proposals: [p] }, me)?.items).toEqual(['a fix was proposed'])
  })

  it('reports a fix that ran or failed', () => {
    const base = { ...prev(), proposals: [proposal(['postgres'])] }
    const ran = { ...base, proposals: [{ ...base.proposals[0], executed: [{ proposal_id: 'p1', step_index: 0, ok: true, output: 'x' }] }] }
    const failed = { ...base, proposals: [{ ...base.proposals[0], executed: [{ proposal_id: 'p1', step_index: 0, ok: false, output: 'x' }] }] }
    expect(catchUpSummary(base, ran, me)?.items).toEqual(['fix ran'])
    expect(catchUpSummary(base, failed, me)?.items).toEqual(['fix failed'])
  })

  it('reports a hand-off for you, a new driver, a closed room and an agent error, in that order', () => {
    const next: RoomState = {
      ...prev(),
      handoffs: [{ handoff_id: 'h1', to: { kind: 'human', id: 'arjun', name: 'Arjun' }, summary: {} as never, stay_as_watcher: false, status: 'pending', from: { kind: 'human' as const, id: 'arjun', name: 'Arjun' }, created_at: '2026-10-14T10:05:00Z' }],
      driver: { id: a.id, name: a.name },
      status: 'closed',
      agent_status: { state: 'error' },
    }
    expect(catchUpSummary(prev(), next, me)?.items).toEqual(['hand-off for you', 'driver → Kabir', 'room closed', 'agent error'])
  })

  it('neutralizes a driver name that fakes a badge or a verdict', () => {
    const next: RoomState = { ...prev(), driver: { id: 'u_9', name: 'Mal ✓ verified' } }
    const [item] = catchUpSummary(prev(), next, me)?.items ?? []
    expect(item).toBe('driver → Mal � �')
  })

  it('ignores a hand-off for someone else', () => {
    const next = { ...prev(), handoffs: [{ handoff_id: 'h1', to: a, summary: {} as never, stay_as_watcher: false, status: 'pending' as const, from: a, created_at: '2026-10-14T10:05:00Z' }] }
    expect(catchUpSummary(prev(), next, me)).toBeNull()
  })

  it('formats the away time in whole minutes, at least one', () => {
    expect(awayLabel(12 * 60_000)).toBe('12m')
    expect(awayLabel(60_000)).toBe('1m')
    expect(awayLabel(20_000)).toBe('1m')
  })
})
