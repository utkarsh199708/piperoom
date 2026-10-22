import { describe, expect, it, vi } from 'vitest'
import { render, screen, within } from '@testing-library/react'
import type { Event } from '../../api/types'
import { applyEvent } from '../../state/reducer'
import { emptyRoomState } from '../../state/types'
import type { ProposalState, RoomState } from '../../state/types'
import { VerificationCard } from './VerificationCard'
import { RecoveryProgress } from './RecoveryCard'
import { BlastRadius } from './BlastRadius'

// The wire shapes of docs/api.md, "X2 recovery actions": agent.recovery_check, agent.verification
// with outcome/before/after/unit, proposal.previewed steps[].preview.count/unit, proposal.created.class.

const agent = { kind: 'agent' as const, id: 'agent', name: 'Agent' }
const system = { kind: 'system' as const, id: 'executor', name: 'Executor' }
const seqAt = (seq: number) => `2026-10-11T10:${String(Math.floor(seq / 60)).padStart(2, '0')}:${String(seq % 60).padStart(2, '0')}Z`
function ev(seq: number, type: Event['type'], payload: unknown, actor: Event['actor'] = agent): Event {
  return { room_id: 'r1', seq, type, actor, payload, at: seqAt(seq) }
}

const created = (cls?: string) =>
  ev(1, 'proposal.created', {
    proposal_id: 'p_1',
    title: 'Re-snapshot missing orders',
    steps: [{ action: 'pg_resnapshot_rows', layer: 'postgres', params: {}, description: 'Re-emit the lost rows' }],
    risk: 'medium',
    rollback: 'none',
    required_layers: ['postgres'],
    params_hash: 'h',
    ...(cls ? { class: cls } : {}),
  })
const executed = (seq: number) => ev(seq, 'action.executed', { proposal_id: 'p_1', step_index: 0, ok: true, output: 'signal sent' }, system)
const check = (seq: number, poll: number, after: number, over: object = {}) =>
  ev(seq, 'agent.recovery_check', { proposal_id: 'p_1', step_index: 0, poll, max_polls: 8, before: 412, after, unit: 'rows', ...over })
const verdict = (seq: number, over: object) =>
  ev(seq, 'agent.verification', { proposal_id: 'p_1', checks: [], recovered: false, verdict: 'v', ...over })

function fold(...events: Event[]): RoomState {
  return events.reduce(applyEvent, { ...emptyRoomState('r1') })
}
const only = (s: RoomState): ProposalState => s.proposals[0]
const nudge = { roomOpen: true, isDriver: true, driver: { id: 'arjun', name: 'Arjun' }, onClose: vi.fn() }

function card(s: RoomState) {
  const p = only(s)
  return render(
    <>
      <RecoveryProgress proposal={p} />
      {p.verification && <VerificationCard verification={p.verification} title={p.title} nudge={nudge} recovery={p.recovery} />}
    </>,
  )
}

describe('recovery verdict (TC1)', () => {
  it('recovered: success word, "412 missing → 0 rows" and how long it took', () => {
    // action.executed at 10:00:02, verdict at 10:03:12 -> 3 min
    const s = fold(created('recovery'), executed(2), verdict(192, { recovered: true, outcome: 'recovered', before: 412, after: 0, unit: 'rows', verdict: 'recovered: 0 rows still missing' }))
    card(s)
    const result = screen.getByTestId('recovery-result')
    expect(result).toHaveAttribute('data-outcome', 'recovered')
    expect(within(result).getByText('Recovered')).toHaveClass('text-success-foreground')
    expect(result).toHaveTextContent('412 missing → 0 rows · recovered in 3 min')
    expect(screen.getByText('Pipeline recovered.')).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /Close room/ })).toBeInTheDocument()
  })

  it('partial: warning word with the remaining count, no close nudge', () => {
    card(fold(created('recovery'), executed(2), verdict(3, { outcome: 'partial', before: 412, after: 90, unit: 'rows' })))
    const result = screen.getByTestId('recovery-result')
    expect(within(result).getByText('Partially recovered')).toHaveClass('text-warning-foreground')
    expect(result).toHaveTextContent('412 missing → 90 rows')
    expect(result).toHaveTextContent('90 rows still missing')
    expect(screen.queryByText('Pipeline recovered.')).toBeNull()
    expect(screen.queryByText(/recovered in/)).toBeNull()
  })

  it('not_recovered: danger word with the remaining count', () => {
    card(fold(created('recovery'), executed(2), verdict(3, { outcome: 'not_recovered', before: 412, after: 412, unit: 'rows' })))
    const result = screen.getByTestId('recovery-result')
    expect(within(result).getByText('Not recovered')).toHaveClass('text-destructive-fg')
    expect(result).toHaveTextContent('412 missing → 412 rows')
    expect(result).toHaveTextContent('412 rows still missing')
    expect(screen.queryByText('Pipeline recovered.')).toBeNull()
  })

  it('unverifiable: neutral word, no counts, never claims recovery', () => {
    card(fold(created('recovery'), executed(2), verdict(3, { outcome: 'unverifiable', verdict: 'not verified: the sink cannot count' })))
    const result = screen.getByTestId('recovery-result')
    expect(within(result).getByText('Could not verify')).toHaveClass('text-muted-foreground')
    expect(result).not.toHaveTextContent('missing →')
    expect(result).toHaveTextContent('could not be counted')
    expect(screen.getByText('not verified: the sink cannot count')).toBeInTheDocument()
    expect(screen.queryByText('Pipeline recovered.')).toBeNull()
  })

  it('after: 0 is printed, an absent after is "?" and never "0"', () => {
    const zero = card(fold(created('recovery'), executed(2), verdict(3, { outcome: 'recovered', recovered: true, before: 5, after: 0, unit: 'records' })))
    expect(screen.getByTestId('recovery-result')).toHaveTextContent('5 missing → 0 records')
    zero.unmount()
    card(fold(created('recovery'), executed(2), verdict(3, { outcome: 'partial', before: 5, after: null, unit: 'records' })))
    expect(screen.getByTestId('recovery-result')).toHaveTextContent('5 missing → ?')
  })

  it('every outcome has an icon-and-word, so colour is never the only signal', () => {
    for (const [outcome, word] of [
      ['recovered', 'Recovered'],
      ['partial', 'Partially recovered'],
      ['not_recovered', 'Not recovered'],
      ['unverifiable', 'Could not verify'],
    ] as const) {
      const { unmount } = card(fold(created('recovery'), executed(2), verdict(3, { outcome, recovered: outcome === 'recovered', before: 3, after: outcome === 'recovered' ? 0 : 1, unit: 'rows' })))
      expect(screen.getByText(word)).toBeInTheDocument()
      unmount()
    }
  })

  it('a fix verification (no outcome) renders exactly as before', () => {
    const fix = fold(created(), ev(2, 'agent.verification', { proposal_id: 'p_1', checks: [], recovered: true, verdict: 'recovered: ok' }))
    card(fix)
    expect(screen.getByText('Verified · recovered')).toBeInTheDocument()
    expect(screen.queryByTestId('recovery-result')).toBeNull()
  })
})

describe('recovery progress (TC2)', () => {
  it('shows a spinner with "checking (2 of N)" and follows the event stream', () => {
    let s = fold(created('recovery'), executed(2), check(3, 1, 412))
    const first = card(s)
    expect(screen.getByTestId('recovery-progress')).toHaveTextContent('checking (1 of 8)')
    expect(screen.getByRole('img', { name: 'checking' })).toBeInTheDocument()
    first.unmount()

    s = applyEvent(s, check(4, 2, 120))
    card(s)
    const progress = screen.getByTestId('recovery-progress')
    expect(progress).toHaveTextContent('checking (2 of 8)')
    expect(progress).toHaveTextContent('412 missing → 120 rows')
    expect(progress).toHaveAttribute('aria-live', 'polite')
  })

  it('goes away when the verdict arrives', () => {
    const s = fold(created('recovery'), executed(2), check(3, 2, 0), verdict(4, { outcome: 'recovered', recovered: true, before: 412, after: 0, unit: 'rows' }))
    card(s)
    expect(screen.queryByTestId('recovery-progress')).toBeNull()
    expect(screen.getByTestId('recovery-result')).toBeInTheDocument()
  })

  it('a replayed older poll of the same step never moves the count back', () => {
    const s = fold(created('recovery'), executed(2), check(3, 3, 50), check(4, 2, 300))
    expect(only(s).recovery?.check).toMatchObject({ poll: 3, after: 50 })
  })

  it('renders nothing before the first poll, for a fix, or for a poll naming no proposal', () => {
    const none = card(fold(created('recovery'), executed(2)))
    expect(screen.queryByTestId('recovery-progress')).toBeNull()
    none.unmount()
    const fix = card(fold(created(), executed(2), check(3, 1, 10)))
    expect(screen.queryByTestId('recovery-progress')).toBeNull()
    fix.unmount()
    expect(only(fold(created('recovery'), check(3, 1, 10, { proposal_id: 'other' }))).recovery).toBeUndefined()
  })
})

describe('preview count', () => {
  const previewed = (count?: unknown, unit?: unknown) =>
    ev(2, 'proposal.previewed', {
      proposal_id: 'p_1',
      at: seqAt(2),
      steps: [{ step_index: 0, preview: { target: 'public.orders', current: 'missing', expected: 'present', reversible: false, ...(count === undefined ? {} : { count, unit }) } }],
    })

  it('says "Will re-emit 412 rows" for a recovery step', () => {
    render(<BlastRadius proposal={only(fold(created('recovery'), previewed(412, 'rows')))} topology={null} />)
    expect(screen.getByTestId('preview-count-0')).toHaveTextContent('Will re-emit 412 rows')
  })

  it('a fix preview (no count) is unchanged', () => {
    render(<BlastRadius proposal={only(fold(created(), previewed()))} topology={null} />)
    expect(screen.queryByTestId('preview-count-0')).toBeNull()
    expect(screen.queryByText(/re-emit/)).toBeNull()
  })

  it('a count that is not a number is ignored', () => {
    render(<BlastRadius proposal={only(fold(created('recovery'), previewed('<img src=x onerror=alert(1)>', 'rows')))} topology={null} />)
    expect(screen.queryByTestId('preview-count-0')).toBeNull()
    expect(document.querySelector('img')).toBeNull()
  })
})

describe('class folding (TC3)', () => {
  it('a fix keeps no class and no recovery state, so it folds as before', () => {
    const s = fold(created(), executed(2))
    expect(only(s)).not.toHaveProperty('class')
    expect(only(s)).not.toHaveProperty('recovery')
    expect(only(fold(created('fix'))).class).toBeUndefined()
  })
  it('recovery and performance are kept', () => {
    expect(only(fold(created('recovery'))).class).toBe('recovery')
    expect(only(fold(created('performance'))).class).toBe('performance')
  })
  it('an unknown class from a newer server is treated as a fix', () => {
    expect(only(fold(created('teleport'))).class).toBeUndefined()
  })
})

describe('hostile text (E3)', () => {
  it('renders the verdict and a hostile outcome as text, never markup', () => {
    const evil = `<img src=x onerror=alert(1)>\u0007${'A'.repeat(10_000)}`
    card(fold(created('recovery'), executed(2), verdict(3, { outcome: 'partial', before: 3, after: 1, unit: '<img src=x>', verdict: evil })))
    expect(document.querySelector('img')).toBeNull()
    expect(screen.getByTestId('recovery-result')).toHaveTextContent('3 missing → 1')
    expect(screen.getByTestId('recovery-result')).not.toHaveTextContent('<img')
  })
  it('an outcome word outside the contract falls back to the plain verification card', () => {
    card(fold(created('recovery'), executed(2), verdict(3, { outcome: '<b>won</b>', verdict: 'NOT recovered' })))
    expect(screen.queryByTestId('recovery-result')).toBeNull()
    expect(screen.getByText('Verified · not recovered')).toBeInTheDocument()
  })
  it('a non-numeric progress payload never throws', () => {
    const s = fold(created('recovery'), executed(2), check(3, 1, 0, { before: 'many', after: {}, poll: '2', max_polls: null, unit: 7 }))
    expect(() => card(s)).not.toThrow()
    expect(screen.getByTestId('recovery-progress')).toHaveTextContent('checking')
  })
})
