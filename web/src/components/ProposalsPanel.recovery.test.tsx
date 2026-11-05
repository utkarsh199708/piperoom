import { describe, expect, it, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import { ProposalsPanel } from './ProposalsPanel'
import type { RoomActions } from '../hooks/useRoom'
import type { CurrentUser } from '../state/permissions'
import { emptyRoomState } from '../state/types'
import type { ProposalState, RoomState } from '../state/types'

// X2d inside the real proposal card: the recovery pieces appear only for a recovery proposal, and a
// fix proposal renders the same markup whether or not the new optional fields exist on its state.

const actions = {
  comment: vi.fn(),
  steer: vi.fn(),
  sendSuggestion: vi.fn(),
  declineSuggestion: vi.fn(),
  requestControl: vi.fn(),
  grantControl: vi.fn(),
  close: vi.fn(),
  approve: vi.fn(),
  approveAll: vi.fn(),
  reject: vi.fn(),
  handoff: vi.fn(),
  acceptHandoff: vi.fn(),
  label: vi.fn(),
  checklist: vi.fn(),
  trace: vi.fn(),
  hypothesisVerdict: vi.fn(),
  nudge: vi.fn().mockResolvedValue({ ok: true }),
} as unknown as RoomActions
const arjun: CurrentUser = { id: 'arjun', name: 'Arjun', layers: ['postgres'], solo: true }
const people = [{ id: 'arjun', name: 'Arjun', layers: ['postgres'] }]

function proposal(over: Partial<ProposalState> = {}): ProposalState {
  return {
    proposal_id: 'p1',
    title: 'Re-snapshot missing orders',
    steps: [{ action: 'pg_resnapshot_rows', layer: 'postgres', params: {}, description: 'Re-emit the lost rows' }],
    risk: 'medium',
    rollback: 'none',
    required_layers: ['postgres'],
    approvals: { postgres: 'approved' },
    decisions: {},
    params_hash: 'abc123',
    status: 'approved',
    executed: [{ proposal_id: 'p1', step_index: 0, ok: true, output: 'signal sent' }],
    created_at: '2026-10-11T10:00:00Z',
    ...over,
  }
}

const state = (p: ProposalState): RoomState => ({ ...emptyRoomState('r1'), pipeline_id: 'orders', proposals: [p] })
const panel = (p: ProposalState) => <ProposalsPanel state={state(p)} user={arjun} actions={actions} people={people} presence={[]} />

function html(p: ProposalState): string {
  const { container, unmount } = render(panel(p))
  // React's useId counts up across renders; the ids are the only thing that differs between two renders.
  const out = container.innerHTML.replace(/_r_[0-9a-z]+_/g, '_r_')
  unmount()
  return out
}

const preview = {
  proposal_id: 'p1',
  at: '2026-10-11T10:00:05Z',
  steps: [{ step_index: 0, preview: { target: 'public.orders', current: 'missing', expected: 'present', reversible: false, count: 412, unit: 'rows' as const } }],
}

describe('recovery proposal in the card', () => {
  it('shows the preview count, then the progress, then the verdict', () => {
    const open = proposal({ class: 'recovery', status: 'open', approvals: { postgres: 'pending' }, executed: [], preview })
    const view = render(panel(open))
    expect(screen.getByText('Will re-emit 412 rows')).toBeInTheDocument()

    const checking = proposal({
      class: 'recovery',
      preview,
      recovery: { check: { proposal_id: 'p1', step_index: 0, poll: 2, max_polls: 8, before: 412, after: 120, unit: 'rows' } },
    })
    view.rerender(panel(checking))
    expect(screen.getByTestId('recovery-progress')).toHaveTextContent('checking (2 of 8)')

    const done = proposal({
      class: 'recovery',
      preview,
      recovery: { ran_at: '2026-10-11T10:00:10Z', verified_at: '2026-10-11T10:03:10Z' },
      verification: { proposal_id: 'p1', checks: [], recovered: true, verdict: 'recovered', outcome: 'recovered', before: 412, after: 0, unit: 'rows' },
    })
    view.rerender(panel(done))
    expect(screen.queryByTestId('recovery-progress')).toBeNull()
    expect(screen.getByTestId('recovery-result')).toHaveTextContent('412 missing → 0 rows · recovered in 3 min')
  })
})

describe('fix proposal in the card (TC3)', () => {
  it('renders identically with or without the X2 fields, and shows no recovery element', () => {
    const fix = proposal({
      steps: [{ action: 'connect_restart_task', layer: 'postgres', params: {}, description: 'restart' }],
      verification: { proposal_id: 'p1', checks: [], recovered: true, verdict: 'recovered: ok' },
    })
    const plain = html(fix)
    expect(plain).toContain('Verified · recovered')
    expect(plain).not.toMatch(/recovery-(progress|result)|Will re-emit/)
    expect(html({ ...fix, class: undefined, recovery: undefined })).toBe(plain)
  })
})
