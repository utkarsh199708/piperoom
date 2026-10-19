import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { act, fireEvent, render, screen } from '@testing-library/react'
import { ProposalsPanel } from './ProposalsPanel'
import type { CancelResult, RoomActions } from '../hooks/useRoom'
import type { CurrentUser } from '../state/permissions'
import { emptyRoomState } from '../state/types'
import type { ProposalState, RoomState } from '../state/types'
import { STALE_COUNTDOWN_GRACE_MS } from '../lib/countdown'

// P2c-3: the policy countdown at the head of a proposal card ("Runs in 0:42 · policy auto_approve
// · Cancel"), with fake timers.

const NOW = Date.parse('2026-10-19T10:00:00Z')
const RUN_AT = '2026-10-19T10:01:00Z' // 60 s after NOW
const cancelSchedule = vi.fn<(pid: string, reason?: string) => Promise<CancelResult>>()
const base: RoomActions = {
  comment: vi.fn(),
  steer: vi.fn(),
  sendSuggestion: vi.fn(),
  declineSuggestion: vi.fn(),
  requestControl: vi.fn(),
  grantControl: vi.fn(),
  close: vi.fn(),
  approve: vi.fn(),
  reject: vi.fn(),
  handoff: vi.fn(),
  acceptHandoff: vi.fn(),
  label: vi.fn(),
  checklist: vi.fn(),
  trace: vi.fn(),
  hypothesisVerdict: vi.fn(),
  nudge: vi.fn().mockResolvedValue({ ok: true }),
}
const actions: RoomActions = { ...base, cancelSchedule }

// Arjun owns consumer, one of the two layers; Lee owns none of them; Sam drives.
const arjun: CurrentUser = { id: 'arjun', name: 'Arjun', layers: ['consumer'] }
const lee: CurrentUser = { id: 'lee', name: 'Lee', layers: ['warehouse'] }
const sam: CurrentUser = { id: 'sam', name: 'Sam', layers: [] }

function proposal(over: Partial<ProposalState> = {}): ProposalState {
  return {
    proposal_id: 'p1',
    title: 'Restart the connector task',
    steps: [{ action: 'connect_restart_task', layer: 'kafka-connect', params: { connector: 'pg-orders' }, description: '' }],
    risk: 'low',
    rollback: '',
    required_layers: ['kafka-connect', 'consumer'],
    approvals: { 'kafka-connect': 'pending', consumer: 'pending' },
    decisions: {},
    params_hash: '9f2c1e7ab04d58c3d6e1f0a7b92c4d835e6a1f0b7c3d9e2a48b5c6d7e8f90a1b',
    status: 'open',
    executed: [],
    scheduled: { run_at: RUN_AT, cancel_until: RUN_AT, policy: 'auto_approve' },
    ...over,
  }
}
function room(p: ProposalState = proposal(), over: Partial<RoomState> = {}): RoomState {
  return {
    ...emptyRoomState('r1'),
    pipeline_id: 'orders',
    status: 'open',
    driver: { id: 'sam', name: 'Sam', lease_until: '2026-10-19T10:30:00Z' },
    proposals: [p],
    ...over,
  }
}
const show = (state: RoomState = room(), user: CurrentUser = arjun, acts: RoomActions = actions) =>
  render(<ProposalsPanel state={state} user={user} actions={acts} />)
const tick = (ms: number) => act(() => void vi.advanceTimersByTime(ms))
const time = () => screen.getByTestId('countdown-time')
const card = () => screen.getByTestId('policy-countdown')

beforeEach(() => {
  vi.useFakeTimers()
  vi.setSystemTime(NOW)
  cancelSchedule.mockReset()
})
afterEach(() => {
  vi.useRealTimers()
})

describe('countdown card', () => {
  it('ticks down once a second from run_at', () => {
    show()
    expect(card()).toHaveAttribute('data-phase', 'counting')
    expect(time()).toHaveTextContent('Runs in 1:00')
    expect(card()).toHaveTextContent('policy auto_approve')
    tick(1000)
    expect(time()).toHaveTextContent('Runs in 0:59')
    tick(17_000)
    expect(time()).toHaveTextContent('Runs in 0:42')
    tick(42_000)
    expect(time()).toHaveTextContent('Running any moment')
    expect(card()).toHaveAttribute('data-phase', 'due')
  })

  it('aria-live is polite only in the last 10 s', () => {
    show()
    expect(time()).toHaveAttribute('aria-live', 'off')
    tick(49_000) // 11 s left
    expect(time()).toHaveAttribute('aria-live', 'off')
    tick(1000) // 10 s left
    expect(time()).toHaveTextContent('Runs in 0:10')
    expect(time()).toHaveAttribute('aria-live', 'polite')
    tick(10_000) // due: nothing left to count
    expect(time()).toHaveAttribute('aria-live', 'off')
  })

  it('shows cancel_until as "the policy may run from <time>", never as a deadline', () => {
    show()
    const from = screen.getByTestId('countdown-from')
    expect(from.textContent).toMatch(/^The policy may run from .+ unless someone cancels\.$/)
    expect(from.querySelector('time')).toHaveAttribute('dateTime', RUN_AT)
    expect(card().textContent).not.toMatch(/until|deadline|cancel by|must/i)
  })

  it('cancel_until is what the "may run from" line shows, apart from run_at', () => {
    const later = '2026-10-19T10:01:30Z'
    show(room(proposal({ scheduled: { run_at: RUN_AT, cancel_until: later, policy: 'auto_approve' } })))
    expect(screen.getByTestId('countdown-from').querySelector('time')).toHaveAttribute('dateTime', later)
    expect(time()).toHaveTextContent('Runs in 1:00')
  })

  it('hides the countdown once run_at is more than 45 s past; the proposal stays approvable and Cancel stays', () => {
    show()
    tick(60_000 + STALE_COUNTDOWN_GRACE_MS)
    expect(card()).toHaveAttribute('data-phase', 'due')
    tick(1000)
    expect(card()).toHaveAttribute('data-phase', 'stale')
    expect(screen.queryByTestId('countdown-time')).not.toBeInTheDocument()
    expect(screen.queryByText(/Runs in/)).not.toBeInTheDocument()
    expect(card()).toHaveTextContent('The policy auto_approve has not run this fix · it waits for approval')
    expect(screen.getByRole('button', { name: 'Cancel the policy' })).toBeEnabled()
    // Manual approval is untouched: the owner's own layer row still offers Approve.
    expect(screen.getByRole('button', { name: 'Approve' })).toBeInTheDocument()
  })

  it('a card first drawn long after run_at (a reload) starts hidden', () => {
    vi.setSystemTime(Date.parse(RUN_AT) + 5 * 60_000)
    show()
    expect(card()).toHaveAttribute('data-phase', 'stale')
    expect(screen.getByRole('button', { name: 'Cancel the policy' })).toBeInTheDocument()
  })

  it('no countdown without a pending schedule, or on a closed room', () => {
    const { unmount } = show(room(proposal({ scheduled: undefined })))
    expect(screen.queryByTestId('policy-countdown')).not.toBeInTheDocument()
    unmount()
    show(room(proposal(), { status: 'closed' }))
    expect(screen.queryByTestId('policy-countdown')).not.toBeInTheDocument()
  })
})

describe('Cancel: who sees it', () => {
  it('an owner of one required layer and the driver see it', () => {
    const { unmount } = show(room(), arjun)
    expect(screen.getByRole('button', { name: 'Cancel' })).toBeInTheDocument()
    unmount()
    show(room(), sam)
    expect(screen.getByRole('button', { name: 'Cancel' })).toBeInTheDocument()
  })

  it('mode.solo may cancel without listed layers', () => {
    show(room(proposal(), { mode: { write: true, solo: true, auto_approve: ['connect_restart_task'] } }), lee)
    expect(screen.getByRole('button', { name: 'Cancel' })).toBeInTheDocument()
  })

  it('a viewer never sees it, not even as owner or driver; nor does a member who owns no layer', () => {
    const { unmount } = show(room(), { ...arjun, role: 'viewer' })
    expect(screen.queryByRole('button', { name: /Cancel/ })).not.toBeInTheDocument()
    expect(time()).toHaveTextContent('Runs in 1:00') // the countdown itself is for everyone
    unmount()
    const driverViewer = show(room(), { ...sam, role: 'viewer' })
    expect(screen.queryByRole('button', { name: /Cancel/ })).not.toBeInTheDocument()
    driverViewer.unmount()
    show(room(), lee)
    expect(screen.queryByRole('button', { name: /Cancel/ })).not.toBeInTheDocument()
    expect(screen.getByTestId('countdown-from')).toHaveTextContent('The driver or an owner of one of its layers can cancel.')
  })

  it('a host without cancelSchedule shows no button', () => {
    show(room(), arjun, base)
    expect(screen.queryByRole('button', { name: /Cancel/ })).not.toBeInTheDocument()
  })
})

describe('Cancel: what it does', () => {
  it('calls the route for this proposal and stays busy until proposal.cancelled arrives', async () => {
    cancelSchedule.mockResolvedValue({ ok: true })
    const view = show()
    fireEvent.click(screen.getByRole('button', { name: 'Cancel' }))
    expect(cancelSchedule).toHaveBeenCalledTimes(1)
    expect(cancelSchedule).toHaveBeenCalledWith('p1')
    await act(async () => {})
    expect(screen.getByRole('button', { name: 'Cancelling…' })).toBeDisabled()
    // The event clears the schedule from the fold: the card loses its countdown.
    view.rerender(<ProposalsPanel state={room(proposal({ scheduled: undefined }))} user={arjun} actions={actions} />)
    expect(screen.queryByTestId('policy-countdown')).not.toBeInTheDocument()
  })

  it('409 not_scheduled: says there was nothing to cancel and offers the button again', async () => {
    cancelSchedule.mockResolvedValue({ ok: false, notScheduled: true })
    show()
    fireEvent.click(screen.getByRole('button', { name: 'Cancel' }))
    await act(async () => {})
    expect(screen.getByRole('status')).toHaveTextContent('Nothing to cancel: the policy has started, or someone cancelled first.')
    expect(screen.getByRole('button', { name: 'Cancel' })).toBeEnabled()
  })

  it('any other failure re-enables the button (the room banner carries the text)', async () => {
    cancelSchedule.mockResolvedValue({ ok: false, error: 'room is closed' })
    show()
    fireEvent.click(screen.getByRole('button', { name: 'Cancel' }))
    await act(async () => {})
    expect(screen.getByRole('button', { name: 'Cancel' })).toBeEnabled()
    expect(screen.queryByRole('status')).not.toBeInTheDocument()
  })
})
