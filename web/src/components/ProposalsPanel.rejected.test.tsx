import { beforeEach, describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen, within } from '@testing-library/react'
import { ProposalsPanel } from './ProposalsPanel'
import type { RoomActions } from '../hooks/useRoom'
import type { CurrentUser } from '../state/permissions'
import { emptyRoomState } from '../state/types'
import type { ProposalState, RoomState } from '../state/types'

// KAN-393: one rejection rejects the whole proposal (the server writes one `approval.rejected` and
// sets the proposal `rejected`). The card must not keep the other layers "Waiting for ..." with a
// Nudge, and must not offer Approve: those layers were cancelled, not left pending.

const reject = vi.fn()
const approveAll = vi.fn()
const actions = {
  comment: vi.fn(),
  steer: vi.fn(),
  sendSuggestion: vi.fn(),
  declineSuggestion: vi.fn(),
  requestControl: vi.fn(),
  grantControl: vi.fn(),
  close: vi.fn(),
  approve: vi.fn(),
  approveAll,
  reject,
  handoff: vi.fn(),
  acceptHandoff: vi.fn(),
  label: vi.fn(),
  checklist: vi.fn(),
  trace: vi.fn(),
  hypothesisVerdict: vi.fn(),
  nudge: vi.fn().mockResolvedValue({ ok: true }),
} as unknown as RoomActions

const HASH = '9f2c1e7ab04d58c3d6e1f0a7b92c4d835e6a1f0b7c3d9e2a48b5c6d7e8f90a1b'
const REASON = 'wrong compatibility, this breaks the next consumer'

// Solo: Arjun may decide every layer, so the card offers one "Approve and run" plus its Reject.
const arjun: CurrentUser = { id: 'arjun', name: 'Arjun', layers: ['schema-registry', 'kafka-connect'], solo: true }
const people = [{ id: 'arjun', name: 'Arjun', layers: ['kafka-connect'] }]

function proposal(over: Partial<ProposalState> = {}): ProposalState {
  return {
    proposal_id: 'p1',
    title: 'Set subject compatibility to NONE, then restart the connector task',
    steps: [],
    risk: 'low',
    rollback: 'set compatibility back to BACKWARD',
    required_layers: ['schema-registry', 'kafka-connect'],
    approvals: { 'schema-registry': 'pending', 'kafka-connect': 'pending' },
    decisions: {},
    params_hash: HASH,
    status: 'open',
    executed: [],
    ...over,
  }
}

const rejected: ProposalState = proposal({
  status: 'rejected',
  approvals: { 'schema-registry': 'rejected', 'kafka-connect': 'pending' },
  decisions: {
    'schema-registry': {
      outcome: 'rejected',
      reason: REASON,
      by: { kind: 'human', id: 'arjun', name: 'Arjun' },
      params_hash: HASH,
    },
  },
})

function room(p: ProposalState): RoomState {
  return { ...emptyRoomState('r1'), pipeline_id: 'orders', proposals: [p] }
}

const show = (p: ProposalState, user: CurrentUser = arjun) =>
  render(<ProposalsPanel state={room(p)} user={user} actions={actions} people={people} presence={[]} />)

beforeEach(() => {
  reject.mockReset()
})

describe('a rejected proposal in solo mode', () => {
  it('one Reject with a reason sends one per-layer call, then every layer reads decided or cancelled', () => {
    const { rerender } = show(proposal())

    // The solo card's one Reject: the reason is typed once and sent with the hash on screen.
    fireEvent.click(screen.getByRole('button', { name: 'Reject' }))
    const reason = screen.getByLabelText('Rejection reason')
    fireEvent.change(reason, { target: { value: REASON } })
    fireEvent.click(screen.getByRole('button', { name: 'Confirm' }))
    expect(reject).toHaveBeenCalledTimes(1)
    expect(reject).toHaveBeenCalledWith('p1', 'schema-registry', REASON, HASH)

    // What the server's approval.rejected leaves behind: the proposal rejected, the other layer
    // still pending in the log. The card must call that cancelled, not waiting.
    rerender(<ProposalsPanel state={room(rejected)} user={arjun} actions={actions} people={people} presence={[]} />)

    const schema = document.querySelector('[data-layer="schema-registry"]') as HTMLElement
    const connect = document.querySelector('[data-layer="kafka-connect"]') as HTMLElement
    expect(schema).toHaveAttribute('data-state', 'rejected')
    expect(within(schema).getByTestId('decision-schema-registry')).toHaveTextContent(
      `rejected by Arjun: ${REASON}`,
    )
    expect(schema).toHaveTextContent(`Reason: ${REASON}`)
    expect(connect).toHaveAttribute('data-state', 'pending')
    expect(within(connect).getByTestId('cancelled-kafka-connect')).toHaveTextContent('Cancelled')
  })

  it('offers no Nudge and no Approve on a rejected proposal', () => {
    show(rejected)
    expect(screen.queryByRole('button', { name: /^Nudge/ })).not.toBeInTheDocument()
    expect(screen.queryByRole('button', { name: /^Approve/ })).not.toBeInTheDocument()
    expect(screen.queryByRole('button', { name: /^Reject/ })).not.toBeInTheDocument()
    expect(screen.queryByText(/Waiting for/)).not.toBeInTheDocument()
    expect(screen.getByText('rejected')).toBeInTheDocument()
  })

  it('keeps a pending layer of an open proposal waiting, with its Nudge (unchanged)', () => {
    const meera: CurrentUser = { id: 'meera', name: 'Meera', layers: ['schema-registry'] }
    show(proposal(), meera)
    expect(screen.getByText(/Waiting for/)).toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Nudge kafka-connect owners' })).toBeInTheDocument()
  })
})
