import { beforeEach, describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen, waitFor, within } from '@testing-library/react'
import { ProposalsPanel } from './ProposalsPanel'
import { ProposalsSummary } from './room/ProposalsSummary'
import type { RoomActions } from '../hooks/useRoom'
import type { CurrentUser } from '../state/permissions'
import { applyEvent } from '../state/reducer'
import { emptyRoomState } from '../state/types'
import type { ProposalState, RoomState } from '../state/types'
import type { Event } from '../api/types'

// Q0 (QA G8, G9): a closed room waits on nobody, and the driver can withdraw an open proposal.
const withdraw = vi.fn()
const actions = {
  comment: vi.fn(), steer: vi.fn(), sendSuggestion: vi.fn(), requestControl: vi.fn(), grantControl: vi.fn(),
  close: vi.fn(), approve: vi.fn(), reject: vi.fn(), handoff: vi.fn(), acceptHandoff: vi.fn(), trace: vi.fn(),
  hypothesisVerdict: vi.fn(), nudge: vi.fn(), withdraw,
} as unknown as RoomActions

const proposal: ProposalState = {
  proposal_id: 'p1',
  title: 'Restart the connector task',
  steps: [{ action: 'connect_restart_task', layer: 'kafka-connect', params: { connector: 'pg-orders', task: 0 }, description: 'Restart it.' }],
  risk: 'low',
  rollback: 'undo',
  required_layers: ['kafka-connect'],
  approvals: { 'kafka-connect': 'pending' },
  status: 'open',
  decisions: {},
  params_hash: 'abc123abc123',
  executed: [],
}
const kabir: CurrentUser = { id: 'kabir', name: 'Kabir', layers: ['consumer'], role: 'member' } // the driver
const arjun: CurrentUser = { id: 'arjun', name: 'Arjun', layers: ['kafka-connect'], role: 'member' } // owns the layer
const vera: CurrentUser = { id: 'vera', name: 'Vera', layers: [], role: 'viewer' }

const room = (over: Partial<RoomState> = {}): RoomState => ({
  ...emptyRoomState('r_1'),
  driver: { id: 'kabir', name: 'Kabir' },
  proposals: [proposal],
  ...over,
})
const withdrawn = (over: Partial<ProposalState> = {}): ProposalState => ({
  ...proposal,
  status: 'withdrawn',
  withdrawn: { reason: 'wrong connector', by: { kind: 'human', id: 'kabir', name: 'Kabir' }, at: '2026-10-15T10:00:00Z' },
  ...over,
})

beforeEach(() => {
  vi.clearAllMocks()
  withdraw.mockResolvedValue({ ok: true })
})

describe('Withdraw (driver)', () => {
  it('is offered to the driver on an open proposal, and to nobody else', () => {
    const d = render(<ProposalsPanel state={room()} user={kabir} actions={actions} />)
    expect(screen.getByRole('button', { name: 'Withdraw' })).toBeInTheDocument()
    d.unmount()
    const o = render(<ProposalsPanel state={room()} user={arjun} actions={actions} />)
    expect(screen.queryByRole('button', { name: 'Withdraw' })).not.toBeInTheDocument()
    o.unmount()
    render(<ProposalsPanel state={room()} user={{ ...vera, id: 'kabir' }} actions={actions} />)
    expect(screen.queryByRole('button', { name: 'Withdraw' })).not.toBeInTheDocument()
  })

  it('is not offered on a proposal that was decided, withdrawn already, or in a closed room', () => {
    const approved: ProposalState = { ...proposal, status: 'approved', approvals: { 'kafka-connect': 'approved' } }
    const a = render(<ProposalsPanel state={room({ proposals: [approved] })} user={kabir} actions={actions} />)
    expect(screen.queryByRole('button', { name: 'Withdraw' })).not.toBeInTheDocument()
    a.unmount()
    const w = render(<ProposalsPanel state={room({ proposals: [withdrawn()] })} user={kabir} actions={actions} />)
    expect(screen.queryByRole('button', { name: 'Withdraw' })).not.toBeInTheDocument()
    w.unmount()
    render(<ProposalsPanel state={room({ status: 'closed' })} user={kabir} actions={actions} />)
    expect(screen.queryByRole('button', { name: 'Withdraw' })).not.toBeInTheDocument()
  })

  it('needs an actions.withdraw to be offered at all', () => {
    const bare = { ...actions, withdraw: undefined } as unknown as RoomActions
    render(<ProposalsPanel state={room()} user={kabir} actions={bare} />)
    expect(screen.queryByRole('button', { name: 'Withdraw' })).not.toBeInTheDocument()
  })

  it('asks for an optional reason first, then sends it', async () => {
    render(<ProposalsPanel state={room()} user={kabir} actions={actions} />)
    fireEvent.click(screen.getByRole('button', { name: 'Withdraw' }))
    expect(withdraw).not.toHaveBeenCalled()
    fireEvent.change(screen.getByLabelText('Reason for withdrawing this fix (optional)'), { target: { value: '  wrong connector ' } })
    fireEvent.click(screen.getByRole('button', { name: 'Withdraw fix' }))
    await waitFor(() => expect(withdraw).toHaveBeenCalledWith('p1', 'wrong connector'))
    // Done: the form is gone, and so is the button until the event that follows turns the card into the
    // withdrawn one (a second click would only earn a 409).
    await waitFor(() => expect(screen.queryByRole('button', { name: 'Withdraw fix' })).not.toBeInTheDocument())
    expect(screen.queryByRole('button', { name: 'Withdraw' })).not.toBeInTheDocument()
  })

  it('works with no reason', async () => {
    render(<ProposalsPanel state={room()} user={kabir} actions={actions} />)
    fireEvent.click(screen.getByRole('button', { name: 'Withdraw' }))
    fireEvent.click(screen.getByRole('button', { name: 'Withdraw fix' }))
    await waitFor(() => expect(withdraw).toHaveBeenCalledWith('p1', undefined))
  })

  it('caps the reason at the 500 characters the server takes', () => {
    render(<ProposalsPanel state={room()} user={kabir} actions={actions} />)
    fireEvent.click(screen.getByRole('button', { name: 'Withdraw' }))
    expect(screen.getByLabelText('Reason for withdrawing this fix (optional)')).toHaveAttribute('maxlength', '500')
  })

  it('Cancel and Escape close the form without a call', () => {
    render(<ProposalsPanel state={room()} user={kabir} actions={actions} />)
    fireEvent.click(screen.getByRole('button', { name: 'Withdraw' }))
    fireEvent.click(screen.getByRole('button', { name: 'Cancel' }))
    expect(screen.getByRole('button', { name: 'Withdraw' })).toBeInTheDocument()
    fireEvent.click(screen.getByRole('button', { name: 'Withdraw' }))
    fireEvent.keyDown(screen.getByLabelText('Reason for withdrawing this fix (optional)'), { key: 'Escape' })
    expect(screen.getByRole('button', { name: 'Withdraw' })).toBeInTheDocument()
    expect(withdraw).not.toHaveBeenCalled()
  })

  it('Cancel puts the focus back on the Withdraw button', async () => {
    render(<ProposalsPanel state={room()} user={kabir} actions={actions} />)
    fireEvent.click(screen.getByRole('button', { name: 'Withdraw' }))
    fireEvent.click(screen.getByRole('button', { name: 'Cancel' }))
    await waitFor(() => expect(screen.getByRole('button', { name: 'Withdraw' })).toHaveFocus())
  })

  it('shows the server\'s refusal under the form and keeps the form open', async () => {
    withdraw.mockResolvedValue({ ok: false, error: 'This fix is already approved or running, so it can no longer be withdrawn' })
    render(<ProposalsPanel state={room()} user={kabir} actions={actions} />)
    fireEvent.click(screen.getByRole('button', { name: 'Withdraw' }))
    fireEvent.click(screen.getByRole('button', { name: 'Withdraw fix' }))
    const alert = await screen.findByRole('alert')
    expect(alert).toHaveTextContent('already approved or running')
    expect(screen.getByRole('button', { name: 'Withdraw fix' })).toBeEnabled()
  })

  it('does not send twice while the call is out', async () => {
    let finish: (r: { ok: boolean }) => void = () => undefined
    withdraw.mockReturnValue(new Promise((resolve) => (finish = resolve)))
    render(<ProposalsPanel state={room()} user={kabir} actions={actions} />)
    fireEvent.click(screen.getByRole('button', { name: 'Withdraw' }))
    fireEvent.click(screen.getByRole('button', { name: 'Withdraw fix' }))
    expect(await screen.findByRole('button', { name: 'Withdrawing…' })).toBeDisabled()
    fireEvent.click(screen.getByRole('button', { name: 'Withdrawing…' }))
    expect(withdraw).toHaveBeenCalledTimes(1)
    finish({ ok: true })
  })
})

describe('a withdrawn proposal', () => {
  it('reads "Withdrawn — <reason>" with who wrote it and offers no decision', () => {
    render(<ProposalsPanel state={room({ proposals: [withdrawn()] })} user={arjun} actions={actions} />)
    const note = screen.getByTestId('withdrawn-note')
    expect(note).toHaveTextContent('Withdrawn — wrong connector')
    expect(note).toHaveTextContent('by Kabir')
    expect(screen.queryByRole('button', { name: 'Approve' })).not.toBeInTheDocument()
    expect(screen.queryByRole('button', { name: 'Reject' })).not.toBeInTheDocument()
    expect(screen.queryByText(/Needs your approval/)).not.toBeInTheDocument()
    // The layer it was waiting on is not "pending" any more, and there is nobody to nudge.
    expect(screen.getByText('Not decided')).toBeInTheDocument()
    expect(screen.queryByText('Pending')).not.toBeInTheDocument()
    expect(screen.queryByRole('button', { name: /Nudge/ })).not.toBeInTheDocument()
    expect(screen.queryByText(/waiting for/i)).not.toBeInTheDocument()
  })

  it('says only "Withdrawn" when there is no reason, and names no one for a system withdrawal', () => {
    const p = withdrawn({ withdrawn: { reason: '', by: { kind: 'system', id: 'system', name: 'System' } } })
    render(<ProposalsPanel state={room({ proposals: [p] })} user={arjun} actions={actions} />)
    const note = screen.getByTestId('withdrawn-note')
    expect(note).toHaveTextContent(/^Withdrawn$/)
  })

  it('a layer that was decided keeps its decision', () => {
    const p = withdrawn({
      required_layers: ['kafka-connect', 'consumer'],
      approvals: { 'kafka-connect': 'approved', consumer: 'pending' },
      decisions: { 'kafka-connect': { outcome: 'approved', by: { kind: 'human', id: 'arjun', name: 'Arjun' } } },
    })
    render(<ProposalsPanel state={room({ proposals: [p] })} user={arjun} actions={actions} />)
    expect(screen.getByTestId('decision-kafka-connect')).toHaveTextContent('approved by Arjun')
    expect(screen.getByText('Not decided')).toBeInTheDocument()
  })
})

describe('a closed room', () => {
  const ev = (seq: number, type: Event['type'], payload: unknown, kind: 'human' | 'agent' = 'agent'): Event => ({
    room_id: 'r_1',
    seq,
    type,
    actor: kind === 'agent' ? { kind, id: 'agent', name: 'Agent' } : { kind, id: 'kabir', name: 'Kabir' },
    payload,
    at: `2026-10-15T10:00:${String(seq).padStart(2, '0')}Z`,
  })
  // The QA run: the room was closed while a proposal was open, and the log has no proposal.withdrawn.
  const closedOldLog = [
    ev(1, 'proposal.created', {
      proposal_id: 'p1', title: 'Restart the connector task', steps: [], risk: 'low', rollback: 'undo',
      required_layers: ['kafka-connect'], params_hash: 'abc123abc123',
    }),
    ev(2, 'room.closed', { resolution: 'fixed by hand' }, 'human'),
  ].reduce(applyEvent, emptyRoomState('r_1'))

  it('shows no pending approval and no Approve or Reject, even for the layer owner', () => {
    expect(closedOldLog.status).toBe('closed')
    render(<ProposalsPanel state={closedOldLog} user={arjun} actions={actions} />)
    expect(screen.queryByRole('button', { name: 'Approve' })).not.toBeInTheDocument()
    expect(screen.queryByRole('button', { name: 'Reject' })).not.toBeInTheDocument()
    expect(screen.queryByText(/waiting for/i)).not.toBeInTheDocument()
    expect(screen.queryByText('Pending')).not.toBeInTheDocument()
    expect(screen.getByTestId('withdrawn-note')).toHaveTextContent('Withdrawn — room closed')
  })

  it('the rail counts no open proposal', () => {
    render(<ProposalsSummary proposals={closedOldLog.proposals} />)
    expect(screen.queryByText(/open/)).not.toBeInTheDocument()
    expect(screen.getByText('Withdrawn')).toBeInTheDocument()
  })

  it('still no buttons if a state hands over an "open" proposal in a closed room', () => {
    // Defence in depth: whatever put it there, a closed room takes no decision.
    render(<ProposalsPanel state={room({ status: 'closed' })} user={arjun} actions={actions} />)
    expect(screen.queryByRole('button', { name: 'Approve' })).not.toBeInTheDocument()
    expect(screen.queryByRole('button', { name: 'Reject' })).not.toBeInTheDocument()
    expect(screen.queryByText(/waiting for/i)).not.toBeInTheDocument()
    expect(screen.getByText('Not decided')).toBeInTheDocument()
  })
})

describe('the rail', () => {
  it('lists a withdrawn proposal as Withdrawn and counts only the open ones', () => {
    const open: ProposalState = { ...proposal, proposal_id: 'p2', title: 'Second try' }
    render(<ProposalsSummary proposals={[withdrawn(), open]} />)
    const heading = screen.getByRole('heading', { name: /Proposals/ })
    expect(within(heading).getByText('1 open')).toBeInTheDocument()
    expect(screen.getByText('Withdrawn')).toBeInTheDocument()
    expect(screen.getByText('Waiting for kafka-connect')).toBeInTheDocument()
  })
})
