import { beforeEach, describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen, waitFor, within } from '@testing-library/react'
import { ProposalsPanel } from './ProposalsPanel'
import { StaleBanner, STALE_TEXT } from './StaleBanner'
import type { RoomActions } from '../hooks/useRoom'
import type { CurrentUser } from '../state/permissions'
import { emptyRoomState } from '../state/types'
import type { ProposalState } from '../state/types'

const approve = vi.fn()
const reject = vi.fn()
const actions: RoomActions = {
  comment: vi.fn(),
  steer: vi.fn(),
  sendSuggestion: vi.fn(),
  requestControl: vi.fn(),
  grantControl: vi.fn(),
  close: vi.fn(),
  approve,
  reject,
  handoff: vi.fn(),
  acceptHandoff: vi.fn(),
  trace: vi.fn(),
}

const H1 = '9f2c1e7ab04d58c3d6e1f0a7b92c4d835e6a1f0b7c3d9e2a48b5c6d7e8f90a1b'
const H2 = '1111111111112222222222223333333333334444444444445555555555556666'

const arjun: CurrentUser = { id: 'arjun', name: 'Arjun', layers: ['kafka-connect'] }
const soloUser: CurrentUser = { id: 'sam', name: 'Sam', layers: ['kafka-connect'], solo: true }

const proposal: ProposalState = {
  proposal_id: 'p1',
  title: 'Restart the task',
  steps: [],
  risk: 'low',
  rollback: 'none',
  required_layers: ['kafka-connect'],
  approvals: { 'kafka-connect': 'pending' },
  decisions: {},
  params_hash: H1,
  status: 'open',
  executed: [],
}
const roomWith = (p: ProposalState) => ({ ...emptyRoomState('r1'), pipeline_id: 'orders', proposals: [p] })

beforeEach(() => {
  approve.mockReset()
  reject.mockReset()
})

describe('params hash in the proposal card', () => {
  it('shows the short form, the full hash in a read-only field and a copy button', () => {
    render(<ProposalsPanel state={roomWith(proposal)} user={arjun} actions={actions} />)
    expect(screen.getByTestId('params-hash-short')).toHaveTextContent(H1.slice(0, 12))
    expect(screen.getByTestId('params-hash-short').textContent).toHaveLength(12)
    const full = screen.getByLabelText('Full params hash')
    expect(full).toHaveValue(H1)
    expect(full).toHaveAttribute('readonly')
    expect(full).toHaveClass('mono')
    expect(screen.getByRole('button', { name: 'Copy hash' })).toBeInTheDocument()
  })

  it('the copy button writes the full hash to the clipboard', async () => {
    const writeText = vi.fn().mockResolvedValue(undefined)
    Object.defineProperty(navigator, 'clipboard', { value: { writeText }, configurable: true })
    render(<ProposalsPanel state={roomWith(proposal)} user={arjun} actions={actions} />)
    fireEvent.click(screen.getByRole('button', { name: 'Copy hash' }))
    expect(writeText).toHaveBeenCalledWith(H1)
    expect(await screen.findByRole('button', { name: 'Copied' })).toBeInTheDocument()
  })

  it('with no hash: says so and disables Approve and Reject', () => {
    render(<ProposalsPanel state={roomWith({ ...proposal, params_hash: '' })} user={arjun} actions={actions} />)
    expect(screen.getByTestId('params-hash-missing')).toHaveTextContent(
      "This fix can't be approved or rejected here: its parameters could not be fingerprinted (see the server log).",
    )
    expect(screen.queryByTestId('params-hash-short')).not.toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Approve' })).toBeDisabled()
    expect(screen.getByRole('button', { name: 'Reject' })).toBeDisabled()
    fireEvent.click(screen.getByRole('button', { name: 'Approve' }))
    fireEvent.click(screen.getByRole('button', { name: 'Reject' }))
    expect(approve).not.toHaveBeenCalled()
    expect(screen.queryByLabelText('Rejection reason for kafka-connect')).not.toBeInTheDocument()
  })

  it('with no hash, a solo user cannot open the confirm box', () => {
    render(<ProposalsPanel state={roomWith({ ...proposal, params_hash: '' })} user={soloUser} actions={actions} />)
    expect(screen.getByRole('button', { name: 'Approve' })).toBeDisabled()
    fireEvent.click(screen.getByRole('button', { name: 'Approve' }))
    expect(screen.queryByLabelText(/Solo mode: type/)).not.toBeInTheDocument()
  })
})

describe('approve and reject send the rendered hash', () => {
  it('Approve sends the hash shown on the card', async () => {
    approve.mockResolvedValue({ ok: true })
    render(<ProposalsPanel state={roomWith(proposal)} user={arjun} actions={actions} />)
    fireEvent.click(screen.getByRole('button', { name: 'Approve' }))
    await waitFor(() => expect(approve).toHaveBeenCalledWith('p1', 'kafka-connect', { params_hash: H1 }))
  })

  it('sends the hash of the version on screen, whichever that is', async () => {
    approve.mockResolvedValue({ ok: true })
    const { rerender } = render(<ProposalsPanel state={roomWith(proposal)} user={arjun} actions={actions} />)
    // The card is re-rendered with a new version of the proposal (after a reload).
    rerender(<ProposalsPanel state={roomWith({ ...proposal, params_hash: H2 })} user={arjun} actions={actions} />)
    expect(screen.getByLabelText('Full params hash')).toHaveValue(H2)
    fireEvent.click(screen.getByRole('button', { name: 'Approve' }))
    await waitFor(() => expect(approve).toHaveBeenCalledWith('p1', 'kafka-connect', { params_hash: H2 }))
  })

  it('Reject sends the rendered hash with the reason', () => {
    render(<ProposalsPanel state={roomWith(proposal)} user={arjun} actions={actions} />)
    fireEvent.click(screen.getByRole('button', { name: 'Reject' }))
    fireEvent.change(screen.getByLabelText('Rejection reason for kafka-connect'), {
      target: { value: 'wrong connector' },
    })
    fireEvent.click(screen.getByRole('button', { name: 'Confirm' }))
    expect(reject).toHaveBeenCalledWith('p1', 'kafka-connect', 'wrong connector', H1)
  })

  it('solo confirm still works and carries the hash with the typed text', async () => {
    approve.mockResolvedValue({ ok: true })
    render(<ProposalsPanel state={roomWith(proposal)} user={soloUser} actions={actions} />)
    fireEvent.click(screen.getByRole('button', { name: 'Approve' }))
    expect(approve).not.toHaveBeenCalled() // the typed confirm comes first
    fireEvent.change(screen.getByLabelText(/Solo mode: type orders\/kafka-connect to approve/), {
      target: { value: 'orders/kafka-connect' },
    })
    fireEvent.click(screen.getAllByRole('button', { name: 'Approve' }).at(-1) as HTMLElement)
    await waitFor(() =>
      expect(approve).toHaveBeenCalledWith('p1', 'kafka-connect', {
        params_hash: H1,
        confirm: 'orders/kafka-connect',
      }),
    )
    expect(approve).toHaveBeenCalledTimes(1)
  })

  it('a params_mismatch answer is not retried and closes the solo box', async () => {
    approve.mockResolvedValue({ ok: false, mismatch: true })
    render(<ProposalsPanel state={roomWith(proposal)} user={soloUser} actions={actions} />)
    fireEvent.click(screen.getByRole('button', { name: 'Approve' }))
    fireEvent.change(screen.getByLabelText(/Solo mode: type/), { target: { value: 'orders/kafka-connect' } })
    fireEvent.click(screen.getAllByRole('button', { name: 'Approve' }).at(-1) as HTMLElement)
    await waitFor(() => expect(approve).toHaveBeenCalledTimes(1))
    await waitFor(() => expect(screen.queryByLabelText(/Solo mode: type/)).not.toBeInTheDocument())
    expect(approve).toHaveBeenCalledTimes(1)
  })

  it('a params_mismatch answer to a plain approve is not retried', async () => {
    approve.mockResolvedValue({ ok: false, mismatch: true })
    render(<ProposalsPanel state={roomWith(proposal)} user={arjun} actions={actions} />)
    fireEvent.click(screen.getByRole('button', { name: 'Approve' }))
    await waitFor(() => expect(approve).toHaveBeenCalledTimes(1))
    expect(screen.queryByRole('alert')).not.toBeInTheDocument() // the banner belongs to the room screen
    await new Promise((r) => setTimeout(r, 20))
    expect(approve).toHaveBeenCalledTimes(1)
  })
})

describe('who decided, and how', () => {
  const decided: ProposalState = {
    ...proposal,
    approvals: { 'kafka-connect': 'approved' },
    decisions: {
      'kafka-connect': {
        outcome: 'approved',
        by: { kind: 'human', id: 'priya', name: 'priya' },
        identity: { iss: 'https://idp', sub: 'u-1', session_id: 's-1', method: 'oidc', grant: 'user:priya' },
        params_hash: H1,
      },
    },
  }

  it('shows approver, grant and method', () => {
    render(<ProposalsPanel state={roomWith(decided)} user={arjun} actions={actions} />)
    expect(screen.getByTestId('decision-kafka-connect')).toHaveTextContent(
      'approved by priya (user:priya, oidc)',
    )
  })

  it('shows a backup grant', () => {
    const backup: ProposalState = {
      ...decided,
      decisions: {
        'kafka-connect': {
          ...decided.decisions['kafka-connect'],
          identity: { method: 'oidc', grant: 'backup:group:sre' },
        },
      },
    }
    render(<ProposalsPanel state={roomWith(backup)} user={arjun} actions={actions} />)
    expect(screen.getByTestId('decision-kafka-connect')).toHaveTextContent(
      'approved by priya (backup:group:sre, oidc)',
    )
  })

  it('shows a rejection with its reason, and tolerates a decision without identity', () => {
    const rejected: ProposalState = {
      ...proposal,
      approvals: { 'kafka-connect': 'rejected' },
      status: 'rejected',
      decisions: {
        'kafka-connect': {
          outcome: 'rejected',
          by: { kind: 'human', id: 'priya', name: 'Priya' },
          reason: 'wrong subject',
        },
      },
    }
    render(<ProposalsPanel state={roomWith(rejected)} user={arjun} actions={actions} />)
    expect(screen.getByTestId('decision-kafka-connect')).toHaveTextContent('rejected by Priya: wrong subject')
  })
})

describe('executed steps and their hash', () => {
  const executed = (hash: string | undefined): ProposalState => ({
    ...proposal,
    approvals: { 'kafka-connect': 'approved' },
    status: 'approved',
    executed: [{ proposal_id: 'p1', step_index: 0, ok: true, output: 'done', params_hash: hash }],
  })

  it('shows the hash a step ran with and does not flag a match', () => {
    render(<ProposalsPanel state={roomWith(executed(H1))} user={arjun} actions={actions} />)
    expect(screen.getByTestId('exec-hash-0')).toHaveTextContent(H1.slice(0, 12))
    expect(screen.queryByText('hash differs from proposal')).not.toBeInTheDocument()
  })

  it('flags a step whose hash differs from the proposal', () => {
    render(<ProposalsPanel state={roomWith(executed(H2))} user={arjun} actions={actions} />)
    expect(screen.getByTestId('exec-hash-0')).toHaveTextContent(H2.slice(0, 12))
    expect(screen.getByText('hash differs from proposal')).toBeInTheDocument()
  })

  it('shows no hash and no flag for a result from before the hash existed', () => {
    render(<ProposalsPanel state={roomWith(executed(undefined))} user={arjun} actions={actions} />)
    expect(screen.queryByTestId('exec-hash-0')).not.toBeInTheDocument()
    expect(screen.queryByText('hash differs from proposal')).not.toBeInTheDocument()
  })
})

describe('stale banner', () => {
  it('shows the stale text and its buttons work', () => {
    const onReload = vi.fn()
    const onDismiss = vi.fn()
    render(<StaleBanner onReload={onReload} onDismiss={onDismiss} />)
    const banner = screen.getByRole('alert')
    expect(banner).toHaveTextContent('This fix changed since you loaded it. Reload to see the new version.')
    expect(STALE_TEXT).toBe('This fix changed since you loaded it. Reload to see the new version.')
    fireEvent.click(within(banner).getByRole('button', { name: 'Reload' }))
    fireEvent.click(within(banner).getByRole('button', { name: 'Dismiss' }))
    expect(onReload).toHaveBeenCalledTimes(1)
    expect(onDismiss).toHaveBeenCalledTimes(1)
  })
})
