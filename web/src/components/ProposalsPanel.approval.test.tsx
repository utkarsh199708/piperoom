import { beforeEach, describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen, within } from '@testing-library/react'
import { ProposalsPanel } from './ProposalsPanel'
import type { RoomActions } from '../hooks/useRoom'
import type { CurrentUser } from '../state/permissions'
import { emptyRoomState } from '../state/types'
import type { ProposalState } from '../state/types'

// The approval moment (ui-refresh spec section 7.8): ribbon, fingerprint row, stale line,
// approval states, keyboard order. Gating itself is covered by ProposalsPanel.test.tsx.

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

const HASH = '7f3a91c0d2e45b6a8c1f0e9d3b7a5c2e4f6081a9b3d5c7e9f1a2b4c6d8e0f4be2'

const arjun: CurrentUser = { id: 'arjun', name: 'Arjun', layers: ['kafka-connect', 'kafka'] }
const priya: CurrentUser = { id: 'priya', name: 'Priya', layers: ['postgres', 'schema-registry'] }
const viewer: CurrentUser = { ...arjun, role: 'viewer' }

// Priya has approved schema-registry already; kafka-connect (Arjun's layer) is pending.
const proposal: ProposalState = {
  proposal_id: 'p1',
  title: 'Set subject compatibility to NONE and restart the connector task',
  steps: [
    {
      action: 'schema_set_compatibility',
      layer: 'schema-registry',
      params: { subject: 'pg.public.orders-value', level: 'NONE' },
      description: 'Allow the new decimal schema to register.',
    },
    {
      action: 'connect_restart_task',
      layer: 'kafka-connect',
      params: { connector: 'pg-orders-connector', task: 0 },
      description: 'Restart Debezium task 0.',
    },
  ],
  risk: 'low',
  rollback: 'set compatibility back to BACKWARD',
  required_layers: ['schema-registry', 'kafka-connect'],
  approvals: { 'schema-registry': 'approved', 'kafka-connect': 'pending' },
  decisions: {},
  params_hash: HASH,
  status: 'open',
  executed: [],
}
const roomWith = (...ps: ProposalState[]) => ({ ...emptyRoomState('r1'), pipeline_id: 'orders', proposals: ps })

beforeEach(() => {
  approve.mockReset()
  reject.mockReset()
})

describe('ribbon', () => {
  it('shows "Needs your approval · <layer>" for the layer owner and marks the card', () => {
    const { container } = render(<ProposalsPanel state={roomWith(proposal)} user={arjun} actions={actions} />)
    expect(screen.getByText('Needs your approval · kafka-connect')).toBeInTheDocument()
    expect(container.querySelector('.proposal')).toHaveClass('proposal--mine')
    expect(screen.getByText('your layer')).toBeInTheDocument()
  })

  it('is absent for a user with nothing left to decide, and for a viewer', () => {
    const first = render(<ProposalsPanel state={roomWith(proposal)} user={priya} actions={actions} />)
    expect(screen.queryByText(/Needs your approval/)).not.toBeInTheDocument()
    expect(first.container.querySelector('.proposal')).not.toHaveClass('proposal--mine')
    first.unmount()
    const second = render(<ProposalsPanel state={roomWith(proposal)} user={viewer} actions={actions} />)
    expect(screen.queryByText(/Needs your approval/)).not.toBeInTheDocument()
    expect(second.container.querySelector('.proposal--mine')).toBeNull()
    expect(screen.queryByRole('button', { name: 'Approve' })).not.toBeInTheDocument()
    expect(screen.queryByRole('button', { name: 'Reject' })).not.toBeInTheDocument()
  })

  it('names the first pending layer that is mine', () => {
    const both: ProposalState = { ...proposal, approvals: { 'schema-registry': 'pending', 'kafka-connect': 'pending' } }
    render(<ProposalsPanel state={roomWith(both)} user={{ ...arjun, layers: ['kafka-connect', 'schema-registry'] }} actions={actions} />)
    expect(screen.getByText('Needs your approval · schema-registry')).toBeInTheDocument()
  })
})

describe('card content', () => {
  it('shows the risk chip, rollback, numbered steps with layer tags and the pending count', () => {
    const { container } = render(<ProposalsPanel state={roomWith(proposal)} user={arjun} actions={actions} />)
    expect(screen.getByText('risk low')).toHaveClass('chip--ok')
    expect(screen.getByText(/rollback: set compatibility back to BACKWARD/)).toBeInTheDocument()
    expect(container.querySelectorAll('.proposal__step-n')).toHaveLength(2)
    expect(container.querySelector('.layer-tag--schema-registry')).toHaveTextContent('schema-registry')
    expect(screen.getByText('1 of 2 approvals pending')).toBeInTheDocument()
  })

  it('marks target params with the target row modifier', () => {
    const { container } = render(<ProposalsPanel state={roomWith(proposal)} user={arjun} actions={actions} />)
    const rows = container.querySelectorAll('.proposal__param--target')
    expect(rows).toHaveLength(3) // subject, connector, task
  })

  it('shows the empty state', () => {
    render(<ProposalsPanel state={roomWith()} user={arjun} actions={actions} />)
    expect(screen.getByText('No fix proposed yet')).toBeInTheDocument()
  })

  it('shows what a waiting layer is waiting for', () => {
    render(<ProposalsPanel state={roomWith(proposal)} user={priya} actions={actions} />)
    expect(screen.getByText('waiting for kafka-connect owner')).toBeInTheDocument()
  })
})

describe('fingerprint row', () => {
  it('groups a 64-hex hash as "7f3a 91c0 … 4be2" and keeps the full hash in the title', () => {
    render(<ProposalsPanel state={roomWith(proposal)} user={arjun} actions={actions} />)
    expect(screen.getByText('Approving exactly this')).toBeInTheDocument()
    const fp = screen.getByTestId('params-hash-fingerprint')
    expect(fp).toHaveTextContent('7f3a 91c0 … 4be2')
    expect(fp).toHaveAttribute('title', HASH)
    expect(screen.getByRole('button', { name: 'Copy hash' })).toHaveTextContent('Copy')
  })

  it('is absent when the proposal has no hash', () => {
    render(<ProposalsPanel state={roomWith({ ...proposal, params_hash: '' })} user={arjun} actions={actions} />)
    expect(screen.queryByTestId('params-hash-fingerprint')).not.toBeInTheDocument()
    expect(screen.queryByText('Approving exactly this')).not.toBeInTheDocument()
  })

  it('shows the hash to a viewer too', () => {
    render(<ProposalsPanel state={roomWith(proposal)} user={viewer} actions={actions} />)
    expect(screen.getByTestId('params-hash-fingerprint')).toHaveTextContent('7f3a 91c0 … 4be2')
  })
})

describe('stale state', () => {
  it('disables Approve and Reject and shows a role=alert line with Reload', () => {
    const onReload = vi.fn()
    render(<ProposalsPanel state={roomWith(proposal)} user={arjun} actions={actions} stale onReload={onReload} />)
    const alert = screen.getByRole('alert')
    expect(alert).toHaveTextContent('This proposal changed since you loaded it. Reload to see the current steps.')
    expect(screen.getByRole('button', { name: 'Approve' })).toBeDisabled()
    expect(screen.getByRole('button', { name: 'Reject' })).toBeDisabled()
    fireEvent.click(within(alert).getByRole('button', { name: 'Reload' }))
    expect(onReload).toHaveBeenCalledTimes(1)
    fireEvent.click(screen.getByRole('button', { name: 'Approve' }))
    expect(approve).not.toHaveBeenCalled()
  })

  it('shows nothing when not stale', () => {
    render(<ProposalsPanel state={roomWith(proposal)} user={arjun} actions={actions} />)
    expect(screen.queryByRole('alert')).not.toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Approve' })).toBeEnabled()
  })

  it('does not show the stale line on a settled card', () => {
    const done: ProposalState = { ...proposal, status: 'approved', approvals: { 'schema-registry': 'approved', 'kafka-connect': 'approved' } }
    render(<ProposalsPanel state={roomWith(done)} user={arjun} actions={actions} stale />)
    expect(screen.queryByRole('alert')).not.toBeInTheDocument()
  })
})

describe('buttons', () => {
  it('Approve is the filled approve button and Reject the reject one', () => {
    render(<ProposalsPanel state={roomWith(proposal)} user={arjun} actions={actions} />)
    expect(screen.getByRole('button', { name: 'Approve' })).toHaveClass('btn', 'btn--small', 'btn--approve')
    expect(screen.getByRole('button', { name: 'Reject' })).toHaveClass('btn', 'btn--small', 'btn--reject')
  })

  it('keyboard order is Approve, Reject, then the Reason field and Confirm', () => {
    const { container } = render(<ProposalsPanel state={roomWith(proposal)} user={arjun} actions={actions} />)
    const order = () =>
      Array.from(container.querySelectorAll<HTMLElement>('button, input, summary'))
        .filter((el) => !el.closest('details:not([open])') || el.tagName === 'SUMMARY')
        .map((el) => el.getAttribute('aria-label') ?? el.textContent?.trim() ?? '')
    const before = order()
    expect(before.indexOf('Approve')).toBeGreaterThan(-1)
    expect(before.indexOf('Reject')).toBe(before.indexOf('Approve') + 1)
    fireEvent.click(screen.getByRole('button', { name: 'Reject' }))
    const after = order()
    const reason = after.indexOf('Rejection reason for kafka-connect')
    expect(reason).toBeGreaterThan(-1)
    expect(after[reason + 1]).toBe('Confirm')
    expect(screen.getByLabelText('Rejection reason for kafka-connect')).toHaveFocus()
  })
})

describe('approval states', () => {
  it('shows an approved layer with a check and who approved it', () => {
    const decided: ProposalState = {
      ...proposal,
      decisions: {
        'schema-registry': {
          outcome: 'approved',
          by: { kind: 'human', id: 'priya', name: 'Priya' },
          identity: { method: 'oidc', grant: 'user:priya' },
        },
      },
    }
    const { container } = render(<ProposalsPanel state={roomWith(decided)} user={arjun} actions={actions} />)
    expect(container.querySelector('.approval--approved')).not.toBeNull()
    expect(screen.getByTestId('decision-schema-registry')).toHaveTextContent('approved by Priya (user:priya, oidc)')
  })

  it('shows a rejected layer with its reason', () => {
    const rejected: ProposalState = {
      ...proposal,
      status: 'rejected',
      approvals: { 'schema-registry': 'rejected', 'kafka-connect': 'pending' },
      decisions: {
        'schema-registry': { outcome: 'rejected', by: { kind: 'human', id: 'priya', name: 'Priya' }, reason: 'wrong subject' },
      },
    }
    const { container } = render(<ProposalsPanel state={roomWith(rejected)} user={arjun} actions={actions} />)
    expect(container.querySelector('.approval--rejected')).not.toBeNull()
    expect(screen.getByTestId('decision-schema-registry')).toHaveTextContent('rejected by Priya: wrong subject')
    expect(screen.queryByRole('button', { name: 'Approve' })).not.toBeInTheDocument()
  })
})

describe('executed list, running bar and settled cards', () => {
  const approved: ProposalState = {
    ...proposal,
    status: 'approved',
    approvals: { 'schema-registry': 'approved', 'kafka-connect': 'approved' },
  }

  it('shows the running bar while fewer steps ran than the proposal has', () => {
    const running = { ...approved, executed: [{ proposal_id: 'p1', step_index: 0, ok: true, output: 'compat set' }] }
    const { container } = render(<ProposalsPanel state={roomWith(running)} user={arjun} actions={actions} />)
    expect(screen.getByText(/Running step 2 of 2/)).toBeInTheDocument()
    expect(container.querySelector('.proposal--ran')).toBeNull()
  })

  it('shows no running bar once everything ran, and marks the card', () => {
    const ran = {
      ...approved,
      executed: [
        { proposal_id: 'p1', step_index: 0, ok: true, output: 'compat set' },
        { proposal_id: 'p1', step_index: 1, ok: true, output: 'task restarted' },
      ],
    }
    const { container } = render(<ProposalsPanel state={roomWith(ran)} user={arjun} actions={actions} />)
    expect(screen.queryByText(/Running step/)).not.toBeInTheDocument()
    expect(container.querySelector('.proposal')).toHaveClass('proposal--ran')
    expect(container.querySelectorAll('.exec-ok')).toHaveLength(2)
  })

  it('shows a failed step as failed and no running bar', () => {
    const failed = { ...approved, executed: [{ proposal_id: 'p1', step_index: 0, ok: false, output: 'boom' }] }
    const { container } = render(<ProposalsPanel state={roomWith(failed)} user={arjun} actions={actions} />)
    expect(container.querySelector('.exec-fail')).toHaveTextContent('boom')
    expect(screen.queryByText(/Running step/)).not.toBeInTheDocument()
  })

  it('folds a settled card into <details> when there are several proposals, and keeps the open one open', () => {
    const open2 = { ...proposal, proposal_id: 'p2', title: 'Second fix' }
    const { container } = render(<ProposalsPanel state={roomWith(approved, open2)} user={arjun} actions={actions} />)
    const folds = container.querySelectorAll('details.proposal-fold')
    expect(folds).toHaveLength(1)
    expect(folds[0]).not.toHaveAttribute('open')
    expect(folds[0].querySelector('summary')).toHaveTextContent('approved')
    // The open card the user can act on is not inside a fold.
    expect(screen.getByRole('button', { name: 'Approve' }).closest('details.proposal-fold')).toBeNull()
  })

  it('does not fold a lone settled card', () => {
    const { container } = render(<ProposalsPanel state={roomWith(approved)} user={arjun} actions={actions} />)
    expect(container.querySelector('details.proposal-fold')).toBeNull()
  })
})
