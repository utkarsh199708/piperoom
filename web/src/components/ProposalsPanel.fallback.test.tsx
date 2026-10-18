import { beforeEach, describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen, waitFor, within } from '@testing-library/react'
import { ProposalsPanel } from './ProposalsPanel'
import type { RoomActions } from '../hooks/useRoom'
import type { CurrentUser } from '../state/permissions'
import { emptyRoomState } from '../state/types'
import type { ProposalState, RoomState } from '../state/types'

// P2b-3: a layer nobody owns (`fallback_layers`) is approved by the room's driver. The row says
// "no owner for <layer> · driver approves"; the driver gets the buttons, everyone else a greyed row.

const approve = vi.fn()
const actions: RoomActions = {
  comment: vi.fn(),
  steer: vi.fn(),
  sendSuggestion: vi.fn(),
  declineSuggestion: vi.fn(),
  requestControl: vi.fn(),
  grantControl: vi.fn(),
  close: vi.fn(),
  approve,
  reject: vi.fn(),
  handoff: vi.fn(),
  acceptHandoff: vi.fn(),
  label: vi.fn(),
  checklist: vi.fn(),
  trace: vi.fn(),
  hypothesisVerdict: vi.fn(),
  nudge: vi.fn().mockResolvedValue({ ok: true }),
}

const HASH = 'abc123abc123def456abc123abc123def456abc123abc123def456abc123abc1'
// Meera drives and owns no required layer; Arjun owns kafka-connect and does not drive.
const meera: CurrentUser = { id: 'meera', name: 'Meera', layers: ['warehouse'] }
const arjun: CurrentUser = { id: 'arjun', name: 'Arjun', layers: ['kafka-connect'] }

const proposal: ProposalState = {
  proposal_id: 'p1',
  title: 'Restart the task',
  steps: [],
  risk: 'low',
  rollback: 'none',
  required_layers: ['kafka-connect', 'consumer'],
  approvals: { 'kafka-connect': 'pending', consumer: 'pending' },
  status: 'open',
  decisions: {},
  params_hash: HASH,
  executed: [],
  fallback_layers: ['consumer'],
}

function room(over: Partial<RoomState> = {}, p: ProposalState = proposal): RoomState {
  return {
    ...emptyRoomState('r1'),
    pipeline_id: 'orders',
    status: 'open',
    driver: { id: 'meera', name: 'Meera' },
    proposals: [p],
    ...over,
  }
}

const row = (layer: string) => document.querySelector<HTMLElement>(`[data-layer="${layer}"]`) as HTMLElement

beforeEach(() => {
  approve.mockReset()
})

describe('fallback approval row (P2b-3)', () => {
  it('the driver sees the fallback row naming the layer, with the buttons', async () => {
    approve.mockResolvedValue({ ok: true })
    render(<ProposalsPanel state={room()} user={meera} actions={actions} />)
    expect(screen.getByTestId('fallback-consumer')).toHaveTextContent('no owner for consumer · driver approves')
    const consumer = within(row('consumer'))
    expect(consumer.getByText('you')).toBeInTheDocument()
    fireEvent.click(consumer.getByRole('button', { name: 'Approve' }))
    await waitFor(() => expect(approve).toHaveBeenCalledWith('p1', 'consumer', { params_hash: HASH }))
    // The owned layer is not the driver's to decide.
    expect(within(row('kafka-connect')).queryByRole('button', { name: 'Approve' })).not.toBeInTheDocument()
    expect(screen.getByText(/Needs your approval · consumer/)).toBeInTheDocument()
  })

  it('a non-driver sees the row greyed with the driver named, and no buttons on it', () => {
    render(<ProposalsPanel state={room()} user={arjun} actions={actions} />)
    const consumer = within(row('consumer'))
    expect(consumer.getByTestId('fallback-consumer')).toHaveTextContent('no owner for consumer · driver approves')
    expect(consumer.getByText('Meera')).toBeInTheDocument()
    expect(consumer.queryByRole('button')).not.toBeInTheDocument()
    expect(row('consumer').className).not.toMatch(/bg-warning-subtle/)
    // Arjun still decides his own layer.
    expect(within(row('kafka-connect')).getByRole('button', { name: 'Approve' })).toBeInTheDocument()
  })

  it('with no driver the row says so, and the header waits on the driver, not an owner', () => {
    render(<ProposalsPanel state={room({ driver: null })} user={{ ...meera, layers: [] }} actions={actions} />)
    expect(within(row('consumer')).getByText('no driver yet')).toBeInTheDocument()
    expect(screen.getByText('waiting for kafka-connect owner and the driver · 0 of 2 approved')).toBeInTheDocument()
  })

  it('a closed room gives the driver no buttons on a fallback layer', () => {
    render(<ProposalsPanel state={room({ status: 'closed' })} user={meera} actions={actions} />)
    expect(screen.queryByRole('button', { name: 'Approve' })).not.toBeInTheDocument()
  })

  it('a decided fallback layer shows the decision like any other', () => {
    const decided: ProposalState = {
      ...proposal,
      approvals: { 'kafka-connect': 'pending', consumer: 'approved' },
      decisions: {
        consumer: {
          outcome: 'approved',
          by: { kind: 'human', id: 'meera', name: 'Meera' },
          identity: { method: 'dev', grant: 'fallback' },
          at: '2026-10-18T10:00:00Z',
        },
      },
    }
    render(<ProposalsPanel state={room({}, decided)} user={arjun} actions={actions} />)
    expect(screen.queryByTestId('fallback-consumer')).not.toBeInTheDocument()
    expect(screen.getByTestId('decision-consumer')).toHaveTextContent(/approved by Meera/)
  })

  it('a proposal without fallback layers renders as before', () => {
    const plain: ProposalState = { ...proposal, fallback_layers: undefined }
    render(<ProposalsPanel state={room({}, plain)} user={arjun} actions={actions} />)
    expect(screen.queryByTestId(/^fallback-/)).not.toBeInTheDocument()
    expect(screen.getByText('pending · your layer')).toBeInTheDocument()
  })

  it('without fallback layers the driver gains nothing and the header names the owners', () => {
    const plain: ProposalState = { ...proposal, fallback_layers: undefined }
    render(<ProposalsPanel state={room({}, plain)} user={meera} actions={actions} />)
    expect(screen.queryByRole('button', { name: 'Approve' })).not.toBeInTheDocument()
    expect(screen.getByText('waiting for kafka-connect, consumer owners · 0 of 2 approved')).toBeInTheDocument()
  })
})
