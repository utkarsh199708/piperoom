import { describe, expect, it, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import { ProposalsPanel } from './ProposalsPanel'
import type { RoomActions } from '../hooks/useRoom'
import type { CurrentUser } from '../state/permissions'
import { emptyRoomState } from '../state/types'
import type { ProposalState } from '../state/types'

const actions: RoomActions = {
  comment: vi.fn(),
  steer: vi.fn(),
  sendSuggestion: vi.fn(),
  requestControl: vi.fn(),
  grantControl: vi.fn(),
  close: vi.fn(),
  approve: vi.fn(),
  reject: vi.fn(),
  handoff: vi.fn(),
  acceptHandoff: vi.fn(),
  trace: vi.fn(),
}

const proposal: ProposalState = {
  proposal_id: 'p1',
  title: 'Fix the subject compatibility',
  steps: [],
  risk: 'low',
  rollback: 'undo',
  required_layers: ['schema-registry', 'warehouse'],
  approvals: { 'schema-registry': 'pending', warehouse: 'pending' },
  status: 'open',
  executed: [],
}

const arjun: CurrentUser = { id: 'arjun', name: 'Arjun', layers: ['kafka', 'schema-registry'] }
const meera: CurrentUser = { id: 'meera', name: 'Meera', layers: ['warehouse'] }
const kabir: CurrentUser = { id: 'kabir', name: 'Kabir', layers: ['consumer'] }

describe('ProposalsPanel approval gating', () => {
  it('shows Approve only on the layers the user owns', () => {
    const state = { ...emptyRoomState('r1'), proposals: [proposal] }
    render(<ProposalsPanel state={state} user={meera} actions={actions} />)
    // One Approve (warehouse) and one Reject (warehouse) visible.
    expect(screen.getAllByRole('button', { name: 'Approve' })).toHaveLength(1)
    expect(screen.getAllByRole('button', { name: 'Reject' })).toHaveLength(1)
  })

  it('shows no Approve buttons for a user who owns none of the layers', () => {
    const state = { ...emptyRoomState('r1'), proposals: [proposal] }
    render(<ProposalsPanel state={state} user={kabir} actions={actions} />)
    expect(screen.queryByRole('button', { name: 'Approve' })).not.toBeInTheDocument()
    expect(screen.queryByRole('button', { name: 'Reject' })).not.toBeInTheDocument()
  })

  it('Arjun owns schema-registry only', () => {
    const state = { ...emptyRoomState('r1'), proposals: [proposal] }
    render(<ProposalsPanel state={state} user={arjun} actions={actions} />)
    expect(screen.getByText('schema-registry')).toBeInTheDocument()
    expect(screen.getByText('warehouse')).toBeInTheDocument()
    expect(screen.getAllByRole('button', { name: 'Approve' })).toHaveLength(1)
  })
})
