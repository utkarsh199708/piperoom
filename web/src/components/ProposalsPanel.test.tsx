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
  required_layers: ['schema-registry', 'kafka-connect'],
  approvals: { 'schema-registry': 'pending', 'kafka-connect': 'pending' },
  status: 'open',
  executed: [],
}

const arjun: CurrentUser = { id: 'arjun', name: 'Arjun', layers: ['kafka-connect', 'kafka'] }
const priya: CurrentUser = { id: 'priya', name: 'Priya', layers: ['postgres', 'schema-registry'] }
const meera: CurrentUser = { id: 'meera', name: 'Meera', layers: ['warehouse'] }
const kabir: CurrentUser = { id: 'kabir', name: 'Kabir', layers: ['consumer'] }

const proposalWithParams: ProposalState = {
  proposal_id: 'p2',
  title: 'Relax compatibility and restart a task',
  steps: [
    {
      action: 'schema_set_compatibility',
      layer: 'schema-registry',
      params: { subject: 'pg.public.orders-value', level: 'NONE' },
      description: 'Allow the new schema to register.',
    },
    {
      action: 'connect_restart_task',
      layer: 'kafka-connect',
      params: { connector: 'pg-orders-connector', task: 0 },
      description: 'Restart Debezium so it picks up the new schema.',
    },
  ],
  risk: 'low',
  rollback: 'undo',
  required_layers: ['schema-registry', 'kafka-connect'],
  approvals: { 'schema-registry': 'pending', 'kafka-connect': 'pending' },
  status: 'open',
  executed: [],
}

describe('ProposalsPanel approval gating', () => {
  it('shows an Approve and Reject pair on every required layer the user owns', () => {
    const state = { ...emptyRoomState('r1'), proposals: [proposal] }
    // Arjun owns kafka-connect and Priya owns schema-registry (dev/users.yaml).
    const arjunView = render(<ProposalsPanel state={state} user={arjun} actions={actions} />)
    expect(screen.getAllByRole('button', { name: 'Approve' })).toHaveLength(1)
    expect(screen.getAllByRole('button', { name: 'Reject' })).toHaveLength(1)
    arjunView.unmount()
    render(<ProposalsPanel state={state} user={priya} actions={actions} />)
    expect(screen.getAllByRole('button', { name: 'Approve' })).toHaveLength(1)
    expect(screen.getAllByRole('button', { name: 'Reject' })).toHaveLength(1)
  })

  it('shows no Approve buttons for a user who owns none of the layers', () => {
    const state = { ...emptyRoomState('r1'), proposals: [proposal] }
    render(<ProposalsPanel state={state} user={kabir} actions={actions} />)
    expect(screen.queryByRole('button', { name: 'Approve' })).not.toBeInTheDocument()
    expect(screen.queryByRole('button', { name: 'Reject' })).not.toBeInTheDocument()
  })

  it('renders every required layer even for a user who cannot decide', () => {
    const state = { ...emptyRoomState('r1'), proposals: [proposal] }
    render(<ProposalsPanel state={state} user={meera} actions={actions} />)
    expect(screen.getByText('schema-registry')).toBeInTheDocument()
    expect(screen.getByText('kafka-connect')).toBeInTheDocument()
    expect(screen.queryByRole('button', { name: 'Approve' })).not.toBeInTheDocument()
  })
})

describe('ProposalsPanel params rendering', () => {
  it('renders params for every step', () => {
    const state = { ...emptyRoomState('r1'), proposals: [proposalWithParams] }
    render(<ProposalsPanel state={state} user={arjun} actions={actions} />)
    // Step 1: schema_set_compatibility
    expect(screen.getByText('subject')).toBeInTheDocument()
    expect(screen.getByText('pg.public.orders-value')).toBeInTheDocument()
    expect(screen.getByText('level')).toBeInTheDocument()
    expect(screen.getByText('NONE')).toBeInTheDocument()
    // Step 2: connect_restart_task
    expect(screen.getByText('connector')).toBeInTheDocument()
    expect(screen.getByText('pg-orders-connector')).toBeInTheDocument()
    expect(screen.getByText('task')).toBeInTheDocument()
    expect(screen.getByText('0')).toBeInTheDocument()
  })

  it('highlights the connector, subject and task target fields', () => {
    const state = { ...emptyRoomState('r1'), proposals: [proposalWithParams] }
    render(<ProposalsPanel state={state} user={arjun} actions={actions} />)
    for (const key of ['connector', 'subject', 'task']) {
      expect(screen.getByText(key)).toHaveClass('proposal__param-key--target')
    }
    expect(screen.getByText('level')).not.toHaveClass('proposal__param-key--target')
  })

  it('renders gracefully when params is null', () => {
    const withNull = {
      ...proposalWithParams,
      steps: [
        {
          action: 'connect_restart_connector',
          layer: 'kafka-connect',
          params: null,
          description: 'This step has no parameters.',
        },
      ],
    }
    const state = { ...emptyRoomState('r1'), proposals: [withNull] }
    render(<ProposalsPanel state={state} user={arjun} actions={actions} />)
    expect(screen.getByText('no parameters')).toBeInTheDocument()
  })
})
