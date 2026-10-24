import { describe, expect, it, vi } from 'vitest'
import { render, screen, within } from '@testing-library/react'
import { ProposalsPanel } from './ProposalsPanel'
import { PresencePanel } from './PresencePanel'
import type { RoomActions } from '../hooks/useRoom'
import type { CurrentUser } from '../state/permissions'
import { emptyRoomState } from '../state/types'
import type { ProposalState } from '../state/types'

// Q8k: the mysql layer (a source_kind: mysql pipeline's source) shows where layers render: the proposal's step and
// approval rows, and the owner chips of the people list. It reads "MySQL" in its own colour, not as postgres.

vi.mock('sonner', () => ({
  toast: Object.assign(vi.fn(), { warning: vi.fn(), dismiss: vi.fn() }),
}))

const actions: RoomActions = {
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

const proposal: ProposalState = {
  proposal_id: 'p1',
  title: 'Raise binlog retention to 7 days, then restart the connector',
  steps: [
    {
      action: 'set_source_setting',
      layer: 'mysql',
      params: { setting: 'binlog_expire_logs_seconds', value: { from: '86400', to: '604800' } },
      description: 'Keep binlogs for 7 days.',
    },
    {
      action: 'restart_connector_task',
      layer: 'kafka-connect',
      params: { connector: 'mysql-orders', task: 0 },
      description: 'Restart the task.',
    },
  ],
  risk: 'low',
  rollback: 'set binlog_expire_logs_seconds back to 86400',
  required_layers: ['mysql', 'kafka-connect'],
  approvals: { mysql: 'pending', 'kafka-connect': 'pending' },
  status: 'open',
  decisions: {},
  params_hash: '9f2c1e7ab04d58c3d6e1f0a7b92c4d835e6a1f0b7c3d9e2a48b5c6d7e8f90a1b',
  executed: [],
}

const priya: CurrentUser = { id: 'priya', name: 'Priya', layers: ['mysql'] }

describe('mysql layer in the proposal', () => {
  it('badges the step and the approval row as MySQL in the mysql colour', () => {
    render(
      <ProposalsPanel state={{ ...emptyRoomState('r1'), pipeline_id: 'shop', proposals: [proposal] }} user={priya} actions={actions} stale={null} />,
    )
    const steps = screen.getAllByRole('listitem').filter((li) => li.querySelector('[data-testid="step-params"]'))
    const stepBadge = within(steps[0]).getByText('MySQL')
    expect(stepBadge.className).toContain('text-layer-mysql')
    expect(within(steps[1]).getByText('kafka-connect')).toBeInTheDocument()

    const row = document.querySelector('[data-layer="mysql"]') as HTMLElement
    expect(row).not.toBeNull()
    expect(within(row).getByText('MySQL').className).toContain('text-layer-mysql')
    // The owner of mysql decides it.
    expect(within(row).getByRole('button', { name: /Approve/ })).toBeInTheDocument()
  })
})

describe('mysql layer in the people list', () => {
  it('shows a MySQL owner chip', () => {
    render(
      <PresencePanel
        members={[{ id: 'priya', name: 'Priya', role: 'member' }]}
        presence={[{ id: 'priya', name: 'Priya', state: 'viewing' }]}
        people={[{ id: 'priya', name: 'Priya', layers: ['mysql', 'postgres'] }]}
      />,
    )
    const chip = screen.getByLabelText('owns MySQL')
    expect(chip).toHaveTextContent('MySQL')
    expect(chip.className).toContain('text-layer-mysql')
    // Postgres keeps its id and colour.
    expect(screen.getByLabelText('owns postgres').className).toContain('text-layer-postgres')
  })
})
