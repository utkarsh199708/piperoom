import { beforeEach, describe, expect, it, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import { DecisionBar } from './DecisionBar'
import { DecisionHandleContext } from '../shell/context'
import type { DecisionHandle } from '../shell/context'
import { emptyRoomState } from '../../state/types'
import type { ProposalState, RoomState } from '../../state/types'
import type { CurrentUser } from '../../state/permissions'

// KAN-560: room.closed leaves a proposal "open" in the log, so the bar must not keep asking for its
// approval ("Needs your approval", "Waiting for kafka-connect · 0 of 2 approved") once the room is
// closed. decisionFor answers null for a closed room, and the driver's quiet bar needs an open room
// too, so the bar renders nothing. These tests keep it that way.

const arjun: CurrentUser = { id: 'arjun', name: 'Arjun', layers: ['kafka-connect'] }
const sam: CurrentUser = { id: 'sam', name: 'Sam', layers: ['schema-registry', 'kafka-connect'], solo: true }
const kabir: CurrentUser = { id: 'kabir', name: 'Kabir', layers: ['consumer'] }
const step = (layer: string, action: string) => ({ action, layer, params: {}, description: '' })
const prop = (over: Partial<ProposalState> = {}): ProposalState => ({
  proposal_id: 'p1',
  title: 'Restart the task',
  steps: [step('schema-registry', 'set_compatibility'), step('kafka-connect', 'restart_connector_task')],
  risk: 'low',
  rollback: '',
  required_layers: ['schema-registry', 'kafka-connect'],
  approvals: { 'schema-registry': 'pending', 'kafka-connect': 'pending' },
  decisions: {},
  params_hash: 'h',
  status: 'open',
  executed: [],
  ...over,
})
const room = (over: Partial<RoomState> = {}): RoomState => ({
  ...emptyRoomState('r'),
  driver: { id: 'kabir', name: 'Kabir' },
  proposals: [prop()],
  ...over,
})
const handle: DecisionHandle = { approve: vi.fn(), reject: vi.fn(), scrollTo: vi.fn() }
const bar = (state: RoomState, user: CurrentUser, onProposeFix?: () => void) => (
  <DecisionHandleContext.Provider value={{ get: () => handle, set: () => undefined }}>
    <DecisionBar state={state} user={user} onProposeFix={onProposeFix} />
  </DecisionHandleContext.Provider>
)

beforeEach(() => {
  window.sessionStorage.clear()
})

describe('DecisionBar on a closed room with a proposal that never got all its approvals', () => {
  it.each([
    ['a layer owner', arjun],
    ['a solo user who could approve every layer', sam],
    ['the driver, who could propose a fix in an open room', kabir],
    ['a viewer', { ...arjun, role: 'viewer' as const }],
  ])('shows no bar to %s: no waiting text, no Approve, Reject or Nudge', (_who, user) => {
    const { container } = render(bar(room({ status: 'closed' }), user, vi.fn()))
    expect(container).toBeEmptyDOMElement()
    expect(screen.queryByRole('region', { name: 'Decision' })).not.toBeInTheDocument()
    expect(screen.queryByText(/waiting for/i)).not.toBeInTheDocument()
    expect(screen.queryByText(/needs your approval/i)).not.toBeInTheDocument()
    expect(screen.queryByRole('button', { name: /^(Approve|Reject|Nudge)/ })).not.toBeInTheDocument()
  })

  it('goes away when the room closes under it', () => {
    const { rerender } = render(bar(room(), arjun))
    expect(screen.getByRole('region', { name: 'Decision' })).toHaveTextContent('Needs your approval · kafka-connect')
    rerender(bar(room({ status: 'closed' }), arjun))
    expect(screen.queryByRole('region', { name: 'Decision' })).not.toBeInTheDocument()
    expect(screen.queryByRole('button', { name: /^(Approve|Reject)/ })).not.toBeInTheDocument()
  })
})

describe('DecisionBar on an open room with a pending proposal (unchanged)', () => {
  it('asks a layer owner to decide', () => {
    render(bar(room(), arjun))
    expect(screen.getByRole('region', { name: 'Decision' })).toHaveTextContent('Needs your approval · kafka-connect — Restart the task')
    expect(screen.getByRole('button', { name: /^Approve kafka-connect/ })).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /^Reject/ })).toBeInTheDocument()
  })

  it('tells someone who owns no pending layer who the room waits for', () => {
    render(bar(room(), { ...kabir, layers: ['consumer'] }))
    expect(screen.getByRole('region', { name: 'Decision' })).toHaveTextContent(
      'Waiting for schema-registry · kafka-connect · 0 of 2 approved',
    )
    expect(screen.queryByRole('button', { name: /^(Approve|Reject)/ })).not.toBeInTheDocument()
  })
})
