import { beforeEach, describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen, within } from '@testing-library/react'
import { ProposalsPanel } from './ProposalsPanel'
import type { Event } from '../api/types'
import type { RoomActions } from '../hooks/useRoom'
import type { CurrentUser } from '../state/permissions'
import { applyEvent } from '../state/reducer'
import { emptyRoomState } from '../state/types'
import type { ProposalState, RoomState } from '../state/types'

// KAN-560: room.closed leaves a proposal "open" in the log, but a closed room takes no decision
// (409 room_closed), so that proposal never ran. The card must say "not run", not keep waiting on a
// layer owner with an approval count that can no longer move, and offer nothing to decide.

const approve = vi.fn()
const approveAll = vi.fn()
const reject = vi.fn()
const nudge = vi.fn()
const actions = {
  comment: vi.fn(),
  steer: vi.fn(),
  sendSuggestion: vi.fn(),
  declineSuggestion: vi.fn(),
  requestControl: vi.fn(),
  grantControl: vi.fn(),
  close: vi.fn(),
  approve,
  approveAll,
  reject,
  handoff: vi.fn(),
  acceptHandoff: vi.fn(),
  label: vi.fn(),
  checklist: vi.fn(),
  trace: vi.fn(),
  hypothesisVerdict: vi.fn(),
  nudge,
} as unknown as RoomActions

const HASH = '9f2c1e7ab04d58c3d6e1f0a7b92c4d835e6a1f0b7c3d9e2a48b5c6d7e8f90a1b'
const NOT_RUN = 'not run · room closed'
const LAYERS = ['schema-registry', 'kafka-connect']

// In an open room each of these would have a control: Arjun and Priya own a layer, Sam is solo and
// decides every layer ("Approve and run"), Meera drives and approves the layer nobody owns.
const arjun: CurrentUser = { id: 'arjun', name: 'Arjun', layers: ['kafka-connect'] }
const priya: CurrentUser = { id: 'priya', name: 'Priya', layers: ['schema-registry'] }
const sam: CurrentUser = { id: 'sam', name: 'Sam', layers: LAYERS, solo: true }
const meera: CurrentUser = { id: 'meera', name: 'Meera', layers: [] }
const vera: CurrentUser = { id: 'vera', name: 'Vera', layers: ['kafka-connect'], role: 'viewer' }
// Owners are known and one of them is here, so an open room names them: "Waiting for Arjun · here".
const people = [
  { id: 'arjun', name: 'Arjun', layers: ['kafka-connect'] },
  { id: 'priya', name: 'Priya', layers: ['schema-registry'] },
]
const presence = [{ id: 'arjun', name: 'Arjun', state: 'viewing' as const }]

function proposal(over: Partial<ProposalState> = {}): ProposalState {
  return {
    proposal_id: 'p1',
    title: 'Set subject compatibility to NONE, then restart the connector task',
    steps: [],
    risk: 'low',
    rollback: 'set compatibility back to BACKWARD',
    required_layers: LAYERS,
    approvals: { 'schema-registry': 'pending', 'kafka-connect': 'pending' },
    decisions: {},
    params_hash: HASH,
    status: 'open',
    executed: [],
    ...over,
  }
}

function room(p: ProposalState, over: Partial<RoomState> = {}): RoomState {
  return {
    ...emptyRoomState('r1'),
    pipeline_id: 'orders',
    driver: { id: 'meera', name: 'Meera' },
    proposals: [p],
    ...over,
  }
}

const closedRoom = (p: ProposalState, over: Partial<RoomState> = {}) => room(p, { status: 'closed', ...over })

const panel = (state: RoomState, user: CurrentUser) => (
  <ProposalsPanel state={state} user={user} actions={actions} people={people} presence={presence} />
)
const show = (state: RoomState, user: CurrentUser) => render(panel(state, user))
const row = (layer: string) => document.querySelector(`[data-layer="${layer}"]`) as HTMLElement

beforeEach(() => {
  for (const f of [approve, approveAll, reject, nudge]) f.mockReset()
})

describe('a closed room with a proposal that never got all its approvals', () => {
  it.each([
    ['a layer owner', arjun],
    ['the other layer owner', priya],
    ['a solo user who could approve every layer', sam],
    ['the driver, who approves a layer nobody owns', meera],
    ['a viewer', vera],
  ])('shows it as not run to %s, with nothing to wait for or decide', (_who, user) => {
    // kafka-connect has no owner here (P2b fallback), so only the driver could have approved it.
    show(closedRoom(proposal({ fallback_layers: ['kafka-connect'] })), user)

    const card = screen.getByRole('article')
    expect(within(card).getByText(NOT_RUN)).toBeInTheDocument()
    // "waiting for <owners> · 0 of 2 approved" in the header, "Waiting for <names>" on the rows.
    expect(screen.queryByText(/waiting for/i)).not.toBeInTheDocument()
    expect(screen.queryByText(/\d+ of \d+ approved/)).not.toBeInTheDocument()
    expect(screen.queryByText(/driver approves/)).not.toBeInTheDocument()
    expect(screen.queryByRole('button', { name: /^(Approve|Reject|Nudge)/ })).not.toBeInTheDocument()
    expect(screen.queryByTestId('approve-all')).not.toBeInTheDocument()
    for (const layer of LAYERS) {
      expect(row(layer)).toHaveAttribute('data-state', 'pending')
      expect(within(row(layer)).getByTestId(`undecided-${layer}`)).toHaveTextContent('Not decided')
    }
  })

  it('does not act on A or R', () => {
    show(closedRoom(proposal()), sam)
    fireEvent.keyDown(window, { key: 'a' })
    fireEvent.keyDown(window, { key: 'r' })
    expect(approve).not.toHaveBeenCalled()
    expect(approveAll).not.toHaveBeenCalled()
    expect(reject).not.toHaveBeenCalled()
    expect(screen.queryByRole('alertdialog')).not.toBeInTheDocument()
  })

  it('keeps what the layers decided before the room closed, and still shows no counter', () => {
    const decided = proposal({
      approvals: { 'schema-registry': 'approved', 'kafka-connect': 'pending' },
      decisions: {
        'schema-registry': {
          outcome: 'approved',
          by: { kind: 'human', id: 'priya', name: 'Priya' },
          params_hash: HASH,
          at: '2026-10-11T10:00:00Z',
        },
      },
    })
    show(closedRoom(decided), arjun)
    expect(within(row('schema-registry')).getByTestId('decision-schema-registry')).toHaveTextContent('approved by Priya')
    expect(within(row('kafka-connect')).getByTestId('undecided-kafka-connect')).toBeInTheDocument()
    expect(within(row('schema-registry')).queryByTestId('undecided-schema-registry')).not.toBeInTheDocument()
    expect(screen.getByText(NOT_RUN)).toBeInTheDocument()
    expect(screen.queryByText(/waiting for/i)).not.toBeInTheDocument()
    expect(screen.queryByText(/\d+ of \d+ approved/)).not.toBeInTheDocument()
  })

  it('does not tell a person to fix a missing fingerprint they can no longer act on', () => {
    show(closedRoom(proposal({ params_hash: '' })), arjun)
    expect(screen.queryByTestId('params-hash-missing')).not.toBeInTheDocument()
    expect(screen.getByTestId('params-hash-none')).toBeInTheDocument()
    expect(screen.getByText(NOT_RUN)).toBeInTheDocument()
  })

  it('does not point at per-layer rows after a refused approve-all', () => {
    show(closedRoom(proposal(), { approve_all_refused: { p1: 'you do not own kafka-connect' } }), sam)
    expect(screen.queryByTestId('approve-all-refused')).not.toBeInTheDocument()
  })

  it('goes from waiting to not run when the room closes under the card', () => {
    // The real fold: a proposal.created, then the room.closed that leaves it "open".
    const at = (n: number) => `2026-10-11T10:00:0${n}Z`
    const created: Event = {
      room_id: 'r1',
      seq: 1,
      type: 'proposal.created',
      actor: { kind: 'agent', id: 'agent', name: 'Agent' },
      at: at(1),
      payload: {
        proposal_id: 'p1',
        title: 'Restart the connector task',
        steps: [],
        risk: 'low',
        rollback: 'none',
        required_layers: LAYERS,
        params_hash: HASH,
      },
    }
    const closed: Event = {
      room_id: 'r1',
      seq: 2,
      type: 'room.closed',
      actor: { kind: 'human', id: 'meera', name: 'Meera' },
      at: at(2),
      payload: { resolution: 'gave up' },
    }
    const open = applyEvent({ ...emptyRoomState('r1'), pipeline_id: 'orders' }, created)
    const after = applyEvent(open, closed)
    expect(after.status).toBe('closed')
    expect(after.proposals[0].status).toBe('open')

    const { rerender } = show(open, arjun)
    expect(screen.getByRole('button', { name: 'Approve' })).toBeInTheDocument()
    expect(screen.getByText(/Waiting for/)).toBeInTheDocument()
    expect(screen.queryByText(NOT_RUN)).not.toBeInTheDocument()

    rerender(panel(after, arjun))
    expect(screen.getByText(NOT_RUN)).toBeInTheDocument()
    expect(screen.queryByText(/waiting for/i)).not.toBeInTheDocument()
    expect(screen.queryByText(/\d+ of \d+ approved/)).not.toBeInTheDocument()
    expect(screen.queryByRole('button', { name: /^(Approve|Reject|Nudge)/ })).not.toBeInTheDocument()
  })
})

describe('an open room with a pending proposal (unchanged)', () => {
  it('still waits on the layer owners, with the count and a Nudge', () => {
    show(room(proposal()), meera)
    expect(screen.getByText(/^waiting for .* owners · 0 of 2 approved$/)).toBeInTheDocument()
    expect(within(row('kafka-connect')).getByText(/Waiting for/)).toHaveTextContent('Waiting for Arjun')
    expect(within(row('schema-registry')).getByText(/Waiting for/)).toHaveTextContent('Waiting for Priya')
    expect(screen.getByRole('button', { name: 'Nudge kafka-connect owners' })).toBeInTheDocument()
    expect(screen.queryByText(/not run/i)).not.toBeInTheDocument()
    expect(screen.queryByTestId('undecided-kafka-connect')).not.toBeInTheDocument()
  })

  it('still offers Approve and Reject to a layer owner', () => {
    show(room(proposal()), arjun)
    expect(screen.getByRole('button', { name: 'Approve' })).toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Reject' })).toBeInTheDocument()
    expect(screen.queryByText(/not run/i)).not.toBeInTheDocument()
  })

  it('still tells a person that an unhashed fix cannot be approved', () => {
    show(room(proposal({ params_hash: '' })), arjun)
    expect(screen.getByTestId('params-hash-missing')).toBeInTheDocument()
  })
})

describe('a closed room whose proposal ran (unchanged)', () => {
  const ran = proposal({
    status: 'approved',
    steps: [{ action: 'connect_restart_task', layer: 'kafka-connect', params: { connector: 'orders' }, description: 'restart' }],
    approvals: { 'schema-registry': 'approved', 'kafka-connect': 'approved' },
    executed: [{ proposal_id: 'p1', step_index: 0, ok: true, output: 'restarted' }],
    verification: { proposal_id: 'p1', checks: [], recovered: true, verdict: 'recovered: all good' },
  })

  it('shows it as approved and ran, not as not run', () => {
    show(closedRoom(ran), arjun)
    expect(screen.getByText('approved')).toBeInTheDocument()
    expect(screen.getByText('Ran 1 of 1')).toBeInTheDocument()
    expect(screen.getByText('Verified · recovered')).toBeInTheDocument()
    expect(screen.queryByText(/not run/i)).not.toBeInTheDocument()
    expect(screen.queryByTestId('undecided-kafka-connect')).not.toBeInTheDocument()
  })
})
