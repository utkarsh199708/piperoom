import { beforeEach, describe, expect, it, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import { MemoryRouter, Route, Routes } from 'react-router-dom'
import type { ConnState, RoomActions } from '../hooks/useRoom'
import type { CurrentUser } from '../state/permissions'
import { emptyRoomState } from '../state/types'
import type { ProposalState, RoomState } from '../state/types'

// QA9: with the live socket down the room showed "Proposals 0", "The agent has not started yet"
// and "No activity yet" for a room whose snapshot had a proposal waiting on approval.
const ctx = vi.hoisted(() => ({ user: null as unknown as CurrentUser }))

vi.mock('../context/user', () => ({
  useCurrentUser: () => ({ user: ctx.user, setUserId: vi.fn(), logout: vi.fn(), sessionEpoch: 0, busy: false }),
}))
vi.mock('../hooks/useRoom', () => ({ useRoom: vi.fn() }))
vi.mock('../hooks/useTopology', () => ({ useTopology: vi.fn() }))

import { useRoom } from '../hooks/useRoom'
import { useTopology } from '../hooks/useTopology'
import { RoomScreen } from './RoomScreen'

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

const arjun: CurrentUser = { id: 'arjun', name: 'Arjun', layers: ['kafka-connect'], role: 'member' }

const snapProposal: ProposalState = {
  proposal_id: 'p1',
  title: 'Restart the connector task',
  steps: [],
  risk: 'low',
  rollback: '',
  required_layers: ['kafka-connect'],
  approvals: { 'kafka-connect': 'pending' },
  decisions: {},
  params_hash: 'abc123',
  status: 'open',
  executed: [],
}

// Only the snapshot: nothing replayed (last_seq 0), so no steps, proposals or activity yet.
function snapOnly(over: Partial<RoomState> = {}): RoomState {
  return {
    ...emptyRoomState('r1'),
    title: 'Warehouse orders stale',
    severity: 'sev2',
    pipeline_id: 'orders',
    opened_at: '2026-10-11T10:00:00Z',
    driver: { id: 'kabir', name: 'Kabir', lease_until: new Date(Date.now() + 3_600_000).toISOString() },
    agent_status: { state: 'waiting_approval' },
    hypothesis: { text: 'Schema change broke the connector', based_on_steps: [], confidence: 'high', confirmed: false },
    snapshot_proposals: [snapProposal],
    ...over,
  }
}

function show(state: RoomState, conn: ConnState, liveDown: boolean) {
  ctx.user = arjun
  vi.mocked(useRoom).mockReturnValue({
    state,
    presence: [],
    conn,
    liveDown,
    error: null,
    actions,
    stale: null,
    dismissStale: vi.fn(),
    reload: vi.fn(),
  })
  vi.mocked(useTopology).mockReturnValue({ topology: null, error: null, updatedAt: null })
  return render(
    <MemoryRouter initialEntries={['/rooms/r1']}>
      <Routes>
        <Route path="/rooms/:roomId" element={<RoomScreen />} />
      </Routes>
    </MemoryRouter>,
  )
}

beforeEach(() => {
  vi.clearAllMocks()
})

describe('RoomScreen with live updates down', () => {
  it('shows a banner, the snapshot proposal and hypothesis, and no false empty states', () => {
    show(snapOnly(), 'reconnecting', true)
    expect(screen.getByTestId('live-down')).toHaveTextContent('Live updates down — showing last snapshot')
    expect(screen.getByText('Schema change broke the connector')).toBeInTheDocument()
    expect(screen.getByRole('heading', { level: 3, name: 'Restart the connector task' })).toBeInTheDocument()
    expect(screen.getByRole('heading', { name: /^Proposals/ })).toHaveTextContent('1 open')
    expect(screen.queryByText(/No fix proposed yet/)).not.toBeInTheDocument()
    expect(screen.queryByText('The agent has not started yet.')).not.toBeInTheDocument()
    expect(screen.getByText('The investigation history is not loaded.')).toBeInTheDocument()
    expect(screen.queryByText('No activity yet.')).not.toBeInTheDocument()
    expect(screen.getByText('Activity is unavailable while live updates are down.')).toBeInTheDocument()
  })

  it('inside the grace there is no banner, and the lists say they are loading', () => {
    show(snapOnly(), 'connecting', false)
    expect(screen.queryByTestId('live-down')).not.toBeInTheDocument()
    expect(screen.getByText('Loading the investigation…')).toBeInTheDocument()
    expect(screen.getByText('Loading activity…')).toBeInTheDocument()
  })

  it('once live, the replayed state is used as it is', () => {
    show(snapOnly({ last_seq: 3 }), 'live', false)
    expect(screen.queryByTestId('live-down')).not.toBeInTheDocument()
    expect(screen.queryByRole('heading', { level: 3, name: 'Restart the connector task' })).not.toBeInTheDocument()
    expect(screen.getByText('The agent has not started yet.')).toBeInTheDocument()
  })

  it('on a phone the card waiting on you is moved first', () => {
    show(snapOnly(), 'reconnecting', true)
    expect(document.getElementById('proposal')).toHaveClass('max-[599px]:order-first')
    expect(screen.getByRole('article')).toHaveAttribute('data-needs-you', 'true')
    expect(screen.getByRole('article')).toHaveClass('max-[599px]:order-first')
  })

  it('humanises the agent status in the header', () => {
    show(snapOnly({ snapshot_proposals: [] }), 'reconnecting', true)
    expect(screen.queryByText(/waiting_approval/)).not.toBeInTheDocument()
  })
})
