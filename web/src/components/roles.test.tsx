import { beforeEach, describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen } from '@testing-library/react'
import { MemoryRouter, Route, Routes } from 'react-router-dom'
import type { RoomActions } from '../hooks/useRoom'
import type { CurrentUser } from '../state/permissions'
import { emptyRoomState } from '../state/types'
import type { RoomState } from '../state/types'
import type { RoomSnapshot } from '../api/types'

// The current user is swapped per test: a member first, then a viewer.
const ctx = vi.hoisted(() => ({
  user: { id: 'arjun', name: 'Arjun', layers: ['kafka-connect'] } as CurrentUser,
}))

vi.mock('../context/user', () => ({
  useCurrentUser: () => ({
    user: ctx.user,
    setUserId: vi.fn(),
    logout: vi.fn(),
    sessionEpoch: 0,
    busy: false,
  }),
}))
vi.mock('../hooks/useRoom', () => ({ useRoom: vi.fn() }))
vi.mock('../hooks/useTopology', () => ({ useTopology: vi.fn() }))
vi.mock('../hooks/useRoomList', () => ({ useRoomList: vi.fn() }))

import { useRoom } from '../hooks/useRoom'
import { useTopology } from '../hooks/useTopology'
import { useRoomList } from '../hooks/useRoomList'
import { RoomScreen } from './RoomScreen'
import { RoomList } from './RoomList'

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
const staleOff = { stale: null, dismissStale: vi.fn(), reload: vi.fn() }

const member: CurrentUser = { id: 'arjun', name: 'Arjun', layers: ['kafka-connect'], role: 'member' }
const viewer: CurrentUser = { id: 'vera', name: 'Vera', layers: ['kafka-connect'], role: 'viewer' }

// A room where a member would have every control: they drive, there is a step
// to comment on, a pending approval, a pending suggestion and a hand-off to accept.
function busyRoom(): RoomState {
  return {
    ...emptyRoomState('r1'),
    title: 'Orders stale',
    pipeline_id: 'orders',
    driver: { id: 'arjun', name: 'Arjun' },
    steps: [{ id: 's1', title: 'Check lag', tool_calls: [], messages: [] }],
    proposals: [
      {
        proposal_id: 'p1',
        title: 'Restart the task',
        steps: [],
        risk: 'low',
        rollback: 'none',
        required_layers: ['kafka-connect'],
        approvals: { 'kafka-connect': 'pending' },
        decisions: {},
        params_hash: 'abc123abc123',
        status: 'open',
        executed: [],
      },
    ],
  }
}

function showRoom(state: RoomState) {
  vi.mocked(useRoom).mockReturnValue({ state, presence: [], conn: 'live', error: null, actions, ...staleOff })
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
  vi.resetAllMocks()
  ctx.user = member
})

describe('role-aware room screen', () => {
  it('a member sees the action controls', () => {
    showRoom(busyRoom())
    expect(screen.getByRole('button', { name: 'Hand off' })).toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Trace a record' })).toBeInTheDocument()
    // The card's buttons and the decision bar's.
    expect(screen.getAllByRole('button', { name: /^Approve/ }).length).toBeGreaterThan(0)
    expect(screen.getAllByRole('button', { name: /^Reject/ }).length).toBeGreaterThan(0)
    expect(screen.getByLabelText('Steer the agent')).toBeInTheDocument()
    expect(screen.getByLabelText('Comment on this step')).toBeInTheDocument()
    expect(screen.queryByText('View only')).not.toBeInTheDocument()
  })

  it('a viewer sees a View only badge and no action control', () => {
    ctx.user = viewer
    showRoom(busyRoom())
    expect(screen.getByText('View only')).toBeInTheDocument()
    // steer, comment, approve/reject, hand-off, trace
    expect(screen.queryByLabelText('Steer the agent')).not.toBeInTheDocument()
    expect(screen.queryByLabelText('Comment on this step')).not.toBeInTheDocument()
    expect(screen.queryByRole('button', { name: 'Approve' })).not.toBeInTheDocument()
    expect(screen.queryByRole('button', { name: 'Reject' })).not.toBeInTheDocument()
    expect(screen.queryByRole('button', { name: 'Hand off' })).not.toBeInTheDocument()
    expect(screen.queryByRole('button', { name: 'Trace a record' })).not.toBeInTheDocument()
    // control: grant, request and take
    expect(screen.queryByRole('button', { name: /Give control/ })).not.toBeInTheDocument()
    expect(screen.queryByRole('button', { name: 'Request control' })).not.toBeInTheDocument()
    expect(screen.queryByRole('button', { name: 'Take control' })).not.toBeInTheDocument()
    // The room is still readable.
    expect(screen.getByText('Check lag')).toBeInTheDocument()
    expect(screen.getByText('Restart the task')).toBeInTheDocument()
  })

  it('a viewer gets no Take control banner on a room without a driver', () => {
    ctx.user = viewer
    showRoom({ ...busyRoom(), driver: null })
    expect(screen.queryByText(/No one is driving/)).not.toBeInTheDocument()
    expect(screen.queryByRole('button', { name: 'Take control' })).not.toBeInTheDocument()
  })

  it('a viewer gets no Accept button for a hand-off', () => {
    ctx.user = viewer
    showRoom({
      ...busyRoom(),
      handoffs: [
        {
          handoff_id: 'h1',
          to: { kind: 'human', id: 'vera', name: 'Vera' },
          summary: { what_broke: '', why: '', done: [], ruled_out: [], open: [], risks: [] },
          stay_as_watcher: false,
          status: 'pending',
        },
      ],
    })
    expect(screen.queryByRole('button', { name: 'Accept' })).not.toBeInTheDocument()
    expect(screen.queryByRole('button', { name: 'Read summary' })).not.toBeInTheDocument()
  })
})

describe('role-aware room list', () => {
  // One open room nobody drives: a member may take it, a viewer may not.
  const rooms: RoomSnapshot[] = [
    {
      id: 'r_nodriver',
      title: 'No driver here',
      severity: 'sev2',
      pipeline_id: 'orders',
      status: 'open',
      opened_at: '2026-10-11T10:00:00Z',
      last_seq: 1,
      driver: null,
      agent_status: { state: 'idle' },
      hypothesis: null,
      open_proposals: [],
      pending_suggestions: [],
      members: [],
    },
  ]
  beforeEach(() => {
    vi.mocked(useRoomList).mockReturnValue({ rooms, loading: false, error: null, reload: vi.fn(), mode: 'push', updatedAt: null })
  })

  it('a member can create a room', () => {
    render(
      <MemoryRouter>
        <RoomList />
      </MemoryRouter>,
    )
    expect(screen.getByRole('button', { name: /Take control/ })).toBeInTheDocument()
    fireEvent.click(screen.getByRole('button', { name: 'New room' }))
    expect(screen.getByRole('button', { name: 'Open room' })).toBeInTheDocument()
    expect(screen.getByLabelText('Title')).toBeInTheDocument()
  })

  it('a viewer cannot create a room or take control', () => {
    ctx.user = viewer
    render(
      <MemoryRouter>
        <RoomList />
      </MemoryRouter>,
    )
    expect(screen.queryByRole('button', { name: 'New room' })).not.toBeInTheDocument()
    expect(screen.queryByRole('button', { name: 'Open room' })).not.toBeInTheDocument()
    expect(screen.queryByLabelText('Title')).not.toBeInTheDocument()
    expect(screen.queryByRole('button', { name: /Take control/ })).not.toBeInTheDocument()
    // The room is still listed, read-only. (The "View only" badge is in the shell's account
    // menu now, and AppShell.test.tsx covers it.)
    expect(screen.getByRole('link', { name: 'No driver here' })).toBeInTheDocument()
    // The C key opens nothing for a viewer.
    fireEvent.keyDown(window, { key: 'c' })
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument()
  })
})
