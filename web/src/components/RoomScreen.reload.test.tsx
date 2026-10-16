import { beforeEach, describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen } from '@testing-library/react'
import { MemoryRouter, Route, Routes } from 'react-router-dom'
import type { ConnState, RoomActions } from '../hooks/useRoom'
import type { RoomState } from '../state/types'
import { emptyRoomState } from '../state/types'

// #550: useRoom passes through a null state while it reloads a room (stale_position, 409
// params_mismatch, Reload). RoomScreen keeps drawing the last state of that room meanwhile, so
// SteerBox is not unmounted and keeps its field and Retry strip.

vi.mock('../context/user', () => ({
  useCurrentUser: () => ({ user: currentUser, setUserId: vi.fn(), logout: vi.fn(), sessionEpoch: 0 }),
}))
vi.mock('../hooks/useRoom', () => ({ useRoom: vi.fn() }))
vi.mock('../hooks/useTopology', () => ({ useTopology: vi.fn() }))

import { useRoom } from '../hooks/useRoom'
import { useTopology } from '../hooks/useTopology'
import { RoomScreen } from './RoomScreen'

let currentUser = { id: 'arjun', name: 'Arjun', layers: ['kafka'] }

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

const room = (id: string): RoomState => ({ ...emptyRoomState(id), title: `Room ${id}`, driver: { id: 'arjun', name: 'Arjun' } })

function hookReturns(state: RoomState | null, conn: ConnState = 'live', error: string | null = null) {
  vi.mocked(useRoom).mockReturnValue({
    state,
    presence: [],
    conn,
    error,
    actions,
    stale: null,
    dismissStale: vi.fn(),
    reload: vi.fn(),
  })
}

function screenAt(path: string) {
  return (
    <MemoryRouter initialEntries={[path]}>
      <Routes>
        <Route path="/rooms/:roomId" element={<RoomScreen />} />
      </Routes>
    </MemoryRouter>
  )
}

beforeEach(() => {
  vi.resetAllMocks()
  currentUser = { id: 'arjun', name: 'Arjun', layers: ['kafka'] }
  vi.mocked(useTopology).mockReturnValue({ topology: null, error: null, updatedAt: null, notFound: false, retry: () => undefined })
})

const field = () => screen.getByLabelText<HTMLTextAreaElement>('Steer the agent')

describe('RoomScreen across a reload of the same room (#550)', () => {
  it('keeps the composer mounted, with its words, while the state is null', () => {
    hookReturns(room('r1'))
    const view = render(screenAt('/rooms/r1'))
    fireEvent.change(field(), { target: { value: 'stop the connector' } })
    const before = field()

    hookReturns(null, 'connecting')
    view.rerender(screenAt('/rooms/r1'))
    expect(field()).toBe(before) // the same element: not unmounted
    expect(field().value).toBe('stop the connector')

    hookReturns(room('r1'))
    view.rerender(screenAt('/rooms/r1'))
    expect(field()).toBe(before)
  })

  it('a failed reload shows the error, not the old room', () => {
    hookReturns(room('r1'))
    const view = render(screenAt('/rooms/r1'))
    hookReturns(null, 'error', 'boom')
    view.rerender(screenAt('/rooms/r1'))
    expect(screen.queryByLabelText('Steer the agent')).toBeNull()
    expect(screen.getByText(/boom/)).toBeInTheDocument()
  })

  it('another user does not see the previous user\'s room state while theirs loads', () => {
    hookReturns(room('r1'))
    const view = render(screenAt('/rooms/r1'))
    currentUser = { id: 'priya', name: 'Priya', layers: ['kafka'] }
    hookReturns(null, 'connecting')
    view.rerender(screenAt('/rooms/r1'))
    expect(screen.queryByLabelText('Steer the agent')).toBeNull()
    expect(screen.queryByText('Room r1')).toBeNull()
  })
})
