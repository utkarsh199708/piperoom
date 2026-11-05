import { beforeEach, describe, expect, it, vi } from 'vitest'
import { act, render, screen } from '@testing-library/react'
import { MemoryRouter, Route, Routes } from 'react-router-dom'
import type { RoomActions } from '../hooks/useRoom'
import type { CurrentUser } from '../state/permissions'
import { emptyRoomState } from '../state/types'
import type { RoomState } from '../state/types'

// KAN-563: the agent-error toast's "Steer" action must focus the composer whatever its accessible
// name is, so a non-driver ("Suggest a step for ...") is not left with a button that does nothing.
const ctx = vi.hoisted(() => ({ user: null as unknown as CurrentUser }))
const toast = vi.hoisted(() => Object.assign(vi.fn(), { error: vi.fn(), success: vi.fn() }))

vi.mock('sonner', () => ({ toast }))
vi.mock('../context/user', () => ({
  useCurrentUser: () => ({ user: ctx.user, setUserId: vi.fn(), logout: vi.fn(), sessionEpoch: 0, busy: false }),
}))
vi.mock('../hooks/useRoom', () => ({ useRoom: vi.fn() }))
vi.mock('../hooks/useTopology', () => ({ useTopology: vi.fn() }))

import { useRoom } from '../hooks/useRoom'
import { useTopology } from '../hooks/useTopology'
import { RoomScreen } from './RoomScreen'

const actions = new Proxy({} as RoomActions, { get: (_, key) => (key === 'sends' ? undefined : vi.fn()) })
const kabir: CurrentUser = { id: 'kabir', name: 'Kabir', layers: ['consumer'], role: 'member' }
const arjun: CurrentUser = { id: 'arjun', name: 'Arjun', layers: ['kafka-connect'], role: 'member', verified: true }

function room(over: Partial<RoomState> = {}): RoomState {
  return {
    ...emptyRoomState('r1'),
    title: 'Warehouse orders stale',
    pipeline_id: 'orders',
    driver: { id: 'kabir', name: 'Kabir', verified: true, lease_until: new Date(Date.now() + 3_600_000).toISOString() },
    agent_status: { state: 'working', detail: 'step 2' },
    members: [
      { id: 'kabir', name: 'Kabir', role: 'driver', verified: true },
      { id: 'arjun', name: 'Arjun', role: 'watching', verified: true },
    ],
    ...over,
  }
}

function mount(user: CurrentUser, state: RoomState) {
  ctx.user = user
  vi.mocked(useRoom).mockReturnValue({
    state,
    presence: [],
    conn: 'live',
    error: null,
    actions,
    stale: null,
    dismissStale: vi.fn(),
    reload: vi.fn(),
  })
  return (
    <MemoryRouter initialEntries={['/rooms/r1']}>
      <Routes>
        <Route path="/rooms/:roomId" element={<RoomScreen />} />
      </Routes>
    </MemoryRouter>
  )
}

describe('agent-error toast Steer action', () => {
  beforeEach(() => {
    toast.mockClear()
    vi.mocked(useTopology).mockReturnValue({
      topology: null,
      updatedAt: null,
      notFound: false,
      retry: vi.fn(),
    } as unknown as ReturnType<typeof useTopology>)
  })

  for (const [who, user, label] of [
    ['a non-driver', arjun, 'Suggest a step for Kabir'],
    ['the driver', kabir, 'Steer the agent'],
  ] as const) {
    it(`focuses the composer for ${who}`, () => {
      const { rerender } = render(mount(user, room()))
      rerender(mount(user, room({ agent_status: { state: 'error', detail: 'x' } })))
      const call = toast.mock.calls.find(([title]) => title === 'Agent error')
      expect(call).toBeDefined()
      const box = screen.getByLabelText(label)
      expect(box).not.toHaveFocus()
      act(() => call![1].action.onClick())
      expect(box).toHaveFocus()
    })
  }
})
