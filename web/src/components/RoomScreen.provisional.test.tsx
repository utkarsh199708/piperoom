import { beforeEach, describe, expect, it, vi } from 'vitest'
import { render } from '@testing-library/react'
import { MemoryRouter, Route, Routes } from 'react-router-dom'
import type { RoomActions } from '../hooks/useRoom'
import type { CurrentUser } from '../state/permissions'
import { emptyRoomState, newSetupState } from '../state/types'
import type { RoomState } from '../state/types'

const user: CurrentUser = { id: 'arjun', name: 'Arjun', layers: ['postgres'], role: 'member', verified: true }

vi.mock('../context/user', () => ({
  useCurrentUser: () => ({ user, setUserId: vi.fn(), logout: vi.fn(), sessionEpoch: 0, busy: false }),
}))
vi.mock('../hooks/useRoom', () => ({ useRoom: vi.fn() }))
vi.mock('../hooks/useTopology', () => ({ useTopology: vi.fn() }))
vi.mock('../hooks/useTrends', () => ({ useTrends: vi.fn() }))
vi.mock('../hooks/useAgentStats', () => ({ useAgentStats: vi.fn(() => null) }))

import { useRoom } from '../hooks/useRoom'
import { useTopology } from '../hooks/useTopology'
import { useTrends } from '../hooks/useTrends'
import { useAgentStats } from '../hooks/useAgentStats'
import { RoomScreen } from './RoomScreen'

function show(watched_by: string | undefined) {
  const base = emptyRoomState('r_set')
  const state: RoomState = {
    ...base,
    title: 'Set up regsetup',
    pipeline_id: 'regsetup',
    opened_at: new Date().toISOString(),
    ...(watched_by === undefined ? {} : { setup: { ...newSetupState(), watched_by } }),
  } as RoomState
  vi.mocked(useRoom).mockReturnValue({
    state,
    presence: [],
    conn: 'live',
    error: null,
    actions: {} as RoomActions,
    stale: null,
    dismissStale: vi.fn(),
    reload: vi.fn(),
  })
  vi.mocked(useTopology).mockReturnValue({ topology: null, error: null, updatedAt: null, notFound: false, retry: () => undefined })
  vi.mocked(useTrends).mockReturnValue({ status: 'idle', series: {} } as never)
  return render(
    <MemoryRouter initialEntries={['/rooms/r_set']}>
      <Routes>
        <Route path="/rooms/:roomId" element={<RoomScreen />} />
      </Routes>
    </MemoryRouter>,
  )
}

beforeEach(() => vi.clearAllMocks())

// KAN-601: a setup room's pipeline is not registered yet; its topology and agent stats would 404.
describe('RoomScreen — setup room before registration', () => {
  it('asks for no topology or agent stats, and the map reads not found as after a 404', () => {
    show('')
    expect(vi.mocked(useTopology).mock.calls.every((c) => c[0] === '')).toBe(true)
    expect(vi.mocked(useAgentStats).mock.calls.every((c) => c[0] === '')).toBe(true)
    expect(document.body.textContent).toContain("is not configured")
  })

  it('loads the topology once the pipeline is registered', () => {
    show('server')
    expect(vi.mocked(useTopology).mock.calls[0][0]).toBe('regsetup')
  })

  it('a normal room loads as before', () => {
    show(undefined)
    expect(vi.mocked(useTopology).mock.calls[0][0]).toBe('regsetup')
  })
})
