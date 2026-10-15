import { beforeEach, describe, expect, it, vi } from 'vitest'
import { render } from '@testing-library/react'
import { MemoryRouter, Route, Routes } from 'react-router-dom'
import type { RoomActions } from '../hooks/useRoom'
import type { CurrentUser } from '../state/permissions'
import { emptyRoomState } from '../state/types'
import type { RoomState } from '../state/types'
import { RoomCommandsContext } from './shell/context'
import type { RoomCommands } from './shell/context'

// Which ⌘K room commands RoomScreen registers, per person. They come from the same
// permission checks as the buttons, so a viewer gets none of the writes.
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

const driver: CurrentUser = { id: 'kabir', name: 'Kabir', layers: ['consumer'], role: 'member' }
const other: CurrentUser = { id: 'arjun', name: 'Arjun', layers: ['kafka-connect'], role: 'member' }
const viewer: CurrentUser = { id: 'vera', name: 'Vera', layers: [], role: 'viewer' }

function room(over: Partial<RoomState> = {}): RoomState {
  return {
    ...emptyRoomState('r1'),
    title: 'Orders stale',
    pipeline_id: 'orders',
    status: 'open',
    // The lease is far in the future: only the driver can steer.
    driver: { id: 'kabir', name: 'Kabir', lease_until: new Date(Date.now() + 3_600_000).toISOString() },
    ...over,
  }
}

function registered(user: CurrentUser, state: RoomState): RoomCommands | null {
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
  vi.mocked(useTopology).mockReturnValue({ topology: null, error: null, updatedAt: null, notFound: false, retry: () => undefined })
  let current: RoomCommands | null = null
  const registry = { get: () => current, set: (c: RoomCommands | null) => (current = c) }
  const { unmount } = render(
    <RoomCommandsContext.Provider value={registry}>
      <MemoryRouter initialEntries={['/rooms/r1']}>
        <Routes>
          <Route path="/rooms/:roomId" element={<RoomScreen />} />
        </Routes>
      </MemoryRouter>
    </RoomCommandsContext.Provider>,
  )
  const seen = current
  // Leaving the screen clears the registration.
  unmount()
  expect(current).toBeNull()
  return seen
}

const offered = (c: RoomCommands | null) =>
  Object.entries(c ?? {})
    .filter(([k, v]) => k !== 'title' && typeof v === 'function')
    .map(([k]) => k)
    .sort()

beforeEach(() => vi.clearAllMocks())

describe('RoomScreen registers ⌘K room commands', () => {
  it('the driver can hand off, close, trace and invite; not take or request control', () => {
    const c = registered(driver, room())
    expect(c?.title).toBe('Orders stale')
    expect(offered(c)).toEqual(['closeRoom', 'exportJson', 'handoff', 'invite', 'postmortem', 'trace'])
  })

  it('a non-driver can request control, trace and invite; not hand off or take control', () => {
    const c = registered(other, room())
    expect(offered(c)).toEqual(['exportJson', 'invite', 'postmortem', 'requestControl', 'trace'])
    c?.requestControl?.()
    expect(actions.requestControl).toHaveBeenCalledTimes(1)
  })

  it('with no driver anyone can take control (it grants control to themselves)', () => {
    const c = registered(other, room({ driver: null }))
    expect(offered(c)).toEqual(['exportJson', 'invite', 'postmortem', 'takeControl', 'trace'])
    c?.takeControl?.()
    expect(actions.grantControl).toHaveBeenCalledWith('arjun')
  })

  it('a viewer gets no write command, export or post-mortem: only the invite link', () => {
    const c = registered(viewer, room({ driver: null }))
    expect(offered(c)).toEqual(['invite'])
  })

  it('a closed room offers no control, hand-off or close', () => {
    const c = registered(driver, room({ status: 'closed' }))
    expect(offered(c)).toEqual(['exportJson', 'invite', 'postmortem', 'trace'])
  })
})
