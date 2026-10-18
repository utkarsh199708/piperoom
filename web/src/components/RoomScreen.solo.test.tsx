import { beforeEach, describe, expect, it, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import { MemoryRouter, Route, Routes } from 'react-router-dom'
import type { RoomActions } from '../hooks/useRoom'
import type { CurrentUser } from '../state/permissions'
import { emptyRoomState } from '../state/types'
import type { RoomState } from '../state/types'
import type { PresenceMember } from '../api/types'

// P2: a room of one has nobody to ask for control and nobody to hand off to.

const ctx = vi.hoisted(() => ({
  user: { id: 'kabir', name: 'Kabir', layers: ['consumer'], role: 'member' } as CurrentUser,
}))

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

const lease = () => new Date(Date.now() + 3_600_000).toISOString()

function room(over: Partial<RoomState> = {}): RoomState {
  return {
    ...emptyRoomState('r1'),
    title: 'Orders stale',
    pipeline_id: 'orders',
    status: 'open',
    // The server made Kabir the driver when he walked into the alert room alone.
    driver: { id: 'kabir', name: 'Kabir', lease_until: lease(), reason: 'sole_member' },
    members: [{ id: 'kabir', name: 'Kabir', role: 'driver' }],
    ...over,
  }
}

const here = (id: string, name: string): PresenceMember => ({ id, name, state: 'viewing' })

function show(state: RoomState, presence: PresenceMember[] = []) {
  vi.mocked(useRoom).mockReturnValue({
    state,
    presence,
    conn: 'live',
    error: null,
    actions,
    stale: null,
    dismissStale: vi.fn(),
    reload: vi.fn(),
  })
  vi.mocked(useTopology).mockReturnValue({ topology: null, error: null, updatedAt: null, notFound: false, retry: () => undefined })
  return render(
    <MemoryRouter initialEntries={['/rooms/r1']}>
      <Routes>
        <Route path="/rooms/:roomId" element={<RoomScreen />} />
      </Routes>
    </MemoryRouter>,
  )
}

beforeEach(() => {
  ctx.user = { id: 'kabir', name: 'Kabir', layers: ['consumer'], role: 'member' }
})

describe('sole member (P2)', () => {
  it('the sole member is the driver: no request-control, no hand-off', () => {
    show(room(), [here('kabir', 'Kabir')])
    expect(screen.getAllByText(/You are the driver/).length).toBeGreaterThan(0)
    expect(screen.queryByRole('button', { name: /Request control/ })).not.toBeInTheDocument()
    expect(screen.queryByRole('button', { name: /Hand off/ })).not.toBeInTheDocument()
    // Closing stays: a room of one still ends with a resolution and a post-mortem draft.
    expect(screen.getByRole('button', { name: /Close room/ })).toBeInTheDocument()
  })

  it('a second person present brings the hand-off back', () => {
    show(room(), [here('kabir', 'Kabir'), here('arjun', 'Arjun')])
    expect(screen.getByRole('button', { name: /Hand off/ })).toBeInTheDocument()
  })

  it('a second member from the event log counts too', () => {
    show(
      room({
        members: [
          { id: 'kabir', name: 'Kabir', role: 'driver' },
          { id: 'arjun', name: 'Arjun', role: 'watching' },
        ],
      }),
    )
    expect(screen.getByRole('button', { name: /Hand off/ })).toBeInTheDocument()
  })

  it('with two members the one who does not drive can request control', () => {
    ctx.user = { id: 'arjun', name: 'Arjun', layers: ['kafka-connect'], role: 'member' }
    show(room(), [here('kabir', 'Kabir'), here('arjun', 'Arjun')])
    expect(screen.getByRole('button', { name: /Request control/ })).toBeInTheDocument()
    expect(screen.queryByRole('button', { name: /Hand off/ })).not.toBeInTheDocument()
  })
})
