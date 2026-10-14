import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen, within } from '@testing-library/react'
import { MemoryRouter, Route, Routes } from 'react-router-dom'
import type { RoomActions } from '../hooks/useRoom'
import type { ConnState } from '../hooks/useRoom'
import type { CurrentUser } from '../state/permissions'
import { emptyRoomState } from '../state/types'
import type { RoomState } from '../state/types'

// F7c: below 1280px the rail is four tabs; below 768px the header actions sit behind a menu.
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
}
const reload = vi.fn()

const kabir: CurrentUser = { id: 'kabir', name: 'Kabir', layers: ['consumer'], role: 'member' }
const arjun: CurrentUser = { id: 'arjun', name: 'Arjun', layers: ['kafka-connect'], role: 'member' }

function room(over: Partial<RoomState> = {}): RoomState {
  return {
    ...emptyRoomState('r_schema_change'),
    title: 'Warehouse orders stale',
    severity: 'sev2',
    pipeline_id: 'orders',
    opened_at: '2026-10-11T10:00:00Z',
    driver: { id: 'kabir', name: 'Kabir', lease_until: new Date(Date.now() + 3_600_000).toISOString() },
    agent_status: { state: 'working', detail: 'step 2' },
    steps: [
      { id: 's1', title: 'Check the connector', tool_calls: [], messages: [] },
      { id: 's2', title: 'Check the DDL log', tool_calls: [], messages: [] },
    ],
    members: [
      { id: 'kabir', name: 'Kabir', role: 'driver' },
      { id: 'arjun', name: 'Arjun', role: 'watching' },
    ],
    ...over,
  }
}

function show(user: CurrentUser, state: RoomState | null, conn: ConnState = 'live', error: string | null = null) {
  ctx.user = user
  vi.mocked(useRoom).mockReturnValue({
    state,
    presence: [],
    conn,
    error,
    actions,
    stale: null,
    dismissStale: vi.fn(),
    reload,
  })
  vi.mocked(useTopology).mockReturnValue({ topology: null, error: null, updatedAt: null })
  return render(
    <MemoryRouter initialEntries={['/rooms/r_schema_change']}>
      <Routes>
        <Route path="/rooms/:roomId" element={<RoomScreen />} />
        <Route path="/" element={<p>rooms home</p>} />
      </Routes>
    </MemoryRouter>,
  )
}



function setWidth(px: number) {
  vi.stubGlobal('matchMedia', (q: string) => {
    const min = /min-width:\s*(\d+)px/.exec(q)
    return {
      matches: min ? px >= Number(min[1]) : false,
      media: q,
      addEventListener: () => undefined,
      removeEventListener: () => undefined,
    }
  })
}

beforeEach(() => {
  vi.clearAllMocks()
  sessionStorage.clear()
})
afterEach(() => {
  vi.unstubAllGlobals()
})

describe('RoomScreen tabs below 1280px', () => {
  it('shows four tabs at 1024 with every panel mounted and only the chosen one visible', () => {
    setWidth(1024)
    show(arjun, room())
    const list = screen.getByRole('tablist', { name: 'Room sections' })
    expect(within(list).getAllByRole('tab').map((t) => t.textContent)).toEqual([
      'Investigation',
      expect.stringMatching(/^Fix/),
      expect.stringMatching(/^People/),
      'Activity',
    ])
    expect(screen.queryByRole('complementary', { name: 'Room details' })).not.toBeInTheDocument()
    expect(document.querySelectorAll('[role="tabpanel"]')).toHaveLength(4)
    expect(document.querySelectorAll('[role="tabpanel"]:not([hidden])')).toHaveLength(1)
    fireEvent.mouseDown(screen.getByRole('tab', { name: /^People/ }))
    fireEvent.click(screen.getByRole('tab', { name: /^People/ }))
    expect(sessionStorage.getItem('piperoom.tab.r_schema_change')).toBe('people')
  })

  it('keeps the two-column layout with the rail at 1440', () => {
    setWidth(1440)
    show(arjun, room())
    expect(screen.queryByRole('tablist')).not.toBeInTheDocument()
    expect(screen.getByRole('complementary', { name: 'Room details' })).toBeInTheDocument()
  })
})

describe('RoomHeader actions below md', () => {
  it('moves Hand off, Close room, Export and Invite behind More actions at 390', async () => {
    setWidth(390)
    show(kabir, room())
    expect(screen.queryByRole('button', { name: 'Hand off' })).not.toBeInTheDocument()
    const more = screen.getByRole('button', { name: 'More actions' })
    fireEvent.keyDown(more, { key: 'Enter' })
    const menu = await screen.findByRole('menu')
    for (const name of [/Hand off/, /Close room/, /Export/, /Invite/]) {
      expect(within(menu).getByRole('menuitem', { name })).toBeInTheDocument()
    }
  })

  it('keeps the buttons from 768', () => {
    setWidth(1024)
    show(kabir, room())
    expect(screen.getByRole('button', { name: 'Hand off' })).toBeInTheDocument()
    expect(screen.queryByRole('button', { name: 'More actions' })).not.toBeInTheDocument()
  })
})
