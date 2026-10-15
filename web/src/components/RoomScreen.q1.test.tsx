// Q1-web-b: "Page owner" (G2) and "Log a fix" (G10) on the room screen, and the pages in its timeline.
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen, waitFor, within } from '@testing-library/react'
import { MemoryRouter, Route, Routes } from 'react-router-dom'
import type { RoomActions } from '../hooks/useRoom'
import type { CurrentUser } from '../state/permissions'
import { emptyRoomState } from '../state/types'
import type { RoomState } from '../state/types'

const ctx = vi.hoisted(() => ({ user: null as unknown as CurrentUser }))

vi.mock('../context/user', () => ({
  useCurrentUser: () => ({ user: ctx.user, setUserId: vi.fn(), logout: vi.fn(), sessionEpoch: 0, busy: false }),
}))
vi.mock('../hooks/useRoom', () => ({ useRoom: vi.fn() }))
vi.mock('../hooks/useTopology', () => ({ useTopology: vi.fn() }))

import { useRoom } from '../hooks/useRoom'
import { useTopology } from '../hooks/useTopology'
import { RoomScreen } from './RoomScreen'

const page = vi.fn()
const fixNote = vi.fn()
const base: RoomActions = {
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

const arjun: CurrentUser = { id: 'arjun', name: 'Arjun', layers: ['kafka-connect'], role: 'member' }
const vera: CurrentUser = { id: 'vera', name: 'Vera', layers: [], role: 'viewer' }

function room(over: Partial<RoomState> = {}): RoomState {
  return {
    ...emptyRoomState('r1'),
    title: 'Warehouse orders stale',
    pipeline_id: 'orders',
    opened_at: '2026-10-11T10:00:00Z',
    driver: { id: 'kabir', name: 'Kabir', lease_until: new Date(Date.now() + 3_600_000).toISOString() },
    members: [{ id: 'kabir', name: 'Kabir', role: 'driver' }],
    ...over,
  }
}

function show(user: CurrentUser, state: RoomState, actions: RoomActions = { ...base, page, fixNote }) {
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
  return render(
    <MemoryRouter initialEntries={['/rooms/r1']}>
      <Routes>
        <Route path="/rooms/:roomId" element={<RoomScreen />} />
      </Routes>
    </MemoryRouter>,
  )
}

function setWidth(px: number) {
  vi.stubGlobal('matchMedia', (q: string) => {
    const min = /min-width:\s*(\d+)px/.exec(q)
    return { matches: min ? px >= Number(min[1]) : false, media: q, addEventListener: () => undefined, removeEventListener: () => undefined }
  })
}

beforeEach(() => {
  vi.clearAllMocks()
  page.mockResolvedValue({ ok: true })
  fixNote.mockResolvedValue({ ok: true })
})
afterEach(() => {
  vi.unstubAllGlobals()
})

const buttons = () => ({
  page: screen.queryByRole('button', { name: 'Page owner' }),
  fix: screen.queryByRole('button', { name: 'Log a fix' }),
})

describe('Page owner', () => {
  it('any member of an open room can open it, pick a layer, give a reason and send the page', async () => {
    show(arjun, room())
    fireEvent.click(buttons().page as HTMLElement)
    const dialog = screen.getByRole('dialog', { name: 'Page an owner' })
    fireEvent.change(within(dialog).getByLabelText('Layer'), { target: { value: 'consumer' } })
    fireEvent.change(within(dialog).getByLabelText('Why now'), { target: { value: 'Lag keeps growing' } })
    fireEvent.click(within(dialog).getByRole('button', { name: 'Page owner' }))
    await waitFor(() => expect(page).toHaveBeenCalledWith({ layer: 'consumer', reason: 'Lag keeps growing' }))
    await waitFor(() => expect(screen.queryByRole('dialog')).toBeNull())
  })

  it('is not offered to a viewer, on a closed room, or when the room cannot page', () => {
    const { unmount } = show(vera, room())
    expect(buttons().page).toBeNull()
    unmount()
    const closed = show(arjun, room({ status: 'closed', closed_at: new Date().toISOString() }))
    expect(buttons().page).toBeNull()
    closed.unmount()
    show(arjun, room(), base)
    expect(buttons().page).toBeNull()
  })

  it('moves behind More actions on a phone', async () => {
    setWidth(390)
    show(arjun, room())
    expect(buttons().page).toBeNull()
    fireEvent.keyDown(screen.getByRole('button', { name: 'More actions' }), { key: 'Enter' })
    const menu = await screen.findByRole('menu')
    expect(within(menu).getByRole('menuitem', { name: /Page owner/ })).toBeInTheDocument()
    expect(within(menu).getByRole('menuitem', { name: /Log a fix/ })).toBeInTheDocument()
  })
})

describe('Log a fix', () => {
  it('opens a dialog and sends a fix note', async () => {
    show(arjun, room())
    fireEvent.click(buttons().fix as HTMLElement)
    const dialog = screen.getByRole('dialog', { name: 'Log a fix' })
    fireEvent.change(within(dialog).getByLabelText('What did you do?'), { target: { value: 'Restarted order-sync by hand' } })
    fireEvent.click(within(dialog).getByRole('button', { name: /Log fix/ }))
    await waitFor(() => expect(fixNote).toHaveBeenCalledWith('Restarted order-sync by hand'))
  })

  it('is offered on a closed room for 24 hours after the close, and then not', () => {
    const recent = new Date(Date.now() - 2 * 3_600_000).toISOString()
    const { unmount } = show(arjun, room({ status: 'closed', closed_at: recent, resolution: 'fixed' }))
    expect(buttons().fix).not.toBeNull()
    expect(buttons().page).toBeNull()
    unmount()
    show(arjun, room({ status: 'closed', closed_at: new Date(Date.now() - 25 * 3_600_000).toISOString(), resolution: 'fixed' }))
    expect(buttons().fix).toBeNull()
  })

  it('is not offered to a viewer', () => {
    show(vera, room())
    expect(buttons().fix).toBeNull()
  })
})

describe('the pages in the timeline', () => {
  it('shows who was paged, with the names the room knows', () => {
    const state = room({
      pages: [{ seq: 3, at: '2026-10-15T04:10:00Z', by: { kind: 'agent', id: 'agent', name: 'Agent' }, layer: 'consumer', owners: ['kabir'], reason: 'Lag keeps growing', auto: false }],
    })
    show(arjun, state)
    expect(screen.getByRole('region', { name: 'Owners paged' })).toHaveTextContent('Agent paged Kabir')
  })
})
