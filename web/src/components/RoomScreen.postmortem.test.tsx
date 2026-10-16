import { beforeEach, describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen, waitFor, within } from '@testing-library/react'
import { MemoryRouter, Route, Routes } from 'react-router-dom'
import type { RoomActions } from '../hooks/useRoom'
import type { CurrentUser } from '../state/permissions'
import { emptyRoomState } from '../state/types'
import type { HandoffState, RoomState } from '../state/types'

// F2a on the room screen: the pinned shift summaries, the header's ⋯ menu, the closed-room header and
// strip, the export's 403 toast and the post-mortem dialog.
const ctx = vi.hoisted(() => ({ user: null as unknown as CurrentUser }))
const toast = vi.hoisted(() => ({ error: vi.fn(), success: vi.fn() }))

vi.mock('sonner', () => ({ toast }))
vi.mock('../context/user', () => ({
  useCurrentUser: () => ({ user: ctx.user, setUserId: vi.fn(), logout: vi.fn(), sessionEpoch: 0, busy: false }),
}))
vi.mock('../hooks/useRoom', () => ({ useRoom: vi.fn() }))
vi.mock('../hooks/useTopology', () => ({ useTopology: vi.fn() }))
vi.mock('../api/client', async (orig) => {
  const real = await orig<typeof import('../api/client')>()
  return { ...real, exportRoom: vi.fn() }
})

import { ApiClientError, api, exportRoom } from '../api/client'
import { useRoom } from '../hooks/useRoom'
import { useTopology } from '../hooks/useTopology'
import { RoomScreen } from './RoomScreen'

// `sends` is the ledger handle (an object, not a function): the hook is mocked, so there is none.
const actions = new Proxy({} as RoomActions, { get: (_, key) => (key === 'sends' ? undefined : vi.fn()) })

const member: CurrentUser = { id: 'arjun', name: 'Arjun', layers: ['kafka-connect'], role: 'member' }
const viewer: CurrentUser = { id: 'vera', name: 'Vera', layers: [], role: 'viewer' }
const priya = { kind: 'human' as const, id: 'priya', name: 'Priya', verified: true }
const kabir = { kind: 'human' as const, id: 'kabir', name: 'Kabir', verified: true }

const accepted: HandoffState = {
  handoff_id: 'h1',
  from: kabir,
  to: priya,
  created_at: '2026-10-14T10:05:00Z',
  accepted_at: '2026-10-14T10:09:00Z',
  summary: { what_broke: 'Orders stopped arriving', why: '', done: [], ruled_out: [], open: [], risks: [] },
  stay_as_watcher: false,
  status: 'accepted',
}

function room(over: Partial<RoomState> = {}): RoomState {
  return {
    ...emptyRoomState('r1'),
    title: 'Orders stale',
    pipeline_id: 'orders',
    status: 'open',
    opened_at: '2026-10-14T10:00:00Z',
    driver: { id: 'priya', name: 'Priya', verified: true },
    hypothesis: { text: 'Compat NONE', based_on_steps: [], confidence: 'high', confirmed: false, unbacked_terms: [], seq: 3, verdicts: [] },
    ...over,
  }
}

const closedRoom = (over: Partial<RoomState> = {}) =>
  room({
    status: 'closed',
    closed_at: '2026-10-14T11:12:00Z',
    closed_by: priya,
    resolution: 'Re-registered the schema',
    postmortem: { status: 'drafting' },
    ...over,
  })

function show(user: CurrentUser, state: RoomState) {
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

const header = () => screen.getByRole('banner')

function openMenu() {
  fireEvent.keyDown(screen.getByRole('button', { name: 'More room actions' }), { key: 'Enter' })
  return screen.getByRole('menu')
}

beforeEach(() => {
  vi.clearAllMocks()
  localStorage.clear()
  vi.spyOn(api, 'postmortem').mockResolvedValue('# Post-mortem: Orders stale\n')
})

describe('pinned shift summaries', () => {
  it('an accepted hand-off is pinned above the hypothesis, open', () => {
    show(member, room({ handoffs: [accepted] }))
    const card = screen.getByRole('region', { name: /Shift 1/ })
    expect(within(card).getByRole('button', { name: 'Shift 1 summary' })).toHaveAttribute('aria-expanded', 'true')
    expect(within(card).getByText('Orders stopped arriving')).toBeVisible()
    const hyp = screen.getByText('Compat NONE')
    expect(card.compareDocumentPosition(hyp) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy()
  })
})

describe('header ⋯ menu (open room)', () => {
  it('holds Export JSON, Post-mortem draft and Copy invite link; the invite icon stays', () => {
    show(member, room())
    expect(screen.getByRole('button', { name: 'Invite (copy link)' })).toBeInTheDocument()
    expect(screen.queryByRole('button', { name: /^Export/ })).toBeNull()
    const menu = openMenu()
    expect(within(menu).getAllByRole('menuitem').map((i) => i.textContent)).toEqual([
      'Export JSON',
      'Post-mortem draft',
      'Copy invite link',
    ])
  })

  it('a viewer gets neither Export JSON nor Post-mortem draft (#251 Done-when #3)', () => {
    show(viewer, room())
    const items = within(openMenu()).getAllByRole('menuitem').map((i) => i.textContent)
    expect(items).toEqual(['Copy invite link'])
  })

  it('Post-mortem draft opens the dialog with the server text', async () => {
    show(member, room())
    fireEvent.click(within(openMenu()).getByRole('menuitem', { name: 'Post-mortem draft' }))
    const dialog = await screen.findByRole('dialog', { name: 'Post-mortem draft' })
    expect(await within(dialog).findByRole('textbox', { name: 'Post-mortem draft (Markdown)' })).toHaveValue('# Post-mortem: Orders stale\n')
  })

  it('export refused with 413 export_too_large toasts that retrying will not help', async () => {
    vi.mocked(exportRoom).mockRejectedValue(new ApiClientError(413, 'export_too_large', 'room has 90000 events'))
    show(member, room())
    fireEvent.click(within(openMenu()).getByRole('menuitem', { name: 'Export JSON' }))
    await waitFor(() =>
      expect(toast.error).toHaveBeenCalledWith('This room is too large to export. Retrying will not help; page through its events instead.'),
    )
  })

  it('export refused with 403 not_allowed toasts the role it needs, never the raw message', async () => {
    vi.mocked(exportRoom).mockRejectedValue(new ApiClientError(403, 'not_allowed', 'driver may not export'))
    show(member, room())
    fireEvent.click(within(openMenu()).getByRole('menuitem', { name: 'Export JSON' }))
    await waitFor(() => expect(toast.error).toHaveBeenCalledWith('Export needs an approver or auditor role for orders'))
  })
})

describe('closed room', () => {
  it('header reads Closed · duration · closed by, without the agent pill or the driver line', () => {
    show(member, closedRoom())
    const h = header()
    expect(h).toHaveTextContent(/Closed\s*\S+.*1 h 12 min.*closed by\s*Priya/)
    expect(h).not.toHaveTextContent('is driving')
    expect(h.querySelector('[data-tone]')).toBeNull()
    expect(within(h).getByRole('button', { name: 'Export JSON' })).toBeInTheDocument()
    expect(within(h).getByRole('button', { name: 'Post-mortem draft' })).toBeInTheDocument()
  })

  it('a retention close names Piperoom, not a person', () => {
    show(member, closedRoom({ closed_by: { kind: 'system', id: 'system', name: 'retention' } }))
    expect(header()).toHaveTextContent('closed by Piperoom')
  })

  it('the Resolved strip offers Export JSON and Post-mortem draft at its end', async () => {
    show(member, closedRoom({ closed_at: new Date(Date.now() - 60_000).toISOString() }))
    const strip = screen.getByText('Re-registered the schema').parentElement as HTMLElement
    expect(within(strip).getByRole('button', { name: 'Export JSON' })).toBeInTheDocument()
    fireEvent.click(within(strip).getByRole('button', { name: 'Post-mortem draft' }))
    const dialog = await screen.findByRole('dialog', { name: 'Post-mortem draft' })
    // Drafting: the agent line shows above the (already available) events-only text.
    expect(within(dialog).getByText('The agent is writing the closing summary…')).toBeInTheDocument()
  })

  it('a viewer gets neither button on a closed room, in the header or the Resolved strip', () => {
    show(viewer, closedRoom())
    expect(screen.queryByRole('button', { name: 'Export JSON' })).toBeNull()
    expect(screen.queryByRole('button', { name: 'Post-mortem draft' })).toBeNull()
  })
})
