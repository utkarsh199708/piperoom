// Q1-web-b (G2): the app-wide banner, and the browser notification, for a person who was paged.
import { afterEach, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen, within } from '@testing-library/react'
import { MemoryRouter, Route, Routes } from 'react-router-dom'
import type { PageSummary, RoomSnapshot } from '@/api/types'

const ctx = vi.hoisted(() => ({
  user: { id: 'kabir', name: 'Kabir', layers: ['consumer'] },
  rooms: [] as unknown[],
  loading: false,
}))
const toastFn = vi.hoisted(() => Object.assign(vi.fn(), { warning: vi.fn(), dismiss: vi.fn(), error: vi.fn() }))

vi.mock('sonner', () => ({ toast: toastFn, Toaster: () => null }))
vi.mock('@/context/user', () => ({
  useCurrentUser: () => ({ user: ctx.user, setUserId: vi.fn(), logout: vi.fn(), sessionEpoch: 0, busy: false }),
}))
vi.mock('@/hooks/useRoomList', () => ({
  useRoomList: () => ({ rooms: ctx.rooms, loading: ctx.loading, error: null, reload: vi.fn() }),
}))
vi.mock('@/hooks/useTopology', () => ({ useTopology: () => ({ topology: null, error: null, updatedAt: null }) }))

import { ThemeProvider } from '@/context/ThemeContext'
import { AppShell } from './AppShell'

const agent = { kind: 'agent' as const, id: 'agent', name: 'Agent' }
const page = (over: Partial<PageSummary> = {}): PageSummary => ({
  layer: 'consumer',
  owners: ['kabir'],
  reason: 'Lag keeps growing on order-sync',
  by: agent,
  at: new Date(Date.now() - 2 * 60_000).toISOString(),
  ...over,
})
const room = (id: string, pages: PageSummary[] = [], over: Partial<RoomSnapshot> = {}) =>
  ({ id, title: `Room ${id}`, severity: 'sev2', pipeline_id: 'orders', status: 'open', open_proposals: [], pages, ...over }) as RoomSnapshot

function shell(path = '/') {
  return (
    <ThemeProvider>
      <MemoryRouter initialEntries={[path]}>
        <Routes>
          <Route element={<AppShell />}>
            <Route path="/" element={<p>rooms home</p>} />
            <Route path="/rooms/:roomId" element={<p>a room</p>} />
          </Route>
        </Routes>
      </MemoryRouter>
    </ThemeProvider>
  )
}

const banner = () => screen.queryByRole('alert', { name: 'You were paged' })

class FakeNotification {
  static permission = 'default'
  static requestPermission = vi.fn(async () => {
    FakeNotification.permission = 'granted'
    return 'granted'
  })
  static shown: { title: string; body?: string; tag?: string }[] = []
  constructor(title: string, opts?: { body?: string; tag?: string }) {
    FakeNotification.shown.push({ title, ...opts })
  }
}

beforeAll(() => {
  globalThis.ResizeObserver ??= class {
    observe() {}
    unobserve() {}
    disconnect() {}
  }
})

beforeEach(() => {
  toastFn.mockClear()
  localStorage.clear()
  document.head.innerHTML = '<link id="favicon" rel="icon" href="/favicon.svg">'
  ctx.user = { id: 'kabir', name: 'Kabir', layers: ['consumer'] }
  ctx.rooms = []
  ctx.loading = false
  FakeNotification.permission = 'default'
  FakeNotification.shown = []
  FakeNotification.requestPermission.mockClear()
  vi.stubGlobal('Notification', FakeNotification)
})

afterEach(() => {
  vi.unstubAllGlobals()
})

describe('the page banner', () => {
  it('names the room, the layer, who paged and why, and links to the room', () => {
    ctx.rooms = [room('r1', [page()])]
    render(shell())
    const b = banner()
    expect(b).not.toBeNull()
    const el = within(b as HTMLElement)
    expect(el.getByText(/You were paged for/)).toHaveTextContent('You were paged for consumer in Room r1')
    expect(b).toHaveTextContent('by the agent')
    expect(b).toHaveTextContent('Lag keeps growing on order-sync')
    expect(el.getByRole('link', { name: 'Open room' })).toHaveAttribute('href', '/rooms/r1')
  })

  it('says so when the server paged by itself', () => {
    ctx.rooms = [room('r1', [page({ auto: true, by: { kind: 'system', id: 'server', name: 'Piperoom' } })])]
    render(shell())
    expect(banner()).toHaveTextContent('automatically')
  })

  it('stays away when the page names someone else, the room is closed, or there is no page', () => {
    ctx.rooms = [
      room('a', [page({ owners: ['meera'] })]),
      room('b', [page()], { status: 'closed' }),
      room('c'),
      room('d', undefined), // an older server sends no pages
    ]
    render(shell())
    expect(banner()).toBeNull()
  })

  it('renders the reason and title as text, never as markup', () => {
    ctx.rooms = [room('r1', [page({ reason: '<img src=x onerror=alert(1)> ok' })], { title: '<b>boom</b>' })]
    const { container } = render(shell())
    expect(container.querySelector('img')).toBeNull()
    expect(container.querySelector('b')).toBeNull()
    expect(banner()).toHaveTextContent('<img src=x onerror=alert(1)> ok')
  })

  it('Dismiss hides it and the browser remembers; a newer page for the same layer brings it back', () => {
    ctx.rooms = [room('r1', [page()])]
    const first = render(shell())
    fireEvent.click(screen.getByRole('button', { name: /Dismiss the page for Room r1/ }))
    expect(banner()).toBeNull()
    expect(JSON.parse(localStorage.getItem('piperoom.pages.seen.kabir') ?? '{}')).toHaveProperty('r1|consumer')
    first.unmount()

    render(shell()) // a reload: the same page stays seen
    expect(banner()).toBeNull()

    ctx.rooms = [room('r1', [page({ at: new Date().toISOString() })])]
    first.unmount()
    render(shell())
    expect(banner()).not.toBeNull()
  })

  it('opening the room counts as seeing the page', () => {
    ctx.rooms = [room('r1', [page()])]
    render(shell())
    fireEvent.click(screen.getByRole('link', { name: 'Open room' }))
    expect(screen.getByText('a room')).toBeInTheDocument()
    expect(banner()).toBeNull()
    expect(localStorage.getItem('piperoom.pages.seen.kabir')).toContain('r1|consumer')
  })

  it('does not show the banner inside the room that paged, and keeps its pages seen afterwards', () => {
    ctx.rooms = [room('r1', [page()])]
    const inRoom = render(shell('/rooms/r1'))
    expect(banner()).toBeNull()
    expect(localStorage.getItem('piperoom.pages.seen.kabir')).toContain('r1|consumer')
    inRoom.unmount()
    render(shell('/'))
    expect(banner()).toBeNull()
  })

  it('lists the first three pages and counts the rest; Dismiss all clears them', () => {
    ctx.rooms = [1, 2, 3, 4, 5].map((n) => room(`r${n}`, [page({ at: new Date(Date.now() - n * 60_000).toISOString() })]))
    render(shell())
    expect(within(banner() as HTMLElement).getAllByRole('link', { name: 'Open room' })).toHaveLength(3)
    expect(banner()).toHaveTextContent('+2 more')
    fireEvent.click(screen.getByRole('button', { name: 'Dismiss all' }))
    expect(banner()).toBeNull()
  })

  it('is per person: another user of the same browser has not seen it', () => {
    ctx.rooms = [room('r1', [page({ owners: ['kabir', 'meera'] })])]
    const kabir = render(shell())
    fireEvent.click(screen.getByRole('button', { name: /Dismiss the page/ }))
    kabir.unmount()
    ctx.user = { id: 'meera', name: 'Meera', layers: ['warehouse'] }
    render(shell())
    expect(banner()).not.toBeNull()
  })

  it('still works when the browser blocks storage', () => {
    vi.stubGlobal('localStorage', {
      getItem: () => {
        throw new Error('blocked')
      },
      setItem: () => {
        throw new Error('blocked')
      },
      removeItem: () => undefined,
    })
    ctx.rooms = [room('r1', [page()])]
    render(shell())
    expect(banner()).not.toBeNull()
    fireEvent.click(screen.getByRole('button', { name: /Dismiss the page/ }))
    expect(banner()).toBeNull() // for this page load
  })
})

describe('the browser notification', () => {
  it('asks for permission only from a click, never on load', () => {
    ctx.rooms = [room('r1', [page()])]
    render(shell())
    expect(FakeNotification.requestPermission).not.toHaveBeenCalled()
    fireEvent.click(screen.getByRole('button', { name: 'Notify me when I am paged' }))
    expect(FakeNotification.requestPermission).toHaveBeenCalledTimes(1)
  })

  it('offers no permission button once it is decided', async () => {
    FakeNotification.permission = 'granted'
    ctx.rooms = [room('r1', [page()])]
    render(shell())
    expect(screen.queryByRole('button', { name: 'Notify me when I am paged' })).toBeNull()
  })

  it('notifies for a page that arrives after the first load, not for the ones already there', () => {
    FakeNotification.permission = 'granted'
    const old = page()
    ctx.rooms = [room('old', [old])]
    const { rerender } = render(shell())
    expect(FakeNotification.shown).toEqual([]) // the first load is a baseline

    ctx.rooms = [room('old', [old]), room('new', [page({ layer: 'warehouse', reason: 'stale table' })], { title: 'Warehouse stale' })]
    rerender(shell())
    rerender(shell())
    expect(FakeNotification.shown).toEqual([
      { title: 'You were paged for warehouse: Warehouse stale', body: 'stale table', tag: 'page:new|warehouse' },
    ])
  })

  it('stays silent without permission', () => {
    ctx.rooms = []
    const { rerender } = render(shell())
    ctx.rooms = [room('r1', [page()])]
    rerender(shell())
    expect(FakeNotification.shown).toEqual([])
    expect(banner()).not.toBeNull() // the banner still tells them
  })

  it('does not notify while the list is loading', () => {
    FakeNotification.permission = 'granted'
    ctx.loading = true
    ctx.rooms = []
    const { rerender } = render(shell())
    ctx.rooms = [room('r1', [page()])]
    rerender(shell())
    expect(FakeNotification.shown).toEqual([])
  })
})
