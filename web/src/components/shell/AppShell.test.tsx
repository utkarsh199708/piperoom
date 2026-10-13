import { beforeAll, beforeEach, describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen, within } from '@testing-library/react'
import { MemoryRouter, Route, Routes, useLocation } from 'react-router-dom'
import type { RoomSnapshot, Topology } from '@/api/types'

const ctx = vi.hoisted(() => ({
  user: { id: 'arjun', name: 'Arjun', layers: ['kafka-connect'] } as {
    id: string
    name: string
    layers: string[]
    role?: 'viewer'
  },
  logout: vi.fn(),
  reload: vi.fn(),
  setUserId: vi.fn(),
  rooms: [] as unknown[],
  topology: null as unknown,
}))

vi.mock('@/context/user', () => ({
  useCurrentUser: () => ({
    user: ctx.user,
    setUserId: ctx.setUserId,
    logout: ctx.logout,
    sessionEpoch: 0,
    busy: false,
  }),
}))
vi.mock('@/hooks/useRoomList', () => ({
  useRoomList: () => ({ rooms: ctx.rooms, loading: false, error: null, reload: ctx.reload }),
}))
const topologyCalls = vi.hoisted(() => [] as { id: string; active?: boolean }[])
vi.mock('@/hooks/useTopology', () => ({
  useTopology: (id: string, opts?: { active?: boolean }) => {
    topologyCalls.push({ id, active: opts?.active })
    return { topology: id ? ctx.topology : null, error: null, updatedAt: null }
  },
}))

import { ThemeProvider } from '@/context/ThemeContext'
import { THEME_KEY } from '@/lib/theme'
import { AppShell } from './AppShell'
import { useRegisterRoomCommands } from './context'
import type { RoomCommands } from './context'

const room = (over: Partial<RoomSnapshot> & { id: string }) =>
  ({ title: over.id, severity: 'sev2', pipeline_id: 'orders', status: 'open', ...over }) as RoomSnapshot

function Where() {
  return <span data-testid="where">{useLocation().pathname}</span>
}

let roomCommands: RoomCommands | null = null
function FakeRoom() {
  useRegisterRoomCommands(roomCommands)
  return <p>room screen</p>
}

function renderShell(path = '/') {
  return render(
    <ThemeProvider>
      <MemoryRouter initialEntries={[path]}>
        <Routes>
          <Route element={<AppShell />}>
            <Route path="/" element={<p>rooms home</p>} />
            <Route path="/rooms/:roomId" element={<FakeRoom />} />
          </Route>
        </Routes>
        <Where />
      </MemoryRouter>
    </ThemeProvider>,
  )
}

beforeAll(() => {
  // jsdom lacks these; cmdk and Radix use them.
  globalThis.ResizeObserver ??= class {
    observe() {}
    unobserve() {}
    disconnect() {}
  }
  Element.prototype.scrollIntoView ??= () => {}
})

beforeEach(() => {
  ctx.user = { id: 'arjun', name: 'Arjun', layers: ['kafka-connect'] }
  ctx.rooms = [
    room({ id: 'r1', title: 'Warehouse orders stale' }),
    room({ id: 'r2', title: 'Old lag', status: 'closed', severity: 'sev3' }),
  ]
  ctx.topology = { pipeline_id: 'orders', nodes: [{ id: 'a', health: 'fail' }], edges: [] } as unknown as Topology
  roomCommands = null
  topologyCalls.length = 0
  window.localStorage.clear()
  document.documentElement.removeAttribute('data-theme')
})

describe('AppShell sidebar', () => {
  it('shows the nav with the open-room count and a pipeline with its health', () => {
    renderShell()
    const main = screen.getByRole('navigation', { name: 'Main' })
    expect(within(main).getByRole('link', { name: /Rooms/ })).toHaveTextContent('1')
    const pipelines = screen.getByRole('list', { name: 'Pipelines' })
    const row = within(pipelines).getByText('orders').closest('li')!
    expect(row).toHaveTextContent('failing')
    expect(row).toHaveTextContent('1 open')
    // Not a link until the room list can filter by pipeline.
    expect(within(pipelines).queryByRole('link')).not.toBeInTheDocument()
    expect(screen.getByText('rooms home')).toBeInTheDocument()
  })

  it('lists Pipelines and Audit log as unavailable, not as dead links', () => {
    renderShell()
    const main = screen.getByRole('navigation', { name: 'Main' })
    expect(within(main).getAllByText(/not available yet/)).toHaveLength(2)
    expect(within(main).queryByRole('link', { name: /Audit log/ })).not.toBeInTheDocument()
  })

  it('shows the user and their layers; a viewer is marked view only', () => {
    const first = renderShell()
    expect(screen.getByText('kafka-connect owner')).toBeInTheDocument()
    first.unmount()
    ctx.user = { id: 'vera', name: 'Vera', layers: [], role: 'viewer' }
    renderShell()
    expect(screen.getByText('View only')).toBeInTheDocument()
  })
})

describe('AppShell theme', () => {
  it('toggles the theme, puts it on <html> and remembers it', () => {
    renderShell()
    fireEvent.click(screen.getByRole('button', { name: /Switch to (light|dark) theme/ }))
    const now = document.documentElement.getAttribute('data-theme')
    expect(now).toBe('light') // jsdom: dark by default, so the first toggle is light
    expect(window.localStorage.getItem(THEME_KEY)).toBe('light')
    fireEvent.click(screen.getByRole('button', { name: 'Switch to dark theme' }))
    expect(document.documentElement.getAttribute('data-theme')).toBe('dark')
  })
})

describe('command menu', () => {
  const open = () => fireEvent.keyDown(window, { key: 'k', ctrlKey: true })
  // The menu is a lazy chunk: it appears a moment after the first open.
  const menu = () => screen.findByRole('dialog', { name: 'Command menu' })

  it('opens with Ctrl+K, lists rooms, and going to one navigates', async () => {
    renderShell()
    open()
    const dialog = await menu()
    expect(within(dialog).getByText('Go to a room')).toBeInTheDocument()
    fireEvent.click(within(dialog).getByText('Warehouse orders stale'))
    await vi.waitFor(() => expect(screen.getByTestId('where')).toHaveTextContent('/rooms/r1'))
  })

  it('has no room actions when no room is open', async () => {
    renderShell()
    open()
    await menu()
    expect(screen.queryByText('Hand off the room')).not.toBeInTheDocument()
    expect(screen.queryByText('Room')).not.toBeInTheDocument()
  })

  it('offers only the room actions the room screen registered, and runs them', async () => {
    const handoff = vi.fn()
    roomCommands = { title: 'Warehouse orders stale', handoff, trace: vi.fn() }
    renderShell('/rooms/r1')
    open()
    const dialog = await menu()
    expect(within(dialog).getByText('Trace a record through the pipeline')).toBeInTheDocument()
    expect(within(dialog).queryByText('Take control')).not.toBeInTheDocument()
    expect(within(dialog).queryByText('Request control')).not.toBeInTheDocument()
    fireEvent.click(within(dialog).getByText('Hand off the room'))
    await vi.waitFor(() => expect(handoff).toHaveBeenCalledTimes(1))
  })

  it('a viewer registers no writes, so the menu shows no Room group', async () => {
    ctx.user = { id: 'vera', name: 'Vera', layers: [], role: 'viewer' }
    roomCommands = { title: 'Warehouse orders stale' }
    renderShell('/rooms/r1')
    open()
    await menu()
    expect(screen.queryByText('Hand off the room')).not.toBeInTheDocument()
    expect(screen.queryByText('Take control')).not.toBeInTheDocument()
  })

  it('Sign out calls logout', async () => {
    renderShell()
    open()
    await menu()
    fireEvent.click(screen.getByText('Sign out'))
    await vi.waitFor(() => expect(ctx.logout).toHaveBeenCalledTimes(1))
  })

  it('Toggle theme works from the menu', async () => {
    renderShell()
    open()
    await menu()
    fireEvent.click(screen.getByText('Toggle theme'))
    await vi.waitFor(() => expect(document.documentElement.getAttribute('data-theme')).toBe('light'))
  })
})

describe('shortcuts', () => {
  it('Ctrl+Shift+L toggles the theme, but not while typing in a field', () => {
    renderShell()
    const field = document.createElement('input')
    document.body.appendChild(field)
    fireEvent.keyDown(field, { key: 'L', ctrlKey: true, shiftKey: true })
    expect(document.documentElement.getAttribute('data-theme')).toBeNull()
    field.remove()
    fireEvent.keyDown(window, { key: 'L', ctrlKey: true, shiftKey: true })
    expect(document.documentElement.getAttribute('data-theme')).toBe('light')
  })

  it('a held key (repeat) or an IME composition does nothing', () => {
    renderShell()
    fireEvent.keyDown(window, { key: 'L', ctrlKey: true, shiftKey: true, repeat: true })
    fireEvent.keyDown(window, { key: 'L', ctrlKey: true, shiftKey: true, isComposing: true })
    expect(document.documentElement.getAttribute('data-theme')).toBeNull()
  })

  it('Ctrl+K opens the menu even from a text field', async () => {
    renderShell()
    const field = document.createElement('textarea')
    document.body.appendChild(field)
    fireEvent.keyDown(field, { key: 'k', ctrlKey: true })
    expect(await screen.findByRole('dialog', { name: 'Command menu' })).toBeInTheDocument()
    field.remove()
  })
})

describe('sidebar polling', () => {
  it('fetches only pipelines that have an open room, at most ten rows', () => {
    ctx.rooms = [
      ...Array.from({ length: 12 }, (_, i) => room({ id: 'o' + i, pipeline_id: 'p' + String(i).padStart(2, '0') })),
      room({ id: 'c1', pipeline_id: 'only-closed', status: 'closed' }),
    ]
    renderShell()
    const list = screen.getByRole('list', { name: 'Pipelines' })
    expect(within(list).getAllByRole('listitem')).toHaveLength(10)
    const fetched = new Set(topologyCalls.map((c) => c.id).filter(Boolean))
    expect(fetched.size).toBe(10)
    expect(fetched.has('only-closed')).toBe(false)
  })

  it('a pipeline with only closed rooms is listed but never fetched', () => {
    ctx.rooms = [room({ id: 'c1', pipeline_id: 'old', status: 'closed' })]
    renderShell()
    expect(screen.getByText('old')).toBeInTheDocument()
    expect(topologyCalls.every((c) => c.id === '')).toBe(true)
  })

  it('does not poll the pipeline of the room on screen (the room screen already does)', () => {
    ctx.rooms = [room({ id: 'r1', pipeline_id: 'orders' }), room({ id: 'r2', pipeline_id: 'billing' })]
    renderShell('/rooms/r1')
    const orders = topologyCalls.filter((c) => c.id === 'orders')
    const billing = topologyCalls.filter((c) => c.id === 'billing')
    expect(orders.length).toBeGreaterThan(0)
    expect(orders.every((c) => c.active === false)).toBe(true)
    expect(billing.every((c) => c.active === true)).toBe(true)
  })

  it('mounts one sidebar only: nothing is rendered or fetched for a hidden second one', () => {
    renderShell()
    expect(screen.getAllByLabelText('Sidebar')).toHaveLength(1)
    expect(screen.getAllByRole('list', { name: 'Pipelines' })).toHaveLength(1)
  })
})
