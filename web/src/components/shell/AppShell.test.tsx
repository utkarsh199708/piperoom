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
  useRoomList: () => ({ rooms: ctx.rooms, loading: false, error: null, reload: vi.fn() }),
}))
vi.mock('@/hooks/useTopology', () => ({
  useTopology: () => ({ topology: ctx.topology, error: null, updatedAt: null }),
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
  window.localStorage.clear()
  document.documentElement.removeAttribute('data-theme')
})

describe('AppShell sidebar', () => {
  it('shows the nav with the open-room count and a pipeline with its health', () => {
    renderShell()
    const main = screen.getByRole('navigation', { name: 'Main' })
    expect(within(main).getByRole('link', { name: /Rooms/ })).toHaveTextContent('1')
    const pipelines = screen.getByRole('navigation', { name: 'Pipelines' })
    const link = within(pipelines).getByRole('link', { name: /orders/ })
    expect(link).toHaveTextContent('failing')
    expect(link).toHaveAttribute('href', '/?pipeline=orders')
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

  it('opens with Ctrl+K, lists rooms, and going to one navigates', async () => {
    renderShell()
    open()
    const dialog = screen.getByRole('dialog', { name: 'Command menu' })
    expect(within(dialog).getByText('Go to a room')).toBeInTheDocument()
    fireEvent.click(within(dialog).getByText('Warehouse orders stale'))
    await vi.waitFor(() => expect(screen.getByTestId('where')).toHaveTextContent('/rooms/r1'))
  })

  it('has no room actions when no room is open', () => {
    renderShell()
    open()
    expect(screen.queryByText('Hand off the room')).not.toBeInTheDocument()
    expect(screen.queryByText('Room')).not.toBeInTheDocument()
  })

  it('offers only the room actions the room screen registered, and runs them', async () => {
    const handoff = vi.fn()
    roomCommands = { title: 'Warehouse orders stale', handoff, trace: vi.fn() }
    renderShell('/rooms/r1')
    open()
    const dialog = screen.getByRole('dialog', { name: 'Command menu' })
    expect(within(dialog).getByText('Trace a record through the pipeline')).toBeInTheDocument()
    expect(within(dialog).queryByText('Take control')).not.toBeInTheDocument()
    expect(within(dialog).queryByText('Request control')).not.toBeInTheDocument()
    fireEvent.click(within(dialog).getByText('Hand off the room'))
    await vi.waitFor(() => expect(handoff).toHaveBeenCalledTimes(1))
  })

  it('a viewer registers no writes, so the menu shows no Room group', () => {
    ctx.user = { id: 'vera', name: 'Vera', layers: [], role: 'viewer' }
    roomCommands = { title: 'Warehouse orders stale' }
    renderShell('/rooms/r1')
    open()
    expect(screen.queryByText('Hand off the room')).not.toBeInTheDocument()
    expect(screen.queryByText('Take control')).not.toBeInTheDocument()
  })

  it('Sign out calls logout', async () => {
    renderShell()
    open()
    fireEvent.click(screen.getByText('Sign out'))
    await vi.waitFor(() => expect(ctx.logout).toHaveBeenCalledTimes(1))
  })

  it('Toggle theme works from the menu', async () => {
    renderShell()
    open()
    fireEvent.click(screen.getByText('Toggle theme'))
    await vi.waitFor(() => expect(document.documentElement.getAttribute('data-theme')).toBe('light'))
  })
})
