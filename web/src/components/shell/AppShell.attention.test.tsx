import { beforeAll, beforeEach, describe, expect, it, vi } from 'vitest'
import { render, screen, within } from '@testing-library/react'
import { MemoryRouter, Route, Routes } from 'react-router-dom'
import type { RoomSnapshot } from '@/api/types'

const ctx = vi.hoisted(() => ({
  user: { id: 'arjun', name: 'Arjun', layers: ['kafka-connect'] },
  rooms: [] as unknown[],
}))
const toastFn = vi.hoisted(() => Object.assign(vi.fn(), { warning: vi.fn(), dismiss: vi.fn(), error: vi.fn() }))

vi.mock('sonner', () => ({ toast: toastFn, Toaster: () => null }))
vi.mock('@/context/user', () => ({
  useCurrentUser: () => ({ user: ctx.user, setUserId: vi.fn(), logout: vi.fn(), sessionEpoch: 0, busy: false }),
}))
vi.mock('@/hooks/useRoomList', () => ({
  useRoomList: () => ({ rooms: ctx.rooms, loading: false, error: null, reload: vi.fn() }),
}))
vi.mock('@/hooks/useTopology', () => ({ useTopology: () => ({ topology: null, error: null, updatedAt: null }) }))

import { ThemeProvider } from '@/context/ThemeContext'
import { AppShell } from './AppShell'

const room = (over: Partial<RoomSnapshot> & { id: string }) =>
  ({ title: over.id, severity: 'sev2', pipeline_id: 'orders', status: 'open', open_proposals: [], ...over }) as RoomSnapshot
const needing = (id: string) =>
  room({
    id,
    title: `Room ${id}`,
    open_proposals: [
      {
        proposal_id: `p-${id}`,
        title: 'fix',
        steps: [],
        risk: '',
        rollback: '',
        required_layers: ['kafka-connect'],
        approvals: { 'kafka-connect': 'pending' },
      },
    ],
  })

const shell = () => (
  <ThemeProvider>
    <MemoryRouter initialEntries={['/']}>
      <Routes>
        <Route element={<AppShell />}>
          <Route path="/" element={<p>rooms home</p>} />
        </Route>
      </Routes>
    </MemoryRouter>
  </ThemeProvider>
)

beforeAll(() => {
  globalThis.ResizeObserver ??= class {
    observe() {}
    unobserve() {}
    disconnect() {}
  }
})

beforeEach(() => {
  toastFn.mockClear()
  document.head.innerHTML = '<link id="favicon" rel="icon" href="/favicon.svg">'
  ctx.rooms = [room({ id: 'r1' })]
})

describe('AppShell attention', () => {
  it('the sidebar says Needs you N while a room needs me, and the favicon carries the dot', () => {
    ctx.rooms = [needing('a'), room({ id: 'b' })]
    render(shell())
    const link = within(screen.getByRole('navigation', { name: 'Main' })).getByRole('link', { name: 'Rooms, 1 need you' })
    expect(link).toHaveTextContent('Needs you 1')
    expect(document.getElementById('favicon')?.getAttribute('href')).toBe('/favicon-alert.svg')
    expect(document.title).toBe('Rooms · Piperoom')
  })

  it('shows the open count when nothing needs me', () => {
    render(shell())
    expect(within(screen.getByRole('navigation', { name: 'Main' })).getByRole('link', { name: /Rooms/ })).toHaveTextContent('1')
    expect(document.getElementById('favicon')?.getAttribute('href')).toBe('/favicon.svg')
  })

  it('a room that newly needs me toasts once; the first load is only a baseline', () => {
    ctx.rooms = [needing('a')]
    const { rerender } = render(shell())
    expect(toastFn).not.toHaveBeenCalled()
    ctx.rooms = [needing('a'), needing('b')]
    rerender(shell())
    rerender(shell())
    expect(toastFn).toHaveBeenCalledTimes(1)
    expect(toastFn.mock.calls[0][0]).toBe('Needs your approval · kafka-connect')
    expect(toastFn.mock.calls[0][1]).toMatchObject({ id: 'proposal:b', action: { label: 'Open' } })
  })
})
