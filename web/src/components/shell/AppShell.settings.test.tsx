import { beforeAll, beforeEach, describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen, within } from '@testing-library/react'
import { MemoryRouter, Route, Routes } from 'react-router-dom'

const ctx = vi.hoisted(() => ({
  user: { id: 'priya', name: 'Priya', layers: [] } as { id: string; name: string; layers: string[]; admin?: boolean },
}))

vi.mock('@/context/user', () => ({
  useCurrentUser: () => ({ user: ctx.user, setUserId: vi.fn(), logout: vi.fn(), sessionEpoch: 0, busy: false }),
}))
vi.mock('@/hooks/useRoomList', () => ({
  useRoomList: () => ({ rooms: [], loading: false, error: null, reload: vi.fn() }),
}))
vi.mock('@/hooks/useTopology', () => ({
  useTopology: () => ({ topology: null, error: null, updatedAt: null }),
}))

import { ThemeProvider } from '@/context/ThemeContext'
import { AppShell } from './AppShell'
import { CommandMenu } from './CommandMenu'

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
  ctx.user = { id: 'priya', name: 'Priya', layers: [] }
})

function renderShell(path = '/') {
  return render(
    <ThemeProvider>
      <MemoryRouter initialEntries={[path]}>
        <Routes>
          <Route element={<AppShell />}>
            <Route path="/" element={<p>rooms home</p>} />
            <Route path="/settings" element={<p>settings page</p>} />
          </Route>
        </Routes>
      </MemoryRouter>
    </ThemeProvider>,
  )
}

describe('Settings navigation', () => {
  it('an admin sees the Settings link, which opens /settings and is the current page there', () => {
    ctx.user = { ...ctx.user, admin: true }
    renderShell()
    const link = within(screen.getByRole('navigation', { name: 'Main' })).getByRole('link', { name: 'Settings' })
    expect(link).toHaveAttribute('href', '/settings')
    fireEvent.click(link)
    expect(screen.getByText('settings page')).toBeInTheDocument()
    expect(within(screen.getByRole('navigation', { name: 'Main' })).getByRole('link', { name: 'Settings' })).toHaveAttribute(
      'aria-current',
      'page',
    )
    expect(screen.getByRole('heading', { level: 1, name: 'Settings' })).toBeInTheDocument()
  })

  it('a member does not see it', () => {
    renderShell()
    expect(within(screen.getByRole('navigation', { name: 'Main' })).queryByRole('link', { name: 'Settings' })).toBeNull()
  })

  it('a user whose admin flag is false does not see it', () => {
    ctx.user = { ...ctx.user, admin: false }
    renderShell()
    expect(screen.queryByRole('link', { name: 'Settings' })).toBeNull()
  })
})

describe('Settings in the ⌘K menu', () => {
  const registry = { get: () => null, set: () => undefined }
  const renderMenu = () =>
    render(
      <ThemeProvider>
        <MemoryRouter>
          <CommandMenu open onOpenChange={() => undefined} rooms={[]} registry={registry} />
        </MemoryRouter>
      </ThemeProvider>,
    )

  it('offers "Go to Settings" to an admin, and finds it by "admin"', () => {
    ctx.user = { ...ctx.user, admin: true }
    renderMenu()
    expect(screen.getByRole('option', { name: /Go to Settings/ })).toBeInTheDocument()
    fireEvent.change(screen.getByRole('combobox'), { target: { value: 'admin' } })
    expect(screen.getByRole('option', { name: /Go to Settings/ })).toBeInTheDocument()
  })

  it('does not offer it to a member', () => {
    renderMenu()
    expect(screen.queryByRole('option', { name: /Go to Settings/ })).toBeNull()
  })
})
