import { beforeAll, describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen } from '@testing-library/react'

// The dev switcher moved from the old header (UserSwitcher) into the account menu:
// the same rules hold. Never in a production build; picking calls setUserId (which
// POSTs /auth/dev-login); Log out calls logout; both are disabled while a login or
// logout is in flight; Log out is hidden in fixture mode (no session).
const flags = vi.hoisted(() => ({ dev: true, fixtures: false }))
const ctx = vi.hoisted(() => ({ setUserId: vi.fn(), logout: vi.fn(), busy: false }))

vi.mock('@/lib/env', () => ({
  get isDev() {
    return flags.dev
  },
}))
vi.mock('@/api/client', () => ({ isFixtures: () => flags.fixtures }))
vi.mock('@/context/user', () => ({
  useCurrentUser: () => ({
    user: { id: 'arjun', name: 'Arjun', layers: ['kafka'] },
    setUserId: ctx.setUserId,
    logout: ctx.logout,
    sessionEpoch: 0,
    busy: ctx.busy,
  }),
}))
vi.mock('@/context/theme', () => ({
  useTheme: () => ({ theme: 'dark', setTheme: vi.fn(), toggleTheme: vi.fn() }),
}))

import { AccountMenu } from './AccountMenu'

beforeAll(() => {
  globalThis.ResizeObserver ??= class {
    observe() {}
    unobserve() {}
    disconnect() {}
  }
})

function openMenu() {
  render(<AccountMenu />)
  // Radix opens a dropdown on Enter / Space / ArrowDown on its trigger.
  fireEvent.keyDown(screen.getByRole('button', { name: 'Account menu' }), { key: 'Enter' })
}

const item = (name: RegExp | string) => screen.queryByRole('menuitem', { name })

describe('AccountMenu dev switcher', () => {
  it('lists the dev users in dev builds, and picking one calls setUserId', () => {
    flags.dev = true
    flags.fixtures = false
    ctx.busy = false
    ctx.setUserId.mockClear()
    openMenu()
    fireEvent.click(item('Switch user to Priya')!)
    expect(ctx.setUserId).toHaveBeenCalledWith('priya')
  })

  it('does not list dev users in a production build', () => {
    flags.dev = false
    flags.fixtures = false
    ctx.busy = false
    openMenu()
    expect(item(/Switch user to/)).not.toBeInTheDocument()
    expect(screen.queryByText('Switch user (dev)')).not.toBeInTheDocument()
    expect(item('Log out')).toBeInTheDocument()
  })

  it('Log out calls logout in dev and in production builds', () => {
    for (const dev of [true, false]) {
      flags.dev = dev
      flags.fixtures = false
      ctx.busy = false
      ctx.logout.mockClear()
      const { unmount } = render(<AccountMenu />)
      fireEvent.keyDown(screen.getByRole('button', { name: 'Account menu' }), { key: 'Enter' })
      fireEvent.click(item('Log out')!)
      expect(ctx.logout).toHaveBeenCalledTimes(1)
      unmount()
    }
  })

  it('disables the switcher and Log out while a login or logout is in flight', () => {
    flags.dev = true
    flags.fixtures = false
    ctx.busy = true
    ctx.logout.mockClear()
    ctx.setUserId.mockClear()
    openMenu()
    expect(item('Switch user to Priya')).toHaveAttribute('aria-disabled', 'true')
    expect(item('Log out')).toHaveAttribute('aria-disabled', 'true')
    fireEvent.click(item('Log out')!)
    fireEvent.click(item('Switch user to Priya')!)
    expect(ctx.logout).not.toHaveBeenCalled()
    expect(ctx.setUserId).not.toHaveBeenCalled()
  })

  it('has no Log out in fixture mode', () => {
    flags.dev = true
    flags.fixtures = true
    ctx.busy = false
    openMenu()
    expect(item('Log out')).not.toBeInTheDocument()
  })
})
