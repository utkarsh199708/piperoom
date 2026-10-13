import { describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen } from '@testing-library/react'

const flags = vi.hoisted(() => ({ dev: true }))

vi.mock('../lib/env', () => ({
  get isDev() {
    return flags.dev
  },
}))

const ctx = vi.hoisted(() => ({ setUserId: vi.fn(), logout: vi.fn(), busy: false }))

vi.mock('../context/user', () => ({
  useCurrentUser: () => ({
    user: { id: 'arjun', name: 'Arjun', layers: ['kafka'] },
    setUserId: ctx.setUserId,
    logout: ctx.logout,
    sessionEpoch: 0,
    busy: ctx.busy,
  }),
}))

import { UserSwitcher } from './UserSwitcher'

describe('UserSwitcher dev gating', () => {
  it('renders the switcher in dev builds', () => {
    flags.dev = true
    render(<UserSwitcher />)
    expect(screen.getByLabelText('Switch user')).toBeInTheDocument()
  })

  it('hides the switcher in non-dev builds', () => {
    flags.dev = false
    render(<UserSwitcher />)
    expect(screen.queryByLabelText('Switch user')).not.toBeInTheDocument()
  })

  it('picking a user in dev calls setUserId (which POSTs /auth/dev-login)', () => {
    flags.dev = true
    render(<UserSwitcher />)
    fireEvent.change(screen.getByLabelText('Switch user'), { target: { value: 'priya' } })
    expect(ctx.setUserId).toHaveBeenCalledWith('priya')
  })

  it('Log out calls logout in dev and in production builds', () => {
    for (const dev of [true, false]) {
      flags.dev = dev
      ctx.logout.mockClear()
      const { unmount } = render(<UserSwitcher />)
      fireEvent.click(screen.getByRole('button', { name: 'Log out' }))
      expect(ctx.logout).toHaveBeenCalledTimes(1)
      unmount()
    }
  })
})

describe('UserSwitcher busy state', () => {
  it('disables the select while a dev-login is in flight', () => {
    flags.dev = true
    ctx.busy = true
    const { unmount } = render(<UserSwitcher />)
    expect(screen.getByLabelText('Switch user')).toBeDisabled()
    unmount()
    ctx.busy = false
    render(<UserSwitcher />)
    expect(screen.getByLabelText('Switch user')).toBeEnabled()
  })
})
