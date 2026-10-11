import { describe, expect, it, vi } from 'vitest'
import { render, screen } from '@testing-library/react'

const flags = vi.hoisted(() => ({ dev: true }))

vi.mock('../lib/env', () => ({
  get isDev() {
    return flags.dev
  },
}))

vi.mock('../context/user', () => ({
  useCurrentUser: () => ({
    user: { id: 'arjun', name: 'Arjun', layers: ['kafka'] },
    setUserId: vi.fn(),
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
})
