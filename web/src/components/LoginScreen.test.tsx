import { describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen } from '@testing-library/react'
import { LoginScreen } from './LoginScreen'
import { DEV_USERS } from '../dev/users'

describe('LoginScreen', () => {
  it('has the Sign in heading and one button per dev user, named by the person', () => {
    render(<LoginScreen onPick={vi.fn()} busy={false} />)
    expect(screen.getByRole('heading', { name: 'Sign in' })).toBeInTheDocument()
    for (const u of DEV_USERS) {
      expect(screen.getByRole('button', { name: u.name })).toBeInTheDocument()
    }
  })

  it('shows each user\'s layers as badges', () => {
    render(<LoginScreen onPick={vi.fn()} busy={false} />)
    expect(screen.getByText('kafka-connect')).toBeInTheDocument()
    expect(screen.getByText('schema-registry')).toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Priya' })).toHaveAccessibleDescription('postgres schema-registry')
  })

  it('picking a user calls onPick with the id; busy disables the picker', () => {
    const onPick = vi.fn()
    const { rerender } = render(<LoginScreen onPick={onPick} busy={false} />)
    fireEvent.click(screen.getByRole('button', { name: 'Priya' }))
    expect(onPick).toHaveBeenCalledWith('priya')
    rerender(<LoginScreen onPick={onPick} busy />)
    expect(screen.getByRole('button', { name: 'Priya' })).toBeDisabled()
  })

  it('only the first user row is highlighted ("on"), has focus, and shows the Enter key', () => {
    render(<LoginScreen onPick={vi.fn()} busy={false} />)
    const [first, ...others] = DEV_USERS.map((u) => screen.getByRole('button', { name: u.name }))
    expect(first).toHaveAttribute('data-on', 'true')
    expect(first.className).toMatch(/(^|\s)bg-accent(\s|$)/)
    expect(first).toHaveTextContent('⏎')
    expect(first).toHaveFocus()
    for (const o of others) {
      expect(o).not.toHaveAttribute('data-on')
      expect(o.className).not.toMatch(/(^|\s)bg-accent(\s|$)/)
    }
  })

  it('the first row drops its highlight while another row is hovered or focused, so one row is lit', () => {
    render(<LoginScreen onPick={vi.fn()} busy={false} />)
    const first = screen.getByRole('button', { name: DEV_USERS[0].name })
    const second = screen.getByRole('button', { name: DEV_USERS[1].name })
    fireEvent.mouseEnter(second)
    expect(first).not.toHaveAttribute('data-on')
    expect(first.className).not.toMatch(/(^|\s)bg-accent(\s|$)/)
    fireEvent.mouseLeave(second)
    expect(first).toHaveAttribute('data-on', 'true')
    fireEvent.focus(second)
    expect(first).not.toHaveAttribute('data-on')
    fireEvent.blur(second)
    expect(first).toHaveAttribute('data-on', 'true')
  })

  it('the SSO button is a link only when the server has SSO', () => {
    const { rerender } = render(<LoginScreen onPick={vi.fn()} busy={false} />)
    expect(screen.getByRole('button', { name: 'Continue with SSO' })).toBeDisabled()
    rerender(<LoginScreen onPick={vi.fn()} busy={false} ssoHref="/auth/login" />)
    expect(screen.getByRole('link', { name: 'Continue with SSO' })).toHaveAttribute('href', '/auth/login')
  })
})
