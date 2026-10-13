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

  it('the SSO button is a link only when the server has SSO', () => {
    const { rerender } = render(<LoginScreen onPick={vi.fn()} busy={false} />)
    expect(screen.getByRole('button', { name: 'Continue with SSO' })).toBeDisabled()
    rerender(<LoginScreen onPick={vi.fn()} busy={false} ssoHref="/auth/login" />)
    expect(screen.getByRole('link', { name: 'Continue with SSO' })).toHaveAttribute('href', '/auth/login')
  })
})
