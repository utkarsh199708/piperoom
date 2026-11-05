import { beforeEach, describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen } from '@testing-library/react'
import { LoginScreen } from './LoginScreen'

const DEV_USERS = [
  { id: 'arjun', name: 'Arjun', layers: ['kafka-connect', 'kafka'] },
  { id: 'priya', name: 'Priya', layers: ['postgres', 'schema-registry'] },
  { id: 'meera', name: 'Meera', layers: ['warehouse'] },
  { id: 'kabir', name: 'Kabir', layers: ['consumer'] },
]
const loaded = vi.hoisted(() => ({ value: {} as Record<string, unknown> }))
vi.mock('../hooks/useAuthUsers', () => ({ useAuthUsers: () => loaded.value }))

beforeEach(() => {
  loaded.value = { status: 'ready', reload: vi.fn(), data: DEV_USERS }
})

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

  it('renders the batch layers orchestrator and transform with their own badge colours', () => {
    loaded.value = {
      status: 'ready',
      reload: vi.fn(),
      data: [{ id: 'nina', name: 'Nina', layers: ['orchestrator', 'transform'] }],
    }
    render(<LoginScreen onPick={vi.fn()} busy={false} />)
    expect(screen.getByText('orchestrator')).toHaveClass('text-layer-orchestrator')
    expect(screen.getByText('transform')).toHaveClass('text-layer-transform')
    expect(screen.getByRole('button', { name: 'Nina' })).toHaveAccessibleDescription('orchestrator transform')
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

  it('the SSO button is a link when the server has SSO, and a quiet footnote when it does not', () => {
    const { rerender } = render(<LoginScreen onPick={vi.fn()} busy={false} />)
    // No greyed-out primary button: a dead control at the top of the card reads as broken.
    expect(screen.queryByRole('button', { name: 'Continue with SSO' })).not.toBeInTheDocument()
    expect(screen.getByText(/SSO is not enabled on this server/)).toBeInTheDocument()
    rerender(<LoginScreen onPick={vi.fn()} busy={false} ssoHref="/auth/login" />)
    expect(screen.getByRole('link', { name: 'Continue with SSO' })).toHaveAttribute('href', '/auth/login')
  })

  it('shows the load state, then an error with Try again, never a built-in list', () => {
    const reload = vi.fn()
    loaded.value = { status: 'loading', reload }
    const { rerender } = render(<LoginScreen onPick={vi.fn()} busy={false} />)
    expect(screen.getByRole('status', { name: 'Loading users' })).toBeInTheDocument()
    expect(screen.queryByRole('button', { name: 'Priya' })).not.toBeInTheDocument()
    loaded.value = { status: 'error', message: 'not_found: nope', reload }
    rerender(<LoginScreen onPick={vi.fn()} busy={false} />)
    expect(screen.getByRole('alert')).toHaveTextContent('Could not load the user list: not_found: nope')
    fireEvent.click(screen.getByRole('button', { name: 'Try again' }))
    expect(reload).toHaveBeenCalledTimes(1)
    expect(screen.queryByRole('button', { name: 'Priya' })).not.toBeInTheDocument()
  })

  it('an empty list says the server has no users', () => {
    loaded.value = { status: 'ready', reload: vi.fn(), data: [] }
    render(<LoginScreen onPick={vi.fn()} busy={false} />)
    expect(screen.getByText(/no users to sign in as/)).toBeInTheDocument()
  })
})

describe('LoginScreen — why you are here', () => {
  it('names the room after an expired session', () => {
    render(<LoginScreen onPick={vi.fn()} busy={false} notice={{ kind: 'expired', title: 'orders stale' }} />)
    expect(screen.getByText('Your session ended \u2014 sign in to return to \u201corders stale\u201d.')).toBeInTheDocument()
  })
  it('explains a sign-in in another tab', () => {
    render(<LoginScreen onPick={vi.fn()} busy={false} notice={{ kind: 'replaced', title: 'orders stale' }} />)
    expect(screen.getByText(/signed in as another user in a different tab/)).toBeInTheDocument()
  })
  it('shows nothing without a notice', () => {
    render(<LoginScreen onPick={vi.fn()} busy={false} />)
    expect(screen.queryByRole('status')).not.toBeInTheDocument()
  })
})
