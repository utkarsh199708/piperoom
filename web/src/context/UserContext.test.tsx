import { beforeEach, describe, expect, it, vi } from 'vitest'
import { act, fireEvent, render, screen, waitFor } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'

vi.mock('../lib/env', () => ({ isDev: true }))

vi.mock('../lib/auth', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../lib/auth')>()
  return { ...actual, redirectToLogin: vi.fn() }
})

vi.mock('../api/client', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../api/client')>()
  return {
    ...actual,
    isFixtures: () => false,
    probeSession: vi.fn(),
    api: { ...actual.api, me: vi.fn(), devLogin: vi.fn(), logout: vi.fn() },
  }
})

import { ApiClientError, api, probeSession } from '../api/client'
import { getAuthEpoch, reportAuthProblem, redirectToLogin } from '../lib/auth'
import { UserProvider } from './UserContext'
import { useCurrentUser } from './user'

function Who() {
  const { user, setUserId, logout, sessionEpoch } = useCurrentUser()
  return (
    <div>
      <span data-testid="who">{user.id}</span>
      <span data-testid="epoch">{sessionEpoch}</span>
      <button onClick={() => setUserId('priya')}>as priya</button>
      <button onClick={logout}>out</button>
    </div>
  )
}

function renderProvider() {
  return render(
    <MemoryRouter>
      <UserProvider>
        <Who />
      </UserProvider>
    </MemoryRouter>,
  )
}

const meOf = (id: string) => ({ id, name: id, layers: [] })
const expired = () => new ApiClientError(401, 'session_expired', 'expired')
const sessionExpired = (extra: { authMode?: 'dev' | 'oidc'; epoch?: number } = {}) =>
  reportAuthProblem({ kind: 'session_expired', message: 'expired', ...extra })

beforeEach(() => {
  vi.mocked(api.me).mockReset()
  vi.mocked(api.devLogin).mockReset()
  vi.mocked(api.logout).mockReset()
  vi.mocked(redirectToLogin).mockReset()
  vi.mocked(redirectToLogin).mockReturnValue(true)
  vi.mocked(probeSession).mockReset()
  vi.mocked(probeSession).mockResolvedValue({ valid: false })
})

describe('UserProvider (live mode)', () => {
  it('loads the user from /api/me without sending an id', async () => {
    vi.mocked(api.me).mockResolvedValue(meOf('arjun'))
    renderProvider()
    expect(await screen.findByTestId('who')).toHaveTextContent('arjun')
    expect(api.me).toHaveBeenCalledWith()
  })

  it('shows the user picker when the 401 says auth_mode dev', async () => {
    vi.mocked(api.me).mockImplementation(() => {
      sessionExpired({ authMode: 'dev' })
      return Promise.reject(expired())
    })
    renderProvider()
    expect(await screen.findByText('Sign in')).toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Priya' })).toBeInTheDocument()
    expect(redirectToLogin).not.toHaveBeenCalled()
  })

  it('picking a user calls dev-login, reloads /api/me and bumps the session epoch', async () => {
    vi.mocked(api.me).mockResolvedValue(meOf('arjun'))
    vi.mocked(api.devLogin).mockResolvedValue(undefined)
    renderProvider()
    await screen.findByTestId('who')
    vi.mocked(api.me).mockResolvedValue(meOf('priya'))
    fireEvent.click(screen.getByText('as priya'))
    await waitFor(() => expect(screen.getByTestId('who')).toHaveTextContent('priya'))
    expect(api.devLogin).toHaveBeenCalledWith('priya')
    expect(screen.getByTestId('epoch')).toHaveTextContent('1')
    expect(getAuthEpoch()).toBe(1)
  })

  it('goes to the IdP login when the server says oidc', async () => {
    vi.mocked(api.me).mockImplementation(() => {
      sessionExpired({ authMode: 'oidc' })
      return Promise.reject(expired())
    })
    renderProvider()
    await waitFor(() => expect(redirectToLogin).toHaveBeenCalledTimes(1))
    expect(screen.queryByText('Sign in')).not.toBeInTheDocument()
  })

  it('shows the Sign in link instead of redirecting when the loop guard says no', async () => {
    vi.mocked(redirectToLogin).mockReturnValue(false)
    vi.mocked(api.me).mockImplementation(() => {
      sessionExpired({ authMode: 'oidc' })
      return Promise.reject(expired())
    })
    renderProvider()
    expect(await screen.findByRole('link', { name: 'Sign in' })).toBeInTheDocument()
  })

  it('a session that ends later swaps the page for the picker', async () => {
    vi.mocked(api.me).mockResolvedValue(meOf('arjun'))
    renderProvider()
    await screen.findByTestId('who')
    act(() => sessionExpired({ authMode: 'dev', epoch: 0 }))
    expect(await screen.findByText('Sign in')).toBeInTheDocument()
  })

  it('without auth_mode it uses the mode from GET /api/me: dev shows the picker', async () => {
    vi.mocked(api.me).mockResolvedValue(meOf('arjun'))
    vi.mocked(probeSession).mockResolvedValue({ valid: false, mode: 'dev' })
    renderProvider()
    await screen.findByTestId('who')
    act(() => sessionExpired())
    expect(await screen.findByRole('button', { name: 'Priya' })).toBeInTheDocument()
  })

  it('without auth_mode it uses the mode from GET /api/me: oidc redirects', async () => {
    vi.mocked(api.me).mockResolvedValue(meOf('arjun'))
    vi.mocked(probeSession).mockResolvedValue({ valid: false, mode: 'oidc' })
    renderProvider()
    await screen.findByTestId('who')
    act(() => sessionExpired())
    await waitFor(() => expect(redirectToLogin).toHaveBeenCalledTimes(1))
  })

  it('with no answer at all it shows a signed-out notice and never redirects', async () => {
    vi.mocked(api.me).mockResolvedValue(meOf('arjun'))
    renderProvider()
    await screen.findByTestId('who')
    act(() => sessionExpired())
    expect(await screen.findByText(/You are signed out\. Reload/)).toBeInTheDocument()
    expect(screen.queryByRole('button', { name: 'Priya' })).not.toBeInTheDocument()
    expect(redirectToLogin).not.toHaveBeenCalled()
  })

  it('a valid session is never "signed out": a 200 probe reloads the user', async () => {
    vi.mocked(api.me).mockResolvedValue(meOf('arjun'))
    renderProvider()
    await screen.findByTestId('who')
    vi.mocked(probeSession).mockResolvedValue({ valid: true })
    vi.mocked(api.me).mockResolvedValue(meOf('priya'))
    act(() => sessionExpired({ authMode: 'dev', epoch: 0 }))
    await waitFor(() => expect(screen.getByTestId('who')).toHaveTextContent('priya'))
    expect(screen.queryByText('Sign in')).not.toBeInTheDocument()
    expect(screen.getByTestId('epoch')).toHaveTextContent('1')
  })

  it('ignores session_expired while the picker is already showing', async () => {
    vi.mocked(api.me).mockResolvedValue(meOf('arjun'))
    renderProvider()
    await screen.findByTestId('who')
    act(() => sessionExpired({ authMode: 'dev', epoch: 0 }))
    await screen.findByText('Sign in')
    vi.mocked(probeSession).mockClear()
    act(() => sessionExpired({ authMode: 'dev', epoch: 1 }))
    await new Promise((r) => setTimeout(r, 20))
    expect(probeSession).not.toHaveBeenCalled()
  })

  it('dev-login revokes the old session: the old socket error frame + close end in "ready"', async () => {
    vi.mocked(api.me).mockResolvedValue(meOf('arjun'))
    renderProvider()
    await screen.findByTestId('who')
    // The server revokes the old cookie during dev-login and closes the old room
    // socket: an error frame, then a close with reason session_expired. Both
    // reach the provider while the dev-login is still in flight.
    vi.mocked(api.devLogin).mockImplementation(async () => {
      sessionExpired({ authMode: 'dev', epoch: 0 })
      sessionExpired({ authMode: 'dev', epoch: 0 })
    })
    vi.mocked(api.me).mockResolvedValue(meOf('priya'))
    fireEvent.click(screen.getByText('as priya'))
    await waitFor(() => expect(screen.getByTestId('who')).toHaveTextContent('priya'))
    // ...and a late report from the old socket after the switch is stale.
    act(() => sessionExpired({ authMode: 'dev', epoch: 0 }))
    await new Promise((r) => setTimeout(r, 20))
    expect(screen.getByTestId('who')).toHaveTextContent('priya')
    expect(screen.queryByText('Sign in')).not.toBeInTheDocument()
    expect(probeSession).not.toHaveBeenCalled()
  })

  it('logout calls POST /auth/logout and shows the picker', async () => {
    vi.mocked(api.me).mockResolvedValue(meOf('arjun'))
    vi.mocked(api.logout).mockResolvedValue(undefined)
    vi.mocked(probeSession).mockResolvedValue({ valid: false, mode: 'dev' })
    renderProvider()
    await screen.findByTestId('who')
    fireEvent.click(screen.getByText('out'))
    expect(await screen.findByText('Sign in')).toBeInTheDocument()
    expect(api.logout).toHaveBeenCalledTimes(1)
  })

  it('logout in oidc mode shows the Sign in link and does not redirect', async () => {
    vi.mocked(api.me).mockResolvedValue(meOf('arjun'))
    // The server closes the user's sockets on logout: that report must not redirect.
    vi.mocked(api.logout).mockImplementation(async () => {
      sessionExpired({ authMode: 'oidc', epoch: 0 })
    })
    vi.mocked(probeSession).mockResolvedValue({ valid: false, mode: 'oidc' })
    renderProvider()
    await screen.findByTestId('who')
    fireEvent.click(screen.getByText('out'))
    expect(await screen.findByRole('link', { name: 'Sign in' })).toBeInTheDocument()
    act(() => sessionExpired({ authMode: 'oidc', epoch: 0 }))
    await new Promise((r) => setTimeout(r, 20))
    expect(redirectToLogin).not.toHaveBeenCalled()
  })

  it.each([
    ['proxy_identity', /PIPEROOM_AUTH_MODE=oidc/],
    ['bad_host', /PIPEROOM_ALLOWED_HOSTS/],
    ['viewer', /read-only/],
  ] as const)('shows a banner for %s', async (kind, text) => {
    vi.mocked(api.me).mockResolvedValue(meOf('arjun'))
    renderProvider()
    await screen.findByTestId('who')
    act(() => reportAuthProblem({ kind, message: '' }))
    expect(await screen.findByRole('alert')).toHaveTextContent(text)
  })
})
