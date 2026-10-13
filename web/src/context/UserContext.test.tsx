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
    probeAuthMode: vi.fn(),
    api: { ...actual.api, me: vi.fn(), devLogin: vi.fn(), logout: vi.fn() },
  }
})

import { ApiClientError, api, probeAuthMode } from '../api/client'
import { reportAuthProblem, redirectToLogin } from '../lib/auth'
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

beforeEach(() => {
  vi.mocked(api.me).mockReset()
  vi.mocked(api.devLogin).mockReset()
  vi.mocked(api.logout).mockReset()
  vi.mocked(redirectToLogin).mockReset()
  vi.mocked(probeAuthMode).mockReset()
  vi.mocked(probeAuthMode).mockResolvedValue(undefined)
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
      reportAuthProblem({ kind: 'session_expired', message: 'expired', authMode: 'dev' })
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
  })

  it('goes to the IdP login when the server says oidc', async () => {
    vi.mocked(api.me).mockImplementation(() => {
      reportAuthProblem({ kind: 'session_expired', message: 'expired', authMode: 'oidc' })
      return Promise.reject(expired())
    })
    renderProvider()
    await waitFor(() => expect(redirectToLogin).toHaveBeenCalledTimes(1))
    expect(screen.queryByText('Sign in')).not.toBeInTheDocument()
  })

  it('a session that ends later swaps the page for the picker', async () => {
    vi.mocked(api.me).mockResolvedValue(meOf('arjun'))
    renderProvider()
    await screen.findByTestId('who')
    act(() => reportAuthProblem({ kind: 'session_expired', message: 'expired', authMode: 'dev' }))
    expect(await screen.findByText('Sign in')).toBeInTheDocument()
  })

  it('without auth_mode it asks GET /api/me: dev shows the picker', async () => {
    vi.mocked(api.me).mockResolvedValue(meOf('arjun'))
    vi.mocked(probeAuthMode).mockResolvedValue('dev')
    renderProvider()
    await screen.findByTestId('who')
    act(() => reportAuthProblem({ kind: 'session_expired', message: 'expired' }))
    expect(await screen.findByRole('button', { name: 'Priya' })).toBeInTheDocument()
    expect(probeAuthMode).toHaveBeenCalledTimes(1)
  })

  it('without auth_mode it asks GET /api/me: oidc redirects', async () => {
    vi.mocked(api.me).mockResolvedValue(meOf('arjun'))
    vi.mocked(probeAuthMode).mockResolvedValue('oidc')
    renderProvider()
    await screen.findByTestId('who')
    act(() => reportAuthProblem({ kind: 'session_expired', message: 'expired' }))
    await waitFor(() => expect(redirectToLogin).toHaveBeenCalledTimes(1))
  })

  it('with no answer at all it shows a signed-out notice and never redirects', async () => {
    vi.mocked(api.me).mockResolvedValue(meOf('arjun'))
    renderProvider()
    await screen.findByTestId('who')
    act(() => reportAuthProblem({ kind: 'session_expired', message: 'expired' }))
    expect(await screen.findByText(/You are signed out\. Reload/)).toBeInTheDocument()
    expect(screen.queryByRole('button', { name: 'Priya' })).not.toBeInTheDocument()
    expect(redirectToLogin).not.toHaveBeenCalled()
  })

  it('logout calls POST /auth/logout and shows the picker', async () => {
    vi.mocked(api.me).mockResolvedValue(meOf('arjun'))
    vi.mocked(api.logout).mockResolvedValue(undefined)
    renderProvider()
    await screen.findByTestId('who')
    vi.mocked(probeAuthMode).mockResolvedValue('dev')
    fireEvent.click(screen.getByText('out'))
    expect(await screen.findByText('Sign in')).toBeInTheDocument()
    expect(api.logout).toHaveBeenCalledTimes(1)
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
