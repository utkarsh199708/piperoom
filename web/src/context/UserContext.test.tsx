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
    api: { ...actual.api, me: vi.fn(), devLogin: vi.fn(), logout: vi.fn() },
  }
})

import { ApiClientError, api } from '../api/client'
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
})

describe('UserProvider (live mode)', () => {
  it('loads the user from /api/me without sending an id', async () => {
    vi.mocked(api.me).mockResolvedValue(meOf('arjun'))
    renderProvider()
    expect(await screen.findByTestId('who')).toHaveTextContent('arjun')
    expect(api.me).toHaveBeenCalledWith()
  })

  it('shows the user picker when /api/me answers 401 session_expired (dev)', async () => {
    vi.mocked(api.me).mockImplementation(() => {
      reportAuthProblem({ kind: 'session_expired', message: 'expired' })
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
    act(() => reportAuthProblem({ kind: 'session_expired', message: 'expired' }))
    expect(await screen.findByText('Sign in')).toBeInTheDocument()
  })

  it('logout calls POST /auth/logout and shows the picker', async () => {
    vi.mocked(api.me).mockResolvedValue(meOf('arjun'))
    vi.mocked(api.logout).mockResolvedValue(undefined)
    renderProvider()
    await screen.findByTestId('who')
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
