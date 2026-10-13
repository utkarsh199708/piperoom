import { useCallback, useEffect, useRef, useState } from 'react'
import type { ReactNode } from 'react'
import { useSearchParams } from 'react-router-dom'
import { DEFAULT_USER_ID, findUser } from '../dev/users'
import type { CurrentUser } from '../state/permissions'
import { ApiClientError, api, isFixtures, probeAuthMode } from '../api/client'
import { loginRedirectUrl, onAuthProblem, redirectToLogin, resolveAuthMode, saveReturnHash } from '../lib/auth'
import type { AuthMode, AuthProblem } from '../lib/auth'
import { errorText } from '../lib/errors'
import { AuthBanner } from '../components/AuthBanner'
import { LoginScreen } from '../components/LoginScreen'
import { UserContext } from './user'

type Phase = 'loading' | 'ready' | 'login' | 'failed'

export function UserProvider({ children }: { children: ReactNode }) {
  return isFixtures() ? <FixtureUserProvider>{children}</FixtureUserProvider> : <LiveUserProvider>{children}</LiveUserProvider>
}

// Fixture mode has no server and no session; the client copy of dev/users.yaml
// stands in and the `user` query parameter picks who you are.
function FixtureUserProvider({ children }: { children: ReactNode }) {
  const [searchParams, setSearchParams] = useSearchParams()
  const id = searchParams.get('user') ?? DEFAULT_USER_ID
  const dev = findUser(id)

  const setUserId = (next: string) => {
    setSearchParams((prev) => {
      const p = new URLSearchParams(prev)
      if (next) p.set('user', next)
      else p.delete('user')
      return p
    })
  }

  if (!dev) {
    return (
      <div className="shell">
        <main className="roomlist">
          <p className="empty">Unknown user: {id}</p>
        </main>
      </div>
    )
  }
  const user: CurrentUser = { id: dev.id, name: dev.name, layers: dev.layers }
  return (
    <UserContext.Provider value={{ user, setUserId, logout: () => undefined, sessionEpoch: 0 }}>
      {children}
    </UserContext.Provider>
  )
}

// Live mode: identity comes from the server session (HttpOnly cookie), read
// through GET /api/me. The client never says who it is.
function LiveUserProvider({ children }: { children: ReactNode }) {
  const [user, setUser] = useState<CurrentUser | null>(null)
  const [phase, setPhase] = useState<Phase>('loading')
  // null = the server did not say (see resolveAuthMode): show a plain signed-out notice.
  const [mode, setMode] = useState<AuthMode | null>(null)
  const [epoch, setEpoch] = useState(0)
  const [busy, setBusy] = useState(false)
  const [problem, setProblem] = useState<AuthProblem | null>(null)
  const [failure, setFailure] = useState<string | null>(null)

  // Auth failures from any fetch or from the WebSocket arrive here. The server
  // mode comes from error.auth_mode (or the VITE_AUTH_MODE override); if the 401
  // did not carry it, ask GET /api/me. Redirect to /auth/login only for "oidc".
  const expiring = useRef(false)
  useEffect(
    () =>
      onAuthProblem((p) => {
        if (p.kind !== 'session_expired') {
          setProblem(p)
          return
        }
        if (expiring.current) return
        expiring.current = true
        void (async () => {
          try {
            const m = resolveAuthMode(p.authMode) ?? (await probeAuthMode())
            if (m === 'oidc') {
              redirectToLogin()
              return
            }
            setMode(m ?? null)
            setUser(null)
            setPhase('login')
            setEpoch((n) => n + 1)
          } finally {
            expiring.current = false
          }
        })()
      }),
    [],
  )

  const loadMe = useCallback(async () => {
    try {
      const me = await api.me()
      setUser({ id: me.id, name: me.name, layers: me.layers })
      setPhase('ready')
      setFailure(null)
    } catch (e) {
      // session_expired was already routed to the login screen by the listener.
      if (e instanceof ApiClientError && e.code === 'session_expired') return
      setFailure(errorText(e))
      setPhase((p) => (p === 'loading' ? 'failed' : p))
    }
  }, [])

  useEffect(() => {
    void loadMe()
  }, [loadMe])

  const setUserId = useCallback(
    (id: string) => {
      setBusy(true)
      void (async () => {
        try {
          await api.devLogin(id)
          await loadMe()
          setEpoch((n) => n + 1) // reload data, reconnect the WebSocket
        } catch (e) {
          setFailure(errorText(e))
        } finally {
          setBusy(false)
        }
      })()
    },
    [loadMe],
  )

  const logout = useCallback(() => {
    void (async () => {
      try {
        await api.logout()
      } catch {
        // an expired session is as good as logged out
      }
      // After logout never jump to the IdP by itself (its own session would sign
      // the person straight back in): show the sign-in choice for the mode.
      setMode(resolveAuthMode() ?? (await probeAuthMode()) ?? null)
      setUser(null)
      setPhase('login')
      setEpoch((n) => n + 1)
    })()
  }, [])

  const banners = (
    <>
      {problem && <AuthBanner problem={problem} onDismiss={() => setProblem(null)} />}
      {failure && <div className="banner banner--error" role="alert">{failure}</div>}
    </>
  )

  if (phase === 'login') {
    return (
      <div className="shell">
        {banners}
        {mode === 'dev' ? (
          <LoginScreen onPick={setUserId} busy={busy} />
        ) : mode === 'oidc' ? (
          <main className="roomlist">
            <p className="empty">
              You are signed out.{' '}
              <a href={loginRedirectUrl()} onClick={() => saveReturnHash()}>
                Sign in
              </a>
            </p>
          </main>
        ) : (
          <main className="roomlist">
            <div className="banner banner--error" role="alert">
              <span>You are signed out. Reload the page to sign in again.</span>
              <button type="button" className="btn btn--ghost btn--small" onClick={() => window.location.reload()}>
                Reload
              </button>
            </div>
          </main>
        )}
      </div>
    )
  }

  if (!user) {
    return (
      <div className="shell">
        {banners}
        <main className="roomlist">
          <p className="empty">{phase === 'failed' ? 'Could not load your user.' : 'Loading user…'}</p>
        </main>
      </div>
    )
  }

  return (
    <UserContext.Provider value={{ user, setUserId, logout, sessionEpoch: epoch }}>
      {banners}
      {children}
    </UserContext.Provider>
  )
}
