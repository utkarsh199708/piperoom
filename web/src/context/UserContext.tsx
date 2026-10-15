import { useCallback, useEffect, useRef, useState } from 'react'
import { clearSigninNotice, noticeForExpiredSession } from '../lib/signinNotice'
import type { SigninNotice } from '../lib/signinNotice'
import type { ReactNode } from 'react'
import { useSearchParams } from 'react-router-dom'
import { DEFAULT_USER_ID, findUser } from '../fixtures/users'
import type { CurrentUser } from '../state/permissions'
import { ApiClientError, api, isFixtures, probeSession } from '../api/client'
import {
  loginRedirectUrl,
  onAuthProblem,
  redirectToLogin,
  resolveAuthMode,
  saveReturnHash,
  setAuthEpoch,
} from '../lib/auth'
import type { AuthMode, AuthProblem } from '../lib/auth'
import { errorText } from '../lib/errors'
import { AuthBanner } from '../components/AuthBanner'
import { LoginScreen } from '../components/LoginScreen'
import { LoginErrorScreen } from '../components/LoginErrorScreen'
import { GroupsMissingNotice } from '../components/GroupsMissingNotice'
import { loginErrorFromLocation } from '../lib/loginError'
import { UserContext } from './user'

type Phase = 'loading' | 'ready' | 'login' | 'failed'

export function UserProvider({ children }: { children: ReactNode }) {
  if (isFixtures()) return <FixtureUserProvider>{children}</FixtureUserProvider>
  // The OIDC callback sent the person back with an error (`?login_error=<code>`):
  // show it and stop. No /api/me call and no redirect, so this cannot loop.
  const loginError = loginErrorFromLocation()
  if (loginError) return <LoginErrorScreen info={loginError} />
  return <LiveUserProvider>{children}</LiveUserProvider>
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
      p.delete('login')
      return p
    })
  }

  // `?login=1` previews the sign-in screen without a server (fixture mode only).
  if (searchParams.get('login') === '1') {
    return (
      <div className="shell">
        <LoginScreen onPick={setUserId} busy={false} />
      </div>
    )
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
  // `?role=viewer` previews the read-only UI without a server.
  const role = searchParams.get('role') === 'viewer' ? 'viewer' : 'member'
  const user: CurrentUser = { id: dev.id, name: dev.name, layers: dev.layers, role }
  return (
    <UserContext.Provider value={{ user, setUserId, logout: () => undefined, sessionEpoch: 0, busy: false }}>
      {children}
    </UserContext.Provider>
  )
}

// Live mode: identity comes from the server session (HttpOnly cookie), read
// through GET /api/me. The client never says who it is.
//
// Session transitions. Dev-login revokes the old cookie session, and the server
// then closes the old room socket with `session_expired`. That is NOT a sign-out.
// Three guards keep it from being treated as one:
//   1. every report carries the session epoch of the request or socket it came
//      from; a report older than the current epoch is dropped;
//   2. reports are ignored while a dev-login or logout is in flight (`transition`);
//   3. before showing "signed out" the provider asks GET /api/me; a 200 means the
//      session is valid, so it reloads the user instead.
function LiveUserProvider({ children }: { children: ReactNode }) {
  const [user, setUser] = useState<CurrentUser | null>(null)
  const [phase, setPhaseState] = useState<Phase>('loading')
  // null = the server did not say (see resolveAuthMode): show a plain signed-out notice.
  const [mode, setMode] = useState<AuthMode | null>(null)
  const [epoch, setEpochState] = useState(0)
  const [busy, setBusy] = useState(false)
  const [problem, setProblem] = useState<AuthProblem | null>(null)
  const [failure, setFailure] = useState<string | null>(null)
  const [notice, setNotice] = useState<SigninNotice | null>(null)

  const phaseRef = useRef<Phase>('loading')
  const epochRef = useRef(0)
  const transition = useRef(false) // a dev-login or logout is in flight
  const expiring = useRef(false) // a session_expired report is being checked

  const setPhase = useCallback((next: Phase | ((p: Phase) => Phase)) => {
    const value = typeof next === 'function' ? next(phaseRef.current) : next
    phaseRef.current = value
    setPhaseState(value)
  }, [])

  const bumpEpoch = useCallback(() => {
    epochRef.current += 1
    setAuthEpoch(epochRef.current)
    setEpochState(epochRef.current)
  }, [])

  const loadMe = useCallback(async (): Promise<boolean> => {
    try {
      const me = await api.me()
      setUser({
        id: me.id,
        name: me.name,
        layers: me.layers,
        role: me.role === 'viewer' ? 'viewer' : 'member',
        groupsMissing: me.groups_missing === true,
        solo: me.solo === true,
      })
      setPhase('ready')
      setFailure(null)
      setNotice(null)
      clearSigninNotice()
      return true
    } catch (e) {
      // session_expired is routed by the listener below.
      if (e instanceof ApiClientError && e.code === 'session_expired') return false
      setFailure(errorText(e))
      setPhase((p) => (p === 'loading' ? 'failed' : p))
      return false
    }
  }, [setPhase])

  // Auth failures from any fetch or from the WebSocket arrive here. The server
  // mode comes from error.auth_mode (VITE_AUTH_MODE is only a fallback); if the
  // 401 did not carry it, ask GET /api/me. Redirect to /auth/login only for "oidc".
  useEffect(
    () =>
      onAuthProblem((p) => {
        if (p.kind !== 'session_expired') {
          setProblem(p)
          return
        }
        if (p.epoch !== undefined && p.epoch < epochRef.current) return // stale
        if (transition.current || expiring.current || phaseRef.current === 'login') return
        expiring.current = true
        const startEpoch = epochRef.current
        void (async () => {
          try {
            // Never show "signed out" for a valid session: ask GET /api/me first.
            const probe = await probeSession()
            if (epochRef.current !== startEpoch || transition.current) return
            if (probe.valid) {
              if (await loadMe()) bumpEpoch()
              return
            }
            const m = resolveAuthMode(p.authMode ?? probe.mode)
            if (m === 'oidc' && redirectToLogin()) return
            // dev, unknown, or oidc with the loop guard on: show the sign-in choice
            setMode(m ?? null)
            setNotice(noticeForExpiredSession(m === 'dev'))
            setUser(null)
            setPhase('login')
            bumpEpoch()
          } finally {
            expiring.current = false
          }
        })()
      }),
    [bumpEpoch, loadMe, setPhase],
  )

  useEffect(() => {
    setAuthEpoch(0)
    void loadMe()
  }, [loadMe])

  const setUserId = useCallback(
    (id: string) => {
      transition.current = true
      setBusy(true)
      void (async () => {
        try {
          await api.devLogin(id)
          if (await loadMe()) bumpEpoch() // reload data, reconnect the WebSocket
          else setFailure((f) => f ?? 'Could not sign in.')
        } catch (e) {
          setFailure(errorText(e))
        } finally {
          transition.current = false
          setBusy(false)
        }
      })()
    },
    [bumpEpoch, loadMe],
  )

  const logout = useCallback(() => {
    transition.current = true
    setBusy(true)
    void (async () => {
      try {
        try {
          await api.logout()
        } catch {
          // an expired session is as good as logged out
        }
        // Never jump to the IdP by itself here (its own session would sign the
        // person straight back in): show the sign-in choice for the mode.
        const probe = await probeSession()
        setMode(probe.mode ?? resolveAuthMode() ?? null)
        setUser(null)
        setPhase('login')
        bumpEpoch()
      } finally {
        transition.current = false
        setBusy(false)
      }
    })()
  }, [bumpEpoch, setPhase])

  const banners = (
    <>
      {problem && <AuthBanner problem={problem} onDismiss={() => setProblem(null)} />}
      {failure && <div className="banner banner--error" role="alert">{failure}</div>}
      {user?.groupsMissing && <GroupsMissingNotice />}
    </>
  )

  if (phase === 'login') {
    return (
      <div className="shell">
        {banners}
        {mode === 'dev' ? (
          <LoginScreen onPick={setUserId} busy={busy} notice={notice ?? undefined} />
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
    <UserContext.Provider value={{ user, setUserId, logout, sessionEpoch: epoch, busy }}>
      {banners}
      {children}
    </UserContext.Provider>
  )
}
