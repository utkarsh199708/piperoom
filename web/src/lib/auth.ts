// Auth helpers shared by the API client and the UI (docs/design/auth.md).
// The session is an HttpOnly cookie the browser sends by itself; nothing here
// touches a credential. This module only decides what an auth failure means.
export type AuthMode = 'dev' | 'oidc'

/** The auth failures the UI shows to the person (docs/api.md, Errors). */
export type AuthProblemKind =
  | 'session_expired'
  | 'proxy_identity'
  | 'bad_host'
  | 'viewer'
  // /auth/* answers (docs/api.md, Errors). A top-level callback navigation shows
  // them through the login-error page (lib/loginError.ts); a fetch gets a banner.
  | 'login_expired'
  | 'not_provisioned'
  | 'rate_limited'
  | 'idp_unavailable'

export interface AuthProblem {
  kind: AuthProblemKind
  message: string
  /** `error.auth_mode` from the 401 body. */
  authMode?: AuthMode
  /**
   * The session epoch the failing request or socket belonged to. A report from an
   * older epoch than the current one is stale (a login or logout happened since)
   * and the UserProvider drops it.
   */
  epoch?: number
}

// The UserProvider bumps this on every login and logout; the client reads it when a
// request starts, so a late 401 from before the switch carries the old epoch.
let currentEpoch = 0
export const getAuthEpoch = (): number => currentEpoch
export const setAuthEpoch = (n: number): void => {
  currentEpoch = n
}

// Bumped by the UserProvider when a login or logout STARTS, before the cookie changes; the
// session epoch above only moves once /api/me has answered. Whatever was queued under the old
// session (the room's send ledger, #536) checks it, so nothing of the previous user's is posted
// under the next one in between.
let sessionChanges = 0
export const getSessionChanges = (): number => sessionChanges
export const beginSessionChange = (): void => {
  sessionChanges += 1
}

/**
 * Map an HTTP status + error code to an auth problem, or null when the error
 * is an ordinary one that the caller shows itself.
 */
export function classifyAuthProblem(
  status: number,
  code: string,
  message: string,
  authMode?: AuthMode,
): AuthProblem | null {
  if (status === 401 && code === 'session_expired') {
    return { kind: 'session_expired', message, authMode }
  }
  if (status === 403 && code === 'proxy_identity') return { kind: 'proxy_identity', message }
  if (status === 403 && code === 'viewer') return { kind: 'viewer', message }
  if (status === 421 || code === 'bad_host') return { kind: 'bad_host', message }
  if (status === 400 && code === 'login_expired') return { kind: 'login_expired', message }
  if (status === 403 && code === 'not_provisioned') return { kind: 'not_provisioned', message }
  if (status === 429 && code === 'rate_limited') return { kind: 'rate_limited', message }
  if (status === 503 && code === 'idp_unavailable') return { kind: 'idp_unavailable', message }
  return null
}

const BANNER_TEXT: Record<Exclude<AuthProblemKind, 'session_expired'>, string> = {
  proxy_identity:
    'This server is behind a reverse proxy that sets the user. A reverse proxy needs ' +
    'PIPEROOM_AUTH_MODE=oidc. Ask the person who runs Piperoom to change it.',
  bad_host:
    'The server does not accept this address. Open Piperoom on localhost, or ask the ' +
    'person who runs it to add this host to PIPEROOM_ALLOWED_HOSTS.',
  viewer: 'You have read-only access. You can read rooms but you cannot change anything.',
  login_expired: 'The sign-in took too long or was already used. Please sign in again.',
  not_provisioned:
    'Your account is not set up for Piperoom. Ask the person who runs Piperoom to add you.',
  rate_limited: 'Too many sign-in attempts. Wait a minute, then try again.',
  idp_unavailable: 'The sign-in service is not available right now. Try again in a few minutes.',
}

/** Text for the banner. The server message is added when it says more. */
export function problemText(p: AuthProblem): string {
  if (p.kind === 'session_expired') return 'Your session has ended. Please sign in again.'
  const base = BANNER_TEXT[p.kind]
  return p.message && !base.includes(p.message) ? `${base} (${p.message})` : base
}

/**
 * Which login to offer after `session_expired`, or undefined when unknown.
 *
 * The source is `error.auth_mode` in the 401 body (docs/api.md, Errors).
 * `VITE_AUTH_MODE` (dev | oidc) is only a FALLBACK for a server that gives no
 * auth_mode; it never overrides the server. There is no guess from the build
 * type: with no answer the caller probes GET /api/me, and if that does not say
 * either, shows a plain signed-out notice.
 */
export function resolveAuthMode(hint?: AuthMode): AuthMode | undefined {
  if (hint === 'dev' || hint === 'oidc') return hint
  const built = import.meta.env.VITE_AUTH_MODE as string | undefined
  if (built === 'dev' || built === 'oidc') return built
  return undefined
}

/**
 * `/auth/login?return=<current path>`: the server accepts only a path, never a URL.
 * Leading slashes (or backslashes) are collapsed to one: `//evil` would be refused
 * by the server's return check and the person would land on `/` instead.
 */
export function loginRedirectUrl(loc: Pick<Location, 'pathname' | 'search'> = window.location): string {
  const ret = `${loc.pathname}${loc.search}`.replace(/^[/\\]+/, '/') || '/'
  return `/auth/login?return=${encodeURIComponent(ret)}`
}

// The app uses a HashRouter, so `#/rooms/r_x` never reaches the server and
// `return=` carries only path and query. Keep the hash across the IdP round trip.
export const RETURN_HASH_KEY = 'piperoom_return_hash'

export function saveReturnHash(hash: string = window.location.hash): void {
  try {
    if (hash && hash !== '#' && hash !== '#/') sessionStorage.setItem(RETURN_HASH_KEY, hash)
  } catch {
    // storage blocked: the person lands on the room list instead
  }
}

/** Restore the saved hash once (only onto an empty location), then delete the key. */
export function restoreReturnHash(): void {
  try {
    const saved = sessionStorage.getItem(RETURN_HASH_KEY)
    if (saved === null) return
    sessionStorage.removeItem(RETURN_HASH_KEY)
    const cur = window.location.hash
    if (saved.startsWith('#/') && (cur === '' || cur === '#' || cur === '#/')) {
      window.location.hash = saved
    }
  } catch {
    // storage blocked: nothing to restore
  }
}

// Redirect-loop guard: if /auth/login sends the person back and the app is still
// signed out, do not bounce again at once; show the Sign in link instead.
export const REDIRECT_AT_KEY = 'piperoom_login_redirect_at'
export const REDIRECT_COOLDOWN_MS = 30_000

export function mayAutoRedirect(
  now: number = Date.now(),
  loc: Pick<Location, 'pathname'> = window.location,
): boolean {
  if (loc.pathname.startsWith('/auth/')) return false
  try {
    const last = Number(sessionStorage.getItem(REDIRECT_AT_KEY))
    if (last && now - last < REDIRECT_COOLDOWN_MS) return false
  } catch {
    // storage blocked: allow; the server-side cap still holds
  }
  return true
}

/**
 * Leave the app for the IdP login. Only call this when the server said
 * `auth_mode: "oidc"`. Returns false (and does nothing) when the loop guard says no.
 */
export function redirectToLogin(): boolean {
  if (!mayAutoRedirect()) return false
  saveReturnHash()
  try {
    sessionStorage.setItem(REDIRECT_AT_KEY, String(Date.now()))
  } catch {
    // ignore
  }
  window.location.replace(loginRedirectUrl())
  return true
}

// ---- a tiny event bus: the client reports, the UserProvider reacts ----

type Listener = (p: AuthProblem) => void
const listeners = new Set<Listener>()

export function onAuthProblem(fn: Listener): () => void {
  listeners.add(fn)
  return () => {
    listeners.delete(fn)
  }
}

export function reportAuthProblem(p: AuthProblem): void {
  for (const fn of [...listeners]) fn(p)
}
