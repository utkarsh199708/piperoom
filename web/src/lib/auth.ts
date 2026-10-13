// Auth helpers shared by the API client and the UI (docs/design/auth.md).
// The session is an HttpOnly cookie the browser sends by itself; nothing here
// touches a credential. This module only decides what an auth failure means.
export type AuthMode = 'dev' | 'oidc'

/** The auth failures the UI shows to the person (docs/api.md, Errors). */
export type AuthProblemKind = 'session_expired' | 'proxy_identity' | 'bad_host' | 'viewer'

export interface AuthProblem {
  kind: AuthProblemKind
  message: string
  /** `error.auth_mode` from the 401 body. */
  authMode?: AuthMode
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
 * The only source is `error.auth_mode` in the 401 body (docs/api.md, Errors).
 * `VITE_AUTH_MODE` (dev | oidc) is a developer override that wins over it.
 * There is no guess from the build type: with no answer the caller probes
 * GET /api/me, and if that does not say either, shows a plain signed-out notice.
 */
export function resolveAuthMode(hint?: AuthMode): AuthMode | undefined {
  const built = import.meta.env.VITE_AUTH_MODE as string | undefined
  if (built === 'dev' || built === 'oidc') return built
  if (hint === 'dev' || hint === 'oidc') return hint
  return undefined
}

/** `/auth/login?return=<current path>`: the server accepts only a path, never a URL. */
export function loginRedirectUrl(loc: Pick<Location, 'pathname' | 'search'> = window.location): string {
  const ret = `${loc.pathname}${loc.search}` || '/'
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

/** Leave the app for the IdP login. Only call this when the server said `auth_mode: "oidc"`. */
export function redirectToLogin(): void {
  saveReturnHash()
  window.location.assign(loginRedirectUrl())
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
