// Auth helpers shared by the API client and the UI (docs/design/auth.md).
// The session is an HttpOnly cookie the browser sends by itself; nothing here
// touches a credential. This module only decides what an auth failure means.
import { isDev } from './env'

export type AuthMode = 'dev' | 'oidc'

/** The auth failures the UI shows to the person (docs/api.md, Errors). */
export type AuthProblemKind = 'session_expired' | 'proxy_identity' | 'bad_host' | 'viewer'

export interface AuthProblem {
  kind: AuthProblemKind
  message: string
  /** Server mode if the 401 body said so (not in the contract yet, see resolveAuthMode). */
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
 * Which login to offer after `session_expired`.
 *
 * The contract has no field that tells the web app the server mode. Order:
 *   1. `auth_mode` in the 401 body, if the server sends it (proposed addition);
 *   2. the build-time `VITE_AUTH_MODE` (dev | oidc);
 *   3. a Vite dev build means dev, any other build means oidc.
 */
export function resolveAuthMode(hint?: AuthMode): AuthMode {
  if (hint === 'dev' || hint === 'oidc') return hint
  const built = import.meta.env.VITE_AUTH_MODE as string | undefined
  if (built === 'dev' || built === 'oidc') return built
  return isDev ? 'dev' : 'oidc'
}

/** `/auth/login?return=<current path>`: the server accepts only a path, never a URL. */
export function loginRedirectUrl(loc: Pick<Location, 'pathname' | 'search'> = window.location): string {
  const ret = `${loc.pathname}${loc.search}` || '/'
  return `/auth/login?return=${encodeURIComponent(ret)}`
}

/** Leave the app for the IdP login (oidc mode). Kept here so tests can replace it. */
export function redirectToLogin(): void {
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
