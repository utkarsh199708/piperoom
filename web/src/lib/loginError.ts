// OIDC callback failures. `GET /auth/callback` is a top-level browser navigation,
// so a JSON error body would show as raw text and the app would never see it.
// The server (or a proxy) sends the person back to the app with
// `/?login_error=<code>`; this module turns that into a readable page.
//
// The page never redirects by itself: "Sign in again" is a link the person clicks,
// so a failing IdP cannot cause a redirect loop (see also mayAutoRedirect()).

export type LoginErrorCode =
  | 'login_expired' // 400: login cookie or row missing, expired or already used
  | 'unauthenticated' // 401: state, nonce, token or audience check failed
  | 'not_provisioned' // 403: the account is not allowed to use Piperoom
  | 'rate_limited' // 429: too many logins from this client
  | 'idp_unavailable' // 503: the IdP cannot be reached right now
  | 'unknown'

export interface LoginErrorInfo {
  code: LoginErrorCode
  title: string
  body: string
  /** Offer the "Sign in again" link. Not useful when only an admin can fix it. */
  canRetry: boolean
}

const KNOWN: Record<Exclude<LoginErrorCode, 'unknown'>, Omit<LoginErrorInfo, 'code'>> = {
  login_expired: {
    title: 'Your sign-in expired',
    body: 'The sign-in took too long, or the link was already used. Start again.',
    canRetry: true,
  },
  unauthenticated: {
    title: 'Sign-in could not be checked',
    body: 'The sign-in service sent an answer Piperoom could not accept. Try once more.',
    canRetry: true,
  },
  not_provisioned: {
    title: 'No access yet',
    body: 'You signed in, but your account is not set up for Piperoom. Ask the person who runs Piperoom to add you.',
    canRetry: false,
  },
  rate_limited: {
    title: 'Too many sign-in attempts',
    body: 'Wait a minute, then try again.',
    canRetry: true,
  },
  idp_unavailable: {
    title: 'Sign-in is not available',
    body: 'Piperoom cannot reach the sign-in service right now. Try again in a few minutes.',
    canRetry: true,
  },
}

const UNKNOWN: Omit<LoginErrorInfo, 'code'> = {
  title: 'Sign-in failed',
  body: 'Something went wrong while signing in. Try again.',
  canRetry: true,
}

export const LOGIN_ERROR_PARAM = 'login_error'

/**
 * The login error in the page address, or null. Reads `?login_error=` from the
 * query string and, for a HashRouter address like `#/?login_error=`, from the hash.
 */
export function loginErrorFromLocation(
  loc: Pick<Location, 'search' | 'hash'> = window.location,
): LoginErrorInfo | null {
  let raw = new URLSearchParams(loc.search).get(LOGIN_ERROR_PARAM)
  if (raw === null) {
    const q = loc.hash.indexOf('?')
    if (q >= 0) raw = new URLSearchParams(loc.hash.slice(q + 1)).get(LOGIN_ERROR_PARAM)
  }
  if (raw === null) return null
  const code: LoginErrorCode = Object.hasOwn(KNOWN, raw) ? (raw as LoginErrorCode) : 'unknown'
  return { code, ...(code === 'unknown' ? UNKNOWN : KNOWN[code]) }
}

/** `/auth/login?return=/`: after an error the person starts from the room list. */
export const LOGIN_AGAIN_URL = '/auth/login?return=%2F'
