import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

const flags = vi.hoisted(() => ({ dev: true }))
vi.mock('./env', () => ({
  get isDev() {
    return flags.dev
  },
}))

import {
  REDIRECT_AT_KEY,
  RETURN_HASH_KEY,
  classifyAuthProblem,
  loginRedirectUrl,
  mayAutoRedirect,
  problemText,
  redirectToLogin,
  resolveAuthMode,
  restoreReturnHash,
  saveReturnHash,
} from './auth'

describe('classifyAuthProblem', () => {
  it('maps the four auth failures', () => {
    expect(classifyAuthProblem(401, 'session_expired', 'm')?.kind).toBe('session_expired')
    expect(classifyAuthProblem(403, 'proxy_identity', 'm')?.kind).toBe('proxy_identity')
    expect(classifyAuthProblem(403, 'viewer', 'm')?.kind).toBe('viewer')
    expect(classifyAuthProblem(421, 'bad_host', 'm')?.kind).toBe('bad_host')
  })

  it('ignores ordinary errors', () => {
    expect(classifyAuthProblem(401, 'unauthenticated', 'm')).toBeNull()
    expect(classifyAuthProblem(403, 'not_driver', 'm')).toBeNull()
    expect(classifyAuthProblem(404, 'not_found', 'm')).toBeNull()
  })
})

describe('problemText', () => {
  it('names PIPEROOM_AUTH_MODE for proxy_identity and PIPEROOM_ALLOWED_HOSTS for bad_host', () => {
    expect(problemText({ kind: 'proxy_identity', message: '' })).toContain('PIPEROOM_AUTH_MODE=oidc')
    expect(problemText({ kind: 'bad_host', message: '' })).toContain('PIPEROOM_ALLOWED_HOSTS')
    expect(problemText({ kind: 'viewer', message: '' })).toContain('read-only')
  })
})

describe('resolveAuthMode', () => {
  afterEach(() => vi.unstubAllEnvs())

  it('uses the server hint and does not guess from the build type', () => {
    expect(resolveAuthMode('oidc')).toBe('oidc')
    expect(resolveAuthMode('dev')).toBe('dev')
    flags.dev = true
    expect(resolveAuthMode()).toBeUndefined()
    flags.dev = false
    expect(resolveAuthMode()).toBeUndefined()
  })

  it('VITE_AUTH_MODE is only a fallback: the server answer wins', () => {
    vi.stubEnv('VITE_AUTH_MODE', 'dev')
    expect(resolveAuthMode('oidc')).toBe('oidc')
    expect(resolveAuthMode()).toBe('dev')
    vi.stubEnv('VITE_AUTH_MODE', 'nonsense')
    expect(resolveAuthMode()).toBeUndefined()
  })
})

describe('redirect-loop guard', () => {
  beforeEach(() => sessionStorage.clear())

  it('allows a first redirect from a normal page', () => {
    expect(mayAutoRedirect(1_000_000, { pathname: '/' })).toBe(true)
  })

  it('never redirects from an /auth/ page', () => {
    expect(mayAutoRedirect(1_000_000, { pathname: '/auth/login' })).toBe(false)
    expect(mayAutoRedirect(1_000_000, { pathname: '/auth/callback' })).toBe(false)
  })

  it('does not redirect again within 30 seconds, then allows it', () => {
    sessionStorage.setItem(REDIRECT_AT_KEY, '1000000')
    expect(mayAutoRedirect(1_000_000 + 29_000, { pathname: '/' })).toBe(false)
    expect(mayAutoRedirect(1_000_000 + 31_000, { pathname: '/' })).toBe(true)
  })

  it('redirectToLogin does nothing and returns false inside the cooldown', () => {
    sessionStorage.setItem(REDIRECT_AT_KEY, String(Date.now()))
    expect(redirectToLogin()).toBe(false)
  })
})

describe('return hash across the IdP round trip', () => {
  beforeEach(() => {
    sessionStorage.clear()
    window.location.hash = ''
  })

  it('saves the hash under piperoom_return_hash', () => {
    window.location.hash = '#/rooms/r_x'
    saveReturnHash()
    expect(sessionStorage.getItem('piperoom_return_hash')).toBe('#/rooms/r_x')
  })

  it('does not save an empty or root hash', () => {
    saveReturnHash('')
    saveReturnHash('#/')
    expect(sessionStorage.getItem(RETURN_HASH_KEY)).toBeNull()
  })

  it('restores it once, then deletes the key', () => {
    sessionStorage.setItem(RETURN_HASH_KEY, '#/rooms/r_x')
    restoreReturnHash()
    expect(window.location.hash).toBe('#/rooms/r_x')
    expect(sessionStorage.getItem(RETURN_HASH_KEY)).toBeNull()
    window.location.hash = '#/rooms/other'
    restoreReturnHash()
    expect(window.location.hash).toBe('#/rooms/other')
  })

  it('does not overwrite a hash that is already there, but still deletes the key', () => {
    window.location.hash = '#/rooms/now'
    sessionStorage.setItem(RETURN_HASH_KEY, '#/rooms/old')
    restoreReturnHash()
    expect(window.location.hash).toBe('#/rooms/now')
    expect(sessionStorage.getItem(RETURN_HASH_KEY)).toBeNull()
  })

  it('ignores a saved value that is not a route hash', () => {
    sessionStorage.setItem(RETURN_HASH_KEY, 'javascript:alert(1)')
    restoreReturnHash()
    expect(window.location.hash).toBe('')
  })
})

describe('loginRedirectUrl', () => {
  it('collapses leading slashes so the server return check accepts it', () => {
    expect(loginRedirectUrl({ pathname: '//evil.test/x', search: '' })).toBe('/auth/login?return=%2Fevil.test%2Fx')
    expect(loginRedirectUrl({ pathname: '/\\evil', search: '' })).toBe('/auth/login?return=%2Fevil')
  })

  it('returns to the current path and query, encoded', () => {
    expect(loginRedirectUrl({ pathname: '/rooms/r_x', search: '?a=1&b=2' })).toBe(
      '/auth/login?return=%2Frooms%2Fr_x%3Fa%3D1%26b%3D2',
    )
    expect(loginRedirectUrl({ pathname: '/', search: '' })).toBe('/auth/login?return=%2F')
  })
})
