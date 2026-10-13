import { describe, expect, it, vi } from 'vitest'

const flags = vi.hoisted(() => ({ dev: true }))
vi.mock('./env', () => ({
  get isDev() {
    return flags.dev
  },
}))

import { classifyAuthProblem, loginRedirectUrl, problemText, resolveAuthMode } from './auth'

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
  it('prefers the server hint', () => {
    flags.dev = true
    expect(resolveAuthMode('oidc')).toBe('oidc')
    flags.dev = false
    expect(resolveAuthMode('dev')).toBe('dev')
  })

  it('falls back to dev for a Vite dev build and oidc for a production build', () => {
    flags.dev = true
    expect(resolveAuthMode()).toBe('dev')
    flags.dev = false
    expect(resolveAuthMode()).toBe('oidc')
  })
})

describe('loginRedirectUrl', () => {
  it('returns to the current path and query, encoded', () => {
    expect(loginRedirectUrl({ pathname: '/rooms/r_x', search: '?a=1&b=2' })).toBe(
      '/auth/login?return=%2Frooms%2Fr_x%3Fa%3D1%26b%3D2',
    )
    expect(loginRedirectUrl({ pathname: '/', search: '' })).toBe('/auth/login?return=%2F')
  })
})
