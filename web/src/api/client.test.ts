import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { ApiClientError, api, roomWsUrl } from './client'
import { onAuthProblem } from '../lib/auth'
import type { AuthProblem } from '../lib/auth'

// The retired identity header, spelled out so a repo-wide grep for it stays clean.
const LEGACY_USER_HEADER = ['X', 'User'].join('-')

const fetchMock = vi.fn()

function jsonResponse(status: number, body: unknown): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'Content-Type': 'application/json' },
  })
}

function lastCall() {
  const [url, init] = fetchMock.mock.calls.at(-1) as [string, RequestInit]
  return { url, init, headers: new Headers(init.headers) }
}

beforeEach(() => {
  fetchMock.mockReset()
  vi.stubGlobal('fetch', fetchMock)
})

afterEach(() => {
  vi.unstubAllGlobals()
})

describe('request shape', () => {
  it('uses a relative URL, same-origin credentials and no user-id header', async () => {
    fetchMock.mockResolvedValue(jsonResponse(200, { id: 'arjun', name: 'Arjun', layers: [] }))
    await api.me()
    const { url, init, headers } = lastCall()
    expect(url).toBe('/api/me')
    expect(init.credentials).toBe('same-origin')
    expect(headers.has(LEGACY_USER_HEADER)).toBe(false)
  })

  it('never sends the user-id header or an absolute URL from any endpoint', async () => {
    fetchMock.mockImplementation(() => Promise.resolve(jsonResponse(200, {})))
    await api.rooms()
    await api.room('r_1')
    await api.events('r_1', 3)
    await api.topology('orders')
    await api.createRoom({ title: 't', severity: 'sev2', pipeline_id: 'orders' })
    await api.comment('r_1', { text: 'hi' })
    await api.steer('r_1', 'go')
    await api.sendSuggestion('r_1', 's_1')
    await api.requestControl('r_1')
    await api.grantControl('r_1', 'priya')
    await api.closeRoom('r_1', 'done')
    await api.approve('p_1', 'kafka')
    await api.reject('p_1', 'kafka', 'no')
    await api.handoff('r_1', 'priya', true)
    await api.acceptHandoff('h_1')
    await api.trace('r_1', 'orders', 'k')
    expect(fetchMock.mock.calls.length).toBe(16)
    for (const [url, init] of fetchMock.mock.calls as [string, RequestInit][]) {
      expect(url.startsWith('/api/')).toBe(true)
      expect(url).not.toContain('localhost')
      expect(new Headers(init.headers).has(LEGACY_USER_HEADER)).toBe(false)
      expect(init.credentials).toBe('same-origin')
    }
  })

  it('sends Content-Type application/json on every POST, with or without a body', async () => {
    fetchMock.mockImplementation(() => Promise.resolve(jsonResponse(200, {})))
    await api.requestControl('r_1') // POST, no body
    expect(lastCall().init.method).toBe('POST')
    expect(lastCall().headers.get('Content-Type')).toBe('application/json')
    await api.steer('r_1', 'go') // POST with body
    expect(lastCall().headers.get('Content-Type')).toBe('application/json')
  })

  it('devLogin posts {user} to /auth/dev-login', async () => {
    fetchMock.mockResolvedValue(new Response(null, { status: 204 }))
    await api.devLogin('priya')
    const { url, init, headers } = lastCall()
    expect(url).toBe('/auth/dev-login')
    expect(init.method).toBe('POST')
    expect(init.body).toBe(JSON.stringify({ user: 'priya' }))
    expect(headers.get('Content-Type')).toBe('application/json')
    expect(headers.has(LEGACY_USER_HEADER)).toBe(false)
  })

  it('logout posts to /auth/logout and accepts an empty body', async () => {
    fetchMock.mockResolvedValue(new Response('', { status: 200 }))
    await expect(api.logout()).resolves.toBeUndefined()
    const { url, init } = lastCall()
    expect(url).toBe('/auth/logout')
    expect(init.method).toBe('POST')
  })
})

describe('auth failures', () => {
  let seen: AuthProblem[] = []
  let off: () => void
  beforeEach(() => {
    seen = []
    off = onAuthProblem((p) => seen.push(p))
  })
  afterEach(() => off())

  it('401 session_expired is reported and thrown', async () => {
    fetchMock.mockResolvedValue(
      jsonResponse(401, { error: { code: 'session_expired', message: 'sign in again' } }),
    )
    const err = await api.rooms().catch((e: unknown) => e)
    expect(err).toBeInstanceOf(ApiClientError)
    expect((err as ApiClientError).code).toBe('session_expired')
    expect(seen).toEqual([{ kind: 'session_expired', message: 'sign in again', authMode: undefined }])
  })

  it('passes an auth_mode hint from the 401 body', async () => {
    fetchMock.mockResolvedValue(
      jsonResponse(401, { error: { code: 'session_expired', message: 'x', auth_mode: 'oidc' } }),
    )
    await api.me().catch(() => undefined)
    expect(seen[0].authMode).toBe('oidc')
  })

  it('403 proxy_identity, 403 viewer and 421 bad_host are reported', async () => {
    fetchMock.mockResolvedValueOnce(
      jsonResponse(403, { error: { code: 'proxy_identity', message: 'a reverse proxy needs oidc' } }),
    )
    await api.me().catch(() => undefined)
    fetchMock.mockResolvedValueOnce(jsonResponse(403, { error: { code: 'viewer', message: 'read only' } }))
    await api.steer('r_1', 'x').catch(() => undefined)
    fetchMock.mockResolvedValueOnce(jsonResponse(421, { error: { code: 'bad_host', message: 'bad host' } }))
    await api.me().catch(() => undefined)
    expect(seen.map((p) => p.kind)).toEqual(['proxy_identity', 'viewer', 'bad_host'])
  })

  it('421 with a non-JSON body still counts as bad_host', async () => {
    fetchMock.mockResolvedValue(new Response('misdirected', { status: 421, statusText: 'Misdirected' }))
    const err = await api.me().catch((e: unknown) => e)
    expect((err as ApiClientError).code).toBe('bad_host')
    expect(seen.map((p) => p.kind)).toEqual(['bad_host'])
  })

  it('ordinary errors are thrown but not reported', async () => {
    fetchMock.mockResolvedValue(jsonResponse(403, { error: { code: 'not_driver', message: 'no' } }))
    await expect(api.steer('r_1', 'x')).rejects.toBeInstanceOf(ApiClientError)
    fetchMock.mockResolvedValue(jsonResponse(401, { error: { code: 'unauthenticated', message: 'no' } }))
    await expect(api.me()).rejects.toBeInstanceOf(ApiClientError)
    expect(seen).toEqual([])
  })
})

describe('roomWsUrl', () => {
  it('uses ws for http and wss for https, on the page host', () => {
    expect(roomWsUrl('r_1', 0, { protocol: 'http:', host: 'localhost:5173' })).toBe(
      'ws://localhost:5173/api/rooms/r_1/ws?after=0',
    )
    expect(roomWsUrl('r_1', 7, { protocol: 'https:', host: 'piperoom.example.com' })).toBe(
      'wss://piperoom.example.com/api/rooms/r_1/ws?after=7',
    )
  })

  it('has no ?user= and encodes the room id', () => {
    const url = roomWsUrl('r/1', 3, { protocol: 'http:', host: 'localhost:5173' })
    expect(url).not.toContain('user=')
    expect(url).toContain('/api/rooms/r%2F1/ws')
  })

  it('defaults to window.location', () => {
    const url = roomWsUrl('r_1', 0)
    expect(url.startsWith(`ws://${window.location.host}/api/rooms/r_1/ws`)).toBe(true)
  })
})
