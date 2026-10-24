import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { ApiClientError, SEND_DEADLINE_MS, SendUnconfirmedError, api, exportRoom, probeSession, roomWsUrl } from './client'
import { onAuthProblem, setAuthEpoch } from '../lib/auth'
import { isNetworkError } from '../lib/errors'
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
  setAuthEpoch(0)
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
    await api.declineSuggestion('r_1', 's_1', 'no')
    await api.requestControl('r_1')
    await api.grantControl('r_1', 'priya')
    await api.closeRoom('r_1', 'done')
    await api.approve('p_1', 'kafka', { params_hash: 'h1' })
    await api.reject('p_1', 'kafka', 'no', 'h1')
    await api.handoff('r_1', 'priya', true)
    await api.acceptHandoff('h_1')
    await api.trace('r_1', 'orders', 'k')
    expect(fetchMock.mock.calls.length).toBe(17)
    for (const [url, init] of fetchMock.mock.calls as [string, RequestInit][]) {
      expect(url.startsWith('/api/')).toBe(true)
      expect(url).not.toContain('localhost')
      expect(new Headers(init.headers).has(LEGACY_USER_HEADER)).toBe(false)
      expect(init.credentials).toBe('same-origin')
    }
  })

  it('sends no Content-Type on GET', async () => {
    fetchMock.mockResolvedValue(jsonResponse(200, { id: 'a', name: 'A', layers: [] }))
    await api.me()
    expect(lastCall().headers.has('Content-Type')).toBe(false)
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
    expect(seen).toEqual([{ kind: 'session_expired', message: 'sign in again', authMode: undefined, epoch: 0 }])
  })

  it('tags the report with the epoch the request started in', async () => {
    setAuthEpoch(3)
    let resolve!: (r: Response) => void
    fetchMock.mockReturnValue(new Promise<Response>((r) => (resolve = r)))
    const pending = api.me().catch(() => undefined)
    setAuthEpoch(4) // a login happened while the request was in flight
    resolve(jsonResponse(401, { error: { code: 'session_expired', message: 'x' } }))
    await pending
    expect(seen[0].epoch).toBe(3)
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

describe('probeSession', () => {
  it('reads error.auth_mode from GET /api/me without reporting a problem', async () => {
    const seen: AuthProblem[] = []
    const off = onAuthProblem((p) => seen.push(p))
    fetchMock.mockResolvedValue(
      jsonResponse(401, { error: { code: 'session_expired', message: 'x', auth_mode: 'dev' } }),
    )
    expect(await probeSession()).toEqual({ valid: false, mode: 'dev' })
    expect(lastCall().url).toBe('/api/me')
    expect(seen).toEqual([])
    off()
  })

  it('has no mode when the body does not say, is valid on 200, and survives a fetch failure', async () => {
    fetchMock.mockResolvedValueOnce(jsonResponse(401, { error: { code: 'session_expired', message: 'x' } }))
    expect(await probeSession()).toEqual({ valid: false, mode: undefined })
    fetchMock.mockResolvedValueOnce(jsonResponse(200, { id: 'a', name: 'A', layers: [] }))
    expect(await probeSession()).toEqual({ valid: true })
    fetchMock.mockRejectedValueOnce(new Error('network'))
    expect(await probeSession()).toEqual({ valid: false })
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

describe('people and approve (auth PR 3)', () => {
  it('GET /api/people returns the list as the server sent it', async () => {
    const list = [{ id: 'priya', name: 'Priya' }, { id: 'u_0a1b2c3d4e5f', name: 'Meera', email: 'm@example.com' }]
    fetchMock.mockResolvedValue(jsonResponse(200, list))
    await expect(api.people()).resolves.toEqual(list)
    expect(lastCall().url).toBe('/api/people')
  })

  it('a viewer gets 403 viewer from /api/people and the banner report', async () => {
    const seen: AuthProblem[] = []
    const off = onAuthProblem((p) => seen.push(p))
    fetchMock.mockResolvedValue(jsonResponse(403, { error: { code: 'viewer', message: 'read-only' } }))
    const err = await api.people().catch((e: unknown) => e)
    off()
    expect(err).toBeInstanceOf(ApiClientError)
    expect((err as ApiClientError).status).toBe(403)
    expect(seen.map((p) => p.kind)).toEqual(['viewer'])
  })

  it('approve sends {layer, params_hash} and adds confirm for solo mode', async () => {
    fetchMock.mockImplementation(() => Promise.resolve(jsonResponse(200, {})))
    await api.approve('p_1', 'kafka', { params_hash: 'h1' })
    expect(lastCall().init.body).toBe(JSON.stringify({ layer: 'kafka', params_hash: 'h1' }))
    await api.approve('p_1', 'kafka', { params_hash: 'h1', confirm: 'orders/kafka' })
    expect(lastCall().url).toBe('/api/proposals/p_1/approve')
    expect(JSON.parse(lastCall().init.body as string)).toEqual({
      layer: 'kafka',
      params_hash: 'h1',
      confirm: 'orders/kafka',
    })
  })

  it('approveAll posts {params_hash, confirm?} to approve-all and answers the events', async () => {
    const events = [{ seq: 7, type: 'approval.given' }, { seq: 8, type: 'approval.given' }]
    fetchMock.mockImplementation(() => Promise.resolve(jsonResponse(200, events)))
    await expect(api.approveAll('p 1', { params_hash: 'h1' })).resolves.toEqual(events)
    expect(lastCall().url).toBe('/api/proposals/p%201/approve-all')
    expect(lastCall().init.method).toBe('POST')
    expect(lastCall().headers.get('Content-Type')).toBe('application/json')
    expect(JSON.parse(lastCall().init.body as string)).toEqual({ params_hash: 'h1' })
    await api.approveAll('p_1', { params_hash: 'h1', confirm: 'orders/*' })
    expect(JSON.parse(lastCall().init.body as string)).toEqual({ params_hash: 'h1', confirm: 'orders/*' })
  })

  it('approveAll rejects with the route error code (403 not_layer_owner)', async () => {
    fetchMock.mockResolvedValue(
      jsonResponse(403, { error: { code: 'not_layer_owner', message: 'layer "kafka": user sam does not own layer "kafka"' } }),
    )
    const err = await api.approveAll('p_1', { params_hash: 'h1' }).catch((e: unknown) => e)
    expect(err).toBeInstanceOf(ApiClientError)
    expect((err as ApiClientError).status).toBe(403)
    expect((err as ApiClientError).code).toBe('not_layer_owner')
    expect((err as ApiClientError).message).toContain('layer "kafka"')
  })

  it('cancelProposal posts {reason?} to cancel and rejects 409 not_scheduled with its code', async () => {
    fetchMock.mockImplementation(() => Promise.resolve(jsonResponse(200, { seq: 9, type: 'proposal.cancelled' })))
    await api.cancelProposal('p 1')
    expect(lastCall().url).toBe('/api/proposals/p%201/cancel')
    expect(lastCall().init.method).toBe('POST')
    expect(JSON.parse(lastCall().init.body as string)).toEqual({})
    await api.cancelProposal('p_1', 'not during the backfill')
    expect(JSON.parse(lastCall().init.body as string)).toEqual({ reason: 'not during the backfill' })

    fetchMock.mockResolvedValue(jsonResponse(409, { error: { code: 'not_scheduled', message: 'no countdown is pending' } }))
    const err = await api.cancelProposal('p_1').catch((e: unknown) => e)
    expect(err).toBeInstanceOf(ApiClientError)
    expect((err as ApiClientError).status).toBe(409)
    expect((err as ApiClientError).code).toBe('not_scheduled')
  })

  it('reject sends {layer, reason, params_hash}', async () => {
    fetchMock.mockImplementation(() => Promise.resolve(jsonResponse(200, {})))
    await api.reject('p_1', 'kafka', 'wrong subject', 'h1')
    expect(lastCall().url).toBe('/api/proposals/p_1/reject')
    expect(JSON.parse(lastCall().init.body as string)).toEqual({
      layer: 'kafka',
      reason: 'wrong subject',
      params_hash: 'h1',
    })
  })

  it.each([
    [400, 'login_expired', 'login_expired'],
    [403, 'not_provisioned', 'not_provisioned'],
    [429, 'rate_limited', 'rate_limited'],
    [503, 'idp_unavailable', 'idp_unavailable'],
  ])('reports %i %s as an auth problem', async (status, code, kind) => {
    const seen: AuthProblem[] = []
    const off = onAuthProblem((p) => seen.push(p))
    fetchMock.mockResolvedValue(jsonResponse(status, { error: { code, message: 'm' } }))
    await api.me().catch(() => undefined)
    off()
    expect(seen.map((p) => p.kind)).toEqual([kind])
  })
})

describe('exportRoom', () => {
  it('returns the body text untouched and the integrity header', async () => {
    fetchMock.mockResolvedValueOnce(
      new Response('{"schema":1}', { status: 200, headers: { 'X-Piperoom-Export-SHA256': 'abc' } }),
    )
    await expect(exportRoom('r 1')).resolves.toEqual({ text: '{"schema":1}', sha256: 'abc' })
    expect(lastCall().url).toBe('/api/rooms/r%201/export')
  })

  it('throws ApiClientError with the server code on 403', async () => {
    fetchMock.mockResolvedValueOnce(jsonResponse(403, { error: { code: 'not_allowed', message: 'no export for you' } }))
    await expect(exportRoom('r1')).rejects.toMatchObject({ status: 403, code: 'not_allowed' })
  })
})

describe('api.postmortem (F2)', () => {
  it('GETs postmortem.md asking for text/markdown and returns the text untouched', async () => {
    fetchMock.mockResolvedValueOnce(new Response('# Post-mortem: x\n', { status: 200, headers: { 'Content-Type': 'text/markdown' } }))
    await expect(api.postmortem('r 1')).resolves.toBe('# Post-mortem: x\n')
    expect(lastCall().url).toBe('/api/rooms/r%201/postmortem.md')
    expect(lastCall().headers.get('Accept')).toBe('text/markdown')
    expect(lastCall().init.credentials).toBe('same-origin')
  })

  it('throws ApiClientError with the server code', async () => {
    fetchMock.mockResolvedValueOnce(jsonResponse(404, { error: { code: 'not_found', message: 'no room' } }))
    await expect(api.postmortem('r1')).rejects.toMatchObject({ status: 404, code: 'not_found' })
  })
})

describe('directory routes (R0)', () => {
  it('authUsers GETs /auth/users and pipelines GETs /api/pipelines', async () => {
    fetchMock.mockResolvedValueOnce(jsonResponse(200, [{ id: 'arjun', name: 'Arjun', layers: [] }]))
    await expect(api.authUsers()).resolves.toEqual([{ id: 'arjun', name: 'Arjun', layers: [] }])
    expect(lastCall().url).toBe('/auth/users')
    expect(lastCall().init.method ?? 'GET').toBe('GET')

    fetchMock.mockResolvedValueOnce(jsonResponse(200, []))
    await expect(api.pipelines()).resolves.toEqual([])
    expect(lastCall().url).toBe('/api/pipelines')
  })

  it('authUsers throws not_found in oidc mode, where the route is not mounted', async () => {
    fetchMock.mockResolvedValueOnce(jsonResponse(404, { error: { code: 'not_found', message: 'not found' } }))
    await expect(api.authUsers()).rejects.toMatchObject({ status: 404, code: 'not_found' })
  })
})

// #390 round 3: a steer or comment POST that never answers must settle, or its words stay
// "in flight" and every Retry of them waits on it until reload.
describe('steer and comment deadline', () => {
  it.each([
    ['steer', () => api.steer('r1', 'stop')],
    ['comment', () => api.comment('r1', { step_id: 's1', text: 'stop' })],
  ])('%s gives up after SEND_DEADLINE_MS as unconfirmed (not a network error) and aborts the request', async (_, send) => {
    vi.useFakeTimers()
    try {
      fetchMock.mockImplementation(() => new Promise(() => {}))
      const result = send().then(
        () => 'resolved',
        (e: unknown) => e,
      )
      await vi.advanceTimersByTimeAsync(SEND_DEADLINE_MS - 1)
      expect(lastCall().init.signal?.aborted).toBe(false)
      await vi.advanceTimersByTimeAsync(1)
      const err = await result
      // Not a TypeError: the server may have stored it, so it must not be queued and re-sent.
      expect(err).toBeInstanceOf(SendUnconfirmedError)
      expect(isNetworkError(err)).toBe(false)
      expect(lastCall().init.signal?.aborted).toBe(true)
    } finally {
      vi.useRealTimers()
    }
  })

  it('an answer inside the deadline is returned as usual', async () => {
    fetchMock.mockResolvedValue(jsonResponse(200, { seq: 1 }))
    await expect(api.steer('r1', 'stop')).resolves.toEqual({ seq: 1 })
  })
})

describe('idempotency key (#459)', () => {
  it('steer and comment send client_id in the body, and leave it out when there is none', async () => {
    fetchMock.mockImplementation(async () => jsonResponse(200, { seq: 1 }))
    await api.steer('r1', 'stop', 'k1')
    expect(JSON.parse(String(lastCall().init.body))).toEqual({ text: 'stop', client_id: 'k1' })
    await api.comment('r1', { step_id: 's1', text: 'ok', client_id: 'k2' })
    expect(JSON.parse(String(lastCall().init.body))).toEqual({ step_id: 's1', text: 'ok', client_id: 'k2' })
    await api.steer('r1', 'stop')
    expect(JSON.parse(String(lastCall().init.body))).toEqual({ text: 'stop' })
  })
})

describe('setup routes (U1-5b, U1-5d; U1-12b controls)', () => {
  it('setupSecret POSTs the name and value to its own route', async () => {
    fetchMock.mockResolvedValue(new Response(null, { status: 204 }))
    await api.setupSecret('r 1', { name: 'DB_PASSWORD', value: 's3cr3t!' })
    const { url, init } = lastCall()
    expect(url).toBe('/api/rooms/r%201/setup/secret')
    expect(init.method).toBe('POST')
    expect(JSON.parse(String(init.body))).toEqual({ name: 'DB_PASSWORD', value: 's3cr3t!' })
  })

  it('setupCredentials asks the runner with no body at all', async () => {
    fetchMock.mockResolvedValue(new Response(null, { status: 202 }))
    await api.setupCredentials('r_1')
    const { url, init } = lastCall()
    expect(url).toBe('/api/rooms/r_1/setup/credentials')
    expect(init.method).toBe('POST')
    expect(init.body).toBeUndefined()
  })

  it('setupTargetClass sends the target as a path segment and the class in the body', async () => {
    fetchMock.mockResolvedValue(jsonResponse(200, { seq: 1 }))
    await api.setupTargetClass('r_1', 'db.acme:5432', { class: 'production' })
    const { url, init } = lastCall()
    expect(url).toBe('/api/rooms/r_1/setup/targets/db.acme%3A5432/class')
    expect(JSON.parse(String(init.body))).toEqual({ class: 'production' })
  })
})
