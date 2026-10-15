import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { api } from './client'

const fetchMock = vi.fn()

beforeEach(() => {
  fetchMock.mockReset()
  fetchMock.mockImplementation(() => Promise.resolve(new Response('{}', { status: 200, headers: { 'Content-Type': 'application/json' } })))
  vi.stubGlobal('fetch', fetchMock)
})
afterEach(() => vi.unstubAllGlobals())

describe('api.withdrawProposal (Q0)', () => {
  it('posts the reason to the room-scoped route, with the ids encoded', async () => {
    await api.withdrawProposal('r_1', 'p/1', 'wrong connector')
    const [url, init] = fetchMock.mock.calls[0] as [string, RequestInit]
    expect(url).toBe('/api/rooms/r_1/proposals/p%2F1/withdraw')
    expect(init.method).toBe('POST')
    expect(init.body).toBe(JSON.stringify({ reason: 'wrong connector' }))
    expect(new Headers(init.headers).get('Content-Type')).toBe('application/json')
    expect(init.credentials).toBe('same-origin')
  })

  it('sends an empty object when there is no reason', async () => {
    await api.withdrawProposal('r_1', 'p_1')
    expect((fetchMock.mock.calls[0] as [string, RequestInit])[1].body).toBe('{}')
    await api.withdrawProposal('r_1', 'p_1', '')
    expect((fetchMock.mock.calls[1] as [string, RequestInit])[1].body).toBe('{}')
  })
})
