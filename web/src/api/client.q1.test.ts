// Q1-web-b: the Q0 request shapes (page, fix note, manual hand-off) and how their refusals read.
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { ApiClientError, api } from './client'
import { setAuthEpoch } from '../lib/auth'
import { errorText } from '../lib/errors'

const fetchMock = vi.fn()

const json = (status: number, body: unknown) =>
  new Response(JSON.stringify(body), { status, headers: { 'Content-Type': 'application/json' } })

function lastCall() {
  const [url, init] = fetchMock.mock.calls.at(-1) as [string, RequestInit]
  return { url, init, body: init.body ? JSON.parse(init.body as string) : undefined }
}

beforeEach(() => {
  fetchMock.mockReset()
  setAuthEpoch(0)
  vi.stubGlobal('fetch', fetchMock)
})

afterEach(() => {
  vi.unstubAllGlobals()
})

describe('POST /api/rooms/{id}/page', () => {
  it('sends layer and reason, and the proposal only when given', async () => {
    fetchMock.mockImplementation(() => Promise.resolve(json(200, {})))
    await api.page('r_1', { layer: 'consumer', reason: 'lag' })
    expect(lastCall()).toMatchObject({ url: '/api/rooms/r_1/page', init: { method: 'POST' }, body: { layer: 'consumer', reason: 'lag' } })
    await api.page('r_1', { layer: 'consumer', reason: 'lag', proposal_id: 'p_1' })
    expect(lastCall().body).toEqual({ layer: 'consumer', reason: 'lag', proposal_id: 'p_1' })
  })

  it('already_paged reaches the person in the server\'s words', async () => {
    const message = 'consumer was paged 3 min ago; try again in 7 min'
    fetchMock.mockResolvedValue(json(409, { error: { code: 'already_paged', message } }))
    const err = await api.page('r_1', { layer: 'consumer', reason: 'lag' }).catch((e: unknown) => e)
    expect(err).toBeInstanceOf(ApiClientError)
    expect((err as ApiClientError).code).toBe('already_paged')
    expect(errorText(err)).toBe(message)
  })

  it('escapes the room id in the path', async () => {
    fetchMock.mockImplementation(() => Promise.resolve(json(200, {})))
    await api.page('r/../x', { layer: 'kafka', reason: 'r' })
    expect(lastCall().url).toBe('/api/rooms/r%2F..%2Fx/page')
  })
})

describe('fix notes', () => {
  it('a comment with kind fix_note carries the kind; a plain comment does not', async () => {
    fetchMock.mockImplementation(() => Promise.resolve(json(200, {})))
    await api.comment('r_1', { text: 'restarted it', kind: 'fix_note' })
    expect(lastCall().body).toEqual({ text: 'restarted it', kind: 'fix_note' })
    await api.comment('r_1', { text: 'hi' })
    expect(lastCall().body).toEqual({ text: 'hi' })
  })
})

describe('hand-offs', () => {
  it('without a summary the body is what it always was', async () => {
    fetchMock.mockImplementation(() => Promise.resolve(json(200, {})))
    await api.handoff('r_1', 'priya', true)
    expect(lastCall().body).toEqual({ to: 'priya', stay_as_watcher: true })
  })

  it('with a summary the driver wrote, it is sent as `summary`', async () => {
    fetchMock.mockImplementation(() => Promise.resolve(json(200, {})))
    const summary = { what_broke: 'b', why: 'w', done: ['d'], ruled_out: [], open: [], risks: [] }
    await api.handoff('r_1', 'priya', false, summary)
    expect(lastCall().body).toEqual({ to: 'priya', stay_as_watcher: false, summary })
  })

  it('503 summary_failed is an ApiClientError with its code, and reads as advice, not as a 500', async () => {
    fetchMock.mockResolvedValue(
      json(503, { error: { code: 'summary_failed', message: 'the agent could not write a summary; retry or send summary yourself' } }),
    )
    const err = await api.handoff('r_1', 'priya', false).catch((e: unknown) => e)
    expect((err as ApiClientError).status).toBe(503)
    expect((err as ApiClientError).code).toBe('summary_failed')
    expect(errorText(err)).toMatch(/write the summary yourself/)
  })
})
