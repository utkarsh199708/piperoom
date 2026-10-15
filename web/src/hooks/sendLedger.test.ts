import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { ApiClientError, SendUnconfirmedError } from '../api/client'
import type { Event } from '../api/types'
import { NOT_CONFIRMED_TEXT, OFFLINE_TEXT, UNCONFIRMED_TEXT } from '../lib/errors'
import { MAX_KEPT, SendLedger, UNCONFIRMED_WINDOW_MS } from './sendLedger'
import type { SendItem } from './sendLedger'

const ME = { kind: 'human' as const, id: 'arjun', name: 'Arjun' }
const stop: SendItem = { kind: 'steer', text: 'stop' }

function event(text: string, clientId?: string, actor = ME): Event {
  return { room_id: 'r1', seq: 1, type: 'steer.sent', actor, payload: { text, client_id: clientId }, at: '' }
}

function ledger(post: (item: SendItem, id: string) => Promise<Event | undefined>) {
  let error: string | null = null
  let n = 0
  const l = new SendLedger({
    post,
    setError: (next) => {
      error = typeof next === 'function' ? next(error) : next
    },
    newId: () => `id${++n}`,
  })
  return { l, error: () => error }
}

beforeEach(() => {
  vi.useFakeTimers()
})

afterEach(() => {
  vi.useRealTimers()
})

describe('SendLedger', () => {
  it('learns whether the server dedupes from the echo of client_id', async () => {
    const echo = ledger(async (item, id) => event(item.text, id))
    await echo.l.send(stop)
    expect(echo.l.dedupes).toBe(true)
    const old = ledger(async (item) => event(item.text))
    await old.l.send(stop)
    expect(old.l.dedupes).toBe(false)
  })

  it('a send without an id is a new action each time', async () => {
    const post = vi.fn(async (item: SendItem, id: string) => event(item.text, id))
    const { l } = ledger(post)
    await l.send(stop)
    await l.send(stop)
    expect(post.mock.calls.map((c) => c[1])).toEqual(['id1', 'id2'])
  })

  it('an event of another user with the same client_id settles nothing', async () => {
    const post = vi.fn().mockRejectedValueOnce(new TypeError('Failed to fetch'))
    const { l, error } = ledger(post)
    await l.send(stop, 'k1')
    l.onEvent(event('stop', 'k1', { kind: 'human', id: 'kabir', name: 'Kabir' }), 'arjun')
    expect(error()).toBe(OFFLINE_TEXT)
    l.onEvent(event('stop', 'k1'), 'arjun')
    expect(error()).toBeNull()
  })

  it('a landed id sent with another body is posted, so the server can answer 409', async () => {
    const post = vi
      .fn()
      .mockResolvedValueOnce(event('stop', 'k1'))
      .mockRejectedValueOnce(new ApiClientError(409, 'client_id_reused', 'reused'))
    const { l } = ledger(post)
    const logged = vi.spyOn(console, 'error').mockImplementation(() => {})
    expect(await l.send(stop, 'k1')).toBe(true)
    expect(await l.send(stop, 'k1')).toBe(true) // the same action again: answered here
    expect(await l.send({ kind: 'steer', text: 'go' }, 'k1')).toBe(false)
    expect(post).toHaveBeenCalledTimes(2)
    logged.mockRestore()
  })

  it(`keeps at most ${MAX_KEPT} actions for the reconnect`, async () => {
    const post = vi.fn().mockRejectedValue(new TypeError('Failed to fetch'))
    const { l } = ledger(post)
    for (let i = 0; i < MAX_KEPT + 2; i++) await l.send({ kind: 'steer', text: `m${i}` })
    post.mockResolvedValue(undefined)
    post.mockClear()
    l.drain()
    expect(post).toHaveBeenCalledTimes(MAX_KEPT)
  })

  it('without a deduping server, an unknown outcome waits out the window, then says "not confirmed"', async () => {
    const post = vi.fn().mockRejectedValueOnce(new SendUnconfirmedError(12_000))
    const { l, error } = ledger(post)
    expect(await l.send(stop, 'k1')).toBe(false)
    expect(error()).toBe(UNCONFIRMED_TEXT)
    l.drain()
    expect(post).toHaveBeenCalledTimes(1)
    vi.advanceTimersByTime(UNCONFIRMED_WINDOW_MS)
    expect(error()).toBe(NOT_CONFIRMED_TEXT)
    post.mockResolvedValueOnce(undefined)
    expect(await l.send(stop, 'k1')).toBe(true)
    expect(post).toHaveBeenCalledTimes(2)
  })

  it('dispose clears its timers', async () => {
    const post = vi.fn().mockRejectedValueOnce(new SendUnconfirmedError(12_000))
    const { l, error } = ledger(post)
    await l.send(stop, 'k1')
    l.dispose()
    vi.advanceTimersByTime(UNCONFIRMED_WINDOW_MS)
    expect(error()).toBe(UNCONFIRMED_TEXT)
  })
})
