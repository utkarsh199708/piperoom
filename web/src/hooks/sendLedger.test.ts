import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { ApiClientError, SendUnconfirmedError } from '../api/client'
import type { Event } from '../api/types'
import { NOT_CONFIRMED_TEXT, OFFLINE_TEXT, UNCONFIRMED_TEXT } from '../lib/errors'
import { INFLIGHT_WAIT_MS, LIVE_RETRY_MS, MAX_KEPT, SendLedger, UNCONFIRMED_WINDOW_MS } from './sendLedger'
import type { SendItem } from './sendLedger'

const ME = { kind: 'human' as const, id: 'arjun', name: 'Arjun' }
const stop: SendItem = { kind: 'steer', text: 'stop' }

function event(text: string, clientId?: string, actor = ME): Event {
  return { room_id: 'r1', seq: 1, type: 'steer.sent', actor, payload: { text, client_id: clientId }, at: '' }
}

function ledger(post: (item: SendItem, id: string) => Promise<Event | undefined>, live = false) {
  let error: string | null = null
  let n = 0
  const setError = vi.fn((next: string | null | ((prev: string | null) => string | null)) => {
    error = typeof next === 'function' ? next(error) : next
  })
  const l = new SendLedger({ post, setError, newId: () => `id${++n}`, isLive: () => live })
  return { l, error: () => error, setError }
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

  // Review ADV-7: a send in flight when the room screen goes away fails later; nothing may then
  // touch the screen or arm a timer.
  it('a send that fails after dispose arms no timer and sets no banner', async () => {
    let fail: (e: unknown) => void = () => {}
    const post = vi.fn(() => new Promise<Event | undefined>((_, reject) => (fail = reject)))
    const { l, setError } = ledger(post)
    const sent = l.send(stop, 'k1')
    l.dispose()
    setError.mockClear()
    fail(new SendUnconfirmedError(12_000))
    expect(await sent).toBe(false)
    expect(setError).not.toHaveBeenCalled()
    expect(vi.getTimerCount()).toBe(0)
  })

  // Review ADV-2: an unknown action already reported "never confirmed" holds no banner up.
  it('an old unknown action does not bring its banner back after a later success or a reconnect', async () => {
    const post = vi.fn().mockRejectedValueOnce(new SendUnconfirmedError(12_000))
    const { l, error } = ledger(post)
    await l.send(stop, 'k1')
    vi.advanceTimersByTime(UNCONFIRMED_WINDOW_MS)
    expect(error()).toBe(NOT_CONFIRMED_TEXT)
    post.mockResolvedValueOnce(undefined)
    expect(await l.send({ kind: 'steer', text: 'check lag' })).toBe(true)
    expect(error()).toBeNull()
    vi.advanceTimersByTime(11 * 60_000)
    expect(l.banner()).toBeNull()
  })

  // Review ADV-9: with the socket open no reconnect drains, so "Retrying" must be true by itself.
  it('with the socket open, a kept action is sent again after LIVE_RETRY_MS', async () => {
    const post = vi.fn().mockRejectedValueOnce(new TypeError('Failed to fetch')).mockResolvedValue(undefined)
    const { l, error } = ledger(post, true)
    expect(await l.send(stop, 'k1')).toBe(false)
    expect(error()).toBe(OFFLINE_TEXT)
    await vi.advanceTimersByTimeAsync(LIVE_RETRY_MS)
    expect(post).toHaveBeenCalledTimes(2)
    expect(post.mock.calls[1][1]).toBe('k1')
    expect(error()).toBeNull()
  })

  it('a kept action is never dropped by age (only MAX_KEPT bounds them)', async () => {
    const post = vi.fn().mockRejectedValue(new TypeError('Failed to fetch'))
    const { l } = ledger(post)
    await l.send(stop, 'k1')
    vi.advanceTimersByTime(20 * 60_000)
    await l.send({ kind: 'steer', text: 'check lag' }, 'k2')
    post.mockReset().mockResolvedValue(undefined)
    l.drain()
    expect(post.mock.calls.map((c) => c[1])).toEqual(['k1', 'k2'])
  })
})

// Review nit 7: the round-4 traps X1-X3. Only the action's own client_id settles it: the same
// words with another id, or with none, are another action (or an old server's) and settle nothing.
describe('SendLedger never settles by words', () => {
  it.each([
    ['no client_id', undefined],
    ['another client_id', 'other'],
  ])('an own event with the same words and %s leaves an unknown action unsettled', async (_, id) => {
    const post = vi.fn().mockRejectedValueOnce(new SendUnconfirmedError(12_000))
    const { l, error } = ledger(post)
    await l.send(stop, 'k1')
    l.onEvent(event('stop', id), 'arjun')
    expect(error()).toBe(UNCONFIRMED_TEXT)
    expect(l.banner()).toBe(UNCONFIRMED_TEXT)
  })

  it.each([
    ['no client_id', undefined],
    ['another client_id', 'other'],
  ])('an own event with the same words and %s leaves a kept action to the reconnect', async (_, id) => {
    const post = vi.fn().mockRejectedValueOnce(new TypeError('Failed to fetch'))
    const { l } = ledger(post)
    await l.send(stop, 'k1')
    l.onEvent(event('stop', id), 'arjun')
    post.mockResolvedValue(undefined)
    l.drain()
    expect(post).toHaveBeenCalledTimes(2)
    expect(post.mock.calls[1][1]).toBe('k1')
  })
})

// Round 7: an id belongs to its ledger entry until the entry ends; a composer may only Retry it
// or cancel it.
describe('SendLedger cancel and isPending', () => {
  it('cancel withdraws a kept action: no drain, no live retry sends it', async () => {
    const post = vi.fn().mockRejectedValueOnce(new TypeError('Failed to fetch'))
    const { l, error } = ledger(post, true)
    await l.send(stop, 'k1')
    expect(l.isPending('k1')).toBe(true)
    l.cancel('k1')
    expect(l.isPending('k1')).toBe(false)
    expect(error()).toBeNull()
    post.mockResolvedValue(undefined)
    l.drain()
    await vi.advanceTimersByTimeAsync(LIVE_RETRY_MS)
    expect(post).toHaveBeenCalledTimes(1)
  })

  // Round 8 (ADV-A, ADV-B): a send given up on while on the wire must not be drained later.
  it.each([
    ['fails', new TypeError('Failed to fetch')],
    ['gets no answer', new SendUnconfirmedError(12_000)],
  ])('cancel on a send on the wire: if it %s, it is dropped, never kept or drained', async (_, err) => {
    let fail: (e: unknown) => void = () => {}
    const post = vi.fn(() => new Promise<Event | undefined>((_r, reject) => (fail = reject)))
    const { l, error } = ledger(post, true)
    const sent = l.send(stop, 'k1')
    l.cancel('k1')
    expect(l.isPending('k1')).toBe(false) // no composer may retry it
    fail(err)
    expect(await sent).toBe(false)
    expect(l.isPending('k1')).toBe(false)
    expect(l.banner()).toBeNull()
    expect(error()).toBeNull()
    post.mockResolvedValue(undefined)
    l.drain()
    await vi.advanceTimersByTimeAsync(LIVE_RETRY_MS)
    expect(post).toHaveBeenCalledTimes(1)
  })

  it('cancel on a send on the wire: if it lands, it lands', async () => {
    let answer: (ev: Event) => void = () => {}
    const post = vi.fn(() => new Promise<Event | undefined>((resolve) => (answer = resolve)))
    const { l } = ledger(post)
    const sent = l.send(stop, 'k1')
    l.cancel('k1')
    answer(event('stop', 'k1'))
    expect(await sent).toBe(true)
    expect(await l.send(stop, 'k1')).toBe(true) // landed: answered without a request
    expect(post).toHaveBeenCalledTimes(1)
  })

  it('a refused send (409) is no longer pending, so no composer keeps its id', async () => {
    const post = vi.fn().mockRejectedValueOnce(new ApiClientError(409, 'client_id_reused', 'reused'))
    const { l } = ledger(post)
    const logged = vi.spyOn(console, 'error').mockImplementation(() => {})
    expect(await l.send(stop, 'k1')).toBe(false)
    expect(l.isPending('k1')).toBe(false)
    logged.mockRestore()
  })

  it('an id landed through another tab\'s event is posted again, so the server decides', async () => {
    const post = vi.fn().mockResolvedValue(event('go', 'k1'))
    const { l } = ledger(post)
    l.onEvent(event('stop', 'k1'), 'arjun') // not this ledger's action: its body is unknown here
    expect(await l.send({ kind: 'steer', text: 'go' }, 'k1')).toBe(true)
    expect(post).toHaveBeenCalledTimes(1)
  })
})

// #519: a send waiting on an entry the person gave up on ends with it, and never posts it again.
describe('a waiter on a cancelled entry', () => {
  it('#519: a Retry waiting on the reconnect copy resolves as cancelled once that copy fails: no re-post', async () => {
    const post = vi.fn().mockRejectedValueOnce(new TypeError('Failed to fetch'))
    const { l, error } = ledger(post)
    await l.send(stop, 'k1') // fails offline: kept for the reconnect
    let failCopy: (e: unknown) => void = () => {}
    post.mockImplementationOnce(() => new Promise<Event | undefined>((_r, reject) => (failCopy = reject)))
    l.drain() // the reconnect copy is on the wire
    const retry = l.send(stop, 'k1') // the person's Retry waits on that copy
    l.cancel('k1') // ...then they edit the words or close the dialog
    post.mockRejectedValueOnce(new TypeError('Failed to fetch'))
    failCopy(new TypeError('Failed to fetch'))
    expect(await retry).toBe(false)
    expect(post).toHaveBeenCalledTimes(2)
    expect(l.isPending('k1')).toBe(false)
    expect(error()).toBeNull()
  })

  it('#519: the same when the copy hangs past the wait: dropped, no re-post', async () => {
    const post = vi.fn().mockRejectedValueOnce(new TypeError('Failed to fetch'))
    const { l, error } = ledger(post)
    await l.send(stop, 'k1')
    post.mockImplementationOnce(() => new Promise<Event | undefined>(() => {}))
    l.drain()
    const retry = l.send(stop, 'k1')
    l.cancel('k1')
    await vi.advanceTimersByTimeAsync(INFLIGHT_WAIT_MS + 1)
    expect(await retry).toBe(false)
    expect(post).toHaveBeenCalledTimes(2)
    expect(l.isPending('k1')).toBe(false)
    expect(error()).toBeNull()
  })

  it('#519: a cancelled copy that lands still settles its waiter', async () => {
    const post = vi.fn().mockRejectedValueOnce(new TypeError('Failed to fetch'))
    const { l } = ledger(post)
    await l.send(stop, 'k1')
    let land: (ev: Event) => void = () => {}
    post.mockImplementationOnce(() => new Promise<Event | undefined>((r) => (land = r)))
    l.drain()
    const retry = l.send(stop, 'k1')
    l.cancel('k1')
    land(event('stop', 'k1'))
    expect(await retry).toBe(true)
    expect(post).toHaveBeenCalledTimes(2)
  })
})
