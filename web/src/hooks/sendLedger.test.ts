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

// Most rows are about a server known to dedupe (an earlier answer echoed client_id), the only
// one whose network failures are kept and re-sent; `dedupes` null is a ledger before any answer.
function ledger(
  post: (item: SendItem, id: string) => Promise<Event | undefined>,
  live = false,
  dedupes: boolean | null = true,
) {
  let error: string | null = null
  let n = 0
  const setError = vi.fn((next: string | null | ((prev: string | null) => string | null)) => {
    error = typeof next === 'function' ? next(error) : next
  })
  const l = new SendLedger({ post, setError, newId: () => `id${++n}`, isLive: () => live })
  l.dedupes = dedupes
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
    const { l, error } = ledger(post, false, null)
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
    const { l, error } = ledger(post, false, null)
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
    const { l, setError } = ledger(post, false, null)
    const sent = l.send(stop, 'k1')
    l.dispose()
    setError.mockClear()
    fail(new SendUnconfirmedError(12_000))
    expect(await sent).toBe(false)
    expect(setError).not.toHaveBeenCalled()
    expect(vi.getTimerCount()).toBe(0)
  })

  // #536: a ledger is one user's session. Disposed, it owes nothing: its kept actions are never
  // drained, retried live, or offered for a Retry under whoever comes next.
  it('dispose drops kept actions: nothing drains and none is pending', async () => {
    const post = vi.fn().mockRejectedValueOnce(new TypeError('Failed to fetch'))
    const { l } = ledger(post, true)
    expect(await l.send(stop, 'k1')).toBe(false)
    expect(l.isPending('k1')).toBe(true)
    l.dispose()
    expect(l.isPending('k1')).toBe(false)
    l.drain()
    vi.advanceTimersByTime(LIVE_RETRY_MS * 2)
    expect(post).toHaveBeenCalledTimes(1)
  })

  // Review of 3d17a65: a login or logout has started (the cookie may already be the next user's)
  // but this ledger is not replaced yet. Each path that could post refuses.
  describe('once the session it was made for is ending', () => {
    function ending(live: boolean) {
      let same = true
      const post = vi.fn().mockRejectedValueOnce(new TypeError('Failed to fetch'))
      let error: string | null = null
      const l = new SendLedger({
        post,
        setError: (next) => {
          error = typeof next === 'function' ? next(error) : next
        },
        newId: () => 'new',
        isLive: () => live,
        sameSession: () => same,
      })
      l.dedupes = true // a kept action: only a server known to dedupe keeps one
      return { l, post, end: () => (same = false), error: () => error }
    }

    it('a reconnect drains nothing', async () => {
      const { l, post, end } = ending(false)
      await l.send(stop, 'k1')
      end()
      l.drain()
      expect(post).toHaveBeenCalledTimes(1)
    })

    it('the live retry sends nothing', async () => {
      const { l, post, end } = ending(true)
      await l.send(stop, 'k1')
      end()
      vi.advanceTimersByTime(LIVE_RETRY_MS * 3)
      expect(post).toHaveBeenCalledTimes(1)
    })

    it('a Retry posts nothing, drops the action and its banner', async () => {
      const { l, post, end, error } = ending(false)
      await l.send(stop, 'k1')
      expect(error()).toBe(OFFLINE_TEXT)
      end()
      expect(await l.send(stop, 'k1')).toBe(false)
      expect(await l.send({ kind: 'steer', text: 'new words' })).toBe(false)
      expect(post).toHaveBeenCalledTimes(1)
      expect(l.isPending('k1')).toBe(false)
      expect(error()).toBeNull()
    })
  })

  it('attach after dispose (React StrictMode remounts an effect) works again', async () => {
    const post = vi.fn().mockRejectedValueOnce(new SendUnconfirmedError(12_000))
    const { l, error } = ledger(post, false, null)
    l.dispose()
    l.attach()
    await l.send(stop, 'k1')
    expect(error()).toBe(UNCONFIRMED_TEXT)
    vi.advanceTimersByTime(UNCONFIRMED_WINDOW_MS)
    expect(error()).toBe(NOT_CONFIRMED_TEXT)
  })

  // Review ADV-2: an unknown action already reported "never confirmed" holds no banner up.
  it('an old unknown action does not bring its banner back after a later success or a reconnect', async () => {
    const post = vi.fn().mockRejectedValueOnce(new SendUnconfirmedError(12_000))
    const { l, error } = ledger(post, false, null)
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
    const { l, error } = ledger(post, false, null)
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

// #551, #545: a ledger is new on every room visit, so before its first answer it cannot know
// whether the server dedupes. A TypeError then may mean the request was stored and its answer
// lost: it is never re-sent automatically, only by a Retry click after the window.
describe('a TypeError before the first answer (dedupes null)', () => {
  // A server without #459: it stores every POST and ignores client_id.
  function oldServer() {
    const stored: string[] = []
    let lose = 0
    const post = vi.fn(async (item: SendItem) => {
      stored.push(item.text)
      if (lose > 0) {
        lose--
        throw new TypeError('Failed to fetch') // stored, then the connection reset
      }
      return event(item.text)
    })
    return { post, stored, loseNext: () => (lose = 1) }
  }

  it('#551: a cold ledger against an old server stores the steer once; Retry is offered, not a re-send', async () => {
    const server = oldServer()
    const { l, error } = ledger(server.post, false, null)
    server.loseNext()
    expect(await l.send(stop, 'k1')).toBe(false)
    expect(l.dedupes).toBeNull()
    expect(error()).toBe(UNCONFIRMED_TEXT)
    expect(l.isPending('k1')).toBe(true) // the Retry strip stays: the action is still owed
    l.drain()
    expect(await l.send(stop, 'k1')).toBe(false) // a Retry inside the window posts nothing
    expect(server.stored).toEqual(['stop'])
    vi.advanceTimersByTime(UNCONFIRMED_WINDOW_MS)
    expect(error()).toBe(NOT_CONFIRMED_TEXT)
    l.drain()
    expect(server.stored).toEqual(['stop'])
  })

  it('#551: the live retry does not re-send it either', async () => {
    const server = oldServer()
    const { l } = ledger(server.post, true, null)
    server.loseNext()
    await l.send(stop, 'k1')
    vi.advanceTimersByTime(LIVE_RETRY_MS * 3)
    expect(server.stored).toEqual(['stop'])
  })

  it('#545: the first keyed post to a new server fails: no automatic re-send; a Retry after the window lands once', async () => {
    const stored: string[] = []
    const post = vi
      .fn()
      .mockRejectedValueOnce(new TypeError('Failed to fetch'))
      .mockImplementation(async (item: SendItem, id: string) => {
        stored.push(id)
        return event(item.text, id)
      })
    const { l, error } = ledger(post, true, null)
    expect(await l.send(stop, 'k1')).toBe(false)
    expect(error()).toBe(UNCONFIRMED_TEXT)
    l.drain()
    vi.advanceTimersByTime(LIVE_RETRY_MS * 3)
    expect(post).toHaveBeenCalledTimes(1)
    vi.advanceTimersByTime(UNCONFIRMED_WINDOW_MS)
    expect(await l.send(stop, 'k1')).toBe(true) // the click, same id
    expect(stored).toEqual(['k1'])
    expect(l.dedupes).toBe(true)
    expect(error()).toBeNull()
  })

  // Review of #559: only this ledger's own answers say whether the server dedupes (docs/api.md,
  // Idempotent posts). Another member's keyed event in the stream proves nothing about the replica
  // that answers this client, and a learned false is never overridden.
  it.each([
    ['null', null],
    ['false', false],
  ])('another member\'s keyed event leaves dedupes %s: the TypeError is not re-sent', async (_, before) => {
    const post = vi.fn().mockRejectedValueOnce(new TypeError('Failed to fetch'))
    const { l, error } = ledger(post, true, before)
    l.onEvent(event('earlier', 'theirs', { kind: 'human', id: 'kabir', name: 'Kabir' }), 'arjun')
    expect(l.dedupes).toBe(before)
    await l.send(stop, 'k1')
    expect(error()).toBe(UNCONFIRMED_TEXT)
    l.drain()
    vi.advanceTimersByTime(LIVE_RETRY_MS * 3)
    expect(post).toHaveBeenCalledTimes(1)
  })

  it('an own event with a client_id settles, but does not set dedupes: only an answer does', async () => {
    const post = vi.fn().mockRejectedValueOnce(new TypeError('Failed to fetch'))
    const { l } = ledger(post, false, null)
    await l.send(stop, 'k1')
    l.onEvent(event('stop', 'k1'), 'arjun')
    expect(l.isPending('k1')).toBe(false)
    expect(l.dedupes).toBeNull()
  })

  it('a learned false stays false after a later answer that echoes', async () => {
    const post = vi
      .fn()
      .mockImplementationOnce(async (item: SendItem) => event(item.text)) // no echo
      .mockImplementationOnce(async (item: SendItem, id: string) => event(item.text, id))
      .mockRejectedValueOnce(new TypeError('Failed to fetch'))
    const { l, error } = ledger(post, false, null)
    await l.send({ kind: 'steer', text: 'a' })
    expect(l.dedupes).toBe(false)
    await l.send({ kind: 'steer', text: 'b' })
    expect(l.dedupes).toBe(false)
    await l.send(stop, 'k1')
    expect(error()).toBe(UNCONFIRMED_TEXT)
    l.drain()
    expect(post).toHaveBeenCalledTimes(3)
  })

  it('this ledger\'s own echoed answer makes it known to dedupe: a later TypeError is kept and drained', async () => {
    const post = vi
      .fn()
      .mockImplementationOnce(async (item: SendItem, id: string) => event(item.text, id))
      .mockRejectedValueOnce(new TypeError('Failed to fetch'))
    const { l, error } = ledger(post, false, null)
    await l.send({ kind: 'steer', text: 'hello' })
    expect(l.dedupes).toBe(true)
    await l.send(stop, 'k1')
    expect(error()).toBe(OFFLINE_TEXT)
    post.mockResolvedValue(undefined)
    l.drain()
    expect(post.mock.calls.map((c) => c[1])).toEqual(['id1', 'k1', 'k1'])
  })

  it('an event without client_id says nothing: still treated as not deduping', async () => {
    const post = vi.fn().mockRejectedValueOnce(new TypeError('Failed to fetch'))
    const { l, error } = ledger(post, false, null)
    l.onEvent(event('earlier'), 'arjun')
    expect(l.dedupes).toBeNull()
    await l.send(stop, 'k1')
    expect(error()).toBe(UNCONFIRMED_TEXT)
    l.drain()
    expect(post).toHaveBeenCalledTimes(1)
  })
})

describe('SendLedger.unconfirmedLeft (KAN-586)', () => {
  it('counts down the window of an unknown action and is 0 once it ends', async () => {
    const post = vi.fn().mockRejectedValueOnce(new TypeError('Failed to fetch'))
    const { l } = ledger(post, false, null)
    expect(await l.send(stop, 'a1')).toBe(false)
    expect(l.unconfirmedLeft('a1')).toBe(UNCONFIRMED_WINDOW_MS)
    vi.advanceTimersByTime(UNCONFIRMED_WINDOW_MS - 1_000)
    expect(l.unconfirmedLeft('a1')).toBe(1_000)
    vi.advanceTimersByTime(1_000)
    expect(l.unconfirmedLeft('a1')).toBe(0)
  })

  it('is undefined for an action kept for a retry (a deduping server) and for an unknown id', async () => {
    const post = vi.fn().mockRejectedValueOnce(new TypeError('Failed to fetch'))
    const { l } = ledger(post, false, true)
    await l.send(stop, 'a1')
    expect(l.unconfirmedLeft('a1')).toBeUndefined()
    expect(l.unconfirmedLeft('nope')).toBeUndefined()
  })
})
