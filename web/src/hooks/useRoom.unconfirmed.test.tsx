import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { act, renderHook, waitFor } from '@testing-library/react'
import type { Event, RoomSnapshot } from '../api/types'

// #390 round 4: the real api.steer / api.comment (12 s deadline) through the hook, over a fake
// server that stores each POST when it arrives and answers late. A send that got no answer may
// be stored, and the API has no idempotency key, so the hook must never post those words again
// on its own: it settles them from the room's event stream. Cases a1-a5 are the review's.

vi.mock('sonner', () => ({ toast: Object.assign(vi.fn(), { warning: vi.fn(), dismiss: vi.fn() }) }))
vi.mock('../context/user', () => ({
  useCurrentUser: () => ({
    user: { id: 'arjun', name: 'Arjun', layers: ['kafka'] },
    setUserId: vi.fn(),
    logout: vi.fn(),
    sessionEpoch: 0,
  }),
}))
// Only the room snapshot, the event pages and the socket are faked; steer and comment are real.
vi.mock('../api/client', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../api/client')>()
  return {
    ...actual,
    isFixtures: () => false,
    openRoomWs: vi.fn(),
    api: { ...actual.api, me: vi.fn(), room: vi.fn(), events: vi.fn() },
  }
})

import { SEND_DEADLINE_MS, api, openRoomWs } from '../api/client'
import { NOT_CONFIRMED_TEXT, UNCONFIRMED_TEXT } from '../lib/errors'
import { useRoom } from './useRoom'

class FakeWebSocket {
  static CONNECTING = 0
  static OPEN = 1
  readyState = FakeWebSocket.OPEN
  onopen: (() => void) | null = null
  onmessage: ((m: { data: string }) => void) | null = null
  onclose: ((ev: { reason: string }) => void) | null = null
  onerror: (() => void) | null = null
  send() {}
  close() {}
}

const snap: RoomSnapshot = {
  id: 'r1',
  title: 'T',
  severity: 'sev2',
  pipeline_id: 'orders',
  status: 'open',
  opened_at: '2026-10-11T10:00:00Z',
  last_seq: 0,
  driver: { id: 'arjun', name: 'Arjun' },
  agent_status: { state: 'idle' },
  hypothesis: null,
  open_proposals: [],
  pending_suggestions: [],
  members: [],
}

const ARJUN = { kind: 'human' as const, id: 'arjun', name: 'Arjun' }

/**
 * Stores every steer/comment POST when it arrives (as the room event the server would log) and
 * answers after `answerAfterMs`. `offline` makes the next POST fail before it is sent.
 */
function fakeServer(answerAfterMs: number) {
  const stored: Event[] = []
  let offlineNext = 0
  let failNext: number | null = null
  const fetchMock = vi.fn((path: string, init: RequestInit) => {
    if (offlineNext > 0) {
      offlineNext--
      return Promise.reject(new TypeError('Failed to fetch'))
    }
    if (failNext !== null) {
      const status = failNext
      failNext = null
      const body = JSON.stringify({ error: { code: 'unavailable', message: 'try again' } })
      return Promise.resolve(new Response(body, { status, headers: { 'Content-Type': 'application/json' } }))
    }
    const body = JSON.parse(String(init.body)) as { text: string; step_id?: string }
    const ev: Event = path.endsWith('/steer')
      ? { room_id: 'r1', seq: stored.length + 1, type: 'steer.sent', actor: ARJUN, payload: { text: body.text }, at: '' }
      : {
          room_id: 'r1',
          seq: stored.length + 1,
          type: 'comment.added',
          actor: ARJUN,
          payload: { comment_id: `c${stored.length + 1}`, step_id: body.step_id, text: body.text, mentions_agent: false },
          at: '',
        }
    stored.push(ev)
    return new Promise<Response>((resolve) =>
      setTimeout(
        () => resolve(new Response(JSON.stringify(ev), { status: 200, headers: { 'Content-Type': 'application/json' } })),
        answerAfterMs,
      ),
    )
  })
  return {
    stored,
    fetchMock,
    offline: (n = 1) => {
      offlineNext = n
    },
    fail: (status: number) => {
      failNext = status
    },
  }
}

async function openRoom(server: ReturnType<typeof fakeServer>) {
  vi.stubGlobal('fetch', server.fetchMock)
  const socket = new FakeWebSocket()
  vi.mocked(api.room).mockResolvedValue(snap)
  vi.mocked(api.events).mockResolvedValue({ events: [], next_after: 0 } as never)
  vi.mocked(openRoomWs).mockReturnValue(socket as unknown as WebSocket)
  const hook = renderHook(() => useRoom('r1'))
  await waitFor(() => expect(openRoomWs).toHaveBeenCalled())
  act(() => socket.onopen?.())
  return { hook, socket }
}

/** The server's live push (or the reconnect replay) of a stored event. */
async function push(socket: FakeWebSocket, ev: Event) {
  await act(async () => {
    socket.onmessage?.({ data: JSON.stringify({ type: 'event', event: ev }) })
    await Promise.resolve()
  })
}

/** Starts a send without waiting for it; the getter says how it ended (undefined while pending). */
function fire(p: () => Promise<boolean>) {
  let ok: boolean | undefined
  act(() => {
    void p().then((v) => (ok = v))
  })
  return () => ok
}

/** Lets fake time pass with the hook's promises settling in between. */
async function pass(ms: number) {
  await act(async () => {
    await vi.advanceTimersByTimeAsync(ms)
  })
}

/** Starts a send and lets it run past the client deadline without an answer. */
async function sendPastDeadline(p: () => Promise<boolean>) {
  let ok: boolean | undefined
  act(() => {
    void p().then((v) => (ok = v))
  })
  await act(async () => {
    await vi.advanceTimersByTimeAsync(SEND_DEADLINE_MS + 100)
  })
  await waitFor(() => expect(ok).toBeDefined())
  return ok
}

beforeEach(() => {
  vi.stubGlobal('WebSocket', FakeWebSocket)
  vi.resetAllMocks()
  vi.useFakeTimers({ shouldAdvanceTime: true })
})

afterEach(() => {
  vi.useRealTimers()
  vi.unstubAllGlobals()
})

describe('a send that got no answer is never posted again on its own', () => {
  it('a1: send (answered at 20 s), then Retry: one stored copy', async () => {
    const server = fakeServer(20_000)
    const { hook, socket } = await openRoom(server)
    const ok = await sendPastDeadline(() => hook.result.current.actions.steer('stop'))
    const retried = fire(() => hook.result.current.actions.steer('stop', { retry: true }))
    await pass(1_000)
    expect(server.stored).toHaveLength(1)
    expect(ok).toBe(false)
    expect(retried()).toBe(false)
    expect(hook.result.current.error).toBe(UNCONFIRMED_TEXT)
    let retry = false

    // The event shows it arrived: the banner clears and a later Retry is that message's.
    await push(socket, server.stored[0])
    expect(hook.result.current.error).toBeNull()
    await act(async () => {
      retry = await hook.result.current.actions.steer('stop', { retry: true })
    })
    expect(retry).toBe(true)
    expect(server.stored).toHaveLength(1)
  })

  it('a2: send (answered at 20 s), then a reconnect with no click: one stored copy', async () => {
    const server = fakeServer(20_000)
    const { hook, socket } = await openRoom(server)
    await sendPastDeadline(() => hook.result.current.actions.steer('stop'))
    act(() => socket.onclose?.({ reason: '' }))
    act(() => socket.onopen?.())
    await act(async () => {
      await vi.advanceTimersByTimeAsync(30_000)
    })
    expect(server.stored).toHaveLength(1)
    expect(server.fetchMock).toHaveBeenCalledTimes(1)
  })

  it('a3: offline, the reconnect copy is answered at 20 s, then Retry: one stored copy', async () => {
    const server = fakeServer(20_000)
    const { hook, socket } = await openRoom(server)
    server.offline()
    await act(async () => {
      await hook.result.current.actions.steer('stop')
    })
    act(() => socket.onopen?.())
    await pass(SEND_DEADLINE_MS + 100)
    const retried = fire(() => hook.result.current.actions.steer('stop', { retry: true }))
    await pass(1_000)
    expect(server.stored).toHaveLength(1)
    expect(retried()).toBe(false)
  })

  it('a4: the same for an unflagged step comment (RollbackButton / Timeline path): one stored copy', async () => {
    const server = fakeServer(20_000)
    const { hook, socket } = await openRoom(server)
    server.offline()
    await act(async () => {
      await hook.result.current.actions.comment('looks right', 's1')
    })
    act(() => socket.onopen?.())
    await pass(SEND_DEADLINE_MS + 100)
    fire(() => hook.result.current.actions.comment('looks right', 's1'))
    await pass(1_000)
    act(() => socket.onopen?.())
    fire(() => hook.result.current.actions.comment('looks right', 's1'))
    await pass(1_000)
    expect(server.stored).toHaveLength(1)
  })

  it('a5: Retry while the reconnect copy is sending (answered at 13 s): one stored copy', async () => {
    const server = fakeServer(13_000)
    const { hook, socket } = await openRoom(server)
    server.offline()
    await act(async () => {
      await hook.result.current.actions.steer('stop')
    })
    act(() => socket.onopen?.())
    const retried = fire(() => hook.result.current.actions.steer('stop', { retry: true }))
    await pass(SEND_DEADLINE_MS + 5_000)
    expect(server.stored).toHaveLength(1)
    expect(retried()).toBe(false)
  })

  it('an event pushed before the deadline settles the send at once: true, no banner', async () => {
    const server = fakeServer(20_000)
    const { hook, socket } = await openRoom(server)
    let ok: boolean | undefined
    act(() => {
      void hook.result.current.actions.steer('stop').then((v) => (ok = v))
    })
    await waitFor(() => expect(server.stored).toHaveLength(1))
    await push(socket, server.stored[0])
    await act(async () => {
      await vi.advanceTimersByTimeAsync(SEND_DEADLINE_MS + 100)
    })
    await waitFor(() => expect(ok).toBe(true))
    expect(hook.result.current.error).toBeNull()
  })

  it('with no event by the end of the window: "not confirmed", and a click posts it once', async () => {
    const server = fakeServer(20_000)
    const { hook } = await openRoom(server)
    // Simulates a server that took the request but never stored it (no event ever comes).
    await sendPastDeadline(() => hook.result.current.actions.steer('stop'))
    server.stored.length = 0
    await act(async () => {
      await vi.advanceTimersByTimeAsync(65_000)
    })
    expect(hook.result.current.error).toBe(NOT_CONFIRMED_TEXT)
    expect(server.fetchMock).toHaveBeenCalledTimes(1)

    act(() => {
      void hook.result.current.actions.steer('stop', { retry: true })
    })
    await waitFor(() => expect(server.stored).toHaveLength(1))
    expect(server.fetchMock).toHaveBeenCalledTimes(2)
  })

  it('a real failure (503 inside the deadline) is still retried by a click: round-2 behaviour', async () => {
    const server = fakeServer(100)
    const { hook } = await openRoom(server)
    server.fail(503)
    let ok = true
    await act(async () => {
      ok = await hook.result.current.actions.steer('stop')
    })
    expect(ok).toBe(false)
    expect(server.stored).toHaveLength(0)
    act(() => {
      void hook.result.current.actions.steer('stop', { retry: true }).then((v) => (ok = v))
    })
    await act(async () => {
      await vi.advanceTimersByTimeAsync(200)
    })
    await waitFor(() => expect(ok).toBe(true))
    expect(server.stored).toHaveLength(1)
  })
})
