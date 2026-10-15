import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { act, renderHook, waitFor } from '@testing-library/react'
import type { Event, RoomSnapshot } from '../api/types'

// #390 / #459: the real api.steer / api.comment (12 s deadline) through the hook, over a fake
// server. `contract` implements docs/api.md "Idempotent posts": it stores a POST when it arrives,
// keyed on (user, client_id); a repeat with the same body appends nothing and answers 200 with
// the stored event; the same key with another body answers 409 client_id_reused; the stored
// event echoes client_id. `old` is a server without #459: it ignores client_id and stores every
// POST. Answers come after `answerAfter` ms, so the 12 s deadline can pass before them.

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
import { NOT_CONFIRMED_TEXT, OFFLINE_TEXT, UNCONFIRMED_TEXT } from '../lib/errors'
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

interface Body {
  text: string
  step_id?: string
  client_id?: string
}

function fakeServer(mode: 'contract' | 'old') {
  const stored: Event[] = []
  const byKey = new Map<string, { route: string; body: string; ev: Event }>()
  const state = { answerAfter: 100, offlineNext: 0 }
  const answer = (status: number, body: unknown, after: number) =>
    new Promise<Response>((resolve) =>
      setTimeout(
        () => resolve(new Response(JSON.stringify(body), { status, headers: { 'Content-Type': 'application/json' } })),
        after,
      ),
    )
  const fetchMock = vi.fn((path: string, init: RequestInit) => {
    if (state.offlineNext > 0) {
      state.offlineNext--
      return Promise.reject(new TypeError('Failed to fetch')) // never left the browser
    }
    const route = path.endsWith('/steer') ? 'steer' : 'comments'
    const body = JSON.parse(String(init.body)) as Body
    const id = mode === 'contract' ? body.client_id : undefined
    const sameBody = JSON.stringify({ text: body.text, step_id: body.step_id || undefined })
    if (id !== undefined) {
      const prev = byKey.get(id)
      if (prev && (prev.route !== route || prev.body !== sameBody)) {
        return answer(409, { error: { code: 'client_id_reused', message: 'client_id was used for another request' } }, 10)
      }
      if (prev) return answer(200, prev.ev, state.answerAfter) // a repeat appends nothing
    }
    const seq = stored.length + 1
    const echo = id !== undefined ? { client_id: id } : {}
    const ev: Event =
      route === 'steer'
        ? { room_id: 'r1', seq, type: 'steer.sent', actor: ARJUN, payload: { text: body.text, ...echo }, at: '' }
        : {
            room_id: 'r1',
            seq,
            type: 'comment.added',
            actor: ARJUN,
            payload: { comment_id: `c${seq}`, step_id: body.step_id, text: body.text, mentions_agent: false, ...echo },
            at: '',
          }
    stored.push(ev)
    if (id !== undefined) byKey.set(id, { route, body: sameBody, ev })
    return answer(200, ev, state.answerAfter)
  })
  return { stored, fetchMock, state }
}

type Server = ReturnType<typeof fakeServer>

async function openRoom(server: Server) {
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

/** Starts a send without waiting for it; the getter says how it ended (undefined while pending). */
function fire(p: () => Promise<boolean>) {
  let ok: boolean | undefined
  act(() => {
    void p().then((v) => (ok = v))
  })
  return () => ok
}

async function pass(ms: number) {
  await act(async () => {
    await vi.advanceTimersByTimeAsync(ms)
  })
}

/** The server's live push (or the reconnect replay) of a stored event. */
async function push(socket: FakeWebSocket, ev: Event) {
  await act(async () => {
    socket.onmessage?.({ data: JSON.stringify({ type: 'event', event: ev }) })
    await Promise.resolve()
  })
}

/** One quick exchange, so the hook has seen whether the server echoes client_id. */
async function warmUp(hook: { result: { current: ReturnType<typeof useRoom> } }) {
  const ok = fire(() => hook.result.current.actions.steer('hello', { clientId: 'warm' }))
  await pass(200)
  expect(ok()).toBe(true)
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

describe('a server with idempotent posts: one action is stored once, whatever is retried', () => {
  it('a1: send (answered at 20 s), then Retry: one stored copy, and the Retry is answered', async () => {
    const server = fakeServer('contract')
    const { hook } = await openRoom(server)
    await warmUp(hook)
    server.state.answerAfter = 20_000
    const sent = fire(() => hook.result.current.actions.steer('stop', { clientId: 'k1' }))
    await pass(SEND_DEADLINE_MS + 100)
    expect(sent()).toBe(false)
    expect(hook.result.current.error).toBe(OFFLINE_TEXT) // kept, and safe to send again

    server.state.answerAfter = 100
    const retried = fire(() => hook.result.current.actions.steer('stop', { clientId: 'k1' }))
    await pass(500)
    expect(retried()).toBe(true)
    expect(server.stored.filter((e) => (e.payload as { text: string }).text === 'stop')).toHaveLength(1)
    expect(hook.result.current.error).toBeNull()
  })

  it('a2: send (answered at 20 s), then a reconnect with no click: re-sent once, stored once', async () => {
    const server = fakeServer('contract')
    const { hook, socket } = await openRoom(server)
    await warmUp(hook)
    server.state.answerAfter = 20_000
    fire(() => hook.result.current.actions.steer('stop', { clientId: 'k1' }))
    await pass(SEND_DEADLINE_MS + 100)
    server.state.answerAfter = 100
    act(() => socket.onclose?.({ reason: '' }))
    act(() => socket.onopen?.())
    await pass(30_000)
    // The reconnect re-sent the action with its id; the server appended nothing for it.
    expect(server.fetchMock).toHaveBeenCalledTimes(3)
    expect(server.stored.filter((e) => (e.payload as { text: string }).text === 'stop')).toHaveLength(1)
    expect(hook.result.current.error).toBeNull()
  })

  it('a3: offline, the reconnect copy is answered at 20 s, then Retry: one stored copy', async () => {
    const server = fakeServer('contract')
    const { hook, socket } = await openRoom(server)
    await warmUp(hook)
    server.state.offlineNext = 1
    fire(() => hook.result.current.actions.steer('stop', { clientId: 'k1' }))
    await pass(10)
    server.state.answerAfter = 20_000
    act(() => socket.onopen?.())
    await pass(SEND_DEADLINE_MS + 100)
    server.state.answerAfter = 100
    const retried = fire(() => hook.result.current.actions.steer('stop', { clientId: 'k1' }))
    await pass(500)
    expect(retried()).toBe(true)
    expect(server.stored.filter((e) => (e.payload as { text: string }).text === 'stop')).toHaveLength(1)
  })

  it('a4: the same for a step comment (Timeline): one stored copy', async () => {
    const server = fakeServer('contract')
    const { hook, socket } = await openRoom(server)
    await warmUp(hook)
    server.state.offlineNext = 1
    fire(() => hook.result.current.actions.comment('looks right', 's1', { clientId: 'k1' }))
    await pass(10)
    server.state.answerAfter = 20_000
    act(() => socket.onopen?.())
    await pass(SEND_DEADLINE_MS + 100)
    server.state.answerAfter = 100
    fire(() => hook.result.current.actions.comment('looks right', 's1', { clientId: 'k1' }))
    await pass(500)
    act(() => socket.onopen?.())
    fire(() => hook.result.current.actions.comment('looks right', 's1', { clientId: 'k1' }))
    await pass(500)
    expect(server.stored.filter((e) => e.type === 'comment.added')).toHaveLength(1)
  })

  it('a5: Retry while the reconnect copy is sending (answered at 13 s): one stored copy', async () => {
    const server = fakeServer('contract')
    const { hook, socket } = await openRoom(server)
    await warmUp(hook)
    server.state.offlineNext = 1
    fire(() => hook.result.current.actions.steer('stop', { clientId: 'k1' }))
    await pass(10)
    server.state.answerAfter = 13_000
    act(() => socket.onopen?.())
    server.state.answerAfter = 100 // only the reconnect copy is slow
    const retried = fire(() => hook.result.current.actions.steer('stop', { clientId: 'k1' }))
    await pass(SEND_DEADLINE_MS + 500)
    // The copy got no answer in time; the server dedupes, so the waiting Retry sent the action
    // again with its id and was answered with the stored event.
    expect(retried()).toBe(true)
    await pass(2_000) // the copy's own late answer changes nothing
    expect(server.stored.filter((e) => (e.payload as { text: string }).text === 'stop')).toHaveLength(1)
  })

  it('the stream settles an action by client_id: the event before the answer clears the banner', async () => {
    const server = fakeServer('contract')
    const { hook, socket } = await openRoom(server)
    server.state.answerAfter = 20_000
    const heard: string[] = []
    hook.result.current.actions.onDelivered?.((s) => (heard.push(s.clientId), true))
    fire(() => hook.result.current.actions.steer('stop', { clientId: 'k1' }))
    await pass(SEND_DEADLINE_MS + 100)
    expect(hook.result.current.error).toBe(UNCONFIRMED_TEXT) // first send: server not known yet
    await push(socket, server.stored[0])
    expect(heard).toEqual(['k1'])
    expect(hook.result.current.error).toBeNull()
    // A Retry of a landed action posts nothing.
    const retried = fire(() => hook.result.current.actions.steer('stop', { clientId: 'k1' }))
    await pass(100)
    expect(retried()).toBe(true)
    expect(server.fetchMock).toHaveBeenCalledTimes(1)
  })

  it('a deliberate double send of the same words is two actions: two stored copies', async () => {
    const server = fakeServer('contract')
    const { hook } = await openRoom(server)
    fire(() => hook.result.current.actions.steer('yes', { clientId: 'k1' }))
    await pass(200)
    fire(() => hook.result.current.actions.steer('yes', { clientId: 'k2' }))
    await pass(200)
    expect(server.stored.filter((e) => (e.payload as { text: string }).text === 'yes')).toHaveLength(2)
  })

  it('409 client_id_reused is a client bug: reported once, never retried', async () => {
    const server = fakeServer('contract')
    const { hook, socket } = await openRoom(server)
    const logged = vi.spyOn(console, 'error').mockImplementation(() => {})
    try {
      fire(() => hook.result.current.actions.steer('yes', { clientId: 'k1' }))
      await pass(200)
      const reused = fire(() => hook.result.current.actions.steer('no', { clientId: 'k1' }))
      await pass(200)
      expect(reused()).toBe(false)
      expect(hook.result.current.error).toMatch(/already used/)
      expect(logged).toHaveBeenCalled()
      act(() => socket.onopen?.())
      await pass(1_000)
      expect(server.fetchMock).toHaveBeenCalledTimes(2)
      expect(server.stored).toHaveLength(1)
    } finally {
      logged.mockRestore()
    }
  })
})

describe('a server without #459 (no client_id echo): never re-post an unknown outcome on its own', () => {
  it('a reconnect does not re-send it; a Retry waits out the window, then a click posts once', async () => {
    const server = fakeServer('old')
    const { hook, socket } = await openRoom(server)
    await warmUp(hook) // answered without an echo: the server does not dedupe
    server.state.answerAfter = 20_000
    fire(() => hook.result.current.actions.steer('stop', { clientId: 'k1' }))
    await pass(SEND_DEADLINE_MS + 100)
    expect(hook.result.current.error).toBe(UNCONFIRMED_TEXT)

    act(() => socket.onopen?.())
    const early = fire(() => hook.result.current.actions.steer('stop', { clientId: 'k1' }))
    await pass(1_000)
    expect(early()).toBe(false)
    expect(server.fetchMock).toHaveBeenCalledTimes(2) // warm-up + the first send

    await pass(65_000)
    expect(hook.result.current.error).toBe(NOT_CONFIRMED_TEXT)
    server.state.answerAfter = 100
    const click = fire(() => hook.result.current.actions.steer('stop', { clientId: 'k1' }))
    await pass(500)
    expect(click()).toBe(true)
    expect(server.fetchMock).toHaveBeenCalledTimes(3)
  })
})
