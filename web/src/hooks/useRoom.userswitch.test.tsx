import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { StrictMode } from 'react'
import { act, renderHook, waitFor } from '@testing-library/react'
import type { Event, RoomSnapshot } from '../api/types'

// #536: the send ledger belongs to one user's session. A dev "Switch user" changes user.id and
// the session epoch without remounting the room, and a kept steer of the previous user must never
// be posted under the new user's session.

const who = vi.hoisted(() => ({ user: { id: 'arjun', name: 'Arjun', layers: ['kafka'] }, sessionEpoch: 0 }))

vi.mock('sonner', () => ({ toast: Object.assign(vi.fn(), { warning: vi.fn(), dismiss: vi.fn() }) }))
vi.mock('../context/user', () => ({
  useCurrentUser: () => ({ user: who.user, setUserId: vi.fn(), logout: vi.fn(), sessionEpoch: who.sessionEpoch }),
}))
vi.mock('../api/client', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../api/client')>()
  return {
    ...actual,
    isFixtures: () => false,
    openRoomWs: vi.fn(),
    api: { ...actual.api, me: vi.fn(), room: vi.fn(), events: vi.fn() },
  }
})

import { api, openRoomWs } from '../api/client'
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

beforeEach(() => {
  vi.stubGlobal('WebSocket', FakeWebSocket)
  vi.resetAllMocks()
  vi.useFakeTimers({ shouldAdvanceTime: true })
  who.user = { id: 'arjun', name: 'Arjun', layers: ['kafka'] }
  who.sessionEpoch = 0
})

afterEach(() => {
  vi.useRealTimers()
  vi.unstubAllGlobals()
})

// Only a server known to dedupe keeps a failed send for the reconnect (#545, #551): the room's
// log shows one, a keyed post of another member, before the first send.
const KEYED: Event = {
  room_id: 'r1',
  seq: 1,
  type: 'comment.added',
  actor: { kind: 'human', id: 'kabir', name: 'Kabir' },
  payload: { comment_id: 'c0', text: 'earlier', mentions_agent: false, client_id: 'theirs' },
  at: '2026-10-11T10:00:01Z',
}

async function knownToDedupe(socket: FakeWebSocket) {
  await act(async () => {
    socket.onmessage?.({ data: JSON.stringify({ type: 'event', event: KEYED }) })
    await Promise.resolve()
  })
}

async function pass(ms: number) {
  await act(async () => {
    await vi.advanceTimersByTimeAsync(ms)
  })
}

describe('a dev user switch (#536)', () => {
  it("drops the previous user's kept steer: nothing is posted under the new user's session", async () => {
    // The session the browser's cookie belongs to when each POST goes out.
    const posts: { as: string; body: string }[] = []
    let offline = true
    vi.stubGlobal(
      'fetch',
      vi.fn((_path: string, init: RequestInit) => {
        posts.push({ as: who.user.id, body: String(init.body) })
        if (offline) return Promise.reject(new TypeError('Failed to fetch'))
        return Promise.resolve(new Response(JSON.stringify({}), { status: 200 }))
      }),
    )
    const sockets: FakeWebSocket[] = []
    vi.mocked(api.room).mockResolvedValue(snap)
    vi.mocked(api.events).mockResolvedValue({ events: [], next_after: 0 } as never)
    vi.mocked(openRoomWs).mockImplementation(() => {
      const s = new FakeWebSocket()
      sockets.push(s)
      return s as unknown as WebSocket
    })
    const hook = renderHook(() => useRoom('r1'))
    await waitFor(() => expect(sockets).toHaveLength(1))
    act(() => sockets[0].onopen?.())
    await knownToDedupe(sockets[0])
    act(() => sockets[0].onclose?.({ reason: '' })) // arjun goes offline
    let ok: boolean | undefined
    act(() => {
      void hook.result.current.actions.steer('arjun says stop', { clientId: 'kA' }).then((v) => (ok = v))
    })
    await pass(10)
    expect(ok).toBe(false)
    expect(hook.result.current.actions.sends?.isPending('kA')).toBe(true) // kept for the reconnect

    // Switch user (dev): setUserId → devLogin → bumpEpoch, no remount.
    offline = false
    who.user = { id: 'priya', name: 'Priya', layers: ['postgres'] }
    who.sessionEpoch = 1
    hook.rerender()
    await waitFor(() => expect(sockets.length).toBeGreaterThan(1))
    const before = posts.length
    act(() => sockets[sockets.length - 1].onopen?.())
    await pass(6_000) // the reconnect drain and the live retry
    expect(posts.slice(before)).toEqual([])
    expect(hook.result.current.actions.sends?.isPending('kA')).toBe(false)
    expect(hook.result.current.error).toBeNull()
  })

  it('the same user keeps the ledger across rerenders, under StrictMode too: a kept steer drains on reconnect', async () => {
    const posts: string[] = []
    let offline = true
    vi.stubGlobal(
      'fetch',
      vi.fn((_path: string, init: RequestInit) => {
        posts.push(String(init.body))
        if (offline) return Promise.reject(new TypeError('Failed to fetch'))
        return Promise.resolve(new Response(JSON.stringify({}), { status: 200 }))
      }),
    )
    const sockets: FakeWebSocket[] = []
    vi.mocked(api.room).mockResolvedValue(snap)
    vi.mocked(api.events).mockResolvedValue({ events: [], next_after: 0 } as never)
    vi.mocked(openRoomWs).mockImplementation(() => {
      const s = new FakeWebSocket()
      sockets.push(s)
      return s as unknown as WebSocket
    })
    const hook = renderHook(() => useRoom('r1'), { wrapper: StrictMode })
    await waitFor(() => expect(sockets.length).toBeGreaterThan(0))
    const last = () => sockets[sockets.length - 1]
    act(() => last().onopen?.())
    await knownToDedupe(last())
    act(() => last().onclose?.({ reason: '' }))
    act(() => {
      void hook.result.current.actions.steer('stop', { clientId: 'k1' })
    })
    await pass(10)
    hook.rerender()
    offline = false
    await pass(2_000) // the reconnect timer opens a new socket
    act(() => last().onopen?.())
    await pass(100)
    expect(posts).toEqual([JSON.stringify({ text: 'stop', client_id: 'k1' }), JSON.stringify({ text: 'stop', client_id: 'k1' })])
  })
})
