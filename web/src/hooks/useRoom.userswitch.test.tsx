import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { StrictMode, useEffect } from 'react'
import { act, renderHook, waitFor } from '@testing-library/react'
import type { Event, RoomSnapshot } from '../api/types'
import type { RoomState } from '../state/types'

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

// The server's answer to a POST: the stored event, echoing client_id like a server with #459.
function answer(init: RequestInit) {
  const { client_id } = JSON.parse(String(init.body)) as { client_id?: string }
  const actor = { kind: 'human' as const, id: who.user.id, name: who.user.name }
  const ev: Event = { room_id: 'r1', seq: 1, type: 'comment.added', actor, payload: { client_id }, at: '' }
  return Promise.resolve(new Response(JSON.stringify(ev), { status: 200 }))
}

/**
 * Only a server known to dedupe keeps a failed send for the reconnect (#545, #551), and only this
 * client's own answer that echoes client_id says so. One answered comment, then forgotten.
 */
async function knownToDedupe(actions: ReturnType<typeof useRoom>['actions'], posts: unknown[]) {
  await act(async () => {
    expect(await actions.comment('warm-up', undefined, { clientId: 'warm-up' })).toBe(true)
  })
  posts.length = 0
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
    let offline = false
    vi.stubGlobal(
      'fetch',
      vi.fn((_path: string, init: RequestInit) => {
        posts.push({ as: who.user.id, body: String(init.body) })
        if (offline) return Promise.reject(new TypeError('Failed to fetch'))
        return answer(init)
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
    await knownToDedupe(hook.result.current.actions, posts)
    offline = true
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
    let offline = false
    vi.stubGlobal(
      'fetch',
      vi.fn((_path: string, init: RequestInit) => {
        posts.push(String(init.body))
        if (offline) return Promise.reject(new TypeError('Failed to fetch'))
        return answer(init)
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
    await knownToDedupe(hook.result.current.actions, posts)
    offline = true
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

// #550 keeps the state through a reload of the same room, but only for the same session: what one
// user saw of a room is never shown to the next one, even for a moment while their snapshot loads.
describe('the room state across a session change in the same room (#536, #550)', () => {
  it.each([
    ['a user switch', { id: 'priya', name: 'Priya', layers: ['postgres'] }],
    ['a new session of the same user', { id: 'arjun', name: 'Arjun', layers: ['kafka'] }],
  ])('%s resets the state: the old session\'s room is never shown', async (_, next) => {
    let release: (s: RoomSnapshot) => void = () => {}
    vi.mocked(api.room)
      .mockResolvedValueOnce({ ...snap, title: 'as seen by arjun, epoch 0' })
      .mockImplementationOnce(() => new Promise((resolve) => (release = resolve)))
    vi.mocked(openRoomWs).mockImplementation(() => new FakeWebSocket() as unknown as WebSocket)
    // The state of each committed render: what a screen drew.
    const seen: (RoomState | null)[] = []
    const hook = renderHook(() => {
      const r = useRoom('r1')
      useEffect(() => {
        seen.push(r.state)
      })
      return r
    })
    await waitFor(() => expect(hook.result.current.state?.title).toBe('as seen by arjun, epoch 0'))

    who.user = next
    who.sessionEpoch = 1
    const from = seen.length
    hook.rerender()
    await pass(100)
    expect(api.room).toHaveBeenCalledTimes(2)
    expect(seen.slice(from).length).toBeGreaterThan(0)
    expect(seen.slice(from).every((st) => st === null)).toBe(true)
    expect(hook.result.current.state).toBeNull()

    await act(async () => release({ ...snap, title: 'the new session' }))
    await waitFor(() => expect(hook.result.current.state?.title).toBe('the new session'))
  })
})
