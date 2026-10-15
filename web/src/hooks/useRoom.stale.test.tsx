import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { act, renderHook, waitFor } from '@testing-library/react'
import type { Event, RoomSnapshot } from '../api/types'

// #464 (the web half of #287): a fake server implementing docs/api.md "Stale position". A socket
// opened with `after` past the stored log gets one `stale_position` error frame and a 1008 close
// with reason `stale_position`; otherwise it replays every stored event with seq > after.

vi.mock('sonner', () => ({ toast: Object.assign(vi.fn(), { warning: vi.fn(), dismiss: vi.fn() }) }))
vi.mock('../context/user', () => ({
  useCurrentUser: () => ({
    user: { id: 'arjun', name: 'Arjun', layers: ['kafka'] },
    setUserId: vi.fn(),
    logout: vi.fn(),
    sessionEpoch: 0,
  }),
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
  static CLOSING = 2
  static CLOSED = 3
  readyState = FakeWebSocket.CONNECTING
  onopen: (() => void) | null = null
  onmessage: ((m: { data: string }) => void) | null = null
  onclose: ((ev: { reason: string }) => void) | null = null
  onerror: (() => void) | null = null
  send() {}
  close() {
    this.readyState = FakeWebSocket.CLOSED
  }
  /** The server drops the connection. */
  drop(reason = '') {
    this.readyState = FakeWebSocket.CLOSED
    this.onclose?.({ reason })
  }
}

const snap: RoomSnapshot = {
  id: 'r1',
  title: 'T',
  severity: 'sev2',
  pipeline_id: 'orders',
  status: 'open',
  opened_at: '2026-10-11T10:00:00Z',
  last_seq: 0,
  driver: null,
  agent_status: { state: 'idle' },
  hypothesis: null,
  open_proposals: [],
  pending_suggestions: [],
  members: [],
}

function ev(seq: number, state = 'working'): Event {
  return {
    room_id: 'r1',
    seq,
    type: 'agent.status',
    actor: { kind: 'agent', id: 'agent', name: 'Agent' },
    payload: { state },
    at: `2026-10-11T10:00:${String(seq).padStart(2, '0')}Z`,
  }
}

function fakeServer() {
  const server = {
    log: [] as Event[],
    /** Refuse every `after`, even 0: the client bug a second stale_position in a row stands for. */
    alwaysStale: false,
    opens: [] as number[],
    sockets: [] as FakeWebSocket[],
  }
  vi.mocked(openRoomWs).mockImplementation((_room: string, after: number) => {
    const sock = new FakeWebSocket()
    server.opens.push(after)
    server.sockets.push(sock)
    queueMicrotask(() => {
      if (sock.readyState === FakeWebSocket.CLOSED) return
      sock.readyState = FakeWebSocket.OPEN
      sock.onopen?.() // the upgrade is accepted before the check
      const last = server.log.length ? server.log[server.log.length - 1].seq : 0
      if (server.alwaysStale || after > last) {
        const message = `after ${after} is past the room's last event ${last}`
        sock.onmessage?.({ data: JSON.stringify({ type: 'error', error: { code: 'stale_position', message } }) })
        sock.drop('stale_position')
        return
      }
      for (const e of server.log) {
        if (e.seq > after) sock.onmessage?.({ data: JSON.stringify({ type: 'event', event: e }) })
      }
    })
    return sock as unknown as WebSocket
  })
  vi.mocked(api.room).mockResolvedValue(snap)
  vi.mocked(api.me).mockResolvedValue(undefined as never)
  return server
}

async function pass(ms: number) {
  await act(async () => {
    await vi.advanceTimersByTimeAsync(ms)
  })
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

describe('stale_position (#464)', () => {
  it('reloads once with after=0, silently, and catches up', async () => {
    const server = fakeServer()
    server.log = [ev(1), ev(2)]
    const hook = renderHook(() => useRoom('r1'))
    await waitFor(() => expect(hook.result.current.state?.last_seq).toBe(2))
    expect(server.opens).toEqual([0])

    // The log behind the client's position is gone (e.g. restored from an older backup).
    server.log = [ev(1, 'idle')]
    act(() => server.sockets[0].drop())
    await pass(15_000)
    expect(server.opens).toEqual([0, 2, 0])
    expect(api.room).toHaveBeenCalledTimes(2) // the room state is rebuilt from the snapshot
    expect(hook.result.current.error).toBeNull()
    expect(hook.result.current.conn).toBe('live')
    expect(hook.result.current.state?.agent_status.state).toBe('idle')
    // Caught up: no more opens.
    await pass(30_000)
    expect(server.opens).toEqual([0, 2, 0])
  })

  it('does not show the first stale_position error frame', async () => {
    const server = fakeServer()
    server.log = [ev(1), ev(2)]
    const hook = renderHook(() => useRoom('r1'))
    await waitFor(() => expect(server.opens).toEqual([0]))
    await pass(10)
    const sock = server.sockets[0]
    act(() =>
      sock.onmessage?.({
        data: JSON.stringify({ type: 'error', error: { code: 'stale_position', message: 'after 2 is past the room\'s last event 0' } }),
      }),
    )
    expect(hook.result.current.error).toBeNull()
  })

  it('a second stale_position in a row shows the error and stops retrying', async () => {
    const server = fakeServer()
    server.alwaysStale = true
    server.log = [ev(1)]
    const hook = renderHook(() => useRoom('r1'))
    await pass(15_000)
    expect(server.opens).toEqual([0, 0])
    expect(hook.result.current.error).toBe("after 0 is past the room's last event 1")
    expect(hook.result.current.conn).toBe('error')
    await pass(60_000)
    expect(server.opens).toEqual([0, 0])
  })

  it('once events arrive after the reload, a later stale_position reloads silently again', async () => {
    const server = fakeServer()
    server.log = [ev(1), ev(2)]
    const hook = renderHook(() => useRoom('r1'))
    await pass(100)
    server.log = [ev(1)]
    act(() => server.sockets[0].drop())
    await pass(15_000)
    expect(server.opens).toEqual([0, 2, 0])
    server.log = [ev(1), ev(2), ev(3)]
    act(() => server.sockets[2].onmessage?.({ data: JSON.stringify({ type: 'event', event: ev(2) }) }))
    act(() => server.sockets[2].onmessage?.({ data: JSON.stringify({ type: 'event', event: ev(3) }) }))
    await pass(10)
    server.log = [ev(1)]
    act(() => server.sockets[2].drop())
    await pass(15_000)
    expect(server.opens).toEqual([0, 2, 0, 3, 0])
    expect(hook.result.current.error).toBeNull()
    expect(hook.result.current.conn).toBe('live')
  })

  it('other 1008 closes keep reconnecting with the highest seen seq', async () => {
    const server = fakeServer()
    server.log = [ev(1), ev(2)]
    renderHook(() => useRoom('r1'))
    await pass(100)
    act(() => server.sockets[0].drop('replay timeout'))
    await pass(15_000)
    expect(server.opens).toEqual([0, 2])
    expect(api.room).toHaveBeenCalledTimes(1)
  })
})
