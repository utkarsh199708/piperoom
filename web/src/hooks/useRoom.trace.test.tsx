import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { act, renderHook, waitFor } from '@testing-library/react'
import type { Actor, Event, RoomSnapshot } from '../api/types'
import { ApiClientError } from '../api/client'

// KAN-557: a trace the server refuses (400 validation) is answered to the trace panel, whose overlay
// covers the room error banner, so the banner stays clear. A trace that works is pinned by its event.

vi.mock('sonner', () => ({ toast: Object.assign(vi.fn(), { warning: vi.fn(), dismiss: vi.fn() }) }))
vi.mock('../context/user', () => ({
  useCurrentUser: () => ({
    user: { id: 'arjun', name: 'Arjun', layers: ['consumer'] },
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
    api: { ...actual.api, me: vi.fn(), room: vi.fn(), events: vi.fn(), trace: vi.fn() },
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
}

const arjun: Actor = { kind: 'human', id: 'arjun', name: 'Arjun' }
const NOT_IN_PIPELINE = 'topology: bad input: no connector streams table "public.nope" in pipeline "orders"'

const snapshot: RoomSnapshot = {
  id: 'r1',
  title: 'T',
  severity: 'sev2',
  pipeline_id: 'orders',
  status: 'open',
  opened_at: '2026-10-19T09:58:00Z',
  last_seq: 0,
  driver: null,
  agent_status: { state: 'idle' },
  hypothesis: null,
  open_proposals: [],
  pending_suggestions: [],
  members: [],
  mode: { write: true, solo: false, auto_approve: [] },
}

const pinned: Event = {
  room_id: 'r1',
  seq: 1,
  type: 'trace.pinned',
  actor: arjun,
  payload: {
    table: 'public.orders',
    key: '42',
    stages: [{ layer: 'postgres', name: 'public.orders', status: 'seen' }],
  },
  at: '2026-10-19T09:59:00Z',
}

const sockets: FakeWebSocket[] = []

async function live() {
  vi.mocked(openRoomWs).mockImplementation(() => {
    const sock = new FakeWebSocket()
    sockets.push(sock)
    queueMicrotask(() => {
      sock.readyState = FakeWebSocket.OPEN
      sock.onopen?.()
    })
    return sock as unknown as WebSocket
  })
  vi.mocked(api.me).mockResolvedValue(undefined as never)
  vi.mocked(api.room).mockResolvedValue(snapshot)
  const hook = renderHook(() => useRoom('r1'))
  await waitFor(() => expect(hook.result.current.conn).toBe('live'))
  return hook
}

beforeEach(() => {
  vi.stubGlobal('WebSocket', FakeWebSocket)
  vi.resetAllMocks()
  sockets.length = 0
})
afterEach(() => {
  vi.unstubAllGlobals()
})

describe('useRoom trace', () => {
  it('answers a refused trace to the caller and keeps it out of the room banner', async () => {
    const hook = await live()
    vi.mocked(api.trace).mockRejectedValue(new ApiClientError(400, 'validation', NOT_IN_PIPELINE))
    let res: { ok: boolean; error?: string } | undefined
    await act(async () => {
      res = await hook.result.current.actions.trace('public.nope', '1')
    })
    expect(api.trace).toHaveBeenCalledWith('r1', 'public.nope', '1')
    expect(res).toEqual({ ok: false, error: NOT_IN_PIPELINE })
    expect(hook.result.current.error).toBeNull()
    expect(hook.result.current.state?.trace).toBeNull()
  })

  it('answers ok for a trace that works, and the trace.pinned event pins it to the room', async () => {
    const hook = await live()
    vi.mocked(api.trace).mockResolvedValue(pinned)
    let res: { ok: boolean; error?: string } | undefined
    await act(async () => {
      res = await hook.result.current.actions.trace('public.orders', '42')
    })
    expect(api.trace).toHaveBeenCalledWith('r1', 'public.orders', '42')
    expect(res).toEqual({ ok: true })
    expect(hook.result.current.error).toBeNull()

    act(() => sockets[0].onmessage?.({ data: JSON.stringify({ type: 'event', event: pinned }) }))
    await waitFor(() => expect(hook.result.current.state?.trace?.table).toBe('public.orders'))
    expect(hook.result.current.state?.trace?.key).toBe('42')
  })
})
