import { beforeEach, describe, expect, it, vi } from 'vitest'
import { act, renderHook, waitFor } from '@testing-library/react'
import type { Event, EventsPage, RoomSnapshot } from '../api/types'

// The hook reads the current user from context; provide a fixed user so it can
// render without a router/provider.
vi.mock('../context/user', () => ({
  useCurrentUser: () => ({
    user: { id: 'arjun', name: 'Arjun', layers: ['kafka-connect', 'kafka'] },
    setUserId: vi.fn(),
  }),
}))

// Mock the transport surface (api.room, api.events, openRoomWs) but keep the
// rest of the client module intact.
vi.mock('../api/client', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../api/client')>()
  return {
    ...actual,
    isFixtures: () => false,
    openRoomWs: vi.fn(),
    api: {
      ...actual.api,
      room: vi.fn(),
      events: vi.fn(),
      grantControl: vi.fn(),
    },
  }
})

import { ApiClientError, api, openRoomWs } from '../api/client'
import { useRoom } from './useRoom'

class FakeWebSocket {
  static CONNECTING = 0
  static OPEN = 1
  static CLOSING = 2
  static CLOSED = 3
  readyState = FakeWebSocket.OPEN
  sent: string[] = []
  closed = false
  onopen: (() => void) | null = null
  onmessage: ((m: { data: string }) => void) | null = null
  onclose: (() => void) | null = null
  onerror: (() => void) | null = null
  send(data: string) {
    this.sent.push(data)
  }
  close() {
    this.closed = true
    this.readyState = FakeWebSocket.CLOSED
  }
}

function snapshot(id: string): RoomSnapshot {
  return {
    id,
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
}

function ev(
  seq: number,
  type: Event['type'] = 'agent.status',
  payload: unknown = { state: 'working' },
): Event {
  return {
    room_id: 'r1',
    seq,
    type,
    actor: { kind: 'agent', id: 'agent', name: 'Agent' },
    payload,
    at: `2026-10-11T10:00:${String(seq).padStart(2, '0')}Z`,
  }
}

function wsMessage(event: Event) {
  return { data: JSON.stringify({ type: 'event', event }) }
}

beforeEach(() => {
  vi.stubGlobal('WebSocket', FakeWebSocket)
  vi.resetAllMocks()
})

describe('useRoom transport', () => {
  it('fills a gap from /events before applying the event, in order', async () => {
    const socket = new FakeWebSocket()
    vi.mocked(api.room).mockResolvedValue(snapshot('r1'))
    vi.mocked(api.events).mockResolvedValue({
      events: [
        ev(1, 'comment.added', { comment_id: 'c1', text: 'hi', mentions_agent: false }),
        ev(2, 'agent.status', { state: 'working' }),
      ],
      last_seq: 2,
    } satisfies EventsPage)
    vi.mocked(openRoomWs).mockReturnValue(socket as unknown as WebSocket)

    const { result } = renderHook(() => useRoom('r1'))
    await waitFor(() => expect(openRoomWs).toHaveBeenCalledWith('r1', 'arjun', 0))
    socket.onopen?.()

    await act(async () => {
      socket.onmessage?.(
        wsMessage(ev(3, 'comment.added', { comment_id: 'c2', text: 'again', mentions_agent: false })),
      )
    })

    await waitFor(() => expect(result.current.state?.last_seq).toBe(3))
    expect(api.events).toHaveBeenCalledWith('arjun', 'r1', 0)
    expect(result.current.state?.comments).toHaveLength(2)
  })

  it('closes the socket when the gap fetch fails, without applying the event', async () => {
    const socket = new FakeWebSocket()
    vi.mocked(api.room).mockResolvedValue(snapshot('r1'))
    vi.mocked(api.events).mockRejectedValue(new Error('boom'))
    vi.mocked(openRoomWs).mockReturnValue(socket as unknown as WebSocket)

    const { result } = renderHook(() => useRoom('r1'))
    await waitFor(() => expect(openRoomWs).toHaveBeenCalledWith('r1', 'arjun', 0))
    socket.onopen?.()

    await act(async () => {
      socket.onmessage?.(wsMessage(ev(3)))
    })

    await waitFor(() => expect(socket.closed).toBe(true))
    expect(result.current.state?.last_seq).toBe(0)
  })

  it('serializes two messages that arrive during one gap fetch', async () => {
    const socket = new FakeWebSocket()
    vi.mocked(api.room).mockResolvedValue(snapshot('r1'))

    let resolveFirst!: (page: EventsPage) => void
    const firstFetch = new Promise<EventsPage>((resolve) => {
      resolveFirst = resolve
    })
    vi.mocked(api.events)
      .mockImplementationOnce(() => firstFetch)
      .mockResolvedValueOnce({ events: [ev(3)], last_seq: 3 })

    vi.mocked(openRoomWs).mockReturnValue(socket as unknown as WebSocket)

    const { result } = renderHook(() => useRoom('r1'))
    await waitFor(() => expect(openRoomWs).toHaveBeenCalledWith('r1', 'arjun', 0))
    socket.onopen?.()

    // Two messages while the first gap fetch is still pending.
    await act(async () => {
      socket.onmessage?.(wsMessage(ev(2)))
      socket.onmessage?.(wsMessage(ev(4)))
    })
    await waitFor(() => expect(api.events).toHaveBeenCalledTimes(1))

    await act(async () => {
      resolveFirst({ events: [ev(1)], last_seq: 1 })
    })

    await waitFor(() => expect(result.current.state?.last_seq).toBe(4))
    expect(api.events).toHaveBeenCalledTimes(2)
    expect(api.events).toHaveBeenNthCalledWith(1, 'arjun', 'r1', 0)
    expect(api.events).toHaveBeenNthCalledWith(2, 'arjun', 'r1', 2)
  })

  it('shows short user text for a lease_held error instead of the raw message', async () => {
    const socket = new FakeWebSocket()
    vi.mocked(api.room).mockResolvedValue(snapshot('r1'))
    vi.mocked(openRoomWs).mockReturnValue(socket as unknown as WebSocket)

    const { result } = renderHook(() => useRoom('r1'))
    await waitFor(() => expect(openRoomWs).toHaveBeenCalledWith('r1', 'arjun', 0))
    socket.onopen?.()

    await act(async () => {
      socket.onmessage?.({
        data: JSON.stringify({
          type: 'error',
          error: { code: 'lease_held', message: 'control/grant: someone holds the lease' },
        }),
      })
    })

    expect(result.current.error).toBe('someone else holds control')
  })

  it('maps a lease_held rejection from control/grant (self-grant race) to the friendly text', async () => {
    const socket = new FakeWebSocket()
    vi.mocked(api.room).mockResolvedValue(snapshot('r1'))
    vi.mocked(openRoomWs).mockReturnValue(socket as unknown as WebSocket)
    vi.mocked(api.grantControl).mockRejectedValue(
      new ApiClientError(409, 'lease_held', 'control/grant: kabir holds the lease until 10:35'),
    )

    const { result } = renderHook(() => useRoom('r1'))
    await waitFor(() => expect(openRoomWs).toHaveBeenCalledWith('r1', 'arjun', 0))
    socket.onopen?.()

    // The no-driver banner grants control to the current user.
    await act(async () => {
      result.current.actions.grantControl('arjun')
    })

    await waitFor(() => expect(result.current.error).toBe('someone else holds control'))
    expect(api.grantControl).toHaveBeenCalledWith('arjun', 'r1', 'arjun')
  })
})
