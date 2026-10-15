import { beforeEach, describe, expect, it, vi } from 'vitest'
import { act, renderHook, waitFor } from '@testing-library/react'
import type { RoomSnapshot } from '../api/types'

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
    api: { ...actual.api, me: vi.fn(), room: vi.fn(), events: vi.fn(), steer: vi.fn(), comment: vi.fn() },
  }
})

import { ApiClientError, api, openRoomWs } from '../api/client'
import { OFFLINE_TEXT } from '../lib/errors'
import { useRoom } from './useRoom'

class FakeWebSocket {
  static CONNECTING = 0
  static OPEN = 1
  readyState = FakeWebSocket.OPEN
  onopen: (() => void) | null = null
  onmessage: ((m: { data: string }) => void) | null = null
  onclose: ((ev: { reason: string }) => void) | null = null
  onerror: (() => void) | null = null
  sent: string[] = []
  send(data: string) {
    this.sent.push(data)
  }
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
  driver: null,
  agent_status: { state: 'idle' },
  hypothesis: null,
  open_proposals: [],
  pending_suggestions: [],
  members: [],
}

async function openRoom() {
  const socket = new FakeWebSocket()
  vi.mocked(api.room).mockResolvedValue(snap)
  vi.mocked(openRoomWs).mockReturnValue(socket as unknown as WebSocket)
  const hook = renderHook(() => useRoom('r1'))
  await waitFor(() => expect(openRoomWs).toHaveBeenCalled())
  return { hook, socket }
}

beforeEach(() => {
  vi.stubGlobal('WebSocket', FakeWebSocket)
  vi.resetAllMocks()
})

describe('useRoom offline sends', () => {
  it('reports false for a network failure, keeps the message, and retries it once on reconnect', async () => {
    const { hook, socket } = await openRoom()
    vi.mocked(api.steer).mockRejectedValueOnce(new TypeError('Failed to fetch'))
    let ok = true
    await act(async () => {
      ok = await hook.result.current.actions.steer('check the slot')
    })
    expect(ok).toBe(false)
    expect(hook.result.current.error).toBe(OFFLINE_TEXT)

    vi.mocked(api.steer).mockResolvedValue(undefined as never)
    act(() => socket.onopen?.())
    await waitFor(() => expect(api.steer).toHaveBeenCalledTimes(2))
    expect(api.steer).toHaveBeenLastCalledWith('r1', 'check the slot')
    expect(hook.result.current.error).toBeNull()

    // Once only: a second reconnect does not resend it.
    act(() => socket.onopen?.())
    expect(api.steer).toHaveBeenCalledTimes(2)
  })

  it('shows the friendly text and drops the message when the retry hits 409 room_closed', async () => {
    const { hook, socket } = await openRoom()
    vi.mocked(api.comment).mockRejectedValueOnce(new TypeError('Failed to fetch'))
    await act(async () => {
      await hook.result.current.actions.comment('hello', 's_1')
    })
    vi.mocked(api.comment).mockRejectedValue(new ApiClientError(409, 'room_closed', 'room is closed'))
    act(() => socket.onopen?.())
    await waitFor(() => expect(hook.result.current.error).toBe('room is closed'))
    act(() => socket.onopen?.())
    expect(api.comment).toHaveBeenCalledTimes(2)
  })

  it('a non-network failure is not queued', async () => {
    const { hook, socket } = await openRoom()
    vi.mocked(api.steer).mockRejectedValue(new ApiClientError(403, 'not_driver', 'only the driver'))
    let ok = true
    await act(async () => {
      ok = await hook.result.current.actions.steer('x')
    })
    expect(ok).toBe(false)
    act(() => socket.onopen?.())
    expect(api.steer).toHaveBeenCalledTimes(1)
  })

  // The composer keeps the words and a Retry button after the reconnect sent them: Retry must not
  // steer the agent a second time.
  it('a manual retry after the reconnect delivered the message does not send it again', async () => {
    const { hook, socket } = await openRoom()
    vi.mocked(api.steer).mockRejectedValueOnce(new TypeError('Failed to fetch'))
    await act(async () => {
      await hook.result.current.actions.steer('restart connector')
    })
    vi.mocked(api.steer).mockResolvedValue(undefined as never)
    act(() => socket.onopen?.())
    await waitFor(() => expect(api.steer).toHaveBeenCalledTimes(2))

    let ok = false
    await act(async () => {
      ok = await hook.result.current.actions.steer('restart connector')
    })
    expect(ok).toBe(true)
    expect(api.steer).toHaveBeenCalledTimes(2)

    // Only that one retry is absorbed: sending the same words again later is a new message.
    await act(async () => {
      await hook.result.current.actions.steer('restart connector')
    })
    expect(api.steer).toHaveBeenCalledTimes(3)
  })
})

describe('useRoom presence', () => {
  // A hidden tab told the room "away"; the 20 s heartbeat must not flip it back to "viewing".
  it('the heartbeat and a reconnect from a hidden tab say away', async () => {
    const hidden = vi.spyOn(document, 'hidden', 'get').mockReturnValue(true)
    vi.useFakeTimers({ shouldAdvanceTime: true })
    try {
      const { socket } = await openRoom()
      act(() => socket.onopen?.())
      act(() => {
        vi.advanceTimersByTime(20_000)
      })
      const states = socket.sent.map((d) => JSON.parse(d)).filter((m) => m.type === 'presence').map((m) => m.state)
      expect(states.length).toBeGreaterThanOrEqual(2)
      expect(states.every((st) => st === 'away')).toBe(true)
    } finally {
      vi.useRealTimers()
      hidden.mockRestore()
    }
  })
})


describe('useRoom deleted room', () => {
  // The server sends room.deleted, closes with reason "room deleted" and answers 404 afterwards.
  it('stops reconnecting once the server closes with "room deleted"', async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true })
    try {
      const { hook, socket } = await openRoom()
      act(() => socket.onopen?.())
      act(() => socket.onclose?.({ reason: 'room deleted' }))
      expect(hook.result.current.conn).toBe('not_found')
      expect(hook.result.current.error).toBe('This room was deleted.')
      act(() => {
        vi.advanceTimersByTime(30_000)
      })
      expect(openRoomWs).toHaveBeenCalledTimes(1)
    } finally {
      vi.useRealTimers()
    }
  })

  it('a 404 snapshot is not_found, not a generic error', async () => {
    vi.mocked(api.room).mockRejectedValue(new ApiClientError(404, 'not_found', 'room not found'))
    const hook = renderHook(() => useRoom('r_gone'))
    await waitFor(() => expect(hook.result.current.conn).toBe('not_found'))
  })
})
