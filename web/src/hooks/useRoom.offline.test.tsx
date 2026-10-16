import { beforeEach, describe, expect, it, vi } from 'vitest'
import { act, renderHook, waitFor } from '@testing-library/react'
import type { Event, RoomSnapshot } from '../api/types'

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

async function openRoom() {
  const socket = new FakeWebSocket()
  vi.mocked(api.room).mockResolvedValue(snap)
  vi.mocked(openRoomWs).mockReturnValue(socket as unknown as WebSocket)
  const hook = renderHook(() => useRoom('r1'))
  await waitFor(() => expect(openRoomWs).toHaveBeenCalled())
  await knownToDedupe(socket)
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
    expect(api.steer).toHaveBeenLastCalledWith('r1', 'check the slot', expect.any(String))
    // The reconnect copy is the same action: it keeps the first send's client_id.
    expect(vi.mocked(api.steer).mock.calls[1][2]).toBe(vi.mocked(api.steer).mock.calls[0][2])
    // The banner says "kept" until the copy is answered.
    await waitFor(() => expect(hook.result.current.error).toBeNull())

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
  // #343, now by client_id (#459): the Retry is the same action as the failed send. Once the
  // reconnect delivered that action, its Retry posts nothing; the same words as a new action do.
  it('a manual retry after the reconnect delivered the message does not send it again', async () => {
    const { hook, socket } = await openRoom()
    vi.mocked(api.steer).mockRejectedValueOnce(new TypeError('Failed to fetch'))
    await act(async () => {
      await hook.result.current.actions.steer('restart connector', { clientId: 'k1' })
    })
    vi.mocked(api.steer).mockResolvedValue(undefined as never)
    act(() => socket.onopen?.())
    await waitFor(() => expect(api.steer).toHaveBeenCalledTimes(2))
    expect(api.steer).toHaveBeenLastCalledWith('r1', 'restart connector', 'k1')

    let ok = false
    await act(async () => {
      ok = await hook.result.current.actions.steer('restart connector', { clientId: 'k1' })
    })
    expect(ok).toBe(true)
    expect(api.steer).toHaveBeenCalledTimes(2)

    // The same words as a new action (a new id) are a new message.
    await act(async () => {
      await hook.result.current.actions.steer('restart connector', { clientId: 'k2' })
    })
    expect(api.steer).toHaveBeenCalledTimes(3)
  })
})

// #390: one Retry is one steer, whatever the reconnect timing (matched by client_id, #459).
describe('useRoom retry after a reconnect', () => {
  async function failOnce(text: string, clientId: string) {
    const opened = await openRoom()
    vi.mocked(api.steer).mockRejectedValueOnce(new TypeError('Failed to fetch'))
    await act(async () => {
      await opened.hook.result.current.actions.steer(text, { clientId })
    })
    return opened
  }

  it('a Retry pressed while the reconnect drain is in flight waits for it and does not post again', async () => {
    const { hook, socket } = await failOnce('restart connector', 'k1')
    let resolveDrain = () => {}
    vi.mocked(api.steer).mockImplementationOnce(
      () => new Promise<never>((r) => (resolveDrain = r as unknown as () => void)),
    )
    vi.mocked(api.steer).mockResolvedValue(undefined as never)
    act(() => socket.onopen?.())
    expect(api.steer).toHaveBeenCalledTimes(2)

    let ok = false
    let retry!: Promise<void>
    act(() => {
      retry = hook.result.current.actions.steer('restart connector', { clientId: 'k1' }).then((v) => {
        ok = v
      })
    })
    await act(async () => {
      resolveDrain()
      await retry
    })
    expect(ok).toBe(true)
    expect(api.steer).toHaveBeenCalledTimes(2)
  })

  it('a Retry pressed while the drain is in flight posts once, with the same id, when the drain fails', async () => {
    const { hook, socket } = await failOnce('restart connector', 'k1')
    let rejectDrain: (e: unknown) => void = () => {}
    vi.mocked(api.steer).mockImplementationOnce(() => new Promise<never>((_, r) => (rejectDrain = r)))
    vi.mocked(api.steer).mockResolvedValue(undefined as never)
    act(() => socket.onopen?.())

    let ok = false
    let retry!: Promise<void>
    act(() => {
      retry = hook.result.current.actions.steer('restart connector', { clientId: 'k1' }).then((v) => {
        ok = v
      })
    })
    await act(async () => {
      rejectDrain(new ApiClientError(503, 'unavailable', 'try again'))
      await retry
    })
    expect(ok).toBe(true)
    expect(api.steer).toHaveBeenCalledTimes(3)
    expect(api.steer).toHaveBeenLastCalledWith('r1', 'restart connector', 'k1')
  })

  it('a Retry pressed long after the drain delivered the action does not post again', async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true })
    try {
      const { hook, socket } = await failOnce('restart connector', 'k1')
      vi.mocked(api.steer).mockResolvedValue(undefined as never)
      act(() => socket.onopen?.())
      await waitFor(() => expect(api.steer).toHaveBeenCalledTimes(2))
      await act(async () => {
        vi.advanceTimersByTime(3 * 60_000)
      })
      let ok = false
      await act(async () => {
        ok = await hook.result.current.actions.steer('restart connector', { clientId: 'k1' })
      })
      expect(ok).toBe(true)
      expect(api.steer).toHaveBeenCalledTimes(2)
    } finally {
      vi.useRealTimers()
    }
  })

  it('tells a listener which action the drain delivered', async () => {
    const { hook, socket } = await failOnce('restart connector', 'k1')
    const heard: unknown[] = []
    const off = hook.result.current.actions.onDelivered?.((sent) => {
      heard.push(sent)
      return true
    })
    vi.mocked(api.steer).mockResolvedValue(undefined as never)
    act(() => socket.onopen?.())
    await waitFor(() => expect(heard).toEqual([{ clientId: 'k1', kind: 'steer', text: 'restart connector' }]))
    off?.()
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
