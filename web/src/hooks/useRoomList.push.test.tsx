import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { act, renderHook } from '@testing-library/react'
import type { RoomSnapshot } from '../api/types'

vi.mock('../context/user', () => ({
  useCurrentUser: () => ({ user: { id: 'arjun', name: 'Arjun', layers: [] }, sessionEpoch: 0 }),
}))
vi.mock('../api/client', () => ({ api: { rooms: vi.fn() }, isFixtures: () => false, openListWs: vi.fn() }))

import { api, openListWs } from '../api/client'
import { BACKOFF_MIN_MS, MAX_FAILED_OPENS, POLL_MS, foldListFrame, useRoomList } from './useRoomList'

// A WebSocket the test drives by hand.
class FakeSocket {
  static OPEN = 1
  readyState = 0
  onopen: (() => void) | null = null
  onmessage: ((m: { data: string }) => void) | null = null
  onclose: ((ev: { reason?: string }) => void) | null = null
  sent: string[] = []
  closed = false
  send(s: string) {
    this.sent.push(s)
  }
  close() {
    this.closed = true
  }
  open() {
    this.readyState = 1
    this.onopen?.()
  }
  frame(msg: unknown) {
    this.onmessage?.({ data: JSON.stringify(msg) })
  }
  drop(reason = '') {
    this.readyState = 3
    this.onclose?.({ reason })
  }
}

let sockets: FakeSocket[] = []

function room(id: string, extra: Partial<RoomSnapshot> = {}): RoomSnapshot {
  return {
    id,
    title: id,
    severity: 'sev2',
    pipeline_id: 'orders',
    status: 'open',
    opened_at: '2026-10-14T03:00:00Z',
    last_seq: 1,
    driver: null,
    agent_status: { state: 'idle' },
    hypothesis: null,
    triage: null,
    open_proposals: [],
    pending_suggestions: [],
    members: [],
    ...extra,
  }
}

beforeEach(() => {
  vi.useFakeTimers()
  vi.stubGlobal('WebSocket', FakeSocket)
  sockets = []
  vi.mocked(openListWs).mockReset()
  vi.mocked(openListWs).mockImplementation(() => {
    const s = new FakeSocket()
    sockets.push(s)
    return s as unknown as WebSocket
  })
  vi.mocked(api.rooms).mockReset()
  vi.mocked(api.rooms).mockResolvedValue([room('polled')])
})

afterEach(() => {
  vi.useRealTimers()
  vi.unstubAllGlobals()
})

describe('useRoomList push', () => {
  it('folds rooms, room and room_deleted frames', () => {
    const { result } = renderHook(() => useRoomList())
    expect(sockets).toHaveLength(1)
    const s = sockets[0]
    act(() => {
      s.open()
      s.frame({ type: 'rooms', rooms: [room('a'), room('b')] })
    })
    expect(result.current.loading).toBe(false)
    expect(result.current.mode).toBe('push')
    expect(result.current.updatedAt).toBeNull()
    expect(result.current.rooms.map((r) => r.id).sort()).toEqual(['a', 'b'])

    act(() => s.frame({ type: 'room', room: room('a', { title: 'changed' }) }))
    expect(result.current.rooms.find((r) => r.id === 'a')?.title).toBe('changed')
    expect(result.current.rooms).toHaveLength(2)

    act(() => s.frame({ type: 'room', room: room('c') }))
    expect(result.current.rooms).toHaveLength(3)

    act(() => s.frame({ type: 'room_deleted', room_id: 'b' }))
    expect(result.current.rooms.map((r) => r.id).sort()).toEqual(['a', 'c'])
    expect(api.rooms).not.toHaveBeenCalled()
  })

  it('pings every 20 s while open', () => {
    renderHook(() => useRoomList())
    act(() => sockets[0].open())
    act(() => vi.advanceTimersByTime(20_000))
    expect(sockets[0].sent).toEqual([JSON.stringify({ type: 'ping' })])
  })

  it('reconnects with backoff after a drop and takes the fresh list', () => {
    const { result } = renderHook(() => useRoomList())
    act(() => {
      sockets[0].open()
      sockets[0].frame({ type: 'rooms', rooms: [room('a')] })
      sockets[0].drop()
    })
    expect(sockets).toHaveLength(1)
    act(() => vi.advanceTimersByTime(BACKOFF_MIN_MS))
    expect(sockets).toHaveLength(2)
    act(() => {
      sockets[1].open()
      sockets[1].frame({ type: 'rooms', rooms: [room('z')] })
    })
    expect(result.current.rooms.map((r) => r.id)).toEqual(['z'])
    expect(result.current.mode).toBe('push')
  })

  it.each([
    [0, 500],
    [0.5, 750],
    [1, 1000],
  ])('with random %s the first reconnect waits %s ms', (r, wait) => {
    renderHook(() => useRoomList({ random: () => r }))
    act(() => sockets[0].drop())
    act(() => vi.advanceTimersByTime(wait - 1))
    expect(sockets).toHaveLength(1)
    act(() => vi.advanceTimersByTime(1))
    expect(sockets).toHaveLength(2)
  })

  it('draws a new delay for every reconnect, from a ceiling that doubles', () => {
    const draws = [0, 1] // 500 ms of a 1 s ceiling, then 2000 ms of a 2 s ceiling
    let i = 0
    renderHook(() => useRoomList({ random: () => draws[i++] }))
    act(() => sockets[0].drop())
    act(() => vi.advanceTimersByTime(499))
    expect(sockets).toHaveLength(1)
    act(() => vi.advanceTimersByTime(1))
    expect(sockets).toHaveLength(2)
    act(() => sockets[1].drop())
    act(() => vi.advanceTimersByTime(1999))
    expect(sockets).toHaveLength(2)
    act(() => vi.advanceTimersByTime(1))
    expect(sockets).toHaveLength(3)
  })

  it(`falls back to polling after ${MAX_FAILED_OPENS} failed opens in a row`, async () => {
    const { result } = renderHook(() => useRoomList())
    act(() => sockets[0].drop())
    act(() => vi.advanceTimersByTime(BACKOFF_MIN_MS))
    act(() => sockets[1].drop())
    act(() => vi.advanceTimersByTime(BACKOFF_MIN_MS * 2))
    act(() => sockets[2].drop())
    expect(result.current.mode).toBe('poll')
    await act(async () => {
      await vi.advanceTimersByTimeAsync(0)
    })
    expect(api.rooms).toHaveBeenCalledTimes(1)
    expect(result.current.rooms.map((r) => r.id)).toEqual(['polled'])
    expect(result.current.updatedAt).not.toBeNull()
    await act(async () => {
      await vi.advanceTimersByTimeAsync(POLL_MS)
    })
    expect(api.rooms).toHaveBeenCalledTimes(2)
    expect(sockets).toHaveLength(3)
  })

  it('a session_expired close stops reconnecting', () => {
    renderHook(() => useRoomList())
    act(() => sockets[0].drop('session_expired'))
    act(() => vi.advanceTimersByTime(60_000))
    expect(sockets).toHaveLength(1)
  })

  it('unmount closes the socket', () => {
    const { unmount } = renderHook(() => useRoomList())
    unmount()
    expect(sockets[0].closed).toBe(true)
  })
})

describe('foldListFrame', () => {
  it('replaces by id and keeps the list sorted, open first', () => {
    const closed = room('old', { status: 'closed', opened_at: '2026-10-14T05:00:00Z' })
    const out = foldListFrame([closed], { type: 'room', room: room('new') })
    expect(out.map((r) => r.id)).toEqual(['new', 'old'])
  })
})

describe('foldListFrame titles', () => {
  // U+202E (right-to-left override) would make "orders missing lag" read reversed in the list.
  it('drops bidi controls from pushed titles', () => {
    const t = 'orders ‮gal gnissim‬ now ⁦x⁩'
    expect(foldListFrame([], { type: 'rooms', rooms: [room('a', { title: t })] })[0].title).toBe('orders gal gnissim now x')
    expect(foldListFrame([], { type: 'room', room: room('b', { title: t }) })[0].title).toBe('orders gal gnissim now x')
  })
})
