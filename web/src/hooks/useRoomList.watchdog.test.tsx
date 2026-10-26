import { beforeEach, afterEach, describe, expect, it, vi } from 'vitest'
import { act, renderHook } from '@testing-library/react'

vi.mock('sonner', () => ({ toast: Object.assign(vi.fn(), { warning: vi.fn(), dismiss: vi.fn() }) }))
vi.mock('../context/user', () => ({
  useCurrentUser: () => ({
    user: { id: 'arjun', name: 'Arjun', layers: ['kafka'] },
    setUserId: vi.fn(),
    logout: vi.fn(),
    sessionEpoch: 0,
  }),
}))

import { api } from '../api/client'
import { OPEN_TIMEOUT_MS, useRoomList } from './useRoomList'

// A socket that stays in CONNECTING forever: no open, no close (KAN-553).
class StuckSocket {
  static CONNECTING = 0
  static OPEN = 1
  static CLOSED = 3
  readyState = StuckSocket.CONNECTING
  onopen: (() => void) | null = null
  onclose: ((ev: { reason?: string }) => void) | null = null
  onmessage: ((m: unknown) => void) | null = null
  sent: string[] = []
  send(data: string) {
    this.sent.push(data)
  }
  close() {
    this.readyState = StuckSocket.CLOSED
    this.onclose?.({})
  }
}

describe('useRoomList watchdog (KAN-553)', () => {
  beforeEach(() => {
    vi.useFakeTimers()
    vi.stubGlobal('WebSocket', StuckSocket)
    vi.spyOn(api, 'rooms').mockResolvedValue([])
  })
  afterEach(() => {
    vi.useRealTimers()
    vi.unstubAllGlobals()
    vi.restoreAllMocks()
  })

  it('falls back to polling when the socket never opens', async () => {
    const { result } = renderHook(() => useRoomList(), {})
    expect(result.current.loading).toBe(true)
    // Each stuck open times out (watchdog) and counts as a failed open;
    // after MAX_FAILED_OPENS the hook flips to polling and fetches the list.
    for (let i = 0; i < 3; i++) {
      // Each cycle needs the watchdog (10 s) plus the reconnect backoff, so
      // advance a little beyond the timeout per iteration.
      await act(async () => {
        await vi.advanceTimersByTimeAsync(OPEN_TIMEOUT_MS + 5_000)
      })
    }
    await act(async () => {
      await vi.advanceTimersByTimeAsync(0)
    })
    expect(api.rooms).toHaveBeenCalled()
    expect(result.current.loading).toBe(false)
  })
})
