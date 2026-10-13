import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { act, renderHook } from '@testing-library/react'
import type { Topology } from '../api/types'

// The hook reads the current user from context and the transport from client.
vi.mock('../context/user', () => ({
  useCurrentUser: () => ({
    user: { id: 'arjun', name: 'Arjun', layers: ['kafka-connect', 'kafka'] },
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
    api: { ...actual.api, topology: vi.fn() },
  }
})

import { api } from '../api/client'
import { TOPOLOGY_REFRESH_MS, useTopology } from './useTopology'

const topology: Topology = { pipeline_id: 'orders', nodes: [], edges: [] }

// Flush any pending promise continuations (async fetch settle + finally) so the
// in-flight guard is back to false before the next interval tick.
const flush = () => act(async () => {})

beforeEach(() => {
  vi.useFakeTimers()
  vi.setSystemTime(new Date('2026-10-11T10:00:00Z'))
  vi.resetAllMocks()
  vi.mocked(api.topology).mockResolvedValue(topology)
})

afterEach(() => {
  vi.useRealTimers()
})

describe('useTopology — refresh cadence', () => {
  it('fetches once on mount and again every 30 s while the room is open', async () => {
    const { result } = renderHook(() => useTopology('orders', { active: true }))
    await flush()

    expect(api.topology).toHaveBeenCalledTimes(1)
    expect(api.topology).toHaveBeenCalledWith('orders')

    await act(async () => {
      vi.advanceTimersByTime(TOPOLOGY_REFRESH_MS)
    })
    expect(api.topology).toHaveBeenCalledTimes(2)

    await act(async () => {
      vi.advanceTimersByTime(TOPOLOGY_REFRESH_MS)
    })
    expect(api.topology).toHaveBeenCalledTimes(3)

    await act(async () => {
      vi.advanceTimersByTime(TOPOLOGY_REFRESH_MS)
    })
    expect(api.topology).toHaveBeenCalledTimes(4)

    expect(result.current.topology).toEqual(topology)
    expect(result.current.updatedAt).not.toBeNull()
  })

  it('refetches immediately when refreshKey changes (one per action.executed)', async () => {
    const { rerender } = renderHook(
      ({ key }) => useTopology('orders', { refreshKey: key, active: true }),
      { initialProps: { key: 0 } },
    )
    await flush()
    expect(api.topology).toHaveBeenCalledTimes(1)

    rerender({ key: 1 })
    expect(api.topology).toHaveBeenCalledTimes(2)

    // Same key re-render must not trigger a fetch.
    rerender({ key: 1 })
    expect(api.topology).toHaveBeenCalledTimes(2)

    rerender({ key: 2 })
    expect(api.topology).toHaveBeenCalledTimes(3)
  })

  it('stops the 30 s refresh once the room closes', async () => {
    const { rerender } = renderHook(
      ({ active }) => useTopology('orders', { active }),
      { initialProps: { active: true } },
    )
    await flush()
    expect(api.topology).toHaveBeenCalledTimes(1)

    await act(async () => {
      vi.advanceTimersByTime(TOPOLOGY_REFRESH_MS)
    })
    expect(api.topology).toHaveBeenCalledTimes(2)

    // Closing re-runs the effect (one final load) but leaves no interval behind.
    rerender({ active: false })
    expect(api.topology).toHaveBeenCalledTimes(3)

    await act(async () => {
      vi.advanceTimersByTime(TOPOLOGY_REFRESH_MS * 3)
    })
    expect(api.topology).toHaveBeenCalledTimes(3)
  })

  it('stops refreshing after unmount', async () => {
    const { unmount } = renderHook(() => useTopology('orders', { active: true }))
    await flush()
    expect(api.topology).toHaveBeenCalledTimes(1)

    unmount()
    await act(async () => {
      vi.advanceTimersByTime(TOPOLOGY_REFRESH_MS * 3)
    })
    expect(api.topology).toHaveBeenCalledTimes(1)
  })

  it('never overlaps fetches: an interval tick while one is in flight is skipped', async () => {
    let resolveTopology!: (t: Topology) => void
    vi.mocked(api.topology).mockImplementation(
      () =>
        new Promise<Topology>((resolve) => {
          resolveTopology = resolve
        }),
    )

    const { result } = renderHook(() => useTopology('orders', { active: true }))
    expect(api.topology).toHaveBeenCalledTimes(1)

    // First fetch is still pending, so this tick must be skipped.
    await act(async () => {
      vi.advanceTimersByTime(TOPOLOGY_REFRESH_MS)
    })
    expect(api.topology).toHaveBeenCalledTimes(1)

    await act(async () => {
      resolveTopology(topology)
    })
    expect(result.current.topology).toEqual(topology)
    expect(result.current.updatedAt).not.toBeNull()

    // Now that the first fetch has settled, the next tick can run.
    await act(async () => {
      vi.advanceTimersByTime(TOPOLOGY_REFRESH_MS)
    })
    expect(api.topology).toHaveBeenCalledTimes(2)
  })
})
