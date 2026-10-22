import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { act, renderHook } from '@testing-library/react'
import type { TrendPoint, WatchMetric } from '../api/types'

vi.mock('../context/user', () => ({
  useCurrentUser: () => ({ user: { id: 'arjun', name: 'Arjun' }, setUserId: vi.fn(), logout: vi.fn(), sessionEpoch: 0 }),
}))

vi.mock('../api/client', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../api/client')>()
  return { ...actual, isFixtures: () => false, api: { ...actual.api, trends: vi.fn() } }
})

const toast = vi.hoisted(() => Object.assign(vi.fn(), { error: vi.fn(), success: vi.fn() }))
vi.mock('sonner', () => ({ toast }))

import { ApiClientError, api } from '../api/client'
import { TRENDS_REFRESH_MS, useTrends } from './useTrends'

// The trends answer exactly as docs/api.md shows it: [{at, value}], oldest first.
const SAMPLES: TrendPoint[] = [
  { at: '2026-10-22T09:30:00Z', value: 60129542144 },
  { at: '2026-10-22T09:45:00Z', value: 64424509440 },
]

const flush = () => act(async () => {})

beforeEach(() => {
  vi.useFakeTimers()
  vi.resetAllMocks()
})
afterEach(() => vi.useRealTimers())

describe('useTrends', () => {
  it('asks for every watcher metric over 1 day and keeps each answer', async () => {
    vi.mocked(api.trends).mockImplementation(async (_pid, metric: WatchMetric) => (metric === 'slot_wal_bytes' ? SAMPLES : []))
    const { result } = renderHook(() => useTrends('orders'))
    expect(result.current.status).toBe('loading')
    await flush()
    expect(api.trends).toHaveBeenCalledTimes(5)
    expect(vi.mocked(api.trends).mock.calls.map((c) => c[1])).toEqual([
      'slot_wal_bytes',
      'dlq_records',
      'group_lag',
      'db_size_bytes',
      'topic_oldest_unread_age',
    ])
    expect(result.current.status).toBe('ready')
    expect(result.current.series.slot_wal_bytes).toEqual(SAMPLES)
    expect(result.current.series.group_lag).toEqual([])
  })

  it.each([
    [404, 'not_found'],
    [403, 'not_allowed'],
  ])('a %i leaves no series and raises no toast (TC3)', async (status, code) => {
    vi.mocked(api.trends).mockRejectedValue(new ApiClientError(status, code, 'pipeline not found'))
    const { result } = renderHook(() => useTrends('other-tenant'))
    await flush()
    expect(result.current.status).toBe('ready')
    expect(result.current.series).toEqual({})
    expect(toast).not.toHaveBeenCalled()
    expect(toast.error).not.toHaveBeenCalled()
  })

  it('a 404 stops the refresh; a 500 on one metric only leaves that one out', async () => {
    vi.mocked(api.trends).mockRejectedValue(new ApiClientError(404, 'not_found', 'pipeline not found'))
    renderHook(() => useTrends('gone'))
    await flush()
    await act(async () => {
      vi.advanceTimersByTime(TRENDS_REFRESH_MS)
    })
    expect(api.trends).toHaveBeenCalledTimes(5)

    vi.mocked(api.trends).mockReset()
    vi.mocked(api.trends).mockImplementation(async (_pid, metric: WatchMetric) => {
      if (metric === 'dlq_records') throw new ApiClientError(500, 'internal', 'boom')
      if (metric === 'group_lag') throw new TypeError('network down')
      return SAMPLES
    })
    const { result } = renderHook(() => useTrends('orders'))
    await flush()
    expect(Object.keys(result.current.series).sort()).toEqual(['db_size_bytes', 'slot_wal_bytes', 'topic_oldest_unread_age'])
    await act(async () => {
      vi.advanceTimersByTime(TRENDS_REFRESH_MS)
    })
    expect(api.trends).toHaveBeenCalledTimes(10)
  })

  it('drops malformed points from a hostile answer', async () => {
    vi.mocked(api.trends).mockResolvedValue([{ at: '<b>x</b>', value: 1 }, { at: SAMPLES[0].at, value: 'NaN' }, SAMPLES[1]] as never)
    const { result } = renderHook(() => useTrends('orders'))
    await flush()
    expect(result.current.series.slot_wal_bytes).toEqual([SAMPLES[1]])
  })

  it('asks nothing while disabled or without a pipeline id', async () => {
    const { result, rerender } = renderHook(({ pid, on }) => useTrends(pid, on), { initialProps: { pid: 'orders', on: false } })
    await flush()
    expect(result.current.status).toBe('idle')
    rerender({ pid: '', on: true })
    await flush()
    expect(api.trends).not.toHaveBeenCalled()
  })
})
