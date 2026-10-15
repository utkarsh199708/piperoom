import { beforeEach, describe, expect, it, vi } from 'vitest'
import { renderHook, waitFor } from '@testing-library/react'
import type { AgentStats } from '../api/types'

vi.mock('../api/client', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../api/client')>()
  return { ...actual, isFixtures: () => false, api: { ...actual.api, agentStats: vi.fn() } }
})

import { api } from '../api/client'
import { useAgentStats } from './useAgentStats'

const stats = { pipeline_id: 'orders', graded: 12, enough_data: true } as AgentStats

beforeEach(() => vi.resetAllMocks())

describe('useAgentStats', () => {
  it('fetches once per mount and returns the stats', async () => {
    vi.mocked(api.agentStats).mockResolvedValue(stats)
    const { result, rerender } = renderHook(() => useAgentStats('orders'))
    await waitFor(() => expect(result.current).toEqual(stats))
    rerender()
    expect(api.agentStats).toHaveBeenCalledTimes(1)
  })

  it('gives null on an error', async () => {
    vi.mocked(api.agentStats).mockRejectedValue(new Error('403'))
    const { result } = renderHook(() => useAgentStats('orders'))
    await waitFor(() => expect(api.agentStats).toHaveBeenCalled())
    expect(result.current).toBeNull()
  })

  it('does not fetch without a pipeline', async () => {
    const { result } = renderHook(() => useAgentStats(''))
    expect(result.current).toBeNull()
    expect(api.agentStats).not.toHaveBeenCalled()
  })
})
