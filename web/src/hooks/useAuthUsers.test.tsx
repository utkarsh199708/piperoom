import { beforeEach, describe, expect, it, vi } from 'vitest'
import { renderHook, waitFor } from '@testing-library/react'

const flags = vi.hoisted(() => ({ fixtures: false }))

vi.mock('../api/client', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../api/client')>()
  return {
    ...actual,
    isFixtures: () => flags.fixtures,
    api: { ...actual.api, authUsers: vi.fn(), pipelines: vi.fn() },
  }
})

import { ApiClientError, api } from '../api/client'
import { useAuthUsers } from './useAuthUsers'
import { usePipelines } from './usePipelines'

beforeEach(() => {
  flags.fixtures = false
  vi.mocked(api.authUsers).mockReset()
  vi.mocked(api.pipelines).mockReset()
})

describe('useAuthUsers', () => {
  it('returns what GET /auth/users answers', async () => {
    vi.mocked(api.authUsers).mockResolvedValue([{ id: 'priya', name: 'Priya', layers: ['postgres'] }])
    const { result } = renderHook(() => useAuthUsers())
    await waitFor(() => expect(result.current.status).toBe('ready'))
    expect(result.current).toMatchObject({ data: [{ id: 'priya' }] })
  })

  it('api.authUsers rejecting (oidc mode: 404) is an error state, not a built-in list', async () => {
    vi.mocked(api.authUsers).mockRejectedValue(new ApiClientError(404, 'not_found', 'not found'))
    const { result } = renderHook(() => useAuthUsers())
    await waitFor(() => expect(result.current.status).toBe('error'))
    expect(result.current).toMatchObject({ status: 'error', message: 'not found' })
  })

  it('a non-array answer is an empty list', async () => {
    vi.mocked(api.authUsers).mockResolvedValue(null as never)
    const { result } = renderHook(() => useAuthUsers())
    await waitFor(() => expect(result.current).toMatchObject({ status: 'ready', data: [] }))
  })

  it('fixture mode does not call the server', async () => {
    flags.fixtures = true
    const { result } = renderHook(() => useAuthUsers())
    await waitFor(() => expect(result.current.status).toBe('ready'))
    expect(api.authUsers).not.toHaveBeenCalled()
  })
})

describe('usePipelines', () => {
  it('[] is a real answer, nothing is made up', async () => {
    vi.mocked(api.pipelines).mockResolvedValue([])
    const { result } = renderHook(() => usePipelines())
    await waitFor(() => expect(result.current).toMatchObject({ status: 'ready', data: [] }))
  })

  it('a failure is an error state', async () => {
    vi.mocked(api.pipelines).mockRejectedValue(new ApiClientError(500, 'internal_error', 'boom'))
    const { result } = renderHook(() => usePipelines())
    await waitFor(() => expect(result.current).toMatchObject({ status: 'error', message: 'boom' }))
  })
})
