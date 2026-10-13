import { beforeEach, describe, expect, it, vi } from 'vitest'
import { renderHook, waitFor, act } from '@testing-library/react'

vi.mock('../context/user', () => ({
  useCurrentUser: () => ({ user: { id: 'arjun', name: 'Arjun', layers: [] }, sessionEpoch: 0 }),
}))
vi.mock('../api/client', () => ({ api: { rooms: vi.fn() }, isFixtures: () => false }))

import { api } from '../api/client'
import { useRoomList } from './useRoomList'

beforeEach(() => {
  vi.mocked(api.rooms).mockReset()
})

describe('useRoomList', () => {
  it('loads on mount', async () => {
    vi.mocked(api.rooms).mockResolvedValue([])
    const { result } = renderHook(() => useRoomList())
    await waitFor(() => expect(result.current.loading).toBe(false))
    expect(api.rooms).toHaveBeenCalledTimes(1)
  })

  it('enabled: false loads nothing (the screen reads the shell list instead)', async () => {
    renderHook(() => useRoomList({ enabled: false }))
    await new Promise((r) => setTimeout(r, 20))
    expect(api.rooms).not.toHaveBeenCalled()
  })

  it('a reload that works clears the error from the one that failed', async () => {
    vi.mocked(api.rooms).mockRejectedValueOnce(new Error('boom')).mockResolvedValueOnce([])
    const { result } = renderHook(() => useRoomList())
    await waitFor(() => expect(result.current.error).toBe('boom'))
    await act(async () => {
      await result.current.reload()
    })
    expect(result.current.error).toBeNull()
  })
})
