import { beforeEach, describe, expect, it, vi } from 'vitest'
import { renderHook, waitFor, act } from '@testing-library/react'

// Fixtures mode: no push socket, the list polls (the server path is in useRoomList.push.test.tsx).
vi.mock('../context/user', () => ({
  useCurrentUser: () => ({ user: { id: 'arjun', name: 'Arjun', layers: [] }, sessionEpoch: 0 }),
}))
vi.mock('../api/client', () => ({ api: { rooms: vi.fn() }, isFixtures: () => true, openListWs: vi.fn() }))
vi.mock('../fixtures/player', () => ({ fixtureRooms: [] }))

import { openListWs } from '../api/client'
import { useRoomList } from './useRoomList'

beforeEach(() => {
  vi.mocked(openListWs).mockReset()
})

describe('useRoomList in fixtures mode', () => {
  it('polls and never opens the push socket', async () => {
    const { result } = renderHook(() => useRoomList())
    await waitFor(() => expect(result.current.loading).toBe(false))
    expect(result.current.mode).toBe('poll')
    expect(result.current.updatedAt).not.toBeNull()
    expect(openListWs).not.toHaveBeenCalled()
  })

  it('enabled: false loads nothing (the screen reads the shell list instead)', async () => {
    const { result } = renderHook(() => useRoomList({ enabled: false }))
    await new Promise((r) => setTimeout(r, 20))
    expect(result.current.loading).toBe(true)
    expect(openListWs).not.toHaveBeenCalled()
  })

  it('reload refetches now', async () => {
    const { result } = renderHook(() => useRoomList())
    await waitFor(() => expect(result.current.loading).toBe(false))
    await act(async () => {
      await result.current.reload()
    })
    expect(result.current.error).toBeNull()
  })
})
