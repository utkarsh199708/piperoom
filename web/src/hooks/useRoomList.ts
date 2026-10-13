import { useCallback, useEffect, useState } from 'react'
import type { RoomSnapshot } from '../api/types'
import { api, isFixtures } from '../api/client'
import { fixtureRooms } from '../fixtures/player'
import { useCurrentUser } from '../context/user'

/**
 * `enabled: false` loads nothing: a screen inside the shell reads the shell's copy
 * (RoomListContext) instead of fetching its own.
 */
export function useRoomList({ enabled = true }: { enabled?: boolean } = {}) {
  const { user, sessionEpoch } = useCurrentUser()
  const [rooms, setRooms] = useState<RoomSnapshot[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  const reload = useCallback(async () => {
    setLoading(true)
    try {
      if (isFixtures()) {
        // A copy: the fixture array is edited in place (new room, take control).
        setRooms([...fixtureRooms])
        setError(null)
        return
      }
      setRooms(await api.rooms())
      setError(null) // a Retry that works clears the error
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
    } finally {
      setLoading(false)
    }
  }, [])

  // Reload when the person or the session changes (login, logout, switch user).
  useEffect(() => {
    if (enabled) void reload()
  }, [enabled, reload, user.id, sessionEpoch])

  return { rooms, loading, error, reload }
}
