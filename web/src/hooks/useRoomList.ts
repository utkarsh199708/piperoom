import { useCallback, useEffect, useState } from 'react'
import type { RoomSnapshot } from '../api/types'
import { api, isFixtures } from '../api/client'
import { fixtureRooms } from '../fixtures/player'
import { useCurrentUser } from '../context/user'

export function useRoomList() {
  const { user, sessionEpoch } = useCurrentUser()
  const [rooms, setRooms] = useState<RoomSnapshot[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  const reload = useCallback(async () => {
    setLoading(true)
    try {
      if (isFixtures()) {
        setRooms(fixtureRooms)
        return
      }
      setRooms(await api.rooms())
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
    } finally {
      setLoading(false)
    }
  }, [])

  // Reload when the person or the session changes (login, logout, switch user).
  useEffect(() => {
    void reload()
  }, [reload, user.id, sessionEpoch])

  return { rooms, loading, error, reload }
}
