import { useCallback, useEffect, useState } from 'react'
import type { RoomSnapshot } from '../api/types'
import { api, isFixtures } from '../api/client'
import { fixtureRooms } from '../fixtures/player'
import { useCurrentUser } from '../context/user'

export function useRoomList() {
  const { user } = useCurrentUser()
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
      setRooms(await api.rooms(user.id))
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
    } finally {
      setLoading(false)
    }
  }, [user.id])

  useEffect(() => {
    void reload()
  }, [reload])

  return { rooms, loading, error, reload }
}
