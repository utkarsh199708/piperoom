import { useEffect, useRef } from 'react'
import { toast } from 'sonner'
import type { RoomSnapshot } from '../api/types'
import type { CurrentUser } from '../state/permissions'
import { layersNeedingYou, needsYou } from '../lib/rooms'
import { plainText, LIST_TITLE } from '../lib/title'
import { setFavicon } from '../lib/favicon'
import { showNotification } from '../lib/notify'

/** Ids of the open rooms with a layer waiting on this person. */
export function roomsNeedingYou(rooms: RoomSnapshot[], user: CurrentUser): RoomSnapshot[] {
  return rooms.filter((r) => needsYou(r, user))
}

/**
 * The room list's attention: a room that newly needs this person raises the same toast and
 * notification as a room screen would, and the favicon carries a dot while any room needs them.
 * The first poll is the baseline, never an alert. While a room is open its screen owns the
 * title and favicon (useRoomAttention).
 */
export function useListAttention(
  rooms: RoomSnapshot[],
  user: CurrentUser,
  loading: boolean,
  inRoom: boolean,
  onOpen: (roomId: string) => void,
) {
  const known = useRef<Set<string> | null>(null)
  const open = useRef(onOpen)
  useEffect(() => {
    open.current = onOpen
  })

  const needing = roomsNeedingYou(rooms, user)
  const key = needing.map((r) => r.id).join('\u0000')

  useEffect(() => {
    if (loading) return
    const now = new Set(needing.map((r) => r.id))
    const before = known.current
    known.current = now
    if (before === null) return
    for (const r of needing) {
      if (before.has(r.id)) continue
      const layer = layersNeedingYou(r, user)[0] ?? ''
      const title = plainText(r.title, 80)
      toast(`Needs your approval · ${layer}`, {
        id: `proposal:${r.id}`,
        duration: 8000,
        description: title,
        action: { label: 'Open', onClick: () => open.current(r.id) },
      })
      if (document.hidden) showNotification(`Your approval is needed: ${title}`, '', `list:${r.id}`)
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [key, loading])

  useEffect(() => {
    if (inRoom) return
    setFavicon(needing.length > 0)
    document.title = LIST_TITLE
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [key, inRoom])
}
