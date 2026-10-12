// Room list ordering: open rooms first, then newest opened first. The server
// already sorts open-first (docs/api.md), but fixture mode and any client-side
// refresh can return arbitrary order, so the sorting rule lives here too.
import type { RoomSnapshot } from '../api/types'

function openedAtMs(room: RoomSnapshot): number {
  const t = Date.parse(room.opened_at)
  return Number.isNaN(t) ? 0 : t
}

/** Comparator: open rooms before closed; within each group, newest opened first. */
export function compareRooms(a: RoomSnapshot, b: RoomSnapshot): number {
  const aRank = a.status === 'open' ? 0 : 1
  const bRank = b.status === 'open' ? 0 : 1
  if (aRank !== bRank) return aRank - bRank
  return openedAtMs(b) - openedAtMs(a)
}

/** Copy of `rooms` in list order (Array.sort is stable, so ties keep input order). */
export function sortRoomsForList(rooms: RoomSnapshot[]): RoomSnapshot[] {
  return [...rooms].sort(compareRooms)
}
