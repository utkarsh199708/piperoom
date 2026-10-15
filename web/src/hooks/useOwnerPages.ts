import { useCallback, useEffect, useRef, useState } from 'react'
import type { RoomSnapshot } from '../api/types'
import type { CurrentUser } from '../state/permissions'
import { markSeen, readSeen, unseenPages, writeSeen } from '../lib/pages'
import type { PagedRoom, SeenPages } from '../lib/pages'
import { notifyPermission, requestNotifyPermission, showNotification } from '../lib/notify'
import type { NotifyPermission } from '../lib/notify'
import { plainText } from '../lib/title'

/**
 * Pages that name this person (Q0, QA G2). The room list rows carry `pages`, so a page written
 * while the person is anywhere in the app reaches them through the list socket. What they have seen
 * is kept per person in this browser (localStorage; a blocked store only brings the banner back
 * after a reload). A page for the room they have open counts as seen: it is on screen.
 *
 * A browser notification is raised for a page that arrives after the list first loaded, when the
 * person allowed notifications; the first load is only a baseline, so opening the app never
 * notifies about old pages. Permission is asked only by `enable`, which a click calls.
 */
export function useOwnerPages(
  rooms: RoomSnapshot[],
  user: CurrentUser,
  loading: boolean,
  currentRoomId: string,
) {
  const [seen, setSeen] = useState<{ userId: string; map: SeenPages }>(() => ({ userId: user.id, map: readSeen(user.id) }))
  // Another person in this browser starts from their own record.
  const mine = seen.userId === user.id ? seen.map : null
  if (mine === null) setSeen({ userId: user.id, map: readSeen(user.id) })
  const map = mine ?? {}

  const all = unseenPages(rooms, user.id, map)
  // The room on screen shows its own pages in the timeline, so the banner leaves it out.
  const unseen = all.filter((p) => p.room.id !== currentRoomId)
  const key = all.map((p) => `${p.key}@${p.page.at}`).join('\u0000')
  const [permission, setPermission] = useState<NotifyPermission>(() => notifyPermission())

  const mark = useCallback(
    (items: PagedRoom[]) => {
      setSeen((cur) => {
        const base = cur.userId === user.id ? cur.map : readSeen(user.id)
        const next = markSeen(base, items)
        if (next !== base) writeSeen(user.id, next)
        return { userId: user.id, map: next }
      })
    },
    [user.id],
  )

  const dismiss = useCallback((item: PagedRoom) => mark([item]), [mark])
  const dismissAll = useCallback(() => mark(unseen), [mark, unseen])

  // Opening the room is seeing its pages: they stay seen after the person leaves it.
  useEffect(() => {
    const here = all.filter((p) => p.room.id === currentRoomId)
    if (currentRoomId && here.length > 0) mark(here)
    // `key` stands for `all`.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [key, currentRoomId, mark])

  // Browser notifications for pages that appear after the first load.
  const notified = useRef<Set<string> | null>(null)
  useEffect(() => {
    if (loading) return
    const seenBefore = notified.current
    const now = new Set(all.map((p) => `${p.key}@${p.page.at}`))
    notified.current = new Set([...(seenBefore ?? []), ...now])
    if (seenBefore === null) return
    for (const p of all) {
      const id = `${p.key}@${p.page.at}`
      if (seenBefore.has(id) || p.room.id === currentRoomId) continue
      const title = plainText(p.room.title, 80)
      showNotification(`You were paged for ${plainText(p.page.layer, 40)}: ${title}`, plainText(p.page.reason, 200), `page:${p.key}`)
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [key, loading])

  const enable = useCallback(async () => setPermission(await requestNotifyPermission()), [])
  return { unseen, dismiss, dismissAll, permission, enable }
}
