import { useCallback, useEffect, useRef, useState } from 'react'
import type { CurrentUser } from '../state/permissions'
import type { RoomState } from '../state/types'
import { attentionItems } from '../lib/attention'
import { notifyPermission, showNotification, withBadge } from '../lib/notify'
import type { NotifyPermission } from '../lib/notify'
import { requestNotifyPermission } from '../lib/notify'

/**
 * Watches the folded room state for events that need a person (a proposal, an approval,
 * a hand-off, a new driver). While the tab is hidden each one adds to a title badge and,
 * if the person allowed it, raises a browser notification. The badge clears when the tab
 * is shown again. Nothing fires during the replay of the log: those events are history.
 */
export function useRoomAttention(state: RoomState | null, user: CurrentUser, replaying: boolean) {
  const [unread, setUnread] = useState(0)
  const [permission, setPermission] = useState<NotifyPermission>(() => notifyPermission())
  const prev = useRef<RoomState | null>(null)
  const userRef = useRef(user)
  useEffect(() => {
    userRef.current = user
  }, [user])

  useEffect(() => {
    if (!state) return
    const before = prev.current
    prev.current = state
    if (!before || replaying || before.id !== state.id) return
    if (typeof document === 'undefined' || !document.hidden) return
    const items = attentionItems(before, state, userRef.current)
    if (items.length === 0) return
    setUnread((n) => n + items.length)
    for (const it of items) showNotification(it.title, it.body, it.tag)
  }, [state, replaying])

  useEffect(() => {
    const clear = () => {
      if (!document.hidden) setUnread(0)
    }
    document.addEventListener('visibilitychange', clear)
    window.addEventListener('focus', clear)
    return () => {
      document.removeEventListener('visibilitychange', clear)
      window.removeEventListener('focus', clear)
    }
  }, [])

  useEffect(() => {
    const base = document.title
    document.title = withBadge(base, unread)
    return () => {
      document.title = withBadge(base, 0)
    }
  }, [unread])

  const enable = useCallback(async () => setPermission(await requestNotifyPermission()), [])
  return { unread, permission, enable }
}
