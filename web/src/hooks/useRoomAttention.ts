import { useCallback, useEffect, useRef, useState } from 'react'
import { toast } from 'sonner'
import type { CurrentUser } from '../state/permissions'
import type { RoomState } from '../state/types'
import { TOAST_KINDS, attentionItems } from '../lib/attention'
import type { AttentionItem } from '../lib/attention'
import { decisionFor } from '../lib/decision'
import { roomStatus, statusInputFromState } from '../lib/roomStatus'
import { documentTitle, plainText } from '../lib/title'
import { setFavicon } from '../lib/favicon'
import { notifyPermission, showNotification } from '../lib/notify'
import type { NotifyPermission } from '../lib/notify'
import { requestNotifyPermission } from '../lib/notify'

/** What a toast's buttons do. RoomScreen wires them to the decision handle, the hand-off banner and the composer. */
export interface AttentionActions {
  approve?: () => void
  view?: () => void
  readHandoff?: () => void
  steer?: () => void
}

const TOAST_MS = 8000

function showToast(it: AttentionItem, room: RoomState, actions: AttentionActions | undefined) {
  const opts = { id: it.tag, duration: TOAST_MS }
  switch (it.kind) {
    case 'approval_needed':
      toast(`Needs your approval · ${it.layer ?? ''}`, {
        ...opts,
        description: it.body,
        action: { label: 'Approve A', onClick: () => actions?.approve?.() },
        cancel: { label: 'View', onClick: () => actions?.view?.() },
      })
      break
    case 'handoff':
      toast(`${it.from ?? 'Someone'} is handing you the room`, {
        ...opts,
        action: { label: 'Read summary', onClick: () => actions?.readHandoff?.() },
      })
      break
    case 'control_granted':
      toast(it.title, opts)
      break
    case 'agent_error':
      toast('Agent error', { ...opts, description: it.body, action: { label: 'Steer', onClick: () => actions?.steer?.() } })
      break
    case 'resolved':
      toast(`Resolved · ${plainText(room.resolution ?? '', 80) || it.body}`, opts)
      break
  }
}

/**
 * Watches the folded room state for events that need a person (a proposal, an approval, a
 * hand-off, control, an agent error, the close). Five kinds also raise a toast, in the
 * foreground too; while the tab is hidden every kind adds to the title count and, if the
 * person allowed it, raises a browser notification. It also owns the tab title and favicon
 * while a room is open. Nothing fires during the replay of the log: those events are history.
 */
export function useRoomAttention(
  state: RoomState | null,
  user: CurrentUser,
  replaying: boolean,
  actions?: AttentionActions,
  /** Another room (from the list) needs this person: keeps the favicon dot on. */
  elsewhereNeedsMe = false,
) {
  const [unread, setUnread] = useState(0)
  const [permission, setPermission] = useState<NotifyPermission>(() => notifyPermission())
  const prev = useRef<RoomState | null>(null)
  // `replaying` flips to false on the render that folds the last replayed event; that event is
  // still history, so the render right after a replay is skipped too.
  const wasReplaying = useRef(replaying)
  const userRef = useRef(user)
  const actionsRef = useRef(actions)
  useEffect(() => {
    userRef.current = user
    actionsRef.current = actions
  })

  useEffect(() => {
    if (!state) return
    const before = prev.current
    const justReplayed = wasReplaying.current
    prev.current = state
    wasReplaying.current = replaying
    if (!before || replaying || justReplayed || before.id !== state.id) return
    const items = attentionItems(before, state, userRef.current)
    if (items.length === 0) return
    for (const it of items) if (TOAST_KINDS.has(it.kind) && it.forMe !== false) showToast(it, state, actionsRef.current)
    if (typeof document === 'undefined' || !document.hidden) return
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

  const needsMe = state ? decisionFor(state, user)?.kind === 'needs-you' : false
  const status = state ? roomStatus(statusInputFromState(state)) : null
  const roomTitle = state?.title ?? ''
  const label = status?.label ?? ''
  const statusKey = status?.kind ?? ''
  useEffect(() => {
    if (!status) return
    const before = document.title
    document.title = documentTitle({ status, roomTitle, unread, needsMe })
    return () => {
      document.title = before
    }
    // `status` is derived from the same inputs as label and kind; those are the stable keys.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [label, statusKey, roomTitle, unread, needsMe])

  const alert = needsMe || unread > 0 || elsewhereNeedsMe
  useEffect(() => {
    setFavicon(alert)
    return () => setFavicon(false)
  }, [alert])

  const enable = useCallback(async () => setPermission(await requestNotifyPermission()), [])
  return { unread, permission, enable }
}
