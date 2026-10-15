import { useEffect, useRef, useState } from 'react'
import { newClientId } from '../lib/clientId'
import type { DeliveredListener } from './sendLedger'

/**
 * The client_id of a composer's failed send, kept only for its Retry (#459). A send that is not
 * a Retry of the failed one is a new action with a new id, whatever its words. The kept id is
 * dropped when that send lands without the composer hearing (the reconnect copy, or its event:
 * `onLanded` lets the composer take its Retry strip down), and when the composer calls `drop`
 * (the words were edited, or the dialog closed).
 */
export function useRetryId(
  onDelivered: ((listener: DeliveredListener) => () => void) | undefined,
  onLanded: () => void,
) {
  const kept = useRef<string | null>(null)
  const landed = useRef(onLanded)
  useEffect(() => {
    landed.current = onLanded
  }, [onLanded])
  useEffect(() => {
    if (!onDelivered) return
    return onDelivered((sent) => {
      if (kept.current === null || sent.clientId !== kept.current) return false
      kept.current = null
      landed.current()
      return true
    })
  }, [onDelivered])
  const [api] = useState(() => ({
    /** The id for a send: the kept id for a Retry of the failed send, else a new action's. */
    next: (retry: boolean): string => (retry && kept.current) || newClientId(),
    /** After a send: keep its id when it failed (for its Retry), forget it when it was taken. */
    settle: (clientId: string, ok: boolean) => {
      kept.current = ok ? null : clientId
    },
    /** The failed send is no longer what a Retry would send. */
    drop: () => {
      kept.current = null
    },
  }))
  return api
}
