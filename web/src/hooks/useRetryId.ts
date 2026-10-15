import { useEffect, useRef, useState } from 'react'
import { newClientId } from '../lib/clientId'
import type { SendControls } from './useRoom'

/**
 * A composer's current action and its client_id (#459). The id belongs to the action's entry in
 * the send ledger until that entry ends; the composer may only send it again (Retry) or cancel it:
 *
 * - `next(retry)`: the id for a send. A Retry of the composer's still-owed action keeps its id;
 *   anything else starts a new action with a new id.
 * - `settle(id, ok)`: after the send. The id stays for a Retry only while the ledger still owes
 *   it (kept or unknown) and nothing was cancelled since the send began; a refused send (an error
 *   answer, 409) is no longer owed, so its id is not kept.
 * - `drop()`: the person gave up on the action (edited the words, closed the dialog). A kept or
 *   unknown entry is cancelled in the ledger, so it is never sent again, before any new action
 *   starts; one on the wire finishes as it is, and the composer no longer owns it.
 * - When the action lands without the composer hearing (the reconnect copy, or its event),
 *   `onLanded` lets the composer take its Retry strip down.
 */
export function useRetryId(sends: SendControls | undefined, onLanded: () => void) {
  const owned = useRef<string | null>(null)
  const generation = useRef(0)
  const startedAt = useRef(new Map<string, number>())
  const landed = useRef(onLanded)
  useEffect(() => {
    landed.current = onLanded
  }, [onLanded])
  useEffect(() => {
    if (!sends) return
    return sends.onDelivered((sent) => {
      if (owned.current === null || sent.clientId !== owned.current) return false
      owned.current = null
      landed.current()
      return true
    })
  }, [sends])
  const [api] = useState(() => ({
    next: (retry: boolean): string => {
      const reuse = retry && owned.current !== null && (sends?.isPending(owned.current) ?? true)
      const id = reuse && owned.current !== null ? owned.current : newClientId()
      owned.current = id
      startedAt.current.set(id, generation.current)
      return id
    },
    settle: (clientId: string, ok: boolean) => {
      const started = startedAt.current.get(clientId)
      startedAt.current.delete(clientId)
      // Given up while it was on the wire: the composer no longer owns it, so its id is not kept.
      if (started !== generation.current || owned.current !== clientId) return
      owned.current = !ok && (sends?.isPending(clientId) ?? true) ? clientId : null
    },
    drop: () => {
      if (owned.current !== null) sends?.cancel(owned.current)
      owned.current = null
      generation.current += 1
    },
  }))
  return api
}
