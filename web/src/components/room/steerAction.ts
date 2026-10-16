import type { SendControls } from '../../hooks/useRoom'

// The steer composer's current action in a room, kept outside SteerBox (#550). A reload of the
// room (stale_position, 409 params_mismatch, the Reload button) passes through a null state, so
// RoomScreen can unmount SteerBox while its send is on the wire or kept for a retry. The next
// SteerBox reads this record to take the action over: it shows the Retry strip only for an action
// the ledger still owes, waits for one still on the wire, and clears its field when the action
// landed. It lives in sessionStorage (the same tab, like the last-sent recall) and is only a
// pointer to the ledger's entry: the ledger still decides what is sent.

/** What the composer sent, and how far it got. */
export interface SteerAction {
  clientId: string
  kind: 'steer' | 'comment'
  /** The words as sent (after the composer's prefix handling). */
  text: string
  /** The field as it was when sent: the field is cleared only while it still says this. */
  typed: string
  /** sending: on the wire; kept: failed and owed a retry; delivered: the room took it. */
  phase: 'sending' | 'kept' | 'delivered'
}

const key = (roomId: string) => `piperoom.steeraction.${roomId}`

// Same-tab subscribers per room: sessionStorage fires no event in the tab that wrote it.
const subscribers = new Map<string, Set<() => void>>()

function isAction(v: unknown): v is SteerAction {
  const a = v as Partial<SteerAction> | null
  return (
    !!a &&
    typeof a.clientId === 'string' &&
    a.clientId !== '' &&
    (a.kind === 'steer' || a.kind === 'comment') &&
    typeof a.text === 'string' &&
    typeof a.typed === 'string' &&
    (a.phase === 'sending' || a.phase === 'kept' || a.phase === 'delivered')
  )
}

/** The room's record, or null (none, unreadable, or storage blocked). */
export function readSteerAction(roomId: string): SteerAction | null {
  try {
    const raw = sessionStorage.getItem(key(roomId))
    if (!raw) return null
    const v: unknown = JSON.parse(raw)
    return isAction(v) ? v : null
  } catch {
    return null
  }
}

/** Replaces (or, with null, removes) the room's record and tells the room's composers. */
export function writeSteerAction(roomId: string, action: SteerAction | null) {
  try {
    if (action) sessionStorage.setItem(key(roomId), JSON.stringify(action))
    else sessionStorage.removeItem(key(roomId))
  } catch {
    /* storage blocked: a remount just cannot take the action over */
  }
  for (const fn of [...(subscribers.get(roomId) ?? [])]) fn()
}

/** Updates the record only while it still names `clientId` (a newer send replaced it otherwise). */
export function updateSteerAction(roomId: string, clientId: string, next: SteerAction | null) {
  if (readSteerAction(roomId)?.clientId === clientId) writeSteerAction(roomId, next)
}

/** Calls `fn` after every write of the room's record. Returns the unsubscribe. */
export function subscribeSteerAction(roomId: string, fn: () => void): () => void {
  let set = subscribers.get(roomId)
  if (!set) {
    set = new Set()
    subscribers.set(roomId, set)
  }
  set.add(fn)
  return () => {
    set.delete(fn)
    if (set.size === 0) subscribers.delete(roomId)
  }
}

/**
 * Marks the record delivered when the ledger reports its action landed without its caller hearing
 * (the reconnect copy, or its event). Unlike SteerBox's own listener this outlives the component,
 * so a landing while no SteerBox is mounted (the reload's skeleton) is not lost. It stops once the
 * record no longer names the action.
 */
export function watchSteerDelivery(roomId: string, clientId: string, sends: SendControls | undefined) {
  if (!sends) return
  // Both are only called later (a write of the record, a delivery), after both are set.
  const stop = () => {
    stopDelivered()
    stopRecord()
  }
  const stopRecord = subscribeSteerAction(roomId, () => {
    const a = readSteerAction(roomId)
    if (!a || a.clientId !== clientId || a.phase === 'delivered') stop()
  })
  const stopDelivered = sends.onDelivered((sent) => {
    if (sent.clientId !== clientId) return false
    const a = readSteerAction(roomId)
    if (a && a.clientId === clientId && a.phase !== 'delivered') writeSteerAction(roomId, { ...a, phase: 'delivered' })
    stop()
    // The composer's own listener is the one that clears a field it holds.
    return false
  })
}

/**
 * What a SteerBox mounting in `roomId` takes over from the record: the action still owed (on the
 * wire or kept), or, for one that landed while no SteerBox heard, whether the restored draft is
 * still its words (then the field starts empty). A record for an action the ledger no longer owes
 * (refused, cancelled, or another ledger after a page load) is not taken over.
 */
export function takeOverSteerAction(
  roomId: string,
  sends: SendControls | undefined,
  draft: string,
): { owed: SteerAction | null; clearDraft: boolean; drop: boolean } {
  const a = readSteerAction(roomId)
  if (!a) return { owed: null, clearDraft: false, drop: false }
  if (a.phase === 'delivered') return { owed: null, clearDraft: draft === a.typed, drop: true }
  if (sends?.isPending(a.clientId)) return { owed: a, clearDraft: false, drop: false }
  return { owed: null, clearDraft: false, drop: true }
}
