// The steers and comments of one room that are not settled yet, keyed by client_id (docs/api.md,
// Idempotent posts; #390, #459). One user action has one client_id: the first send, the Retry
// click, the reconnect copy and a retry after an unknown outcome all reuse it, and the server
// appends at most one event for it. Nothing here matches text: a deliberate repeat of the same
// words is a new action with a new id.
import type { Event } from '../api/types'
import {
  NOT_CONFIRMED_TEXT,
  OFFLINE_TEXT,
  UNCONFIRMED_TEXT,
  errorText,
  isClientIdReused,
  isNetworkError,
  isUnconfirmed,
} from '../lib/errors'

/** What a steer or comment says. */
export interface SendItem {
  kind: 'steer' | 'comment'
  text: string
  stepId?: string
}

/** One action that reached the room, as told to composers. */
export interface Sent extends SendItem {
  clientId: string
}

/**
 * Called when an action landed without its caller hearing about it (the reconnect copy, or its
 * event in the stream). Return true when the composer took it (cleared its field).
 */
export type DeliveredListener = (sent: Sent) => boolean

/** At most this many actions are kept for the reconnect. */
export const MAX_KEPT = 5
/** A send waits this long for an earlier send of the same action before treating it as unknown. */
export const INFLIGHT_WAIT_MS = 15_000
/**
 * Without a deduping server, an action that got no answer is not sent again for this long after
 * it started: the server gives the steer and comment routes 60 s, so a copy it stores at all is
 * stored by then.
 */
export const UNCONFIRMED_WINDOW_MS = 65_000
/** Settled and stale entries are forgotten after this long. */
const FORGET_MS = 15 * 60_000

type Outcome = 'ok' | 'kept' | 'unknown' | 'refused'

interface Action {
  clientId: string
  item: SendItem
  /** queued: not sent, or safe to send again; unknown: maybe stored, the server may not dedupe. */
  state: 'inflight' | 'queued' | 'unknown'
  /** When the latest send of it started. */
  since: number
  flight?: Promise<Outcome>
  /**
   * The person gave up on it while it was on the wire (edited the words, closed the dialog). A
   * success still lands; a failure or an unknown outcome drops it, never kept or drained (#390).
   */
  cancelled?: boolean
}

export interface LedgerDeps {
  /** POSTs the action with its client_id; resolves with the server's event. */
  post: (item: SendItem, clientId: string) => Promise<Event | undefined>
  setError: (next: string | null | ((prev: string | null) => string | null)) => void
  newId: () => string
  /** The room socket is open: then nothing reconnects, so the ledger retries kept actions itself. */
  isLive?: () => boolean
  now?: () => number
}

/** With the socket open, kept actions are sent again after this long (no reconnect will do it). */
export const LIVE_RETRY_MS = 5_000

const BANNERS = [OFFLINE_TEXT, UNCONFIRMED_TEXT, NOT_CONFIRMED_TEXT]

export class SendLedger {
  private readonly actions = new Map<string, Action>()
  // Actions that reached the room, with what they said (an event from another tab of this user
  // carries no item): a send of one again is answered here without a request.
  private readonly landed = new Map<string, { at: number; item?: SendItem }>()
  private readonly listeners = new Set<DeliveredListener>()
  private readonly timers = new Set<ReturnType<typeof setTimeout>>()
  private liveRetry: ReturnType<typeof setTimeout> | undefined
  // The room screen is gone: answers that arrive later change nothing and arm no timer.
  private disposed = false
  /**
   * Whether the server dedupes on client_id: true once an answer echoed it, false once one did
   * not, null before the first answer. Only true makes a retry of an unknown outcome automatic.
   */
  dedupes: boolean | null = null

  private readonly deps: LedgerDeps

  constructor(deps: LedgerDeps) {
    this.deps = deps
  }

  private now() {
    return this.deps.now ? this.deps.now() : Date.now()
  }

  onDelivered(listener: DeliveredListener): () => void {
    this.listeners.add(listener)
    return () => {
      this.listeners.delete(listener)
    }
  }

  /**
   * The banner for what is outstanding now, from each action's state: an action kept for a
   * retry says "Retrying", one that may be stored says "waiting" until its window ends. An
   * unknown action past its window was reported once ("never confirmed", by its timer) and no
   * longer holds a banner up.
   */
  banner(): string | null {
    const now = this.now()
    const actions = [...this.actions.values()]
    if (actions.some((a) => a.state === 'unknown' && now - a.since < UNCONFIRMED_WINDOW_MS)) return UNCONFIRMED_TEXT
    if (actions.some((a) => a.state === 'queued')) return OFFLINE_TEXT
    return null
  }

  private setError(next: string | null | ((prev: string | null) => string | null)) {
    if (!this.disposed) this.deps.setError(next)
  }

  /**
   * Sends one action. `clientId` names the action: pass the same id for every retry of it, a
   * new one (the default) for a new action. Resolves true when the server took it.
   */
  async send(item: SendItem, clientId: string = this.deps.newId()): Promise<boolean> {
    this.prune()
    if (this.isLanded(clientId, item)) return this.settled()
    let a = this.actions.get(clientId)
    if (a?.state === 'inflight' && a.flight) {
      // Another send of this action (the reconnect copy, or a double click) is on its way.
      const out = await this.wait(a.flight)
      if (out === 'ok' || this.isLanded(clientId, item)) return this.settled()
      if (out === 'timeout' && this.actions.get(clientId) === a && a.state === 'inflight') {
        if (a.cancelled) this.actions.delete(clientId) // given up on: dropped, not kept as unknown
        else this.toUnknown(a)
      }
      a = this.actions.get(clientId)
      if (a?.state === 'inflight') return false
    }
    if (a?.state === 'unknown' && this.dedupes !== true && this.now() - a.since < UNCONFIRMED_WINDOW_MS) {
      // Maybe stored, and the server may not dedupe: a second copy could double the steer.
      this.setError(UNCONFIRMED_TEXT)
      return false
    }
    return (await this.post(item, clientId)) === 'ok' ? this.settled() : false
  }

  /**
   * The person gave up on this action: the words were edited or the dialog closed. A kept or
   * unknown entry is removed, so no Retry, reconnect or live retry sends it. An entry on the wire
   * cannot be called back, so it is marked: if it lands, it lands; if it fails or gets no
   * answer, it is dropped instead of kept. Either way it is never sent again, so the composer
   * may start a new action with a new id at once.
   */
  cancel(clientId: string): void {
    const a = this.actions.get(clientId)
    if (!a) return
    if (a.state === 'inflight') {
      a.cancelled = true
      return
    }
    this.actions.delete(clientId)
    this.setError(this.banner())
  }

  /** The ledger still holds this action (on the wire, kept, or unknown): a Retry of it is owed. */
  isPending(clientId: string): boolean {
    const a = this.actions.get(clientId)
    return a !== undefined && !a.cancelled
  }

  /** On reconnect: sends again every action that is safe to send again, with its own id. */
  drain(): void {
    for (const a of [...this.actions.values()]) {
      if (a.state === 'queued') void this.post(a.item, a.clientId, true)
    }
  }

  /** Every event goes through here: the current user's event with a known client_id settles it. */
  onEvent(ev: Event, userId: string): void {
    if (ev.actor?.kind !== 'human' || ev.actor.id !== userId) return
    const id = (ev.payload as { client_id?: unknown } | null)?.client_id
    if (typeof id !== 'string' || id === '') return
    this.dedupes = true // only a server with #459 puts it in the payload
    this.land(id, true)
  }

  /** Stops the timers (the room screen is gone); later answers no longer touch the screen. */
  dispose(): void {
    this.disposed = true
    for (const t of this.timers) clearTimeout(t)
    this.timers.clear()
    if (this.liveRetry !== undefined) clearTimeout(this.liveRetry)
    this.liveRetry = undefined
  }

  /** `notify`: the caller is not waiting for the answer (the reconnect copy), so tell the composers. */
  private post(item: SendItem, clientId: string, notify = false): Promise<Outcome> {
    const a: Action = { clientId, item, state: 'inflight', since: this.now() }
    this.actions.set(clientId, a)
    a.flight = this.deps.post(item, clientId).then(
      (ev) => {
        // A server that dedupes echoes the key in the stored event (Idempotent posts, Payload).
        const echoed = (ev?.payload as { client_id?: unknown } | undefined)?.client_id
        if (ev) this.dedupes = echoed === clientId
        this.land(clientId, notify)
        return 'ok' as const
      },
      (e: unknown) => this.failed(a, e),
    )
    return a.flight
  }

  private failed(a: Action, e: unknown): Outcome {
    if (this.isLanded(a.clientId, a.item)) return 'ok' // its event arrived meanwhile
    if (this.actions.get(a.clientId) !== a) return 'refused' // superseded by a newer send of it
    if (a.cancelled) {
      // Given up on while on the wire, and it did not land: dropped, never kept or drained.
      this.actions.delete(a.clientId)
      this.setError(this.banner())
      return 'refused'
    }
    if (isClientIdReused(e)) {
      // One id, two bodies: a bug in this client. Never retried, so it cannot loop.
      console.error('piperoom: client_id_reused for', a.clientId)
      this.actions.delete(a.clientId)
      this.setError(errorText(e))
      return 'refused'
    }
    if (isUnconfirmed(e)) {
      if (this.dedupes === true) {
        // The server dedupes: sending it again with the same id appends nothing new.
        a.state = 'queued'
        this.setError(OFFLINE_TEXT)
        this.retryWhileLive()
        return 'kept'
      }
      this.toUnknown(a)
      return 'unknown'
    }
    if (isNetworkError(e) && this.count('queued') < MAX_KEPT) {
      // Gap shared with main (pinned by a test): a TypeError can also mean the request reached
      // the server and the answer was lost; a server without #459 then stores the re-send again (#492).
      a.state = 'queued'
      this.setError(OFFLINE_TEXT)
      this.retryWhileLive()
      return 'kept'
    }
    // Refused (an error answer) or nothing kept: nothing was stored, the id is unused.
    this.actions.delete(a.clientId)
    this.setError(errorText(e))
    return 'refused'
  }

  /**
   * With the socket open, the "Retrying" banner would otherwise be false: only a reconnect drains.
   * One timer at a time sends the kept actions again.
   */
  private retryWhileLive() {
    if (this.disposed || this.liveRetry !== undefined || !this.deps.isLive?.()) return
    this.liveRetry = setTimeout(() => {
      this.liveRetry = undefined
      if (!this.disposed && this.deps.isLive?.()) this.drain()
    }, LIVE_RETRY_MS)
  }

  /** Maybe stored and not safe to send again yet: say so, and say when it may be sent again. */
  private toUnknown(a: Action) {
    a.state = 'unknown'
    if (this.disposed) return
    this.setError(UNCONFIRMED_TEXT)
    const t = setTimeout(
      () => {
        this.timers.delete(t)
        if (this.actions.get(a.clientId) === a && a.state === 'unknown') this.setError(NOT_CONFIRMED_TEXT)
      },
      Math.max(a.since + UNCONFIRMED_WINDOW_MS - this.now(), 0),
    )
    this.timers.add(t)
  }

  /**
   * The action already reached the room. Another body under its id is a client bug: it is posted,
   * so the server answers 409 client_id_reused instead of this client hiding it.
   */
  private isLanded(clientId: string, item: SendItem): boolean {
    const l = this.landed.get(clientId)
    if (!l) return false
    // Landed through an event of an action this ledger did not hold (another tab): its body is
    // unknown, so post and let the server answer (200 with its event, or 409 for another body).
    const was = l.item
    return !!was && was.kind === item.kind && was.text === item.text && (was.stepId ?? '') === (item.stepId ?? '')
  }

  private land(clientId: string, notify: boolean) {
    const a = this.actions.get(clientId)
    this.actions.delete(clientId)
    this.landed.set(clientId, { at: this.now(), item: a?.item ?? this.landed.get(clientId)?.item })
    if (notify && a) {
      for (const l of this.listeners) l({ clientId, ...a.item })
    }
    const next = this.banner()
    this.setError((prev) => (prev === null || BANNERS.includes(prev) ? next : prev))
  }

  private settled(): true {
    this.setError(this.banner())
    return true
  }

  private count(state: Action['state']) {
    return [...this.actions.values()].filter((a) => a.state === state).length
  }

  private wait(flight: Promise<Outcome>): Promise<Outcome | 'timeout'> {
    let timer: ReturnType<typeof setTimeout> | undefined
    const late = new Promise<'timeout'>((resolve) => {
      timer = setTimeout(() => resolve('timeout'), INFLIGHT_WAIT_MS)
      this.timers.add(timer)
    })
    return Promise.race([flight, late]).finally(() => {
      clearTimeout(timer)
      if (timer !== undefined) this.timers.delete(timer)
    })
  }

  /**
   * Forgets landed actions and unknown ones (already reported "never confirmed") by age. Kept
   * (queued) actions are never dropped by age: they are what the person sent and the reconnect
   * still owes; MAX_KEPT bounds them.
   */
  private prune() {
    const old = this.now() - FORGET_MS
    for (const [id, l] of this.landed) if (l.at < old) this.landed.delete(id)
    for (const [id, a] of this.actions) if (a.state === 'unknown' && a.since < old) this.actions.delete(id)
  }
}
