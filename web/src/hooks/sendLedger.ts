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
}

export interface LedgerDeps {
  /** POSTs the action with its client_id; resolves with the server's event. */
  post: (item: SendItem, clientId: string) => Promise<Event | undefined>
  setError: (next: string | null | ((prev: string | null) => string | null)) => void
  newId: () => string
  now?: () => number
}

const BANNERS = [OFFLINE_TEXT, UNCONFIRMED_TEXT, NOT_CONFIRMED_TEXT]

export class SendLedger {
  private readonly actions = new Map<string, Action>()
  // Actions that reached the room, with what they said (an event from another tab of this user
  // carries no item): a send of one again is answered here without a request.
  private readonly landed = new Map<string, { at: number; item?: SendItem }>()
  private readonly listeners = new Set<DeliveredListener>()
  private readonly timers = new Set<ReturnType<typeof setTimeout>>()
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

  /** The banner for what is still outstanding, or null. */
  banner(): string | null {
    const states = [...this.actions.values()].map((a) => a.state)
    if (states.includes('unknown')) return UNCONFIRMED_TEXT
    if (states.includes('queued')) return OFFLINE_TEXT
    return null
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
      if (out === 'timeout' && this.actions.get(clientId) === a && a.state === 'inflight') this.toUnknown(a)
      a = this.actions.get(clientId)
      if (a?.state === 'inflight') return false
    }
    if (a?.state === 'unknown' && this.dedupes !== true && this.now() - a.since < UNCONFIRMED_WINDOW_MS) {
      // Maybe stored, and the server may not dedupe: a second copy could double the steer.
      this.deps.setError(UNCONFIRMED_TEXT)
      return false
    }
    return (await this.post(item, clientId)) === 'ok' ? this.settled() : false
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

  /** Stops the timers (the room screen is gone). */
  dispose(): void {
    for (const t of this.timers) clearTimeout(t)
    this.timers.clear()
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
    if (isClientIdReused(e)) {
      // One id, two bodies: a bug in this client. Never retried, so it cannot loop.
      console.error('piperoom: client_id_reused for', a.clientId)
      this.actions.delete(a.clientId)
      this.deps.setError(errorText(e))
      return 'refused'
    }
    if (isUnconfirmed(e)) {
      if (this.dedupes === true) {
        // The server dedupes: sending it again with the same id appends nothing new.
        a.state = 'queued'
        this.deps.setError(OFFLINE_TEXT)
        return 'kept'
      }
      this.toUnknown(a)
      return 'unknown'
    }
    if (isNetworkError(e) && this.count('queued') < MAX_KEPT) {
      a.state = 'queued'
      this.deps.setError(OFFLINE_TEXT)
      return 'kept'
    }
    // Refused (an error answer) or nothing kept: nothing was stored, the id is unused.
    this.actions.delete(a.clientId)
    this.deps.setError(errorText(e))
    return 'refused'
  }

  /** Maybe stored and not safe to send again yet: say so, and say when it may be sent again. */
  private toUnknown(a: Action) {
    a.state = 'unknown'
    this.deps.setError(UNCONFIRMED_TEXT)
    const t = setTimeout(
      () => {
        this.timers.delete(t)
        if (this.actions.get(a.clientId) === a && a.state === 'unknown') this.deps.setError(NOT_CONFIRMED_TEXT)
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
    const was = l.item
    return !was || (was.kind === item.kind && was.text === item.text && (was.stepId ?? '') === (item.stepId ?? ''))
  }

  private land(clientId: string, notify: boolean) {
    const a = this.actions.get(clientId)
    this.actions.delete(clientId)
    this.landed.set(clientId, { at: this.now(), item: a?.item ?? this.landed.get(clientId)?.item })
    if (notify && a) {
      for (const l of this.listeners) l({ clientId, ...a.item })
    }
    const next = this.banner()
    this.deps.setError((prev) => (prev === null || BANNERS.includes(prev) ? next : prev))
  }

  private settled(): true {
    this.deps.setError(this.banner())
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

  private prune() {
    const old = this.now() - FORGET_MS
    for (const [id, l] of this.landed) if (l.at < old) this.landed.delete(id)
    for (const [id, a] of this.actions) if (a.state !== 'inflight' && a.since < old) this.actions.delete(id)
  }
}
