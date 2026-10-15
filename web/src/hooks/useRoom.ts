import { useCallback, useEffect, useReducer, useRef, useState } from 'react'
import type {
  CommentAddedPayload,
  ChecklistStepDonePayload,
  Event,
  HypothesisVerdict,
  LabelInput,
  PresenceMember,
  RoomSnapshot,
  SteerSentPayload,
  SteerSuggestedPayload,
  WsServerMessage,
} from '../api/types'
import { ApiClientError, api, isFixtures, openRoomWs } from '../api/client'
import type { ApproveExtras } from '../api/client'
import { isSoloConfirmError } from '../lib/solo'
import { classifyAuthProblem, reportAuthProblem } from '../lib/auth'
import { emptySnapshot, fixtureEvents, fixtureRoom, FIXTURE_EVENT_DELAY_MS } from '../fixtures/player'
import { useCurrentUser } from '../context/user'
import { findUser } from '../fixtures/users'
import { NOT_CONFIRMED_TEXT, UNCONFIRMED_TEXT, errorText, friendlyError, isNetworkError, isUnconfirmed } from '../lib/errors'
import { mentionsAgent } from '../lib/mentions'
import { applyEvent } from '../state/reducer'
import { fromSnapshot } from '../state/types'
import type { RoomState } from '../state/types'
import { hideStaleToast, showStaleToast } from '../lib/staleToast'
import { equalJitter } from '../lib/backoff'

// HTTP status an error frame code stands for (docs/api.md, Errors).
const WS_FRAME_STATUS: Record<string, number> = {
  session_expired: 401,
  viewer: 403,
  proxy_identity: 403,
  bad_host: 421,
}

export interface ApproveResult {
  ok: boolean
  /** Solo mode: the server wants `confirm: "<pipeline_id>/<layer>"`. */
  needsConfirm?: boolean
  /**
   * 409 params_mismatch: the fix changed since the UI loaded it. The hook shows the
   * stale banner and reloads the room; the caller must not retry.
   */
  mismatch?: boolean
}

/** True for the 409 the server sends when the params_hash sent is not the proposal's. */
export function isParamsMismatch(e: unknown): boolean {
  return e instanceof ApiClientError && e.status === 409 && e.code === 'params_mismatch'
}

/**
 * What the 409 params_mismatch was about: the proposal and the hash the person was looking at.
 * Lives here, not in the panel, because the room reload unmounts the panel.
 */
export interface StaleInfo {
  proposalId: string
  sentHash: string
}

/** What a nudge came to; `code` is the server error code on a failure. */
export interface NudgeResult {
  ok: boolean
  code?: string
}

/** What a close attempt came to; `error` is the text for the person. */
export interface CloseResult {
  ok: boolean
  error?: string
}

/** A steer or comment that could not reach the server, kept for one retry. */
export interface PendingSend {
  kind: 'steer' | 'comment'
  text: string
  stepId?: string
}

/** How a steer or comment is sent. */
export interface SendOptions {
  /**
   * The caller knows this is a retry of words whose last send failed (SteerBox's Retry).
   * Every send of words the reconnect is sending, or sent less than 2 minutes ago, is already
   * treated as a retry; the flag stretches the second case to 15 minutes (#390).
   */
  retry?: boolean
}

/**
 * Called when the reconnect delivered words a composer still holds. Return true when the
 * composer took them (cleared its field): a later retry of the same words is then a new message.
 */
export type DeliveredListener = (sent: PendingSend) => boolean

const MAX_PENDING = 5
/** A send of the same words this soon after the reconnect delivered them is a retry of them (#343). */
const DELIVERED_DEDUPE_MS = 2 * 60_000
/** A flagged retry still matches a delivered copy this old; older copies are dropped (#390). */
const DELIVERED_KEEP_MS = 15 * 60_000
/** How long a send waits for the reconnect's copy of the same words before it stops waiting. */
const INFLIGHT_WAIT_MS = 15_000
/**
 * A send that got no answer (SendUnconfirmedError) is settled from the room's events for this long
 * after it started: the server gives the steer and comment routes 60 s
 * (internal/httpapi/server.go), so a copy it stores at all is stored by then.
 */
const UNCONFIRMED_WINDOW_MS = 65_000
/** After a reconnect, the replay of missed events gets this long before an overdue check gives up. */
const CATCH_UP_MS = 5_000

/**
 * A send whose outcome is unknown. Never posted again on its own: a matching event of the
 * current user after `afterSeq` settles it as delivered; with none by the end of the window it
 * becomes 'unconfirmed', and only the person's next send of those words posts it, once.
 */
interface Unconfirmed {
  item: PendingSend
  afterSeq: number
  since: number
  state: 'checking' | 'unconfirmed'
}

/** The words of an event the current user sent, in the shape sendKept matches on. */
function ownSend(ev: Event, userId: string): PendingSend | null {
  if (ev.actor?.kind !== 'human' || ev.actor.id !== userId) return null
  if (ev.type === 'steer.sent') {
    const p = ev.payload as SteerSentPayload
    // A forwarded suggestion carries someone else's words.
    return p.suggestion_id ? null : { kind: 'steer', text: p.text }
  }
  if (ev.type === 'steer.suggested') return { kind: 'steer', text: (ev.payload as SteerSuggestedPayload).text }
  if (ev.type === 'comment.added') {
    const p = ev.payload as CommentAddedPayload
    return { kind: 'comment', text: p.text, stepId: p.step_id }
  }
  return null
}

/** How a reconnect copy's send ended: taken, refused or not sent, or no answer (maybe stored). */
type SendOutcome = 'ok' | 'failed' | 'unknown'

interface Delivered {
  item: PendingSend
  at: number
}

const samePending = (a: PendingSend, b: PendingSend) =>
  a.kind === b.kind && a.text === b.text && (a.stepId ?? undefined) === (b.stepId ?? undefined)

/**
 * Removes one delivered copy of `item` younger than `maxAge`; true when there was one.
 * Copies older than DELIVERED_KEEP_MS are dropped on the way, so none lives forever.
 */
function takeDelivered(ref: { current: Delivered[] }, item: PendingSend, maxAge: number): boolean {
  const now = Date.now()
  const live = ref.current.filter((d) => now - d.at < DELIVERED_KEEP_MS)
  const i = live.findIndex((d) => samePending(d.item, item) && now - d.at < maxAge)
  ref.current = i < 0 ? live : live.filter((_, j) => j !== i)
  return i >= 0
}

/** Forgets every delivered copy of `item`: a newer send of those words supersedes them. */
function dropDelivered(ref: { current: Delivered[] }, item: PendingSend) {
  ref.current = ref.current.filter((d) => !samePending(d.item, item))
}

type Timers = Set<ReturnType<typeof setTimeout>>

/** `done`'s value, or 'timeout' when it has not settled within `ms`. The timer is kept in `timers` so unmount can clear it. */
function within<T>(done: Promise<T>, ms: number, timers: Timers): Promise<T | 'timeout'> {
  let timer: ReturnType<typeof setTimeout> | undefined
  const late = new Promise<'timeout'>((resolve) => {
    timer = setTimeout(() => resolve('timeout'), ms)
    timers.add(timer)
  })
  return Promise.race([done, late]).finally(() => {
    clearTimeout(timer)
    if (timer !== undefined) timers.delete(timer)
  })
}

export type ConnState = 'loading' | 'connecting' | 'live' | 'reconnecting' | 'error' | 'not_found'

export interface RoomActions {
  /** Resolves true when the server took it. A failed network call is kept and retried once when the socket reopens. */
  comment: (text: string, stepId?: string, opts?: SendOptions) => Promise<boolean>
  /** Same contract as `comment`. */
  steer: (text: string, opts?: SendOptions) => Promise<boolean>
  /** Hear about kept words the reconnect delivered; returns the unsubscribe function. */
  onDelivered?: (listener: DeliveredListener) => () => void
  sendSuggestion: (suggestionId: string) => void
  /** The driver declines a pending suggestion, with an optional note to its author. */
  declineSuggestion: (suggestionId: string, reason?: string) => void
  requestControl: () => void
  grantControl: (to: string) => void
  /** Confirm or dispute the agent's current hypothesis (F9). Errors go to the room error banner. */
  hypothesisVerdict: (verdict: HypothesisVerdict, note?: string) => void
  /**
   * Ask a pending layer's owners to look (F9). Resolves with what happened; an
   * `already_nudged` answer is for the caller's toast and stays out of the banner.
   */
  nudge: (proposalId: string, layer: string) => Promise<NudgeResult>
  /**
   * Resolves when the call is done. On a failure the error is also in the room error
   * banner; `ok: false` lets the close dialog, which covers that banner, show it too.
   */
  close: (resolution: string) => Promise<CloseResult>
  /**
   * Resolves when the call is done. `needsConfirm` means the server wants the
   * solo-mode string (docs/design/auth.md 5.3): the caller asks for it and calls
   * again with `extra.confirm`. `mismatch` means the stale banner is up and the room
   * is reloading; nothing is retried. Any other error is already in the room error banner.
   * `extra.params_hash` is the hash the UI rendered with the proposal, sent as is.
   */
  approve: (proposalId: string, layer: string, extra: ApproveExtras) => Promise<ApproveResult>
  /** `paramsHash` is the hash the UI rendered with the proposal, sent as is. */
  reject: (proposalId: string, layer: string, reason: string, paramsHash: string) => void
  handoff: (to: string, stayAsWatcher: boolean) => void
  /** Resolves true once the server took the accept, false when it failed (the error is in the room banner). */
  acceptHandoff: (handoffId: string) => Promise<boolean> | void
  trace: (table: string, key: string) => void
  /**
   * Label the closed room (POST /api/rooms/{id}/label). Resolves when the call is done; a failure
   * is returned, not put in the room banner, so the label form keeps its text and shows it.
   */
  label: (input: LabelInput) => Promise<CloseResult>
  /** Tick or untick one runbook item (POST /api/rooms/{id}/checklist). A failure shows in the room banner. */
  checklist: (step: ChecklistStepDonePayload) => void
}

type RoomAction =
  | { type: 'init'; snapshot: RoomSnapshot }
  | { type: 'event'; event: Event }
  | { type: 'reset' }

function roomReducer(state: RoomState | null, action: RoomAction): RoomState | null {
  switch (action.type) {
    case 'reset':
      return null
    case 'init':
      return fromSnapshot(action.snapshot)
    case 'event':
      return state ? applyEvent(state, action.event) : state
  }
}

/**
 * `random` is the jitter source for reconnect delays; tests pass a fixed one. It is read through
 * a ref so a new function on every render does not restart the connection.
 */
export function useRoom(roomId: string, random: () => number = Math.random) {
  const { user, sessionEpoch } = useCurrentUser()
  const [state, dispatch] = useReducer(roomReducer, null)
  const [presence, setPresence] = useState<PresenceMember[]>([])
  const [conn, setConn] = useState<ConnState>('loading')
  const [error, setError] = useState<string | null>(null)
  // 409 params_mismatch: the stale-UI banner. Kept apart from `error` because the
  // reload it triggers clears `error`, and the banner must outlive the reload.
  const [stale, setStale] = useState<StaleInfo | null>(null)
  const [reloadKey, setReloadKey] = useState(0)

  const lastSeqRef = useRef(0)
  const stateRef = useRef<RoomState | null>(null)
  const localIdRef = useRef(0)
  const pendingRef = useRef<PendingSend[]>([])
  // What the reconnect drain is sending now; `done` says how that send ended.
  const inflightRef = useRef<{ item: PendingSend; afterSeq: number; since: number; done: Promise<SendOutcome> }[]>([])
  // Sends that got no answer in time (see Unconfirmed).
  const unconfirmedRef = useRef<Unconfirmed[]>([])
  // The socket is open: an unconfirmed send's window only ends while events can arrive.
  const liveRef = useRef(false)
  // The user's own recent sends seen in the stream: the event can arrive before the send that
  // stored it gives up waiting for the answer.
  const seenOwnRef = useRef<{ item: PendingSend; seq: number }[]>([])
  // What the drain sent and no composer has taken yet: a send of those words soon after is
  // that message's Retry, not a new message (see DELIVERED_DEDUPE_MS / DELIVERED_KEEP_MS).
  const deliveredRef = useRef<Delivered[]>([])
  const deliveredListenersRef = useRef(new Set<DeliveredListener>())
  // Timers of sends waiting on the reconnect's copy and of unconfirmed checks; cleared on unmount.
  const waitTimersRef = useRef<Timers>(new Set())
  useEffect(() => {
    const timers = waitTimersRef.current
    return () => {
      for (const t of timers) clearTimeout(t)
      timers.clear()
    }
  }, [])
  // Words the server took (an answer, or the user's own event in the stream): tell the composers,
  // and keep a copy for a Retry of them that no composer took.
  const announceDelivered = useCallback((item: PendingSend) => {
    let taken = false
    for (const l of deliveredListenersRef.current) taken = l(item) || taken
    if (!taken) deliveredRef.current = [...deliveredRef.current, { item, at: Date.now() }].slice(-MAX_PENDING)
  }, [])

  const clearUnconfirmedBanner = useCallback(() => {
    if (unconfirmedRef.current.length > 0) return
    setError((prev) => (prev === UNCONFIRMED_TEXT || prev === NOT_CONFIRMED_TEXT ? null : prev))
  }, [])

  // Ends a check's window: with the socket open the words are 'unconfirmed' (only a click sends
  // them again); with it closed the check waits for the next reconnect's replay (onopen).
  const armUnconfirmed = useCallback((u: Unconfirmed, ms: number) => {
    const timers = waitTimersRef.current
    const t = setTimeout(() => {
      timers.delete(t)
      if (!unconfirmedRef.current.includes(u) || u.state !== 'checking' || !liveRef.current) return
      u.state = 'unconfirmed'
      setError(NOT_CONFIRMED_TEXT)
    }, Math.max(ms, 0))
    timers.add(t)
  }, [])

  // Returns true when the stream already showed the words arrived: then nothing is left to check.
  const markUnconfirmed = useCallback(
    (item: PendingSend, afterSeq: number, since: number): boolean => {
      if (seenOwnRef.current.some((x) => samePending(x.item, item) && x.seq > afterSeq)) return true
      unconfirmedRef.current = [
        ...unconfirmedRef.current.filter((u) => !samePending(u.item, item)),
        { item, afterSeq, since, state: 'checking' as const },
      ]
      const u = unconfirmedRef.current[unconfirmedRef.current.length - 1]
      armUnconfirmed(u, since + UNCONFIRMED_WINDOW_MS - Date.now())
      setError(UNCONFIRMED_TEXT)
      return false
    },
    [armUnconfirmed],
  )

  // Fixture mode: buffer synthetic (user) events until the story replay ends so
  // their seq can never collide with an upcoming story event's seq.
  const storyPlayingRef = useRef(false)
  const synthQueueRef = useRef<Array<{ type: Event['type']; payload: unknown; at: string }>>([])

  useEffect(() => {
    stateRef.current = state
  }, [state])

  const randomRef = useRef(random)
  useEffect(() => {
    randomRef.current = random
  }, [random])

  useEffect(() => {
    let cancelled = false
    let ws: WebSocket | null = null
    let reconnectTimer: number | undefined
    let replayTimer: number | undefined

    dispatch({ type: 'reset' })
    lastSeqRef.current = 0
    seenOwnRef.current = [] // the replay from 0 fills it again
    setError(null)
    setPresence([])

    if (isFixtures()) {
      const snapshot = fixtureRoom(roomId) ?? emptySnapshot(roomId, roomId, 'sev3', 'orders')
      const events = fixtureEvents(roomId)
      // R10: the scripted story is not a log replay; last_seq 0 keeps the replay progress bar off.
      dispatch({ type: 'init', snapshot: { ...snapshot, last_seq: 0 } })
      lastSeqRef.current = 0
      setPresence(snapshot.members.map((m) => ({ id: m.id, name: m.name, verified: m.verified, state: 'viewing' })))
      setConn('live')

      storyPlayingRef.current = events.length > 0
      synthQueueRef.current = []

      const drainSynth = () => {
        for (const item of synthQueueRef.current) {
          const ev: Event = {
            room_id: roomId,
            seq: lastSeqRef.current + 1,
            type: item.type,
            actor: { kind: 'human', id: user.id, name: user.name },
            payload: item.payload,
            at: item.at,
          }
          dispatch({ type: 'event', event: ev })
          lastSeqRef.current = ev.seq
        }
        synthQueueRef.current = []
      }

      let i = 0
      const playNext = () => {
        if (cancelled) return
        if (i >= events.length) {
          storyPlayingRef.current = false
          drainSynth()
          return
        }
        const ev = events[i]
        i += 1
        dispatch({ type: 'event', event: ev })
        lastSeqRef.current = ev.seq
        replayTimer = window.setTimeout(playNext, FIXTURE_EVENT_DELAY_MS)
      }
      replayTimer = window.setTimeout(playNext, FIXTURE_EVENT_DELAY_MS)

      return () => {
        cancelled = true
        if (replayTimer !== undefined) window.clearTimeout(replayTimer)
      }
    }

    // ---- live mode: snapshot header, then full-log replay over WebSocket ----
    setConn('loading')

    let backoff = 1000
    let probed = false
    let firstConnect = true
    let queue: Promise<void> = Promise.resolve()

    // A hidden tab is away: the heartbeat and a reconnect must not flip it back to viewing.
    const currentPresence = (): PresenceMember['state'] => (document.hidden ? 'away' : 'viewing')
    const sendPresence = (pstate: PresenceMember['state']) => {
      if (ws && ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({ type: 'presence', state: pstate }))
      }
    }

    const applyEventSeq = (ev: Event) => {
      if (ev.seq <= lastSeqRef.current) return
      dispatch({ type: 'event', event: ev })
      lastSeqRef.current = ev.seq
      // The stream is the truth for a send that got no answer: the user's own words, stored
      // after the send started, mean it arrived.
      const mine = ownSend(ev, user.id)
      if (mine) seenOwnRef.current = [...seenOwnRef.current, { item: mine, seq: ev.seq }].slice(-20)
      const u = mine && unconfirmedRef.current.find((x) => samePending(x.item, mine) && ev.seq > x.afterSeq)
      if (u) {
        unconfirmedRef.current = unconfirmedRef.current.filter((x) => x !== u)
        announceDelivered(u.item)
        clearUnconfirmedBanner()
      }
    }

    // Page /events until the gap before `ev` closes. On failure (fetch throws or
    // makes no progress) close `from`, the socket that delivered `ev`, so the reconnect
    // replays from lastSeq. Not `ws`: the fetch is slow, and by the time it fails `ws`
    // may already be a newer, healthy connection that this event has nothing to do with.
    const fillGap = async (ev: Event, from: WebSocket): Promise<boolean> => {
      while (ev.seq > lastSeqRef.current + 1) {
        if (cancelled) return false
        let page
        try {
          page = await api.events(roomId, lastSeqRef.current)
        } catch {
          from.close()
          return false
        }
        if (cancelled) return false
        const missing = page.events
          .filter((e) => e.seq > lastSeqRef.current)
          .sort((a, b) => a.seq - b.seq)
        if (missing.length === 0) {
          from.close()
          return false
        }
        let advanced = false
        for (const e of missing) {
          if (e.seq <= lastSeqRef.current) continue
          if (e.seq !== lastSeqRef.current + 1) break // a hole remains; page again
          applyEventSeq(e)
          advanced = true
        }
        if (!advanced) {
          from.close()
          return false
        }
      }
      return ev.seq === lastSeqRef.current + 1
    }

    // One event through the single queue, so messages that arrive during a gap
    // fetch still apply in seq order.
    const processEvent = async (ev: Event, from: WebSocket) => {
      if (cancelled) return
      if (ev.seq <= lastSeqRef.current) return // duplicate or older — drop
      const ready = await fillGap(ev, from)
      if (cancelled || !ready) return
      applyEventSeq(ev)
    }

    const connect = () => {
      if (cancelled) return
      if (ws && (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING)) return
      const sock = openRoomWs(roomId, firstConnect ? 0 : lastSeqRef.current)
      ws = sock
      firstConnect = false
      let opened = false
      sock.onopen = () => {
        if (cancelled) return
        opened = true
        liveRef.current = true
        probed = false
        backoff = 1000
        setConn('live')
        setError(unconfirmedRef.current.length > 0 ? UNCONFIRMED_TEXT : null)
        // Checks whose window ended while the socket was down: give the replay time to arrive.
        for (const u of unconfirmedRef.current) {
          if (u.state === 'checking') armUnconfirmed(u, Math.max(u.since + UNCONFIRMED_WINDOW_MS - Date.now(), CATCH_UP_MS))
        }
        sendPresence(currentPresence())
        // Messages typed while the server was away: one retry each, in order. Success
        // emits nothing here (the event arrives over the socket); a failure shows its text.
        // Each item is marked in flight in the same tick its request starts, so a Retry
        // pressed meanwhile waits for this send instead of posting a second copy.
        const kept = pendingRef.current
        pendingRef.current = []
        for (const item of kept) {
          const afterSeq = lastSeqRef.current
          const since = Date.now()
          const call =
            item.kind === 'steer'
              ? api.steer(roomId, item.text)
              : api.comment(roomId, { step_id: item.stepId, text: item.text })
          const entry = {
            item,
            afterSeq,
            since,
            done: call.then(
              (): SendOutcome => {
                // A send that stopped waiting on this copy marked it unconfirmed: it arrived after all.
                const before = unconfirmedRef.current.length
                unconfirmedRef.current = unconfirmedRef.current.filter((u) => !samePending(u.item, item))
                if (unconfirmedRef.current.length !== before) clearUnconfirmedBanner()
                announceDelivered(item)
                return 'ok'
              },
              (e: unknown): SendOutcome => {
                if (isUnconfirmed(e)) {
                  // Maybe stored: never posted again on its own, settled from the event stream.
                  if (!markUnconfirmed(item, afterSeq, since)) return 'unknown'
                  announceDelivered(item)
                  return 'ok'
                }
                // A real failure: the words were not stored. A waiting or later Retry posts them.
                unconfirmedRef.current = unconfirmedRef.current.filter((u) => !samePending(u.item, item))
                setError(errorText(e))
                return 'failed'
              },
            ),
          }
          inflightRef.current = [...inflightRef.current, entry]
          void entry.done.then(() => {
            inflightRef.current = inflightRef.current.filter((x) => x !== entry)
          })
        }
      }
      sock.onmessage = (m) => {
        if (cancelled) return // a socket from an older session epoch
        let msg: WsServerMessage
        try {
          msg = JSON.parse(m.data as string) as WsServerMessage
        } catch {
          return
        }
        if (msg.type === 'event') {
          queue = queue.then(() => processEvent(msg.event, sock)).catch(() => undefined)
        } else if (msg.type === 'presence') {
          setPresence(msg.members)
        } else if (msg.type === 'error') {
          // An error frame can say the session ended (auth.md 1.6) or that the
          // user may not act (viewer, proxy_identity): same banners as HTTP.
          const problem = classifyAuthProblem(
            WS_FRAME_STATUS[msg.error.code] ?? 0,
            msg.error.code,
            msg.error.message,
          )
          if (problem) reportAuthProblem({ ...problem, epoch: sessionEpoch })
          setError(friendlyError(msg.error.code, msg.error.message))
        }
      }
      sock.onclose = (ev: CloseEvent) => {
        if (cancelled) return
        if (ws === sock) liveRef.current = false
        if (ev?.reason === 'session_expired') {
          // The server closed the socket because the session ended (auth.md 1.6).
          // Stop retrying; the login screen (or redirect) takes over.
          reportAuthProblem({ kind: 'session_expired', message: 'session_expired', epoch: sessionEpoch })
          setConn('error')
          return
        }
        if (ev?.reason === 'room deleted') {
          // An admin or retention erased the room (docs/api.md WebSocket): a reconnect gets 404,
          // so retrying forever would only keep showing a room that no longer exists.
          setConn('not_found')
          setError('This room was deleted.')
          return
        }
        if (!opened && !probed) {
          // A refused upgrade (401/403/421) shows up as a bare close. Ask the API
          // why, once until a connection succeeds: api.me() reports an auth
          // problem through the same path. Not on every retry, or a dismissed
          // 403/421 banner would come back every few seconds.
          probed = true
          void api.me().catch(() => undefined)
        }
        setConn('reconnecting')
        reconnectTimer = window.setTimeout(connect, equalJitter(backoff, randomRef.current))
        backoff = Math.min(backoff * 2, 10000)
      }
      sock.onerror = () => {
        // onclose handles reconnect
      }
    }

    void (async () => {
      try {
        const snapshot = await api.room(roomId)
        if (cancelled) return
        dispatch({ type: 'init', snapshot })
        // lastSeqRef stays 0: open with after=0 so the full log replays and the
        // reducer rebuilds timeline/comments/proposals/trace/handoffs (item D).
        setConn('connecting')
        connect()
      } catch (e) {
        if (cancelled) return
        setConn(e instanceof ApiClientError && e.status === 404 ? 'not_found' : 'error')
        setError(errorText(e))
      }
    })()
    const presenceTimer = window.setInterval(() => sendPresence(currentPresence()), 20000)

    const onVisibility = () => {
      sendPresence(currentPresence())
    }
    document.addEventListener('visibilitychange', onVisibility)

    return () => {
      cancelled = true
      if (reconnectTimer !== undefined) window.clearTimeout(reconnectTimer)
      if (presenceTimer !== undefined) window.clearInterval(presenceTimer)
      document.removeEventListener('visibilitychange', onVisibility)
      ws?.close()
      ws = null
    }
  }, [roomId, user.id, user.name, sessionEpoch, reloadKey, announceDelivered, armUnconfirmed, clearUnconfirmedBanner, markUnconfirmed])

  // Fetch the snapshot and replay the log again (a fresh socket, after=0).
  // Reload closes the stale toast and it stays closed: the toast is raised only where the 409
  // happens (onMismatch), never when the banner mounts.
  const reload = useCallback(() => {
    hideStaleToast()
    setReloadKey((k) => k + 1)
  }, [])
  const clearStale = useCallback(() => {
    setStale(null)
    hideStaleToast()
  }, [])
  const dismissStale = clearStale

  // Leaving the room takes the toast down with it.
  useEffect(() => hideStaleToast, [])

  // A decision call failed with 409 params_mismatch: say so and reload the room.
  // Never retry; the person reviews the reloaded fix and decides again.
  const onMismatch = useCallback(
    (proposalId: string, sentHash: string) => {
      setStale({ proposalId, sentHash })
      showStaleToast(reload)
      setReloadKey((k) => k + 1)
    },
    [reload],
  )

  // Steer and comment keep the person's words: a call that never reached the server is
  // queued (max 5) and the caller learns it failed, so the field is not cleared.
  const sendKept = useCallback(
    async (item: PendingSend, opts?: SendOptions): Promise<boolean> => {
      // An earlier send of these words got no answer and may be stored: while the event stream
      // is checked, no send of them posts. Once the check found nothing, this send is the
      // person's explicit re-send, and it posts once.
      const unknown = unconfirmedRef.current.find((u) => samePending(u.item, item))
      if (unknown?.state === 'checking') {
        setError(UNCONFIRMED_TEXT)
        return false
      }
      if (unknown) {
        unconfirmedRef.current = unconfirmedRef.current.filter((u) => u !== unknown)
        clearUnconfirmedBanner()
      }
      // A manual send replaces the queued copy, so the text is not sent twice.
      const queued = pendingRef.current.some((p) => samePending(p, item))
      pendingRef.current = pendingRef.current.filter((p) => !samePending(p, item))
      // Every caller is covered, flag or not: RollbackButton and the Timeline reply boxes
      // retry with a plain steer/comment call.
      if (!queued) {
        const flying = inflightRef.current.find((f) => samePending(f.item, item))
        if (flying) {
          // The reconnect is sending these words now: wait for it rather than post a second copy.
          const outcome = await within(flying.done, INFLIGHT_WAIT_MS, waitTimersRef.current)
          if (outcome === 'timeout') {
            // Still unanswered. api.steer/comment give up at SEND_DEADLINE_MS, before this, so
            // this only guards a client without that deadline: the copy may be stored, so treat
            // it like one that got no answer.
            inflightRef.current = inflightRef.current.filter((f) => f !== flying)
            if (!markUnconfirmed(item, flying.afterSeq, flying.since)) return false
            announceDelivered(item)
            takeDelivered(deliveredRef, item, DELIVERED_KEEP_MS)
            setError(null)
            return true
          }
          if (outcome === 'ok') {
            takeDelivered(deliveredRef, item, DELIVERED_KEEP_MS)
            setError(null)
            return true
          }
          if (outcome === 'unknown') return false // marked unconfirmed by the drain
          // The reconnect's copy failed, so this send is the one that has to reach the server.
        } else if (takeDelivered(deliveredRef, item, opts?.retry ? DELIVERED_KEEP_MS : DELIVERED_DEDUPE_MS)) {
          // The reconnect already sent them: posting again would steer the agent twice.
          setError(null)
          return true
        }
      }
      // This send supersedes any older delivered copy: a Retry of it must not be answered by one.
      dropDelivered(deliveredRef, item)
      const afterSeq = lastSeqRef.current
      const since = Date.now()
      try {
        await (item.kind === 'steer'
          ? api.steer(roomId, item.text)
          : api.comment(roomId, { step_id: item.stepId, text: item.text }))
        setError(null)
        return true
      } catch (e) {
        if (isUnconfirmed(e)) {
          // Sent, but no answer: maybe stored. Not queued, so no reconnect posts it again.
          if (!markUnconfirmed(item, afterSeq, since)) return false
          setError(null) // the stream already showed it arrived
          return true
        }
        if (isNetworkError(e) && pendingRef.current.length < MAX_PENDING) pendingRef.current.push(item)
        setError(errorText(e))
        return false
      }
    },
    [roomId, announceDelivered, clearUnconfirmedBanner, markUnconfirmed],
  )

  const onDelivered = useCallback((listener: DeliveredListener) => {
    const set = deliveredListenersRef.current
    set.add(listener)
    return () => {
      set.delete(listener)
    }
  }, [])

  const runAction = useCallback((p: Promise<unknown>) => {
    p.then(() => setError(null)).catch((e: unknown) => setError(errorText(e)))
  }, [])

  const synthesize = useCallback(
    (type: Event['type'], payload: unknown) => {
      if (isFixtures() && storyPlayingRef.current) {
        synthQueueRef.current.push({ type, payload, at: new Date().toISOString() })
        return
      }
      const ev: Event = {
        room_id: roomId,
        seq: lastSeqRef.current + 1,
        type,
        actor: { kind: 'human', id: user.id, name: user.name },
        payload,
        at: new Date().toISOString(),
      }
      dispatch({ type: 'event', event: ev })
      lastSeqRef.current = ev.seq
    },
    [roomId, user.id, user.name],
  )

  const nextLocalId = useCallback(() => {
    localIdRef.current += 1
    return `l_${localIdRef.current}`
  }, [])

  const actions: RoomActions = {
    onDelivered,
    comment: async (text, stepId, opts) => {
      if (isFixtures()) {
        synthesize('comment.added', {
          comment_id: nextLocalId(),
          step_id: stepId,
          text,
          mentions_agent: mentionsAgent(text),
        })
        return true
      }
      return sendKept({ kind: 'comment', text, stepId }, opts)
    },
    steer: async (text, opts) => {
      if (isFixtures()) {
        const s = stateRef.current
        const isDriver = s?.driver?.id === user.id
        synthesize(
          isDriver ? 'steer.sent' : 'steer.suggested',
          isDriver ? { text } : { suggestion_id: nextLocalId(), text },
        )
        return true
      }
      return sendKept({ kind: 'steer', text }, opts)
    },
    sendSuggestion: (suggestionId) => {
      if (isFixtures()) {
        const s = stateRef.current?.suggestions.find((x) => x.suggestion_id === suggestionId)
        synthesize('steer.sent', {
          suggestion_id: suggestionId,
          text: s?.text ?? '',
        })
        return
      }
      void runAction(api.sendSuggestion(roomId, suggestionId))
    },
    declineSuggestion: (suggestionId, reason) => {
      if (isFixtures()) {
        synthesize('steer.declined', { suggestion_id: suggestionId, reason })
        return
      }
      // already_decided is also the approval code ("layer ... already decided"), so its friendlier
      // wording for a suggestion lives here and not in lib/errors.ts.
      api
        .declineSuggestion(roomId, suggestionId, reason)
        .then(() => setError(null))
        .catch((e: unknown) =>
          setError(
            e instanceof ApiClientError && e.code === 'already_decided'
              ? 'this suggestion was already sent or declined'
              : errorText(e),
          ),
        )
    },
    requestControl: () => {
      if (isFixtures()) {
        synthesize('control.requested', {})
        return
      }
      void runAction(api.requestControl(roomId))
    },
    grantControl: (to) => {
      if (isFixtures()) {
        const target = findUser(to)
        synthesize('control.granted', {
          to: { kind: 'human', id: target?.id ?? to, name: target?.name ?? to },
          lease_until: new Date(Date.now() + 30 * 60 * 1000).toISOString(),
        })
        return
      }
      void runAction(api.grantControl(roomId, to))
    },
    hypothesisVerdict: (verdict, note) => {
      const h = stateRef.current?.hypothesis
      if (!h) return
      if (isFixtures()) {
        synthesize('hypothesis.verdict', { verdict, ...(note ? { note } : {}), hypothesis_seq: h.seq })
        return
      }
      void runAction(api.hypothesis(roomId, verdict, note))
    },
    nudge: async (proposalId, layer) => {
      if (isFixtures()) {
        synthesize('approval.nudged', { proposal_id: proposalId, layer })
        return { ok: true }
      }
      try {
        await api.nudge(roomId, proposalId, layer)
        setError(null)
        return { ok: true }
      } catch (e) {
        const code = e instanceof ApiClientError ? e.code : undefined
        if (code !== 'already_nudged') setError(errorText(e))
        return { ok: false, code }
      }
    },
    close: async (resolution) => {
      if (isFixtures()) {
        synthesize('room.closed', { resolution })
        return { ok: true }
      }
      try {
        await api.closeRoom(roomId, resolution)
        setError(null)
        return { ok: true }
      } catch (e) {
        const text = errorText(e)
        setError(text)
        return { ok: false, error: text }
      }
    },
    approve: async (proposalId, layer, extra) => {
      if (isFixtures()) {
        synthesize('approval.given', {
          proposal_id: proposalId,
          layer,
          params_hash: extra.params_hash,
          identity: { method: 'dev', grant: `user:${user.id}` },
        })
        return { ok: true }
      }
      try {
        await api.approve(proposalId, layer, extra)
        setError(null)
        clearStale()
        return { ok: true }
      } catch (e) {
        if (isParamsMismatch(e)) {
          setError(null)
          onMismatch(proposalId, extra.params_hash)
          return { ok: false, mismatch: true }
        }
        // Solo mode asks for the typed string; the confirm box handles it, so it
        // is not an error banner.
        if (isSoloConfirmError(e)) {
          setError(null)
          return { ok: false, needsConfirm: true }
        }
        setError(errorText(e))
        return { ok: false }
      }
    },
    reject: (proposalId, layer, reason, paramsHash) => {
      if (isFixtures()) {
        synthesize('approval.rejected', {
          proposal_id: proposalId,
          layer,
          reason,
          params_hash: paramsHash,
          identity: { method: 'dev', grant: `user:${user.id}` },
        })
        return
      }
      api
        .reject(proposalId, layer, reason, paramsHash)
        .then(() => {
          setError(null)
          clearStale()
        })
        .catch((e: unknown) => {
          if (isParamsMismatch(e)) {
            setError(null)
            onMismatch(proposalId, paramsHash)
          } else {
            setError(errorText(e))
          }
        })
    },
    handoff: (to, stayAsWatcher) => {
      if (isFixtures()) {
        const target = findUser(to)
        synthesize('handoff.created', {
          handoff_id: nextLocalId(),
          to: { kind: 'human', id: target?.id ?? to, name: target?.name ?? to },
          summary: {
            what_broke: '',
            why: '',
            done: [],
            ruled_out: [],
            open: [],
            risks: [],
          },
          stay_as_watcher: stayAsWatcher,
        })
        return
      }
      void runAction(api.handoff(roomId, to, stayAsWatcher))
    },
    acceptHandoff: (handoffId) => {
      if (isFixtures()) {
        synthesize('handoff.accepted', { handoff_id: handoffId })
        return Promise.resolve(true)
      }
      return api.acceptHandoff(handoffId).then(
        () => {
          setError(null)
          return true
        },
        (e: unknown) => {
          setError(errorText(e))
          return false
        },
      )
    },
    label: async (input) => {
      if (isFixtures()) {
        synthesize('room.labelled', input)
        return { ok: true }
      }
      try {
        await api.label(roomId, input)
        return { ok: true }
      } catch (e) {
        return { ok: false, error: errorText(e) }
      }
    },
    checklist: (step) => {
      if (isFixtures()) {
        synthesize('checklist.step_done', step)
        return
      }
      void runAction(api.checklist(roomId, step))
    },
    trace: (table, key) => {
      if (isFixtures()) {
        synthesize('trace.pinned', {
          table,
          key,
          stages: [
            { layer: 'postgres', name: table, status: 'seen', facts: { updated_at: '2026-10-11T09:47:00Z' } },
            { layer: 'kafka-connect', name: 'pg-orders-connector', status: 'seen' },
            { layer: 'kafka', name: `pg.${table}`, status: 'seen', facts: { offset: '48213', partition: '0' } },
            { layer: 'consumer', name: 'order-sync', status: 'stopped_here', facts: { committed: '48212' } },
            { layer: 'warehouse', name: 'analytics.orders', status: 'not_seen' },
          ],
        })
        return
      }
      void runAction(api.trace(roomId, table, key))
    },
  }

  return { state, presence, conn, error, actions, stale, dismissStale, reload }
}
