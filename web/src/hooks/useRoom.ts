import { useCallback, useEffect, useMemo, useReducer, useRef, useState } from 'react'
import type {
  ChecklistStepDonePayload,
  Event,
  HypothesisVerdict,
  LabelInput,
  PresenceMember,
  RoomSnapshot,
  WsServerMessage,
} from '../api/types'
import { ApiClientError, api, isFixtures, openRoomWs } from '../api/client'
import type { ApproveAllExtras, ApproveExtras } from '../api/client'
import { isSoloConfirmError } from '../lib/solo'
import { classifyAuthProblem, getSessionChanges, reportAuthProblem } from '../lib/auth'
import { emptySnapshot, fixtureEvents, fixtureRoom, FIXTURE_EVENT_DELAY_MS } from '../fixtures/player'
import { useCurrentUser } from '../context/user'
import { findUser } from '../fixtures/users'
import { errorText, friendlyError } from '../lib/errors'
import { newClientId } from '../lib/clientId'
import { SendLedger } from './sendLedger'
import type { DeliveredListener, SendItem } from './sendLedger'
import { mentionsAgent } from '../lib/mentions'
import { applyEvent, applyModeRefresh } from '../state/reducer'
import { fromSnapshot } from '../state/types'
import type { RoomState } from '../state/types'
import { hideStaleToast, showStaleToast } from '../lib/staleToast'
import { equalJitter } from '../lib/backoff'

// How long a reconnect waits for GET /api/rooms/{id} (the write mode) before it resumes the replay.
const MODE_WAIT_MS = 4000

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
  /**
   * 409 staging_required (X8): the fix needs a healthy stage first. The server's text is on the
   * proposal (`staging_message`) and the card says it; the room error banner does not.
   */
  stagingRequired?: boolean
}

/** What an approve-all (POST /api/proposals/{pid}/approve-all) came to. */
export interface ApproveAllResult extends ApproveResult {
  /**
   * 403 not_layer_owner: the person may not approve every layer. The hook keeps the server's text
   * in `approve_all_refused`, so the card and the decision bar go back to the per-layer rows.
   */
  refused?: boolean
  /** Any other failure, for the confirm dialog, which covers the room error banner. */
  error?: string
}

/** What a "Try in staging" (POST /api/proposals/{pid}/stage) came to; `code` is the server error code on a failure. */
export interface StageAttempt {
  ok: boolean
  code?: string
  /** The text for the person (the card shows it; the room error banner does not). */
  error?: string
}

/** True for the 409 the server sends when approve needs a healthy stage first (X8). */
export function isStagingRequired(e: unknown): boolean {
  return e instanceof ApiClientError && e.status === 409 && e.code === 'staging_required'
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

/** What a cancel of a policy countdown (POST /api/proposals/{pid}/cancel) came to. */
export interface CancelResult {
  ok: boolean
  /**
   * 409 not_scheduled: no countdown was pending any more (the policy started, or someone cancelled
   * first). The log says which; the card says so next to the button and stays out of the banner.
   */
  notScheduled?: boolean
  /** Any other failure's text; it is also in the room error banner. */
  error?: string
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

/** What a record trace (POST /api/rooms/{id}/trace) came to; `error` is the text for the person. */
export interface TraceResult {
  ok: boolean
  error?: string
}

export type { DeliveredListener, Sent } from './sendLedger'
/** What a steer or comment says (kept for older imports). */
export type PendingSend = SendItem

/**
 * What a composer may do with the action of its failed send (#459): hear that it landed, or
 * cancel it when the person gives up on it (edits the words, closes the dialog). An id belongs to
 * its ledger entry until that entry ends; only then may the composer start a new action.
 */
export interface SendControls {
  onDelivered: (listener: DeliveredListener) => () => void
  /** Withdraws a kept or unknown action; one on the wire finishes, and is dropped if it fails. */
  cancel: (clientId: string) => void
  /** The action is still owed (on the wire, kept or unknown), so its Retry may reuse its id. */
  isPending: (clientId: string) => boolean
}

/** How a steer or comment is sent. */
export interface SendOptions {
  /**
   * The idempotency key of the user action this send belongs to (docs/api.md, Idempotent posts;
   * #459). A component makes one with newClientId() per click or submit and passes the same id
   * on every Retry of it; without one, the send is a new action.
   */
  clientId?: string
}

export type ConnState = 'loading' | 'connecting' | 'live' | 'reconnecting' | 'error' | 'not_found'

export interface RoomActions {
  /**
   * Resolves true when the server took it. A send that did not get through is kept and sent
   * again, with the same client_id, when the socket reopens (see hooks/sendLedger.ts).
   */
  comment: (text: string, stepId?: string, opts?: SendOptions) => Promise<boolean>
  /** Same contract as `comment`. */
  steer: (text: string, opts?: SendOptions) => Promise<boolean>
  /** Hear about actions that landed without their caller hearing (reconnect copy, event stream). */
  onDelivered?: (listener: DeliveredListener) => () => void
  /** The composers' handle on their actions (see SendControls). */
  sends?: SendControls
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
  /**
   * Approve every pending required layer in one call (P2c "Approve and run"). Same rules as
   * `approve` (the hash on screen, `needsConfirm` for the solo user's "<pipeline_id>/*",
   * `mismatch` and no retry); a 403 not_layer_owner is `refused` and recorded on the room state;
   * any other error is in the room error banner and in `error`. useRoom always sets it; it is
   * optional only for hosts and test doubles from before it, which keep the per-layer rows.
   */
  approveAll?: (proposalId: string, extra: ApproveAllExtras) => Promise<ApproveAllResult>
  /**
   * Stop a pending policy countdown (P2c-3). The proposal stays open for manual approval; the
   * countdown leaves the card when proposal.cancelled arrives over the socket. useRoom always sets
   * it; optional only for hosts and test doubles from before it, which show no Cancel.
   */
  cancelSchedule?: (proposalId: string, reason?: string) => Promise<CancelResult>
  /**
   * Try the proposal on the staging stack (X8, the driver's). Resolves when the server took it (the
   * running state and the result arrive as events) or said no; the answer is for the card and stays
   * out of the room error banner. Optional for hosts and test doubles from before X8, which show no button.
   */
  stage?: (proposalId: string) => Promise<StageAttempt>
  /** `paramsHash` is the hash the UI rendered with the proposal, sent as is. */
  reject: (proposalId: string, layer: string, reason: string, paramsHash: string) => void
  handoff: (to: string, stayAsWatcher: boolean) => void
  /** Resolves true once the server took the accept, false when it failed (the error is in the room banner). */
  acceptHandoff: (handoffId: string) => Promise<boolean> | void
  /**
   * Trace one record (POST /api/rooms/{id}/trace). Resolves when the call is done; a failure is returned,
   * not put in the room banner, because the trace panel's overlay covers the banner and shows it itself.
   */
  trace: (table: string, key: string) => Promise<TraceResult>
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
  // A reconnect asked the server for the room again: only the write mode in its answer is used.
  | { type: 'mode_refreshed'; snapshot: RoomSnapshot }
  // The server refused this person's approve-all (403 not_layer_owner): kept until the room reloads.
  | { type: 'approve_all_refused'; proposalId: string; text: string }
  // The server answered 409 staging_required: the proposal is held for a healthy stage (X8).
  | { type: 'staging_required'; proposalId: string; text: string }
  | { type: 'reset' }

function roomReducer(state: RoomState | null, action: RoomAction): RoomState | null {
  switch (action.type) {
    case 'reset':
      return null
    case 'init':
      return fromSnapshot(action.snapshot)
    case 'event':
      return state ? applyEvent(state, action.event) : state
    case 'mode_refreshed':
      return state ? applyModeRefresh(state, action.snapshot) : state
    case 'staging_required':
      return state
        ? {
            ...state,
            proposals: state.proposals.map((p) =>
              p.proposal_id === action.proposalId ? { ...p, staging_required: true, staging_message: action.text } : p,
            ),
          }
        : state
    case 'approve_all_refused':
      return state
        ? { ...state, approve_all_refused: { ...state.approve_all_refused, [action.proposalId]: action.text } }
        : state
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
  // The room whose socket last closed with stale_position, until an event arrives (docs/api.md,
  // Stale position; #464). Outlives the reload that the close starts, so a second stale_position
  // in a row is told apart from the first. Not cleared on open: the server accepts the upgrade
  // before it refuses the position.
  const staleRoomRef = useRef<string | null>(null)
  const stateRef = useRef<RoomState | null>(null)
  const localIdRef = useRef(0)
  // Steers and comments not settled yet, keyed by client_id. Each entry keeps the room it was
  // typed in (roomIdRef when it was created) and posts only there; a reconnect drains only the
  // entries of the room on screen (#491).
  const roomIdRef = useRef(roomId)
  // The socket is open (set by onopen/onclose, cleared on cleanup): with it open, the ledger
  // retries kept actions itself, since no reconnect will.
  const liveRef = useRef(false)
  useEffect(() => {
    roomIdRef.current = roomId
  }, [roomId])
  // One ledger per user and session (#536): a dev "Switch user" or a new session changes them
  // without a remount, and the previous user's kept actions must never be posted under the new
  // session. The old ledger is disposed (its entries dropped) by the effect below.
  const session = `${user.id}|${sessionEpoch}`
  const newLedger = () => {
    const changes = getSessionChanges()
    return new SendLedger({
      post: (item, clientId, room) =>
        item.kind === 'steer'
          ? api.steer(room, item.text, clientId)
          : api.comment(room, { step_id: item.stepId, text: item.text, client_id: clientId }),
      room: () => roomIdRef.current,
      setError,
      newId: newClientId,
      isLive: () => liveRef.current,
      sameSession: () => getSessionChanges() === changes,
    })
  }
  const [held, setHeld] = useState(() => ({ session, ledger: newLedger() }))
  let ledger = held.ledger
  if (held.session !== session) {
    // Set during render (React's derived-state pattern), so no effect of this render sees the old one.
    const next = { session, ledger: newLedger() }
    setHeld(next)
    ledger = next.ledger
  }
  useEffect(() => {
    ledger.attach()
    return () => ledger.dispose()
  }, [ledger])
  // Another room or another session starts empty, reset during render (like the ledger above) so
  // not one render shows the old room under the new id, or one user's room to the next (#536).
  const view = `${roomId}|${session}`
  const [shownView, setShownView] = useState(view)
  if (shownView !== view) {
    setShownView(view)
    dispatch({ type: 'reset' })
  }
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

    // No reset here: a reload of the room on screen (stale_position, 409 params_mismatch, Reload)
    // keeps the old state until the new snapshot's init replaces it, so nothing on screen
    // unmounts (#550). Another room or session was reset during render, above.
    lastSeqRef.current = 0
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
    // What the room GET said last, kept here (not read from state, which lags a render) for the
    // X8 refresh below: the log position the replay runs to, and whether the pipeline has staging.
    let replayTarget = 0
    let stagingOn = false

    // A hidden tab is away: the heartbeat and a reconnect must not flip it back to viewing.
    const currentPresence = (): PresenceMember['state'] => (document.hidden ? 'away' : 'viewing')
    const sendPresence = (pstate: PresenceMember['state']) => {
      if (ws && ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({ type: 'presence', state: pstate }))
      }
    }

    const applyEventSeq = (ev: Event) => {
      if (ev.seq <= lastSeqRef.current) return
      staleRoomRef.current = null
      const replaying = ev.seq <= replayTarget
      dispatch({ type: 'event', event: ev })
      lastSeqRef.current = ev.seq
      // The stream settles a send by its client_id (an answer may have been lost).
      ledger.onEvent(ev, user.id)
      // X8: what can be staged and whether a stage is still owed are not in the log. A proposal or a
      // stage that arrives live asks the server again; the room GET at load already covers the replay.
      if (!replaying && stagingOn && (ev.type === 'proposal.created' || ev.type === 'proposal.staged')) {
        void refreshMode()
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

    // Write mode changes only when the server restarts, and a restart drops the socket. So every
    // reconnect (not the first connect, whose snapshot is fresh) asks for the room again and takes
    // its `mode` and `blocked` marks before the replay resumes, in both directions. A failed ask
    // changes nothing: the replay goes on with the mode already known, and the next reconnect asks
    // again. The reconnect waits for the answer only briefly (MODE_WAIT_MS): a server that is up
    // for the socket but slow to answer must not hold the replay back, and a late answer is still
    // applied, because applyModeRefresh re-judges what the replay has already folded in.
    const refreshMode = async () => {
      try {
        const snapshot = await api.room(roomId)
        if (cancelled || !snapshot) return
        stagingOn = snapshot.mode?.staging === true
        dispatch({ type: 'mode_refreshed', snapshot })
      } catch {
        // see above
      }
    }

    const connect = async () => {
      if (cancelled) return
      if (ws && (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING)) return
      if (!firstConnect) {
        let waitTimer: number | undefined
        await Promise.race([
          refreshMode(),
          new Promise<void>((resolve) => {
            waitTimer = window.setTimeout(resolve, MODE_WAIT_MS)
          }),
        ])
        window.clearTimeout(waitTimer)
        if (cancelled) return
        // Another connect may have opened a socket while the room was being fetched.
        if (ws && (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING)) return
      }
      const sock = openRoomWs(roomId, firstConnect ? 0 : lastSeqRef.current)
      ws = sock
      firstConnect = false
      let opened = false
      // This socket sent a stale_position frame: its close takes the stale path even when the
      // reason is lost (a 1006, or a proxy that drops it), or it would reconnect with the same
      // stale seq forever (#550).
      let staleFrame = false
      sock.onopen = () => {
        if (cancelled) return
        opened = true
        probed = false
        backoff = 1000
        setConn('live')
        liveRef.current = true
        setError(ledger.banner())
        sendPresence(currentPresence())
        // Messages that did not get through: sent again, each with its own client_id, so a
        // server that already stored one appends nothing (docs/api.md, Idempotent posts).
        // Success emits nothing here (the event arrives over the socket).
        ledger.drain()
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
          if (msg.error.code === 'stale_position') staleFrame = true
          // The first stale_position is handled silently by a reload (see onclose).
          if (msg.error.code === 'stale_position' && staleRoomRef.current !== roomId) return
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
        if (ev?.reason === 'stale_position' || staleFrame) {
          if (staleRoomRef.current === roomId) {
            // A second one in a row: after=0 cannot cause it, so it is a client bug. The error
            // frame is on screen; retrying would only repeat it.
            setConn('error')
            setError((prev) => prev ?? "This room's event log could not be replayed (stale_position).")
            return
          }
          // The position is past the stored log: drop the room state and replay it all, once.
          staleRoomRef.current = roomId
          setReloadKey((k) => k + 1)
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
        reconnectTimer = window.setTimeout(() => void connect(), equalJitter(backoff, randomRef.current))
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
        replayTarget = snapshot.last_seq ?? 0
        stagingOn = snapshot.mode?.staging === true
        // lastSeqRef stays 0: open with after=0 so the full log replays and the
        // reducer rebuilds timeline/comments/proposals/trace/handoffs (item D).
        setConn('connecting')
        void connect()
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
      liveRef.current = false
      if (reconnectTimer !== undefined) window.clearTimeout(reconnectTimer)
      if (presenceTimer !== undefined) window.clearInterval(presenceTimer)
      document.removeEventListener('visibilitychange', onVisibility)
      ws?.close()
      ws = null
    }
  }, [roomId, user.id, user.name, sessionEpoch, reloadKey, ledger])

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

  const onDelivered = useCallback((listener: DeliveredListener) => ledger.onDelivered(listener), [ledger])
  const sends = useMemo<SendControls>(
    () => ({
      onDelivered: (listener) => ledger.onDelivered(listener),
      cancel: (clientId) => ledger.cancel(clientId),
      isPending: (clientId) => ledger.isPending(clientId),
    }),
    [ledger],
  )

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
    sends,
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
      return ledger.send({ kind: 'comment', text, stepId }, opts?.clientId)
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
      return ledger.send({ kind: 'steer', text }, opts?.clientId)
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
        if (isStagingRequired(e)) {
          setError(null)
          dispatch({ type: 'staging_required', proposalId, text: errorText(e) })
          return { ok: false, stagingRequired: true }
        }
        setError(errorText(e))
        return { ok: false }
      }
    },
    approveAll: async (proposalId, extra) => {
      if (isFixtures()) {
        const p = stateRef.current?.proposals.find((x) => x.proposal_id === proposalId)
        for (const layer of p?.required_layers ?? []) {
          if ((p?.approvals[layer] ?? 'pending') !== 'pending') continue
          synthesize('approval.given', {
            proposal_id: proposalId,
            layer,
            params_hash: extra.params_hash,
            identity: { method: 'dev', grant: `user:${user.id}` },
          })
        }
        return { ok: true }
      }
      try {
        await api.approveAll(proposalId, extra)
        setError(null)
        clearStale()
        return { ok: true }
      } catch (e) {
        if (isParamsMismatch(e)) {
          setError(null)
          onMismatch(proposalId, extra.params_hash)
          return { ok: false, mismatch: true }
        }
        if (isSoloConfirmError(e)) {
          setError(null)
          return { ok: false, needsConfirm: true }
        }
        const text = errorText(e)
        if (isStagingRequired(e)) {
          setError(null)
          dispatch({ type: 'staging_required', proposalId, text })
          return { ok: false, stagingRequired: true, error: text }
        }
        if (e instanceof ApiClientError && e.status === 403 && e.code === 'not_layer_owner') {
          // The card says it, next to the rows that are back; the banner would say it twice.
          setError(null)
          dispatch({ type: 'approve_all_refused', proposalId, text })
          return { ok: false, refused: true, error: text }
        }
        setError(text)
        return { ok: false, error: text }
      }
    },
    cancelSchedule: async (proposalId, reason) => {
      if (isFixtures()) {
        synthesize('proposal.cancelled', {
          proposal_id: proposalId,
          by: { kind: 'human', id: user.id, name: user.name },
          reason: reason ?? '',
        })
        return { ok: true }
      }
      try {
        await api.cancelProposal(proposalId, reason)
        setError(null)
        return { ok: true }
      } catch (e) {
        if (e instanceof ApiClientError && e.status === 409 && e.code === 'not_scheduled') {
          setError(null)
          return { ok: false, notScheduled: true }
        }
        const text = errorText(e)
        setError(text)
        return { ok: false, error: text }
      }
    },
    stage: async (proposalId) => {
      if (isFixtures()) {
        const p = stateRef.current?.proposals.find((x) => x.proposal_id === proposalId)
        const steps = p?.stageable ?? []
        const hash = p?.params_hash ?? ''
        synthesize('proposal.stage_started', {
          proposal_id: proposalId,
          params_hash: hash,
          steps,
          expires_at: new Date(Date.now() + 150_000).toISOString(),
        })
        synthesize('proposal.staged', {
          proposal_id: proposalId,
          params_hash: hash,
          steps: steps.map((i) => ({
            step_index: i,
            result: { target: 'connector orders-src', applied: true, healthy: true, detail: 'RUNNING, 1 of 1 tasks RUNNING for 60 s', duration_ms: 61_000 },
          })),
          at: new Date().toISOString(),
        })
        return { ok: true }
      }
      try {
        await api.stageProposal(proposalId)
        setError(null)
        return { ok: true }
      } catch (e) {
        // The card shows the reason next to the button; the banner would say it twice.
        const code = e instanceof ApiClientError ? e.code : undefined
        return { ok: false, code, error: errorText(e) }
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
    trace: async (table, key) => {
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
        return { ok: true }
      }
      try {
        await api.trace(roomId, table, key)
        setError(null)
        return { ok: true }
      } catch (e) {
        return { ok: false, error: errorText(e) }
      }
    },
  }

  return { state, presence, conn, error, actions, stale, dismissStale, reload }
}
