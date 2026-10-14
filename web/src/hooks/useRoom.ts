import { useCallback, useEffect, useReducer, useRef, useState } from 'react'
import type { Event, PresenceMember, RoomSnapshot, WsServerMessage } from '../api/types'
import { ApiClientError, api, isFixtures, openRoomWs } from '../api/client'
import type { ApproveExtras } from '../api/client'
import { isSoloConfirmError } from '../lib/solo'
import { classifyAuthProblem, reportAuthProblem } from '../lib/auth'
import { emptySnapshot, fixtureEvents, fixtureRoom, FIXTURE_EVENT_DELAY_MS } from '../fixtures/player'
import { useCurrentUser } from '../context/user'
import { findUser } from '../dev/users'
import { errorText, friendlyError } from '../lib/errors'
import { mentionsAgent } from '../lib/mentions'
import { applyEvent } from '../state/reducer'
import { fromSnapshot } from '../state/types'
import type { RoomState } from '../state/types'
import { hideStaleToast, showStaleToast } from '../lib/staleToast'

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

/** What a close attempt came to; `error` is the text for the person. */
export interface CloseResult {
  ok: boolean
  error?: string
}

export type ConnState = 'loading' | 'connecting' | 'live' | 'reconnecting' | 'error' | 'not_found'

export interface RoomActions {
  comment: (text: string, stepId?: string) => void
  steer: (text: string) => void
  sendSuggestion: (suggestionId: string) => void
  requestControl: () => void
  grantControl: (to: string) => void
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
  acceptHandoff: (handoffId: string) => void
  trace: (table: string, key: string) => void
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

export function useRoom(roomId: string) {
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
  // Fixture mode: buffer synthetic (user) events until the story replay ends so
  // their seq can never collide with an upcoming story event's seq.
  const storyPlayingRef = useRef(false)
  const synthQueueRef = useRef<Array<{ type: Event['type']; payload: unknown; at: string }>>([])

  useEffect(() => {
    stateRef.current = state
  }, [state])

  useEffect(() => {
    let cancelled = false
    let ws: WebSocket | null = null
    let reconnectTimer: number | undefined
    let replayTimer: number | undefined

    dispatch({ type: 'reset' })
    lastSeqRef.current = 0
    setError(null)
    setPresence([])

    if (isFixtures()) {
      const snapshot = fixtureRoom(roomId) ?? emptySnapshot(roomId, roomId, 'sev3', 'orders')
      const events = fixtureEvents(roomId)
      dispatch({ type: 'init', snapshot })
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

    const sendPresence = (pstate: PresenceMember['state']) => {
      if (ws && ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({ type: 'presence', state: pstate }))
      }
    }

    const applyEventSeq = (ev: Event) => {
      if (ev.seq <= lastSeqRef.current) return
      dispatch({ type: 'event', event: ev })
      lastSeqRef.current = ev.seq
    }

    // Page /events until the gap before `ev` closes. On failure (fetch throws or
    // makes no progress) close the socket so the reconnect replays from lastSeq.
    const fillGap = async (ev: Event): Promise<boolean> => {
      while (ev.seq > lastSeqRef.current + 1) {
        if (cancelled) return false
        let page
        try {
          page = await api.events(roomId, lastSeqRef.current)
        } catch {
          ws?.close()
          return false
        }
        if (cancelled) return false
        const missing = page.events
          .filter((e) => e.seq > lastSeqRef.current)
          .sort((a, b) => a.seq - b.seq)
        if (missing.length === 0) {
          ws?.close()
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
          ws?.close()
          return false
        }
      }
      return ev.seq === lastSeqRef.current + 1
    }

    // One event through the single queue, so messages that arrive during a gap
    // fetch still apply in seq order.
    const processEvent = async (ev: Event) => {
      if (cancelled) return
      if (ev.seq <= lastSeqRef.current) return // duplicate or older — drop
      const ready = await fillGap(ev)
      if (cancelled || !ready) return
      applyEventSeq(ev)
    }

    const connect = () => {
      if (cancelled) return
      if (ws && (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING)) return
      ws = openRoomWs(roomId, firstConnect ? 0 : lastSeqRef.current)
      firstConnect = false
      let opened = false
      ws.onopen = () => {
        if (cancelled) return
        opened = true
        probed = false
        backoff = 1000
        setConn('live')
        setError(null)
        sendPresence('viewing')
      }
      ws.onmessage = (m) => {
        if (cancelled) return // a socket from an older session epoch
        let msg: WsServerMessage
        try {
          msg = JSON.parse(m.data as string) as WsServerMessage
        } catch {
          return
        }
        if (msg.type === 'event') {
          queue = queue.then(() => processEvent(msg.event)).catch(() => undefined)
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
      ws.onclose = (ev: CloseEvent) => {
        if (cancelled) return
        if (ev?.reason === 'session_expired') {
          // The server closed the socket because the session ended (auth.md 1.6).
          // Stop retrying; the login screen (or redirect) takes over.
          reportAuthProblem({ kind: 'session_expired', message: 'session_expired', epoch: sessionEpoch })
          setConn('error')
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
        reconnectTimer = window.setTimeout(connect, backoff)
        backoff = Math.min(backoff * 2, 10000)
      }
      ws.onerror = () => {
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
        setConn('error')
        setError(errorText(e))
      }
    })()
    const presenceTimer = window.setInterval(() => sendPresence('viewing'), 20000)

    const onVisibility = () => {
      sendPresence(document.hidden ? 'away' : 'viewing')
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
  }, [roomId, user.id, user.name, sessionEpoch, reloadKey])

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
    comment: (text, stepId) => {
      if (isFixtures()) {
        synthesize('comment.added', {
          comment_id: nextLocalId(),
          step_id: stepId,
          text,
          mentions_agent: mentionsAgent(text),
        })
        return
      }
      void runAction(api.comment(roomId, { step_id: stepId, text }))
    },
    steer: (text) => {
      if (isFixtures()) {
        const s = stateRef.current
        const isDriver = s?.driver?.id === user.id
        synthesize(
          isDriver ? 'steer.sent' : 'steer.suggested',
          isDriver ? { text } : { suggestion_id: nextLocalId(), text },
        )
        return
      }
      void runAction(api.steer(roomId, text))
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
        return
      }
      void runAction(api.acceptHandoff(handoffId))
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
