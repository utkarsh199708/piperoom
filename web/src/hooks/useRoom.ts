import { useCallback, useEffect, useReducer, useRef, useState } from 'react'
import type { Event, PresenceMember, RoomSnapshot, WsServerMessage } from '../api/types'
import { api, isFixtures, openRoomWs } from '../api/client'
import { emptySnapshot, fixtureEvents, fixtureRoom, FIXTURE_EVENT_DELAY_MS } from '../fixtures/player'
import { useCurrentUser } from '../context/user'
import { findUser } from '../dev/users'
import { errorText, friendlyError } from '../lib/errors'
import { applyEvent } from '../state/reducer'
import { fromSnapshot } from '../state/types'
import type { RoomState } from '../state/types'

export type ConnState = 'loading' | 'connecting' | 'live' | 'reconnecting' | 'error' | 'not_found'

export interface RoomActions {
  comment: (text: string, stepId?: string) => void
  steer: (text: string) => void
  sendSuggestion: (suggestionId: string) => void
  requestControl: () => void
  grantControl: (to: string) => void
  close: (resolution: string) => void
  approve: (proposalId: string, layer: string) => void
  reject: (proposalId: string, layer: string, reason: string) => void
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
  const { user } = useCurrentUser()
  const [state, dispatch] = useReducer(roomReducer, null)
  const [presence, setPresence] = useState<PresenceMember[]>([])
  const [conn, setConn] = useState<ConnState>('loading')
  const [error, setError] = useState<string | null>(null)

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
      setPresence(snapshot.members.map((m) => ({ id: m.id, name: m.name, state: 'viewing' })))
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
          page = await api.events(user.id, roomId, lastSeqRef.current)
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
      ws = openRoomWs(roomId, user.id, firstConnect ? 0 : lastSeqRef.current)
      firstConnect = false
      ws.onopen = () => {
        if (cancelled) return
        backoff = 1000
        setConn('live')
        setError(null)
        sendPresence('viewing')
      }
      ws.onmessage = (m) => {
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
          setError(friendlyError(msg.error.code, msg.error.message))
        }
      }
      ws.onclose = () => {
        if (cancelled) return
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
        const snapshot = await api.room(user.id, roomId)
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
  }, [roomId, user.id, user.name])

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
          mentions_agent: text.includes('@agent'),
        })
        return
      }
      void runAction(api.comment(user.id, roomId, { step_id: stepId, text }))
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
      void runAction(api.steer(user.id, roomId, text))
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
      void runAction(api.sendSuggestion(user.id, roomId, suggestionId))
    },
    requestControl: () => {
      if (isFixtures()) {
        synthesize('control.requested', {})
        return
      }
      void runAction(api.requestControl(user.id, roomId))
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
      void runAction(api.grantControl(user.id, roomId, to))
    },
    close: (resolution) => {
      if (isFixtures()) {
        synthesize('room.closed', { resolution })
        return
      }
      void runAction(api.closeRoom(user.id, roomId, resolution))
    },
    approve: (proposalId, layer) => {
      if (isFixtures()) {
        synthesize('approval.given', { proposal_id: proposalId, layer })
        return
      }
      void runAction(api.approve(user.id, proposalId, layer))
    },
    reject: (proposalId, layer, reason) => {
      if (isFixtures()) {
        synthesize('approval.rejected', { proposal_id: proposalId, layer, reason })
        return
      }
      void runAction(api.reject(user.id, proposalId, layer, reason))
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
      void runAction(api.handoff(user.id, roomId, to, stayAsWatcher))
    },
    acceptHandoff: (handoffId) => {
      if (isFixtures()) {
        synthesize('handoff.accepted', { handoff_id: handoffId })
        return
      }
      void runAction(api.acceptHandoff(user.id, handoffId))
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
      void runAction(api.trace(user.id, roomId, table, key))
    },
  }

  return { state, presence, conn, error, actions }
}
