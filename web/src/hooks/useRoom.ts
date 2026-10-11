import { useCallback, useEffect, useReducer, useRef, useState } from 'react'
import type { Event, PresenceMember, RoomSnapshot, WsServerMessage } from '../api/types'
import { api, isFixtures, openRoomWs } from '../api/client'
import { emptySnapshot, fixtureEvents, fixtureRoom, FIXTURE_EVENT_DELAY_MS } from '../fixtures/player'
import { useCurrentUser } from '../context/user'
import { findUser } from '../dev/users'
import { applyEvent, detectGap } from '../state/reducer'
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

    const applyEvents = (events: Event[]) => {
      for (const ev of events) {
        if (ev.seq <= lastSeqRef.current) continue
        dispatch({ type: 'event', event: ev })
        lastSeqRef.current = ev.seq
      }
    }

    if (isFixtures()) {
      const snapshot = fixtureRoom(roomId) ?? emptySnapshot(roomId, roomId, 'sev3', 'orders')
      const events = fixtureEvents(roomId)
      dispatch({ type: 'init', snapshot })
      lastSeqRef.current = snapshot.last_seq
      setPresence(snapshot.members.map((m) => ({ id: m.id, name: m.name, state: 'viewing' })))
      setConn('live')

      let i = 0
      const playNext = () => {
        if (cancelled) return
        while (i < events.length && events[i].seq <= lastSeqRef.current) i += 1
        if (i >= events.length) return
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

    // Live mode: snapshot, then WebSocket replay + live.
    setConn('loading')

    const backoffRef = { current: 1000 }

    const sendPresence = (pstate: PresenceMember['state']) => {
      if (ws && ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({ type: 'presence', state: pstate }))
      }
    }

    const handleEvent = async (ev: Event) => {
      if (ev.seq <= lastSeqRef.current) return
      if (detectGap(lastSeqRef.current, ev)) {
        // Gap: fetch the missing events before applying.
        try {
          const page = await api.events(user.id, roomId, lastSeqRef.current)
          const missing = page.events
            .filter((e) => e.seq > lastSeqRef.current)
            .sort((a, b) => a.seq - b.seq)
          applyEvents(missing)
        } catch {
          // leave the gap; the next reconnect replays from lastSeq
        }
        if (ev.seq <= lastSeqRef.current) return
      }
      dispatch({ type: 'event', event: ev })
      lastSeqRef.current = ev.seq
    }

    const connect = () => {
      if (cancelled) return
      ws = openRoomWs(roomId, user.id, lastSeqRef.current)
      ws.onopen = () => {
        if (cancelled) return
        setConn('live')
      }
      ws.onmessage = (m) => {
        let msg: WsServerMessage
        try {
          msg = JSON.parse(m.data as string) as WsServerMessage
        } catch {
          return
        }
        if (msg.type === 'event') {
          void handleEvent(msg.event)
        } else if (msg.type === 'presence') {
          setPresence(msg.members)
        } else if (msg.type === 'error') {
          setError(msg.error.message)
        }
      }
      ws.onclose = () => {
        if (cancelled) return
        setConn('reconnecting')
        reconnectTimer = window.setTimeout(connect, backoffRef.current)
        backoffRef.current = Math.min(backoffRef.current * 2, 10000)
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
        lastSeqRef.current = snapshot.last_seq
        setConn('connecting')
        connect()
      } catch (e) {
        if (cancelled) return
        setConn('error')
        setError(e instanceof Error ? e.message : String(e))
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
  }, [roomId, user.id])

  const synthesize = useCallback(
    (type: Event['type'], payload: unknown) => {
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
      void api.comment(user.id, roomId, { step_id: stepId, text }).catch((e) =>
        setError(e instanceof Error ? e.message : String(e)),
      )
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
      void api.steer(user.id, roomId, text).catch((e) =>
        setError(e instanceof Error ? e.message : String(e)),
      )
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
      void api.sendSuggestion(user.id, roomId, suggestionId).catch((e) =>
        setError(e instanceof Error ? e.message : String(e)),
      )
    },
    requestControl: () => {
      if (isFixtures()) {
        synthesize('control.requested', {})
        return
      }
      void api.requestControl(user.id, roomId).catch((e) =>
        setError(e instanceof Error ? e.message : String(e)),
      )
    },
    grantControl: (to) => {
      if (isFixtures()) {
        const target = findUser(to)
        synthesize('control.granted', {
          to: { kind: 'human', id: target.id, name: target.name },
          lease_until: new Date(Date.now() + 30 * 60 * 1000).toISOString(),
        })
        return
      }
      void api.grantControl(user.id, roomId, to).catch((e) =>
        setError(e instanceof Error ? e.message : String(e)),
      )
    },
    close: (resolution) => {
      if (isFixtures()) {
        synthesize('room.closed', { resolution })
        return
      }
      void api.closeRoom(user.id, roomId, resolution).catch((e) =>
        setError(e instanceof Error ? e.message : String(e)),
      )
    },
    approve: (proposalId, layer) => {
      if (isFixtures()) {
        synthesize('approval.given', { proposal_id: proposalId, layer })
        return
      }
      void api.approve(user.id, proposalId, layer).catch((e) =>
        setError(e instanceof Error ? e.message : String(e)),
      )
    },
    reject: (proposalId, layer, reason) => {
      if (isFixtures()) {
        synthesize('approval.rejected', { proposal_id: proposalId, layer, reason })
        return
      }
      void api.reject(user.id, proposalId, layer, reason).catch((e) =>
        setError(e instanceof Error ? e.message : String(e)),
      )
    },
    handoff: (to, stayAsWatcher) => {
      if (isFixtures()) {
        const target = findUser(to)
        synthesize('handoff.created', {
          handoff_id: nextLocalId(),
          to: { kind: 'human', id: target.id, name: target.name },
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
      void api.handoff(user.id, roomId, to, stayAsWatcher).catch((e) =>
        setError(e instanceof Error ? e.message : String(e)),
      )
    },
    acceptHandoff: (handoffId) => {
      if (isFixtures()) {
        synthesize('handoff.accepted', { handoff_id: handoffId })
        return
      }
      void api.acceptHandoff(user.id, handoffId).catch((e) =>
        setError(e instanceof Error ? e.message : String(e)),
      )
    },
    trace: (table, key) => {
      if (isFixtures()) {
        synthesize('trace.pinned', {
          table,
          key,
          stages: [
            { layer: 'postgres', name: table, status: 'seen' },
            { layer: 'kafka-connect', name: 'pg-orders-connector', status: 'seen' },
            { layer: 'warehouse', name: 'analytics.orders', status: 'stopped_here' },
          ],
        })
        return
      }
      void api.trace(user.id, roomId, table, key).catch((e) =>
        setError(e instanceof Error ? e.message : String(e)),
      )
    },
  }

  return { state, presence, conn, error, actions }
}
