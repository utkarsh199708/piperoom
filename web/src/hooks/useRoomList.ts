import { useCallback, useEffect, useRef, useState } from 'react'
import type { ListWsServerMessage, RoomSnapshot } from '../api/types'
import { api, isFixtures, openListWs } from '../api/client'
import { fixtureRooms } from '../fixtures/player'
import { useCurrentUser } from '../context/user'
import { classifyAuthProblem, reportAuthProblem } from '../lib/auth'
import { sortRoomsForList } from '../lib/rooms'

/** Poll period when the push socket is not available (fixtures, refused, failing). */
export const POLL_MS = 15_000
/** Reconnect backoff of the push socket: 1 s, doubling to 30 s. */
export const BACKOFF_MIN_MS = 1_000
export const BACKOFF_MAX_MS = 30_000
/** Opens that fail in a row before the list falls back to polling. */
export const MAX_FAILED_OPENS = 3
/** The server closes a list socket silent for 60 s; ping well inside that. */
const PING_MS = 20_000

/** How the list stays current: pushed over GET /api/rooms/ws, or polled. */
export type RoomListMode = 'push' | 'poll'

/** Folds one push frame into the list. Exported for tests. */
export function foldListFrame(rooms: RoomSnapshot[], msg: ListWsServerMessage): RoomSnapshot[] {
  switch (msg.type) {
    case 'rooms':
      return sortRoomsForList(msg.rooms ?? [])
    case 'room': {
      const rest = rooms.filter((r) => r.id !== msg.room.id)
      return sortRoomsForList([...rest, msg.room])
    }
    case 'room_deleted':
      return rooms.filter((r) => r.id !== msg.room_id)
    default:
      return rooms
  }
}

/**
 * The rooms list. It is pushed (GET /api/rooms/ws, F6): the first frame is the whole list,
 * then one frame per changed room. When the socket cannot be opened three times in a row, or
 * in fixtures mode, it polls GET /api/rooms every 15 s instead. `updatedAt` is the time of the
 * last poll, null while pushed (the list is live). `reload` refetches now (the Retry button).
 *
 * `enabled: false` loads nothing: a screen inside the shell reads the shell's copy
 * (RoomListContext) instead of fetching its own.
 */
export function useRoomList({ enabled = true }: { enabled?: boolean } = {}) {
  const { user, sessionEpoch } = useCurrentUser()
  const [rooms, setRooms] = useState<RoomSnapshot[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [mode, setMode] = useState<RoomListMode>(() => (isFixtures() ? 'poll' : 'push'))
  const [updatedAt, setUpdatedAt] = useState<number | null>(null)
  const modeRef = useRef(mode)
  useEffect(() => {
    modeRef.current = mode
  }, [mode])

  const reload = useCallback(async () => {
    setLoading(true)
    try {
      if (isFixtures()) {
        // A copy: the fixture array is edited in place (new room, take control).
        setRooms([...fixtureRooms])
      } else {
        setRooms(await api.rooms())
      }
      setError(null) // a Retry that works clears the error
      if (modeRef.current === 'poll') setUpdatedAt(Date.now())
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
    } finally {
      setLoading(false)
    }
  }, [])

  // A new person or session starts over with the push socket.
  const [identity, setIdentity] = useState(`${user.id}|${sessionEpoch}`)
  if (identity !== `${user.id}|${sessionEpoch}`) {
    setIdentity(`${user.id}|${sessionEpoch}`)
    setMode(isFixtures() ? 'poll' : 'push')
  }

  // Push: one socket, reconnected with backoff; three failed opens in a row fall back to polling.
  useEffect(() => {
    if (!enabled || mode !== 'push') return
    let cancelled = false
    let ws: WebSocket | null = null
    let reconnectTimer: number | undefined
    let pingTimer: number | undefined
    let backoff = BACKOFF_MIN_MS
    let failedOpens = 0

    const retry = () => {
      if (cancelled) return
      if (failedOpens >= MAX_FAILED_OPENS) {
        // Refused (429 too_many_connections, a proxy without WebSocket): poll, no toast.
        setMode('poll')
        return
      }
      reconnectTimer = window.setTimeout(connect, backoff)
      backoff = Math.min(backoff * 2, BACKOFF_MAX_MS)
    }

    const connect = () => {
      if (cancelled) return
      let gotList = false
      let sock: WebSocket
      try {
        sock = openListWs()
      } catch {
        failedOpens++
        retry()
        return
      }
      ws = sock
      sock.onopen = () => {
        pingTimer = window.setInterval(() => {
          if (sock.readyState === WebSocket.OPEN) sock.send(JSON.stringify({ type: 'ping' }))
        }, PING_MS)
      }
      sock.onmessage = (m: MessageEvent) => {
        if (cancelled) return
        let msg: ListWsServerMessage
        try {
          msg = JSON.parse(m.data as string) as ListWsServerMessage
        } catch {
          return
        }
        if (msg.type === 'error') {
          const status = msg.error.code === 'session_expired' ? 401 : 0
          const problem = classifyAuthProblem(status, msg.error.code, msg.error.message)
          if (problem) reportAuthProblem({ ...problem, epoch: sessionEpoch })
          return
        }
        if (msg.type === 'rooms') {
          gotList = true
          failedOpens = 0
          backoff = BACKOFF_MIN_MS
          setError(null)
          setLoading(false)
          setUpdatedAt(null)
        }
        setRooms((cur) => foldListFrame(cur, msg))
      }
      sock.onclose = (ev: CloseEvent) => {
        window.clearInterval(pingTimer)
        if (cancelled) return
        if (ev?.reason === 'session_expired') {
          reportAuthProblem({ kind: 'session_expired', message: 'session_expired', epoch: sessionEpoch })
          return
        }
        if (!gotList) failedOpens++
        retry()
      }
    }

    connect()
    return () => {
      cancelled = true
      window.clearTimeout(reconnectTimer)
      window.clearInterval(pingTimer)
      if (ws) {
        ws.onclose = null
        ws.close()
      }
    }
  }, [enabled, mode, user.id, sessionEpoch])

  // Poll: now, then every 15 s while the tab is visible, and on coming back to it.
  useEffect(() => {
    if (!enabled || mode !== 'poll') return
    void reload()
    const t = window.setInterval(() => {
      if (!document.hidden) void reload()
    }, POLL_MS)
    const onVisible = () => {
      if (!document.hidden) void reload()
    }
    document.addEventListener('visibilitychange', onVisible)
    return () => {
      window.clearInterval(t)
      document.removeEventListener('visibilitychange', onVisible)
    }
  }, [enabled, mode, reload, user.id, sessionEpoch])

  return { rooms, loading, error, reload, mode, updatedAt }
}
