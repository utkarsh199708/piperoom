import { useCallback, useEffect, useRef, useState } from 'react'
import type { CurrentUser } from '../state/permissions'
import type { RoomState } from '../state/types'
import { awayLabel, catchUpSummary } from '../lib/catchup'
import type { CatchUpTarget } from '../lib/catchup'
import type { ConnState } from './useRoom'

/** A strip is shown only after being away this long (hidden tab or a dropped connection). */
export const AWAY_MIN_MS = 60_000
const CAUGHT_UP_MS = 2_000

export type CatchUpStrip =
  | { kind: 'reconnecting'; text: string }
  | { kind: 'caught_up'; text: string }
  | { kind: 'away'; text: string; target: CatchUpTarget; stepId?: string }

interface Away {
  at: number
  snapshot: RoomState
  /** Set once the tab is visible and the socket live again. */
  backAt?: number
}

/**
 * The one-line "what happened" strip. Keeps the folded state from the moment the tab was hidden (or
 * the socket dropped) and diffs it against the current state when the person is back. Dismissed
 * strips do not return for the same hide. The summary is derived at render time, so events that
 * a reconnect replays after `live` still count.
 */
export function useCatchUp(state: RoomState | null, conn: ConnState, user: CurrentUser) {
  const [hidden, setHidden] = useState(() => typeof document !== 'undefined' && document.hidden)
  const [away, setAway] = useState<Away | null>(null)
  const [dismissedAt, setDismissedAt] = useState<number | null>(null)
  const [caughtUp, setCaughtUp] = useState(false)
  const prevConn = useRef(conn)

  useEffect(() => {
    const onVisibility = () => setHidden(document.hidden)
    document.addEventListener('visibilitychange', onVisibility)
    return () => document.removeEventListener('visibilitychange', onVisibility)
  }, [])

  useEffect(() => {
    // A reload (state null, then a fresh snapshot with last_seq 0) makes the old baseline wrong.
    if (!state || (away && state.last_seq < away.snapshot.last_seq)) {
      if (away) setAway(null)
      return
    }
    // Replay is not over until last_seq reaches the snapshot's: a half-replayed state is no baseline
    // (it would count the room's whole history as news).
    if (state.last_seq < state.replay_target) return
    const out = hidden || conn === 'reconnecting'
    if (out && (!away || away.backAt !== undefined)) setAway({ at: Date.now(), snapshot: state })
    else if (!out && away && away.backAt === undefined) setAway({ ...away, backAt: Date.now() })
  }, [state, hidden, conn, away])

  // "Caught up" for 2 s once a dropped socket is live again.
  useEffect(() => {
    const was = prevConn.current
    prevConn.current = conn
    if (was !== 'reconnecting' || conn !== 'live') return
    setCaughtUp(true)
    const t = window.setTimeout(() => setCaughtUp(false), CAUGHT_UP_MS)
    return () => window.clearTimeout(t)
  }, [conn])

  const dismiss = useCallback(() => {
    if (away) setDismissedAt(away.at)
  }, [away])

  let strip: CatchUpStrip | null = null
  if (state && conn === 'reconnecting') {
    strip = { kind: 'reconnecting', text: `Reconnecting… events after #${state.last_seq} pending` }
  } else if (caughtUp) {
    strip = { kind: 'caught_up', text: 'Caught up' }
  } else if (state && away?.backAt !== undefined && dismissedAt !== away.at) {
    const gone = away.backAt - away.at
    const summary = gone >= AWAY_MIN_MS ? catchUpSummary(away.snapshot, state, user) : null
    if (summary) {
      strip = { kind: 'away', text: `While you were away (${awayLabel(gone)}): ${summary.items.join(' · ')}`, target: summary.target, stepId: summary.lastStepId }
    }
  }
  return { strip, dismiss }
}
