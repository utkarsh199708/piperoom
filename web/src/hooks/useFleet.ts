import { useCallback, useEffect, useRef, useState } from 'react'
import { api, isFixtures } from '../api/client'
import type { FleetPipeline, PipelineChange } from '../api/types'
import { useCurrentUser } from '../context/user'
import { cleanChangesPage, cleanFleet, fleetErrorText } from '../lib/fleet'

/** The server caches the fleet for 10 s per caller; the watcher ticks about once a minute. */
export const FLEET_REFRESH_MS = 30_000

/** Rows per changes page (the server's default is 100, its cap 200). */
export const CHANGES_PAGE = 50

// Fixture mode has no server: its one pipeline, as the room fixtures tell it.
const fixtureFleet = (): FleetPipeline[] => [
  {
    pipeline_id: 'orders',
    title: 'orders',
    kind: 'cdc',
    tenant: '',
    team: '',
    health: 'fail',
    worst_node: 'connect:pg-orders-connector',
    open_rooms: 1,
    signals_firing: ['connector_down'],
    changes_24h: 1,
    forecasts: [],
    last_verdict: null,
  },
]

export interface FleetState {
  /** null until the first answer for this session. */
  rows: FleetPipeline[] | null
  /** The last load failed; rows (if any) are the previous answer. */
  error: string | null
  updatedAt: number | null
  reload: () => void
}

/**
 * GET /api/fleet, again every FLEET_REFRESH_MS. A refresh keeps the rows on screen; a failed one keeps the last
 * rows and sets `error`. A new session (login, user switch) never shows the previous person's rows.
 */
export function useFleet(): FleetState {
  const { user, sessionEpoch } = useCurrentUser()
  const [rows, setRows] = useState<FleetPipeline[] | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [updatedAt, setUpdatedAt] = useState<number | null>(null)
  const [tick, setTick] = useState(0)
  const key = `${user.id}|${sessionEpoch}`
  const [rowsFor, setRowsFor] = useState(key)
  if (rowsFor !== key) {
    setRowsFor(key)
    setRows(null)
    setError(null)
  }

  useEffect(() => {
    let cancelled = false
    let inFlight = false
    const load = async () => {
      if (inFlight) return
      inFlight = true
      try {
        const data = isFixtures() ? fixtureFleet() : cleanFleet(await api.fleet())
        if (cancelled) return
        setRows(data)
        setError(null)
        setUpdatedAt(Date.now())
      } catch (e) {
        if (!cancelled) setError(fleetErrorText(e))
      } finally {
        inFlight = false
      }
    }
    void load()
    const timer = window.setInterval(() => void load(), FLEET_REFRESH_MS)
    return () => {
      cancelled = true
      window.clearInterval(timer)
    }
  }, [key, tick])

  const reload = useCallback(() => setTick((n) => n + 1), [])
  return { rows, error, updatedAt, reload }
}

export interface FleetChangesState {
  status: 'idle' | 'loading' | 'ready' | 'error'
  changes: PipelineChange[]
  /** The cursor of the next page; '' on the last page. */
  next: string
  loadingMore: boolean
  /** A first-page failure (status 'error') or a load-more failure (rows kept). */
  error: string | null
  loadMore: () => void
  reload: () => void
}

const rowKey = (c: PipelineChange) => `${c.id}|${c.pipeline_id}|${c.fingerprint}`

/**
 * GET /api/fleet/changes, one page at a time: the first page while `enabled`, then one more per loadMore. The
 * server's cursor is a position, so a row recorded after the first page never shifts a later one; a row seen twice
 * anyway is kept once.
 */
export function useFleetChanges(enabled: boolean): FleetChangesState {
  const { user, sessionEpoch } = useCurrentUser()
  const [status, setStatus] = useState<FleetChangesState['status']>('idle')
  const [changes, setChanges] = useState<PipelineChange[]>([])
  const [next, setNext] = useState('')
  const [loadingMore, setLoadingMore] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [tick, setTick] = useState(0)
  // Bumped by every first-page load, so a load-more answer for an earlier list is dropped.
  const gen = useRef(0)
  const key = `${user.id}|${sessionEpoch}`

  useEffect(() => {
    if (!enabled) return
    // The counter itself, not a number read now: the cleanup must invalidate whatever is in flight at that time.
    const counter = gen
    const mine = ++counter.current
    setStatus('loading')
    setChanges([])
    setNext('')
    setError(null)
    setLoadingMore(false)
    const load = isFixtures() ? Promise.resolve({ changes: [], next: '' }) : api.fleetChanges({ limit: CHANGES_PAGE })
    load.then(
      (raw) => {
        if (counter.current !== mine) return
        const page = cleanChangesPage(raw)
        setChanges(page.changes)
        setNext(page.next)
        setStatus('ready')
      },
      (e: unknown) => {
        if (counter.current !== mine) return
        setError(fleetErrorText(e))
        setStatus('error')
      },
    )
    return () => {
      counter.current++
    }
  }, [enabled, key, tick])

  const loadMore = useCallback(() => {
    if (!next || loadingMore) return
    const mine = gen.current
    setLoadingMore(true)
    setError(null)
    api.fleetChanges({ limit: CHANGES_PAGE, cursor: next }).then(
      (raw) => {
        if (gen.current !== mine) return
        const page = cleanChangesPage(raw)
        setChanges((prev) => {
          const seen = new Set(prev.map(rowKey))
          return [...prev, ...page.changes.filter((c) => !seen.has(rowKey(c)))]
        })
        setNext(page.next)
        setLoadingMore(false)
      },
      (e: unknown) => {
        if (gen.current !== mine) return
        setError(fleetErrorText(e))
        setLoadingMore(false)
      },
    )
  }, [next, loadingMore])

  const reload = useCallback(() => setTick((n) => n + 1), [])
  return { status: enabled ? status : 'idle', changes, next, loadingMore, error, loadMore, reload }
}
