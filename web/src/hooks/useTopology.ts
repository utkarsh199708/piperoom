import { useCallback, useEffect, useRef, useState } from 'react'
import type { Topology } from '../api/types'
import { ApiClientError, api, isFixtures } from '../api/client'
import { fixtureTopology } from '../fixtures/player'
import { useCurrentUser } from '../context/user'

export const TOPOLOGY_REFRESH_MS = 30_000

export interface TopologyOptions {
  // Bump to force an immediate refetch (one per action.executed).
  refreshKey?: number
  // When false the periodic 30 s refresh stops (room closed). The first load
  // still happens so a closed room's pipeline map renders.
  active?: boolean
}

// Loads the pipeline topology, refreshing every 30 s while the room is open
// (docs/api.md) and again whenever `refreshKey` changes. Fetch calls never
// overlap: an interval tick while a fetch is in flight is skipped.
export function useTopology(pipelineId: string, options: TopologyOptions = {}) {
  const { user, sessionEpoch } = useCurrentUser()
  const [topology, setTopology] = useState<Topology | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [updatedAt, setUpdatedAt] = useState<number | null>(null)
  // 404 is terminal for a pipeline id (docs/api.md: unknown pipeline): no timer retries it.
  // Only a new pipeline, session, or retry() starts fetching again; a refreshKey bump does not.
  const [notFound, setNotFound] = useState(false)
  const [retryKey, setRetryKey] = useState(0)
  const deadKeyRef = useRef<string | null>(null)
  const retry = useCallback(() => setRetryKey((k) => k + 1), [])

  const refreshKey = options.refreshKey ?? 0
  const active = options.active !== false

  useEffect(() => {
    let cancelled = false
    let inFlight = false
    const key = `${pipelineId}|${user.id}|${sessionEpoch}|${retryKey}`
    if (deadKeyRef.current === key) return // already a 404 for exactly this load
    deadKeyRef.current = null
    setNotFound(false)

    const load = async () => {
      if (inFlight || cancelled) return
      inFlight = true
      try {
        if (isFixtures()) {
          if (!cancelled) {
            setTopology(pipelineId === fixtureTopology.pipeline_id ? fixtureTopology : null)
            setUpdatedAt(Date.now())
          }
          return
        }
        const t = await api.topology(pipelineId)
        if (!cancelled) {
          setTopology(t)
          setUpdatedAt(Date.now())
        }
      } catch (e) {
        if (cancelled) return
        if (e instanceof ApiClientError && e.status === 404) {
          deadKeyRef.current = key
          setNotFound(true)
          setError(null)
          window.clearInterval(timer)
          return
        }
        setError(e instanceof Error ? e.message : String(e))
      } finally {
        inFlight = false
      }
    }

    // The pipeline id is not known until the room snapshot/event log loads; do
    // not fetch a topology for an empty/placeholder id.
    if (!pipelineId) {
      setTopology(null)
      setUpdatedAt(null)
      return
    }

    // Declared before load() runs: a 404 on the first call clears it.
    let timer = 0
    void load()
    if (!active) return

    timer = window.setInterval(load, TOPOLOGY_REFRESH_MS)
    return () => {
      cancelled = true
      window.clearInterval(timer)
    }
  }, [pipelineId, user.id, sessionEpoch, refreshKey, active, retryKey])

  return { topology, error, updatedAt, notFound, retry }
}
