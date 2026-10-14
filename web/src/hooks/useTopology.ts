import { useEffect, useRef, useState } from 'react'
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

  const refreshKey = options.refreshKey ?? 0
  const active = options.active !== false
  // Pipelines (per session) the server answered 404 for. An unknown pipeline stays unknown for
  // the life of the room, so it is not asked again on every tick and every executed action (QA9).
  const notFound = useRef(new Set<string>())

  useEffect(() => {
    let cancelled = false
    let inFlight = false
    const key = `${sessionEpoch}|${pipelineId}`

    const load = async () => {
      if (inFlight || cancelled || notFound.current.has(key)) return
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
        // Later ticks see the key and return without a request.
        if (e instanceof ApiClientError && e.status === 404) notFound.current.add(key)
        if (!cancelled) setError(e instanceof Error ? e.message : String(e))
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

    if (notFound.current.has(key)) return
    void load()
    if (!active) return

    const timer = window.setInterval(load, TOPOLOGY_REFRESH_MS)
    return () => {
      cancelled = true
      window.clearInterval(timer)
    }
  }, [pipelineId, user.id, sessionEpoch, refreshKey, active])

  return { topology, error, updatedAt }
}
