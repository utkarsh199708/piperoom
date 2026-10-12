import { useEffect, useState } from 'react'
import type { Topology } from '../api/types'
import { api, isFixtures } from '../api/client'
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
  const { user } = useCurrentUser()
  const [topology, setTopology] = useState<Topology | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [updatedAt, setUpdatedAt] = useState<number | null>(null)

  const refreshKey = options.refreshKey ?? 0
  const active = options.active !== false

  useEffect(() => {
    let cancelled = false
    let inFlight = false

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
        const t = await api.topology(user.id, pipelineId)
        if (!cancelled) {
          setTopology(t)
          setUpdatedAt(Date.now())
        }
      } catch (e) {
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

    void load()
    if (!active) return

    const timer = window.setInterval(load, TOPOLOGY_REFRESH_MS)
    return () => {
      cancelled = true
      window.clearInterval(timer)
    }
  }, [pipelineId, user.id, refreshKey, active])

  return { topology, error, updatedAt }
}
