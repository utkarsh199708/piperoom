import { useEffect, useState } from 'react'
import type { Topology } from '../api/types'
import { api, isFixtures } from '../api/client'
import { fixtureTopology } from '../fixtures/player'
import { useCurrentUser } from '../context/user'

// Loads the pipeline topology, refreshing every 30 s (docs/api.md).
export function useTopology(pipelineId: string) {
  const { user } = useCurrentUser()
  const [topology, setTopology] = useState<Topology | null>(null)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    let cancelled = false

    const load = async () => {
      try {
        if (isFixtures()) {
          setTopology(pipelineId === fixtureTopology.pipeline_id ? fixtureTopology : null)
          return
        }
        const t = await api.topology(user.id, pipelineId)
        if (!cancelled) setTopology(t)
      } catch (e) {
        if (!cancelled) setError(e instanceof Error ? e.message : String(e))
      }
    }

    void load()
    const timer = window.setInterval(load, 30000)
    return () => {
      cancelled = true
      window.clearInterval(timer)
    }
  }, [pipelineId, user.id])

  return { topology, error }
}
