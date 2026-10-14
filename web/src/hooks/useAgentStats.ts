import { useCallback } from 'react'
import { api, isFixtures } from '../api/client'
import type { AgentStats } from '../api/types'
import { useLoad } from './useLoad'

/**
 * The agent's track record on a pipeline (GET /api/pipelines/{pid}/agent-stats), fetched once per
 * mount. Any failure (403, 404, network) and fixture mode give `null`: the record is simply not shown.
 */
export function useAgentStats(pipelineId: string): AgentStats | null {
  const load = useCallback(
    (): Promise<AgentStats | null> =>
      !pipelineId || isFixtures() ? Promise.resolve(null) : api.agentStats(pipelineId).catch(() => null),
    [pipelineId],
  )
  const state = useLoad(load)
  return state.status === 'ready' ? state.data : null
}
