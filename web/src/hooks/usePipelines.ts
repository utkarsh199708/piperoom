import { api, isFixtures } from '../api/client'
import type { PipelineInfo } from '../api/types'
import { useLoad } from './useLoad'
import type { LoadState } from './useLoad'

// Fixture mode has no server; its story is about this one pipeline.
const fixturePipelines = () => Promise.resolve<PipelineInfo[]>([{ id: 'orders', title: 'orders', shadow: false }])
const livePipelines = () => api.pipelines().then((p) => (Array.isArray(p) ? p : []))

/** The pipelines this server knows (GET /api/pipelines). `[]` is a real answer: nothing is made up. */
export function usePipelines(): LoadState<PipelineInfo[]> & { reload: () => void } {
  return useLoad(isFixtures() ? fixturePipelines : livePipelines)
}
