import type { RoomSnapshot, Topology } from '@/api/types'

export interface PipelineSummary {
  id: string
  /** Rooms still open on this pipeline. */
  open: number
}

/**
 * The server has no "list pipelines" call, so the sidebar lists the pipelines that
 * rooms mention. Most open rooms first, then by name.
 */
export function pipelineSummaries(rooms: RoomSnapshot[]): PipelineSummary[] {
  const counts = new Map<string, number>()
  for (const r of rooms) {
    if (!r.pipeline_id) continue
    counts.set(r.pipeline_id, (counts.get(r.pipeline_id) ?? 0) + (r.status === 'open' ? 1 : 0))
  }
  return [...counts.entries()]
    .map(([id, open]) => ({ id, open }))
    .sort((a, b) => b.open - a.open || a.id.localeCompare(b.id))
}

export type Health = 'ok' | 'warn' | 'fail' | 'unknown'

/** The worst node health in a pipeline: fail beats warn beats ok. No topology (yet) is unknown. */
export function pipelineHealth(topology: Topology | null): Health {
  if (!topology || topology.nodes.length === 0) return 'unknown'
  let worst: Health = 'ok'
  for (const n of topology.nodes) {
    if (n.health === 'fail') return 'fail'
    if (n.health === 'warn') worst = 'warn'
    else if (n.health === 'unknown' && worst === 'ok') worst = 'unknown'
  }
  return worst
}

export const HEALTH_LABEL: Record<Health, string> = {
  ok: 'healthy',
  warn: 'degraded',
  fail: 'failing',
  unknown: 'health unknown',
}
