import type { RoomSnapshot, Topology } from '@/api/types'
import { isProvisionalSetup } from '@/lib/setup'

export interface PipelineSummary {
  id: string
  /** Rooms still open on this pipeline. */
  open: number
  /** Every open room is a setup room: the pipeline is not registered, so it has no topology (KAN-601). */
  provisional?: boolean
}

/**
 * The server has no "list pipelines" call, so the sidebar lists the pipelines that
 * rooms mention. Most open rooms first, then by name; at most 10 (each row costs
 * one topology call).
 */
export const MAX_SIDEBAR_PIPELINES = 10

export function pipelineSummaries(rooms: RoomSnapshot[], limit = MAX_SIDEBAR_PIPELINES): PipelineSummary[] {
  const counts = new Map<string, number>()
  const normalOpen = new Map<string, number>()
  for (const r of rooms) {
    if (!r.pipeline_id) continue
    const open = r.status === 'open'
    counts.set(r.pipeline_id, (counts.get(r.pipeline_id) ?? 0) + (open ? 1 : 0))
    // A setup room does not vouch for its pipeline; a normal open room does (the pipeline is registered).
    if (open && !isProvisionalSetup(r.setup)) normalOpen.set(r.pipeline_id, (normalOpen.get(r.pipeline_id) ?? 0) + 1)
  }
  return [...counts.entries()]
    .map(([id, open]) => (open > 0 && !normalOpen.has(id) ? { id, open, provisional: true } : { id, open }))
    .sort((a, b) => b.open - a.open || a.id.localeCompare(b.id))
    .slice(0, limit)
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

/** The pipeline kind the UI shows: the server's `kind`, or `cdc` when an older server omits it. */
export function pipelineKind(kind: 'cdc' | 'batch' | undefined): 'cdc' | 'batch' {
  return kind === 'batch' ? 'batch' : 'cdc'
}
