import type { TopologyNode } from '../api/types'

const RANK: Record<TopologyNode['health'], number> = { fail: 3, warn: 2, unknown: 1, ok: 0 }

/** A dead-letter node only earns a place in the lane when records are parked or it is unhealthy. */
export function dlqVisible(node: TopologyNode): boolean {
  if (node.kind !== 'dlq_topic') return true
  if (node.health !== 'ok') return true
  const records = Number.parseInt(node.facts?.records ?? '', 10)
  return Number.isFinite(records) && records > 0
}

/** The nodes the lane draws, in lane order (hidden DLQ nodes dropped). */
export function visibleNodes(nodes: TopologyNode[]): TopologyNode[] {
  return nodes.filter(dlqVisible)
}

/** True when the topology has a DLQ node that the lane is hiding as empty. */
export function hasEmptyDlq(nodes: TopologyNode[]): boolean {
  return nodes.some((n) => !dlqVisible(n))
}

/** The node the lane should bring into view: fail > warn > unknown > ok, first in lane order on a tie. */
export function worstNode(nodes: TopologyNode[]): TopologyNode | null {
  let best: TopologyNode | null = null
  for (const n of nodes) {
    if (!best || (RANK[n.health] ?? 0) > (RANK[best.health] ?? 0)) best = n
  }
  return best
}

export function laneStorageKey(pipelineId: string): string {
  return `piperoom.lane.expanded.${pipelineId}`
}
