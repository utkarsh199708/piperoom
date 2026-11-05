import { describe, expect, it } from 'vitest'
import type { RoomSnapshot, Topology, TopologyNode } from '@/api/types'
import { pipelineHealth, pipelineKind, pipelineSummaries } from './pipelines'

const room = (id: string, pipeline_id: string, status = 'open') => ({ id, pipeline_id, status }) as RoomSnapshot
const node = (health: TopologyNode['health']) => ({ id: health, health }) as TopologyNode
const topo = (...h: TopologyNode['health'][]): Topology => ({ pipeline_id: 'p', nodes: h.map(node), edges: [] })

describe('pipelineSummaries', () => {
  it('lists each pipeline once with its open rooms, busiest first, then by name', () => {
    const list = pipelineSummaries([
      room('1', 'orders'),
      room('2', 'orders'),
      room('3', 'billing', 'closed'),
      room('4', 'customers'),
      room('5', ''),
    ])
    expect(list).toEqual([
      { id: 'orders', open: 2 },
      { id: 'customers', open: 1 },
      { id: 'billing', open: 0 },
    ])
  })
})

describe('pipelineHealth', () => {
  it('is the worst node health', () => {
    expect(pipelineHealth(topo('ok', 'ok'))).toBe('ok')
    expect(pipelineHealth(topo('ok', 'warn'))).toBe('warn')
    expect(pipelineHealth(topo('warn', 'fail', 'ok'))).toBe('fail')
  })

  it('is unknown without a topology or with unknown nodes', () => {
    expect(pipelineHealth(null)).toBe('unknown')
    expect(pipelineHealth(topo())).toBe('unknown')
    expect(pipelineHealth(topo('ok', 'unknown'))).toBe('unknown')
  })
})

describe('pipelineKind', () => {
  it('reads batch only for an explicit batch kind; anything else is cdc', () => {
    expect(pipelineKind('batch')).toBe('batch')
    expect(pipelineKind('cdc')).toBe('cdc')
    expect(pipelineKind(undefined)).toBe('cdc')
  })
})
