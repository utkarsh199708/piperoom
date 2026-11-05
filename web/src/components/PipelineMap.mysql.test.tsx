import { afterEach, describe, expect, it } from 'vitest'
import { fireEvent, render, screen, within } from '@testing-library/react'
import type { Topology, TopologyNode } from '../api/types'
import { PipelineMap } from './PipelineMap'
import { layerLabel, layerVariant } from '@/components/ui/badge'
import { toolLayer } from '../lib/toolLayers'

// Q8k: a source_kind: mysql pipeline's map (internal/topology/mysql.go, Q8d). The source node is
// "mysql:<db>.<table>" in layer mysql with the server's binlog facts; the Postgres lane is guarded by the CDC
// snapshot in PipelineMap.cdc.test.tsx, which this row must leave as it is.

afterEach(() => sessionStorage.clear())

function mysqlTopology(source: Partial<TopologyNode> = {}): Topology {
  return {
    pipeline_id: 'shop',
    nodes: [
      {
        id: 'mysql:inventory.orders',
        kind: 'source_table',
        name: 'inventory.orders',
        layer: 'mysql',
        owner: 'priya',
        health: 'ok',
        // Sorted by key, as Go's encoding/json writes a map.
        facts: { binlog_expire: '7d', binlog_files: '12', binlog_oldest: 'mysql-bin.000042', gtid_mode: 'ON' },
        ...source,
      },
      {
        id: 'connect:mysql-orders',
        kind: 'connector',
        name: 'mysql-orders',
        layer: 'kafka-connect',
        owner: 'arjun',
        health: 'ok',
      },
      {
        id: 'topic:shop.inventory.orders',
        kind: 'topic',
        name: 'shop.inventory.orders',
        layer: 'kafka',
        owner: 'arjun',
        health: 'ok',
      },
    ],
    edges: [
      { from: 'mysql:inventory.orders', to: 'connect:mysql-orders' },
      { from: 'connect:mysql-orders', to: 'topic:shop.inventory.orders' },
    ],
  }
}

const sourceNode = () => screen.getAllByRole('button').find((b) => b.hasAttribute('data-node'))!

describe('PipelineMap — MySQL source node', () => {
  it('names the source as MySQL in the compact strip, with the mysql layer edge', () => {
    render(<PipelineMap topology={mysqlTopology()} />)
    const node = sourceNode()
    expect(within(node).getByText('MySQL source')).toBeInTheDocument()
    expect(within(node).getByText('inventory.orders')).toBeInTheDocument()
    expect(node.className).toContain('border-l-layer-mysql')
    expect(node.className).not.toContain('border-l-layer-postgres')
    expect(node.getAttribute('title')).toMatch(/^MySQL\n/)
    // The tooltip carries the binlog facts, oldest binlog first.
    expect(node.getAttribute('title')).toContain('oldest binlog mysql-bin.000042\nbinlog files 12\nbinlogs kept 7d\nGTID mode ON')
  })

  it('shows the MySQL badge and leads with the oldest binlog when expanded', () => {
    render(<PipelineMap topology={mysqlTopology()} />)
    fireEvent.click(screen.getByRole('button', { name: 'Expand' }))
    const node = sourceNode()
    const badge = within(node).getByText('MySQL')
    expect(badge.className).toContain('text-layer-mysql')
    expect(badge.className).not.toContain('text-layer-postgres')
    // The lead fact sits next to the health word; the rest follow in MYSQL_FACT_ORDER.
    expect(within(node).getByTitle('binlog_oldest')).toHaveTextContent('oldest binlog mysql-bin.000042')
    const rest = within(node)
      .getAllByRole('definition')
      .map((d) => d.textContent)
    expect(rest).toEqual(['12', '7d', 'ON'])
  })

  it('draws "binlog: unknown" as an unknown node', () => {
    render(<PipelineMap topology={mysqlTopology({ health: 'unknown', facts: { binlog: 'unknown' } })} />)
    fireEvent.click(screen.getByRole('button', { name: 'Expand' }))
    const node = sourceNode()
    expect(node).toHaveAttribute('data-health', 'unknown')
    expect(node.className).toContain('border-dashed')
    expect(within(node).getByTitle('binlog')).toHaveTextContent('binlog unknown')
  })

  it('draws "error: unreachable" on the source', () => {
    render(<PipelineMap topology={mysqlTopology({ health: 'unknown', facts: { error: 'unreachable' } })} />)
    fireEvent.click(screen.getByRole('button', { name: 'Expand' }))
    expect(within(sourceNode()).getByTitle('error')).toHaveTextContent('error unreachable')
  })

  it('selects the mysql layer when the source is clicked', () => {
    const picked: (string | null)[] = []
    render(<PipelineMap topology={mysqlTopology()} onSelectLayer={(l) => picked.push(l)} />)
    fireEvent.click(sourceNode())
    expect(picked).toEqual(['mysql'])
  })
})

describe('PipelineMap — unknown layer fallback', () => {
  it('draws a node of a layer the web does not know with the neutral edge, badge and its raw id', () => {
    render(<PipelineMap topology={mysqlTopology({ id: 'oracle:HR.EMP', name: 'HR.EMP', layer: 'oracle', facts: {} })} />)
    let node = sourceNode()
    // Only a mysql-layer source says "MySQL source"; any other source stays "Source".
    expect(within(node).getByText('Source')).toBeInTheDocument()
    expect(node.className).toContain('border-l-input')
    expect(node.getAttribute('title')).toMatch(/^oracle\n/)
    fireEvent.click(screen.getByRole('button', { name: 'Expand' }))
    node = sourceNode()
    const badge = within(node).getByText('oracle')
    expect(badge.className).toContain('bg-muted')
    expect(badge.className).not.toMatch(/layer-/)
  })
})

describe('layer helpers', () => {
  it.each([
    ['mysql', 'MySQL', 'layer-mysql'],
    ['postgres', 'postgres', 'layer-postgres'],
    ['kafka-connect', 'kafka-connect', 'layer-kafka-connect'],
    ['oracle', 'oracle', 'neutral'],
    ['', '', 'neutral'],
    // An inherited property name is not a layer.
    ['toString', 'toString', 'neutral'],
  ])('%s → label %s, variant %s', (layer, label, variant) => {
    expect(layerLabel(layer)).toBe(label)
    expect(layerVariant(layer)).toBe(variant)
  })

  it('puts mysql_ tools in the mysql layer', () => {
    expect(toolLayer('mysql_binlog_status')).toBe('mysql')
    expect(toolLayer('pg_replication_slots')).toBe('postgres')
  })
})
