import { afterEach, describe, expect, it, vi } from 'vitest'
import { act, render, screen } from '@testing-library/react'
import type { Topology } from '../api/types'
import { PipelineMap } from './PipelineMap'
import fixtureTopology from '../fixtures/topology.json'
import { readFileSync } from 'node:fs'
import { join } from 'node:path'

const topology: Topology = {
  pipeline_id: 'orders',
  nodes: [
    { id: 'pg:x', kind: 'source_table', name: 'x', layer: 'postgres', owner: 'p', health: 'ok' },
  ],
  edges: [],
}

afterEach(() => {
  vi.useRealTimers()
})

describe('PipelineMap — updated Ns ago', () => {
  it('shows "updated Ns ago" and ticks forward as time passes', () => {
    vi.useFakeTimers()
    vi.setSystemTime(new Date('2026-10-11T10:00:00Z'))

    render(
      <PipelineMap
        topology={topology}
        updatedAt={Date.parse('2026-10-11T09:59:55Z')}
        onTrace={() => undefined}
      />,
    )
    expect(screen.getByText('updated 5s ago')).toBeInTheDocument()

    act(() => {
      vi.advanceTimersByTime(5000)
    })
    expect(screen.getByText('updated 10s ago')).toBeInTheDocument()
  })

  it('shows no updated label before the first topology fetch', () => {
    render(<PipelineMap topology={topology} updatedAt={null} onTrace={() => undefined} />)
    expect(screen.queryByText(/updated/)).not.toBeInTheDocument()
  })
})

const orders = fixtureTopology as unknown as Topology

describe('PipelineMap — lane (UI refresh §7.4)', () => {
  it('renders the nodes in edge order with the stalled edge right after the failed node', () => {
    const { container } = render(<PipelineMap topology={orders} />)
    const lane = container.querySelector('.flow__lane') as HTMLElement
    const kids = Array.from(lane.children)
    const names = kids
      .filter((k) => k.classList.contains('node-card'))
      .map((k) => k.querySelector('.node-card__name')?.textContent)
    expect(names).toEqual([
      'public.orders',
      'pg-orders-connector',
      'pg.public.orders',
      'order-sync',
      'analytics.orders',
    ])
    // Four edges between five nodes; only the one after the failed connector is stalled.
    const edges = kids.filter((k) => k.classList.contains('edge'))
    expect(edges).toHaveLength(4)
    expect(edges.map((e) => e.classList.contains('edge--stalled'))).toEqual([false, true, false, false])
    const failed = kids.findIndex((k) => k.classList.contains('node-card--fail'))
    expect(kids[failed + 1]).toHaveClass('edge--stalled')
    expect(kids[failed + 1]).toHaveTextContent('no flow · 48m')
  })

  it('captions a stalled edge just "stalled" when the next node has no newest_record', () => {
    const t: Topology = {
      pipeline_id: 'orders',
      nodes: [
        { id: 'a', kind: 'connector', name: 'a', layer: 'kafka-connect', owner: 'p', health: 'fail' },
        { id: 'b', kind: 'topic', name: 'b', layer: 'kafka', owner: 'p', health: 'ok' },
      ],
      edges: [{ from: 'a', to: 'b' }],
    }
    render(<PipelineMap topology={t} />)
    expect(screen.getByText('stalled')).toBeInTheDocument()
  })

  it('draws a healthy edge as a static flow edge with no caption', () => {
    const t: Topology = {
      pipeline_id: 'orders',
      nodes: [
        { id: 'a', kind: 'source_table', name: 'a', layer: 'postgres', owner: 'p', health: 'warn' },
        { id: 'b', kind: 'connector', name: 'b', layer: 'kafka-connect', owner: 'p', health: 'ok' },
      ],
      edges: [{ from: 'a', to: 'b' }],
    }
    const { container } = render(<PipelineMap topology={t} />)
    expect(container.querySelector('.edge--flow')).toBeInTheDocument()
    expect(container.querySelector('.edge--stalled')).not.toBeInTheDocument()
    expect(container.querySelector('.edge__cap')).not.toBeInTheDocument()
  })

  it('falls back to a plain list in node order when the edges branch', () => {
    const t: Topology = {
      pipeline_id: 'orders',
      nodes: [
        { id: 'a', kind: 'source_table', name: 'a', layer: 'postgres', owner: 'p', health: 'ok' },
        { id: 'b', kind: 'topic', name: 'b', layer: 'kafka', owner: 'p', health: 'ok' },
        { id: 'c', kind: 'topic', name: 'c', layer: 'kafka', owner: 'p', health: 'ok' },
      ],
      edges: [
        { from: 'a', to: 'b' },
        { from: 'a', to: 'c' },
      ],
    }
    const { container } = render(<PipelineMap topology={t} />)
    expect(container.querySelector('.flow__lane--list')).toBeInTheDocument()
    expect(container.querySelectorAll('.edge')).toHaveLength(0)
    expect(container.querySelectorAll('.node-card')).toHaveLength(3)
  })

  it('makes every node reachable by Tab, with a name, kind, layer and health in its label', () => {
    render(<PipelineMap topology={orders} />)
    const connector = screen.getByLabelText('pg-orders-connector, connector, kafka-connect, failed')
    expect(connector).toHaveAttribute('tabindex', '0')
    expect(screen.getByLabelText('public.orders, source table, postgres, degraded')).toHaveAttribute('tabindex', '0')
    expect(screen.getByLabelText('order-sync, consumer group, consumer, healthy')).toHaveAttribute('tabindex', '0')
    connector.focus()
    expect(connector).toHaveFocus()
  })

  it('shows the first two facts, coloured by health on the first one of a flagged node', () => {
    const { container } = render(<PipelineMap topology={orders} />)
    const connector = screen.getByLabelText(/pg-orders-connector/)
    expect(connector.querySelector('dd.is-fail')).toHaveTextContent('1')
    expect(container.querySelector('.node-card--warn dd')).toHaveClass('is-warn')
  })

  it('shows skeleton nodes and the loading text before the topology arrives', () => {
    const { container } = render(<PipelineMap topology={null} />)
    expect(screen.getByText('Loading topology…')).toBeInTheDocument()
    expect(container.querySelectorAll('.node-card--skeleton')).toHaveLength(5)
    expect(container.querySelector('.flow')).toHaveAttribute('aria-busy', 'true')
  })

  it('shows one muted node when the pipeline has no nodes', () => {
    const { container } = render(<PipelineMap topology={{ pipeline_id: 'orders', nodes: [], edges: [] }} />)
    expect(container.querySelector('.node-card--unknown')).toHaveTextContent('No topology for orders')
  })
})

describe('PipelineMap — roles', () => {
  it('shows "Trace one record" only when a trace handler is given (a viewer gets none)', () => {
    const { rerender } = render(<PipelineMap topology={orders} onTrace={() => undefined} />)
    expect(screen.getByRole('button', { name: 'Trace one record' })).toBeInTheDocument()
    rerender(<PipelineMap topology={orders} />)
    expect(screen.queryByRole('button', { name: 'Trace one record' })).not.toBeInTheDocument()
  })
})

describe('flow.css', () => {
  // Read from disk: vitest replaces imported CSS with an empty string.
  const raw = readFileSync(join(process.cwd(), 'src/styles/flow.css'), 'utf8')
  const css = raw.replace(/\/\*[\s\S]*?\*\//g, '')

  it('is not empty', () => {
    expect(css).toContain('.flow__lane')
  })

  it('has no animation, keyframes or transition rules: the lane never moves', () => {
    expect(css).not.toMatch(/animation|@keyframes|transition/)
  })

  it('stacks the lane in a column at phone width', () => {
    const phone = css.slice(css.indexOf('@media (max-width: 600px)'))
    expect(phone).toMatch(/\.flow__lane\s*\{[^}]*flex-direction:\s*column/)
  })
})
