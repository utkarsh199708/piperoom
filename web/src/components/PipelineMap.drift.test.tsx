import { describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen } from '@testing-library/react'
import type { ForecastSummary, Topology, TrendPoint } from '../api/types'
import cdc from '../fixtures/topology.json'
import evidence from '../fixtures/watcher-drift.json'
import { driftFacts } from '../lib/drift'
import { PipelineMap } from './PipelineMap'

const topology = cdc as unknown as Topology
const W = { from: '2026-10-21T10:00:00Z', to: '2026-10-21T11:00:00Z' }

function node(name: string): HTMLElement {
  const el = screen.getByTitle(name).closest('[data-node]')
  if (!(el instanceof HTMLElement)) throw new Error(`no node ${name}`)
  return el
}

function expand() {
  fireEvent.click(screen.getByRole('button', { name: 'Expand' }))
}

describe('PipelineMap drift facts (X1)', () => {
  it('shows count_drift with its percentage and window on the sink node, dup_keys on the topic node', () => {
    render(<PipelineMap topology={topology} drift={driftFacts(evidence, 'watcher')} />)
    const sink = node('analytics.orders')
    expect(sink).toHaveTextContent('drift 2.1 % rows missing over 1 h')
    expect(sink.title).toContain('10:00–11:00 UTC')
    // The fact is part of the node's accessible name, next to the health word.
    expect(screen.getByRole('button', { name: /analytics\.orders/ })).toHaveAccessibleName(/2\.1 % rows missing over 1 h/)
    expect(node('pg.public.orders')).toHaveTextContent('drift 3 duplicate events on 2 keys over 1 h')
    // dlq_growth shares the evidence list but is not drift; other nodes carry nothing.
    for (const name of ['public.orders', 'pg-orders-connector', 'order-sync']) {
      expect(node(name).querySelector('[data-drift]')).toBeNull()
    }

    expand()
    expect(node('analytics.orders')).toHaveTextContent('2.1 % rows missing over 1 h')
    expect(node('pg.public.orders')).toHaveTextContent('scan capped at 50000 records')
  })

  it('renders a hostile detail and table name as text, never HTML', () => {
    const hostile = '<img src=x onerror="alert(1)"><script>alert(2)</script>'
    const facts = driftFacts(
      { signals: [{ signal: 'count_drift', severity: 'sev2', table: '', window: W, source: 10, sink: 9, missing: 1, mismatched: 0, sample: 0, detail: hostile + 'x'.repeat(10_000) }] },
      'watcher',
    )
    const { container } = render(<PipelineMap topology={topology} drift={facts} />)
    expand()
    expect(container.querySelector('img')).toBeNull()
    expect(container.querySelector('script')).toBeNull()
    expect(node('analytics.orders')).toHaveTextContent('<img src=x onerror="alert(1)">')
  })

  it('shows an unknown or missing kind as a neutral fact and never throws', () => {
    const facts = driftFacts(
      {
        signals: [
          { signal: 'null_drift', severity: 'sev3', table: 'public.orders', window: W },
          { severity: 'sev3', topic: 'pg.public.orders', window: { from: 'bad', to: null } },
        ],
      },
      'watcher',
    )
    render(<PipelineMap topology={topology} drift={facts} />)
    const sink = node('analytics.orders')
    expect(sink).toHaveTextContent('drift null drift over 1 h')
    expect(sink.querySelector('[data-drift="unknown"]')).toHaveClass('text-muted-foreground')
    expect(node('pg.public.orders')).toHaveTextContent(/drift drift\b/)
  })

  it('counts the facts past the first on a compact node and past two on a card', () => {
    const many = driftFacts(
      { signals: [1, 2, 3].map((n) => ({ signal: 'dup_keys', severity: 'sev3', topic: 'pg.public.orders', window: W, source: 10, sink: 0, missing: n, mismatched: 0, sample: 1 })) },
      'watcher',
    )
    render(<PipelineMap topology={topology} drift={many} />)
    expect(node('pg.public.orders')).toHaveTextContent('+2')
    expand()
    expect(node('pg.public.orders')).toHaveTextContent('+1 more drift')
  })

  it('draws nothing extra without drift facts, while loading, or for a fact with no node', () => {
    const { container, rerender } = render(<PipelineMap topology={null} drift={driftFacts(evidence, 'watcher')} />)
    expect(container.querySelector('[data-drift]')).toBeNull()
    rerender(<PipelineMap topology={topology} drift={[]} />)
    expect(container.querySelector('[data-drift]')).toBeNull()
    const elsewhere = driftFacts({ signals: [{ signal: 'dup_keys', topic: 'other.topic', window: W }] }, 'watcher')
    rerender(<PipelineMap topology={topology} drift={elsewhere} />)
    expect(container.querySelector('[data-drift]')).toBeNull()
  })

  it('draws drift next to the X3 sparkline and ETA badge on the same node, compact and expanded', () => {
    const now = Date.parse('2026-10-22T10:00:00Z')
    const points: TrendPoint[] = Array.from({ length: 8 }, (_, i) => ({
      at: new Date(now - (7 - i) * 15 * 60_000).toISOString(),
      value: 100 + i,
    }))
    const forecast: ForecastSummary = {
      metric: 'topic_oldest_unread_age',
      object: 'pg.public.orders',
      current: 500_000,
      limit: 604_800,
      eta: new Date(now + 18 * 3_600_000 + 60_000).toISOString(),
      slope_per_hour: 3600,
      r2: 0.9,
    }
    vi.useFakeTimers()
    vi.setSystemTime(now)
    try {
      render(
        <PipelineMap
          topology={topology}
          drift={driftFacts(evidence, 'watcher')}
          trends={{ topic_oldest_unread_age: points, group_lag: points }}
          forecast={forecast}
        />,
      )
      const check = () => {
        const topic = node('pg.public.orders')
        expect(topic.querySelector('[data-drift]')).not.toBeNull()
        expect(topic.querySelector('[data-sparkline]')).not.toBeNull()
        expect(topic.querySelector('[data-eta]')).toHaveTextContent('data loss in 18 h')
        expect(node('order-sync').querySelector('[data-sparkline]')).not.toBeNull()
        expect(node('analytics.orders').querySelector('[data-drift]')).not.toBeNull()
      }
      check()
      expand()
      check()
    } finally {
      vi.useRealTimers()
    }
  })
})
