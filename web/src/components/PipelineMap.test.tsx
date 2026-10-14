import { afterEach, describe, expect, it, vi } from 'vitest'
import { act, fireEvent, render, screen } from '@testing-library/react'
import type { Topology } from '../api/types'
import { PipelineMap } from './PipelineMap'

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

const five: Topology = {
  pipeline_id: 'orders',
  nodes: [
    { id: 'a', kind: 'source_table', name: 'public.orders', layer: 'postgres', owner: 'priya', health: 'ok' },
    { id: 'b', kind: 'connector', name: 'pg-orders-connector', layer: 'kafka-connect', owner: 'arjun', health: 'fail', facts: { tasks_failed: '1' } },
    { id: 'c', kind: 'topic', name: 'pg.public.orders', layer: 'kafka', owner: 'arjun', health: 'warn' },
    { id: 'd', kind: 'consumer_group', name: 'orders-sync', layer: 'consumer', owner: 'kabir', health: 'warn' },
    { id: 'e', kind: 'sink_table', name: 'analytics.orders', layer: 'warehouse', owner: 'meera', health: 'unknown' },
  ],
  edges: [],
}

describe('PipelineMap — lane', () => {
  it('labels the five kinds Source, Connector, Topic, Consumer and Sink', () => {
    render(<PipelineMap topology={five} />)
    for (const label of ['Source', 'Connector', 'Topic', 'Consumer', 'Sink']) {
      expect(screen.getByText(label)).toBeInTheDocument()
    }
  })

  it('labels a dead-letter topic node DLQ topic', () => {
    const withDLQ: Topology = {
      ...five,
      nodes: [
        ...five.nodes,
        {
          id: 'dlq:orders-dlq',
          kind: 'dlq_topic',
          name: 'orders-dlq',
          layer: 'kafka-connect',
          owner: 'arjun',
          health: 'warn',
          facts: { records: '1204', recent: 'true' },
        },
      ],
    }
    render(<PipelineMap topology={withDLQ} />)
    expect(screen.getByText('DLQ topic')).toBeInTheDocument()
    expect(screen.getByText('orders-dlq')).toBeInTheDocument()
  })

  it('shows health as a word, not only a colour', () => {
    render(<PipelineMap topology={five} />)
    expect(screen.getByText('healthy')).toBeInTheDocument()
    expect(screen.getByText('failed')).toBeInTheDocument()
    expect(screen.getAllByText('degraded')).toHaveLength(2)
    expect(screen.getByText('unknown')).toBeInTheDocument()
  })

  it('gives only the failed node a danger border', () => {
    const { container } = render(<PipelineMap topology={five} />)
    const nodes = container.querySelectorAll<HTMLElement>('[data-health]')
    expect(nodes).toHaveLength(5)
    const danger = [...nodes].filter((n) => n.className.includes('border-destructive'))
    expect(danger).toHaveLength(1)
    expect(danger[0].dataset.health).toBe('fail')
  })

  it('captions the edge after the failed node, with how long', () => {
    render(<PipelineMap topology={five} stalledFor="48m" />)
    expect(screen.getAllByText(/no flow/)).toHaveLength(1)
    expect(screen.getByText('no flow · 48m')).toBeInTheDocument()
  })

  it('has no stalled edge when nothing failed', () => {
    render(<PipelineMap topology={topology} stalledFor="48m" />)
    expect(screen.queryByText(/no flow/)).not.toBeInTheDocument()
  })

  it('scrolls sideways inside its own region and each node keeps a 200px minimum', () => {
    const { container } = render(<PipelineMap topology={five} />)
    const lane = screen.getByRole('group', { name: /Pipeline/ })
    expect(lane.className).toContain('overflow-x-auto')
    expect(container.querySelector('[data-health]')?.className).toContain('min-w-[200px]')
  })

  it('shows skeleton nodes and a loading label before the topology arrives', () => {
    render(<PipelineMap topology={null} pipelineId="orders" />)
    expect(screen.getByText('Loading topology…')).toBeInTheDocument()
  })

  it('offers Trace one record only when given a handler', () => {
    const onTrace = vi.fn()
    const { rerender } = render(<PipelineMap topology={five} onTrace={onTrace} />)
    fireEvent.click(screen.getByRole('button', { name: 'Trace one record' }))
    expect(onTrace).toHaveBeenCalledTimes(1)
    rerender(<PipelineMap topology={five} />)
    expect(screen.queryByRole('button', { name: 'Trace one record' })).not.toBeInTheDocument()
  })
})

describe('PipelineMap — pipeline not configured', () => {
  it('shows the dashed card with a heading and Try again, not the skeleton', () => {
    const retry = vi.fn()
    render(<PipelineMap topology={null} pipelineId="payments" notFound onRetry={retry} />)
    expect(screen.getByRole('heading', { name: 'Pipeline "payments" is not configured' })).toBeInTheDocument()
    expect(screen.getByText(/owners\.yaml/)).toBeInTheDocument()
    expect(screen.queryByText('Loading topology…')).not.toBeInTheDocument()
    fireEvent.click(screen.getByRole('button', { name: 'Try again' }))
    expect(retry).toHaveBeenCalledTimes(1)
  })
})
