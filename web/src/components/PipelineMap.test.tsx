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
  sessionStorage.clear()
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

  it('scrolls sideways inside its own region and each compact node keeps a 168px minimum', () => {
    const { container } = render(<PipelineMap topology={five} />)
    const lane = screen.getByRole('group', { name: /Pipeline/ })
    expect(lane.className).toContain('overflow-x-auto')
    expect(container.querySelector('[data-health]')?.className).toContain('min-w-[168px]')
  })

  it('shows skeleton nodes and a loading label before the topology arrives', () => {
    render(<PipelineMap topology={null} pipelineId="orders" />)
    expect(screen.getByText('Loading topology…')).toBeInTheDocument()
  })

  it('offers Trace a record only when given a handler', () => {
    const onTrace = vi.fn()
    const { rerender } = render(<PipelineMap topology={five} onTrace={onTrace} />)
    fireEvent.click(screen.getByRole('button', { name: 'Trace a record' }))
    expect(onTrace).toHaveBeenCalledTimes(1)
    rerender(<PipelineMap topology={five} />)
    expect(screen.queryByRole('button', { name: 'Trace a record' })).not.toBeInTheDocument()
  })
})

describe('PipelineMap — compact lane, worst node, filter', () => {
  it('is compact by default, expands from the heading and remembers the choice', () => {
    const { container, unmount } = render(<PipelineMap topology={five} />)
    expect(container.querySelector('[data-health]')?.className).toContain('h-14')
    const btn = screen.getByRole('button', { name: 'Expand' })
    expect(btn).toHaveAttribute('aria-pressed', 'false')
    fireEvent.click(btn)
    expect(container.querySelector('[data-health]')?.className).toContain('min-h-[92px]')
    expect(sessionStorage.getItem('piperoom.lane.expanded.orders')).toBe('1')
    unmount()
    const again = render(<PipelineMap topology={five} />)
    expect(again.container.querySelector('[data-health]')?.className).toContain('min-h-[92px]')
    expect(screen.getByRole('button', { name: 'Compact' })).toHaveAttribute('aria-pressed', 'true')
  })

  it('marks the failed node as the worst one', () => {
    const { container } = render(<PipelineMap topology={five} />)
    const worst = container.querySelectorAll('[data-worst]')
    expect(worst).toHaveLength(1)
    expect(worst[0].getAttribute('data-health')).toBe('fail')
  })

  it('hides an empty DLQ node behind a chip and shows it once records are parked', () => {
    const dlq = (records: string): Topology => ({
      ...five,
      nodes: [...five.nodes, { id: 'dlq', kind: 'dlq_topic', name: 'orders-dlq', layer: 'kafka-connect', owner: 'a', health: 'ok', facts: { records } }],
    })
    const { rerender } = render(<PipelineMap topology={dlq('0')} />)
    expect(screen.queryByText('orders-dlq')).not.toBeInTheDocument()
    expect(screen.getByText('DLQ empty')).toBeInTheDocument()
    rerender(<PipelineMap topology={dlq('3')} />)
    expect(screen.getByText('orders-dlq')).toBeInTheDocument()
    expect(screen.queryByText('DLQ empty')).not.toBeInTheDocument()
  })

  it('clicking a node selects its layer; clicking it again clears', () => {
    const onSelectLayer = vi.fn()
    const { rerender } = render(<PipelineMap topology={five} onSelectLayer={onSelectLayer} />)
    const node = screen.getByText('pg-orders-connector').closest('button')!
    expect(node).toHaveAttribute('aria-pressed', 'false')
    fireEvent.click(node)
    expect(onSelectLayer).toHaveBeenLastCalledWith('kafka-connect')
    rerender(<PipelineMap topology={five} onSelectLayer={onSelectLayer} selectedLayer="kafka-connect" />)
    expect(screen.getByText('pg-orders-connector').closest('button')).toHaveAttribute('aria-pressed', 'true')
    fireEvent.click(screen.getByText('pg-orders-connector').closest('button')!)
    expect(onSelectLayer).toHaveBeenLastCalledWith(null)
  })
})

describe('PipelineMap — overflow pill', () => {
  it('shows "+N →" with the count of clipped nodes when the lane overflows', () => {
    class RO {
      observe() {}
      disconnect() {}
      unobserve() {}
    }
    vi.stubGlobal('ResizeObserver', RO)
    const define = (prop: string, get: () => number) =>
      Object.defineProperty(HTMLElement.prototype, prop, { configurable: true, get })
    define('clientWidth', () => 400)
    define('scrollWidth', () => 1000)
    define('offsetWidth', () => 168)
    define('offsetLeft', function (this: HTMLElement) {
      const i = [...(this.parentElement?.parentElement?.querySelectorAll('[data-node]') ?? [])].indexOf(this)
      return i * 200
    } as unknown as () => number)
    try {
      render(<PipelineMap topology={five} />)
      expect(screen.getByRole('button', { name: 'Show 3 more nodes' })).toHaveTextContent('+3 →')
    } finally {
      for (const p of ['clientWidth', 'scrollWidth', 'offsetWidth', 'offsetLeft']) {
        delete (HTMLElement.prototype as unknown as Record<string, unknown>)[p]
      }
      vi.unstubAllGlobals()
    }
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

describe('PipelineMap — heal flash', () => {
  const node = (health: 'fail' | 'ok') => ({ ...topology.nodes[0], health })
  it('flashes a node that goes from fail to ok, and not one that was already ok', () => {
    const { rerender, container } = render(<PipelineMap topology={{ ...topology, nodes: [node('fail')] }} />)
    expect(container.querySelector('.motion-safe\\:animate-heal')).toBeNull()
    rerender(<PipelineMap topology={{ ...topology, nodes: [node('ok')] }} />)
    expect(container.querySelector('[data-node]')?.className).toContain('motion-safe:animate-heal')
  })
  it('does not flash on first render with an ok node', () => {
    const { container } = render(<PipelineMap topology={{ ...topology, nodes: [node('ok')] }} />)
    expect(container.querySelector('[data-node]')?.className).not.toContain('animate-heal')
  })
})

// Q3 S9: cards size to their content and are never squeezed under their neighbours (the lane scrolls instead).
describe('PipelineMap — cards do not overlap', () => {
  it('keeps each node at its content width and lets the lane scroll', () => {
    render(<PipelineMap topology={topology} updatedAt={null} onTrace={() => undefined} />)
    const node = document.querySelector('[data-node]') as HTMLElement
    expect(node.className).toMatch(/shrink-0/)
    expect(node.className).not.toMatch(/basis-0/)
    expect(node.className).toMatch(/overflow-hidden/)
    expect(screen.getByRole('group', { name: /Pipeline, source to sink/ }).className).toMatch(/overflow-x-auto/)
  })
})
