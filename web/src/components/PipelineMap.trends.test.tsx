import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen } from '@testing-library/react'
import type { ForecastSummary, Topology, TrendPoint } from '../api/types'
import { PipelineMap } from './PipelineMap'

const NOW = Date.parse('2026-10-22T10:00:00Z')

function series(n: number): TrendPoint[] {
  return Array.from({ length: n }, (_, i) => ({
    at: new Date(NOW - (n - 1 - i) * 15 * 60_000).toISOString(),
    value: 1000 + i,
  }))
}

// The lane as GET /api/pipelines/{pid}/topology answers it for a CDC pipeline.
const topology: Topology = {
  pipeline_id: 'orders',
  nodes: [
    { id: 'pg:public.orders', kind: 'source_table', name: 'public.orders', layer: 'postgres', owner: 'priya', health: 'warn' },
    { id: 'connect:pg-orders', kind: 'connector', name: 'pg-orders', layer: 'kafka-connect', owner: 'arjun', health: 'ok' },
    { id: 'topic:pg.public.orders', kind: 'topic', name: 'pg.public.orders', layer: 'kafka', owner: 'arjun', health: 'ok' },
    { id: 'group:order-sync', kind: 'consumer_group', name: 'order-sync', layer: 'consumer', owner: 'kabir', health: 'ok' },
    { id: 'wh:analytics.orders', kind: 'sink_table', name: 'analytics.orders', layer: 'warehouse', owner: 'meera', health: 'ok' },
  ],
  edges: [],
}

const forecast: ForecastSummary = {
  metric: 'topic_oldest_unread_age',
  object: 'pg.public.orders',
  current: 500_000,
  limit: 604_800,
  eta: '2026-10-23T04:00:00Z',
  slope_per_hour: 3600,
  r2: 0.9,
}

const trends = { slot_wal_bytes: series(96), group_lag: series(96), dlq_records: [] }

beforeEach(() => {
  vi.useFakeTimers()
  vi.setSystemTime(NOW)
})
afterEach(() => {
  vi.useRealTimers()
  sessionStorage.clear()
})

const nodeOf = (name: string) => screen.getAllByText(name)[0].closest('[data-node]') as HTMLElement

describe('PipelineMap — X3 sparklines and ETA badges', () => {
  it('draws the 24 h line on the nodes that have history, in the compact lane', () => {
    render(<PipelineMap topology={topology} trends={trends} />)
    expect(nodeOf('public.orders').querySelector('[data-sparkline]')).not.toBeNull()
    expect(nodeOf('order-sync').querySelector('[data-sparkline]')).not.toBeNull()
    expect(nodeOf('pg-orders').querySelector('[data-sparkline]')).toBeNull()
    expect(nodeOf('analytics.orders').querySelector('[data-sparkline]')).toBeNull()
    expect(screen.getByRole('img', { name: 'WAL retained by the slot, last 24 h, now 1.1 KiB' })).toBeInTheDocument()
    expect(nodeOf('public.orders').querySelector('path')?.getAttribute('d')?.match(/[ML]/g)).toHaveLength(96)
  })

  it('puts the forecast badge on its node, compact and expanded, with a text alternative', () => {
    render(<PipelineMap topology={topology} trends={trends} forecast={forecast} />)
    let topic = nodeOf('pg.public.orders')
    expect(topic.querySelector('[data-eta]')).toHaveTextContent('data loss in 18 h')
    expect(topic).toHaveAccessibleName(expect.stringContaining('Oldest unread record (pg.public.orders): unread records are deleted in about 18 h'))

    fireEvent.click(screen.getByRole('button', { name: 'Expand' }))
    topic = nodeOf('pg.public.orders')
    expect(topic.querySelector('[data-trend] [data-eta]')).toHaveTextContent('data loss in 18 h')
    expect(nodeOf('public.orders').querySelector('[data-trend] [data-sparkline]')).not.toBeNull()
  })

  it('adds no focus stop: the lane has the same buttons with or without trends (E4)', () => {
    const { unmount } = render(<PipelineMap topology={topology} />)
    const before = screen.getAllByRole('button').length
    unmount()
    render(<PipelineMap topology={topology} trends={trends} forecast={forecast} />)
    expect(screen.getAllByRole('button')).toHaveLength(before)
    expect(document.querySelectorAll('svg[tabindex], [data-eta][tabindex]')).toHaveLength(0)
  })

  it('draws nothing new without trends, with empty series, or while loading (E2)', () => {
    const { container, rerender } = render(<PipelineMap topology={topology} />)
    expect(container.querySelector('[data-sparkline], [data-eta], [data-trend]')).toBeNull()
    rerender(<PipelineMap topology={topology} trends={{ slot_wal_bytes: [], group_lag: [] }} />)
    expect(container.querySelector('[data-sparkline], [data-eta], [data-trend]')).toBeNull()
    rerender(<PipelineMap topology={null} trends={trends} forecast={forecast} />)
    expect(container.querySelector('[data-sparkline], [data-eta]')).toBeNull()
  })

  it('shows no badge for a forecast more than a week out', () => {
    render(<PipelineMap topology={topology} forecast={{ ...forecast, eta: '2026-11-30T00:00:00Z' }} />)
    expect(document.querySelector('[data-eta]')).toBeNull()
  })
})
