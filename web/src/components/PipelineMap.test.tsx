import { afterEach, describe, expect, it, vi } from 'vitest'
import { act, render, screen } from '@testing-library/react'
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
