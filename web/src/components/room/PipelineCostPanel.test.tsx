import { describe, expect, it, vi } from 'vitest'
import { render, screen, within } from '@testing-library/react'
import type { AgentStats, AgentStatsInfra } from '../../api/types'

const stats = vi.hoisted(() => ({ current: null as AgentStats | null }))
vi.mock('../../hooks/useAgentStats', () => ({ useAgentStats: () => stats.current }))

import { PipelineCostPanel } from './PipelineCostPanel'

const base: AgentStats = {
  pipeline_id: 'orders',
  window_days: 90,
  generated_at: '2026-10-22T10:00:00Z',
  rooms: 23,
  hypothesised: 21,
  labelled: 12,
  graded: 12,
  layer_correct: 11,
  root_cause: { yes: 11, partial: 0, no: 1 },
  by_confidence: [],
  median_time_to_first_hypothesis_s: 48,
  median_human_mttr_s: 720,
  median_cost_usd: 0.42,
  cost_available: true,
  enough_data: true,
  shadow: { rooms: 0, graded: 0, layer_correct: 0 },
}
const infra: AgentStatsInfra = {
  from: '2026-10-15T10:00:00Z',
  to: '2026-10-22T10:00:00Z',
  samples: 672,
  wal_retained_gb_days: 30,
  dlq_records: 1200,
  oldest_unread_age_s: 5400,
}
const show = (s: AgentStats | null) => {
  stats.current = s
  return render(<PipelineCostPanel pipelineId="orders" />)
}

describe('PipelineCostPanel', () => {
  it('shows the model cost next to the infrastructure hints (TC4)', () => {
    show({ ...base, infra })
    const panel = screen.getByRole('region', { name: 'Cost on orders' })
    const model = within(panel).getByRole('group', { name: 'Model' })
    expect(model).toHaveTextContent('median $0.42 per room · 23 closed rooms · 90 d')
    const hints = within(panel).getByRole('group', { name: 'Infrastructure · last 7 d' })
    expect(within(hints).getByText('WAL retained').nextElementSibling).toHaveTextContent('30 GB-days')
    expect(within(hints).getByText('Dead-letter queue').nextElementSibling).toHaveTextContent('1,200 records')
    expect(within(hints).getByText('Oldest unread').nextElementSibling).toHaveTextContent('1 h 30 min')
    expect(hints).toHaveTextContent('672 samples · units only, priced by your platform')
    // Infrastructure is never priced: the only currency is the model's median.
    expect(hints.textContent).not.toMatch(/\$/)
  })

  it('says "not enough history" when infra is null, never zeros (TC2)', () => {
    show({ ...base, infra: null })
    const hints = screen.getByRole('group', { name: 'Infrastructure · last 7 d' })
    expect(within(hints).getByRole('status')).toHaveTextContent('not enough history')
    expect(within(hints).queryByText(/GB-days|records/)).toBeNull()
    // The model section still shows.
    expect(screen.getByTestId('pipeline-cost-model')).toHaveTextContent('median $0.42')
  })

  it('treats an older server without infra as no history', () => {
    show(base)
    expect(screen.getByText('not enough history')).toBeInTheDocument()
  })

  it('marks a metric with no sample instead of showing 0', () => {
    show({ ...base, infra: { ...infra, samples: 1, wal_retained_gb_days: null, dlq_records: null } })
    const hints = screen.getByRole('group', { name: 'Infrastructure · last 7 d' })
    expect(within(hints).getByText('WAL retained').nextElementSibling).toHaveTextContent('no sample')
    expect(within(hints).getByText('Dead-letter queue').nextElementSibling).toHaveTextContent('no sample')
    expect(hints).toHaveTextContent('1 sample ·')
  })

  it('never shows an unpriced record as $0', () => {
    show({ ...base, cost_available: false, median_cost_usd: -1, infra })
    expect(screen.getByTestId('pipeline-cost-model')).toHaveTextContent('23 closed rooms · not priced')
    expect(screen.getByTestId('pipeline-cost-model').textContent).not.toMatch(/\$/)
  })

  it('renders nothing while the stats are unavailable', () => {
    const { container } = show(null)
    expect(container).toBeEmptyDOMElement()
  })
})
