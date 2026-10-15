import { describe, expect, it, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import type { AgentStats } from '../../api/types'
import { emptyInsight } from '../../state/types'

const stats = vi.hoisted(() => ({ current: null as AgentStats | null }))
vi.mock('../../hooks/useAgentStats', () => ({ useAgentStats: () => stats.current }))

import { HypothesisCard } from './HypothesisCard'

const hyp = (confidence: 'low' | 'medium' | 'high' = 'high') => ({
  text: 'Schema change broke the sink',
  based_on_steps: [],
  confidence,
  confirmed: false,
})
const base: AgentStats = {
  pipeline_id: 'orders',
  window_days: 90,
  generated_at: '2026-10-14T10:00:00Z',
  rooms: 23,
  hypothesised: 21,
  labelled: 12,
  graded: 12,
  layer_correct: 11,
  root_cause: { yes: 11, partial: 0, no: 1 },
  by_confidence: [
    { confidence: 'high', runs: 12, correct: 11, accuracy: 0.917 },
    { confidence: 'medium', runs: 4, correct: 2, accuracy: 0.5 },
  ],
  median_time_to_first_hypothesis_s: 48,
  median_human_mttr_s: 720,
  median_cost_usd: 0.42,
  cost_available: true,
  enough_data: true,
  shadow: { rooms: 0, graded: 0, layer_correct: 0 },
}
const show = (s: AgentStats | null, conf: 'low' | 'medium' | 'high' = 'high') => {
  stats.current = s
  return render(<HypothesisCard hypothesis={hyp(conf)} steps={[]} pipelineId="orders" />)
}

describe('HypothesisCard track record', () => {
  it('words the record at this confidence once there is enough data', () => {
    show(base)
    const badge = screen.getByText('right 11 of 12 times at high on orders (90 d)')
    expect(badge).toHaveAttribute('title', 'high 11/12 · medium 2/4 · median first insight 48 s')
  })

  it('says there is no record at a confidence with no runs', () => {
    show(base, 'low')
    expect(screen.getByText('no record at low yet on orders')).toBeInTheDocument()
  })

  it('never shows a rate under the data threshold', () => {
    show({ ...base, graded: 3, enough_data: false })
    expect(screen.getByText('3 labelled rooms on orders · not enough data yet')).toBeInTheDocument()
    expect(screen.queryByText(/right \d/)).toBeNull()
  })

  it('shows nothing extra with no graded rooms, only a hover on the confidence badge', () => {
    show({ ...base, graded: 0, enough_data: false })
    expect(screen.getByText('high confidence')).toHaveAttribute('title', 'no labelled rooms yet')
    expect(screen.queryByText(/labelled rooms on|times at/)).toBeNull()
  })

  it('shows no record when the stats are unavailable (403, 404, network)', () => {
    show(null)
    expect(screen.queryByText(/times at|not enough data|no record/)).toBeNull()
    expect(screen.getByText('high confidence')).not.toHaveAttribute('title')
  })

  it('shows the first-insight line from the insight', () => {
    stats.current = null
    render(
      <HypothesisCard
        hypothesis={hyp()}
        steps={[]}
        insight={{ ...emptyInsight(), time_to_first_hypothesis_s: 48 }}
      />,
    )
    expect(screen.getByText('first hypothesis in 48 s')).toBeInTheDocument()
  })
})
