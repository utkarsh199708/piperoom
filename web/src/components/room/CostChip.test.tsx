import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import { CostChip } from './CostChip'
import { emptyInsight, emptyRoomState } from '../../state/types'
import type { RoomState } from '../../state/types'

const room = (over: Partial<RoomState> = {}): RoomState => {
  const i = emptyInsight()
  return {
    ...emptyRoomState('r'),
    opened_at: '2026-10-14T10:00:00Z',
    insight: {
      ...i,
      steps: 11,
      time_to_first_hypothesis_s: 48,
      usage: { ...i.usage, calls: 9, tokens: 812_200, cost_usd: 0.42, cache_read_tokens: 41_000, models: { 'claude-opus-5-5': 9 } },
    },
    ...over,
  }
}

beforeEach(() => {
  vi.useFakeTimers()
  vi.setSystemTime(new Date('2026-10-14T10:02:10Z'))
})
afterEach(() => vi.useRealTimers())

describe('CostChip', () => {
  it('shows cost, steps and a running duration while open', () => {
    render(<CostChip state={room()} />)
    expect(screen.getByTestId('cost-chip')).toHaveTextContent('$0.42 · 11 steps · 2m 10s')
  })

  it('freezes the duration at closed_at', () => {
    render(<CostChip state={room({ status: 'closed', closed_at: '2026-10-14T10:01:00Z' })} />)
    expect(screen.getByTestId('cost-chip')).toHaveTextContent('$0.42 · 11 steps · 1m 0s')
  })

  it('describes itself with the tooltip text and is not a live region', () => {
    render(<CostChip state={room()} />)
    const chip = screen.getByTestId('cost-chip')
    const desc = document.getElementById(chip.getAttribute('aria-describedby') as string)
    expect(desc).toHaveTextContent('9 model calls · 812,200 tokens (41k cache reads) · claude-opus-5-5 ×9 · first hypothesis in 48 s')
    expect(chip.closest('[aria-live]')).toBeNull()
  })

  it('is hidden in a shadow room', () => {
    render(<CostChip state={room({ shadow: true })} />)
    expect(screen.queryByTestId('cost-chip')).toBeNull()
  })
})
