import { describe, expect, it } from 'vitest'
import { render, screen } from '@testing-library/react'
import { CostBadge } from './CostBadge'
import { emptyInsight, emptyRoomState } from '../../state/types'
import type { RoomState } from '../../state/types'

type Usage = RoomState['insight']['usage']

// A room whose folded usage is `usage`, as the reducer leaves it after a full replay.
const room = (usage: Partial<Usage>, over: Partial<RoomState> = {}): RoomState => {
  const i = emptyInsight()
  return {
    ...emptyRoomState('r'),
    opened_at: '2026-10-14T10:00:00Z',
    insight: { ...i, usage: { ...i.usage, ...usage } },
    ...over,
  }
}

describe('CostBadge (Q0, QA G14)', () => {
  it('shows the estimated cost, with tokens and calls in the description', () => {
    render(<CostBadge state={room({ calls: 9, tokens: 812_200, cost_usd: 1.42 })} />)
    const badge = screen.getByTestId('cost-badge')
    expect(badge).toHaveTextContent(/^\$1\.42 est\.$/)
    const tip = document.getElementById(badge.getAttribute('aria-describedby') as string)
    expect(tip).toHaveTextContent('812,200 tokens · 9 model calls')
    expect(badge.closest('[aria-live]')).toBeNull()
  })

  it('says "at least" while some calls had no price', () => {
    render(<CostBadge state={room({ calls: 9, unpriced_calls: 2, tokens: 5000, cost_usd: 1.42 })} />)
    const badge = screen.getByTestId('cost-badge')
    expect(badge).toHaveTextContent(/^≥ \$1\.42 est\.$/)
    const tip = document.getElementById(badge.getAttribute('aria-describedby') as string)
    expect(tip).toHaveTextContent('2 of 9 calls had no price, so the real cost is higher')
  })

  it('rounds a tiny cost up to "<$0.01", never to $0.00', () => {
    render(<CostBadge state={room({ calls: 1, tokens: 90, cost_usd: 0.004 })} />)
    expect(screen.getByTestId('cost-badge')).toHaveTextContent('<$0.01 est.')
  })

  it('is hidden while no call is priced (an unknown cost is not $0)', () => {
    render(<CostBadge state={room({ calls: 9, unpriced_calls: 9, tokens: 812_200, cost_usd: null })} />)
    expect(screen.queryByTestId('cost-badge')).toBeNull()
  })

  it('is hidden before the first model call and in a shadow room', () => {
    const none = render(<CostBadge state={room({})} />)
    expect(screen.queryByTestId('cost-badge')).toBeNull()
    none.unmount()
    render(<CostBadge state={room({ calls: 3, tokens: 100, cost_usd: 0.5 }, { shadow: true })} />)
    expect(screen.queryByTestId('cost-badge')).toBeNull()
  })

  it('reads the snapshot figure until the first replay has caught up, then the live fold', () => {
    const snapshot = { calls: 40, unpriced_calls: 0, tokens: 2_000_000, cost_usd: 3.1, latency_ms_total: 0 }
    const replaying = room({ calls: 2, tokens: 100, cost_usd: 0.1 }, { replay_target: 500, last_seq: 20, snapshot_usage: snapshot })
    const view = render(<CostBadge state={replaying} />)
    expect(screen.getByTestId('cost-badge')).toHaveTextContent('$3.10 est.')
    view.rerender(<CostBadge state={{ ...replaying, last_seq: 500 }} />)
    expect(screen.getByTestId('cost-badge')).toHaveTextContent('$0.10 est.')
  })

  it('shows the snapshot figure even though no event has been folded yet', () => {
    const snapshot = { calls: 40, unpriced_calls: 1, tokens: 2_000_000, cost_usd: 3.1, latency_ms_total: 0 }
    render(<CostBadge state={room({}, { replay_target: 500, last_seq: 0, snapshot_usage: snapshot })} />)
    expect(screen.getByTestId('cost-badge')).toHaveTextContent('≥ $3.10 est.')
  })

  it('ignores a snapshot figure with no replay target (fixtures, an empty room)', () => {
    const snapshot = { calls: 40, unpriced_calls: 0, tokens: 1, cost_usd: 9, latency_ms_total: 0 }
    render(<CostBadge state={room({ calls: 1, tokens: 1, cost_usd: 0.2 }, { replay_target: 0, snapshot_usage: snapshot })} />)
    expect(screen.getByTestId('cost-badge')).toHaveTextContent('$0.20 est.')
  })
})
