import { describe, expect, it } from 'vitest'
import { chipText, costBadge, headerUsage } from './insight'
import { emptyInsight, emptyRoomState, fromSnapshot } from '../state/types'
import type { RoomSnapshot } from '../api/types'
import type { InsightState, RoomState } from '../state/types'

const usage = (u: Partial<InsightState['usage']>): InsightState['usage'] => ({ ...emptyInsight().usage, ...u })

describe('costBadge', () => {
  it('formats the estimate and marks a lower bound', () => {
    expect(costBadge(usage({ calls: 3, cost_usd: 1.42 }))?.text).toBe('$1.42 est.')
    expect(costBadge(usage({ calls: 3, unpriced_calls: 1, cost_usd: 1.42 }))?.text).toBe('≥ $1.42 est.')
    expect(costBadge(usage({ calls: 1, cost_usd: 0.004 }))?.text).toBe('<$0.01 est.')
  })

  it('is null while the cost is unknown, never "$0.00"', () => {
    expect(costBadge(usage({ calls: 3, unpriced_calls: 3, cost_usd: null }))).toBeNull()
    expect(costBadge(usage({}))).toBeNull()
  })

  it('puts tokens and calls in the title, singular for one call', () => {
    expect(costBadge(usage({ calls: 1, tokens: 1500, cost_usd: 0.2 }))?.title).toBe(
      '1,500 tokens · 1 model call · estimated from the configured model prices',
    )
    expect(costBadge(usage({ calls: 4, unpriced_calls: 1, tokens: 10, cost_usd: 0.2 }))?.title).toContain(
      '1 of 4 calls had no price, so the real cost is higher',
    )
  })
})

describe('chipText without the cost', () => {
  it('leaves the cost out and keeps steps and duration', () => {
    const i = { ...emptyInsight(), steps: 11, usage: usage({ calls: 9, cost_usd: 0.42 }) }
    expect(chipText(i, 130_000, false)).toBe('11 steps · 2m 10s')
    expect(chipText(i, 130_000)).toBe('$0.42 · 11 steps · 2m 10s')
  })
})

describe('headerUsage', () => {
  const snapshot: RoomSnapshot = {
    id: 'r1',
    title: 't',
    severity: 'sev2',
    pipeline_id: 'orders',
    status: 'open',
    opened_at: '2026-10-14T10:00:00Z',
    last_seq: 120,
    driver: null,
    agent_status: { state: 'working' },
    hypothesis: null,
    open_proposals: [],
    pending_suggestions: [],
    members: [],
    insight: {
      first_hypothesis_at: null,
      time_to_first_hypothesis_s: -1,
      final_confidence: '',
      steps: 0,
      tool_calls: 0,
      tool_errors: 0,
      usage: { calls: 12, unpriced_calls: 0, tokens: 99_000, cost_usd: 1.42, latency_ms_total: 1 },
    },
  }

  it('fromSnapshot keeps the snapshot usage apart from the fold', () => {
    const s = fromSnapshot(snapshot)
    expect(s.snapshot_usage?.cost_usd).toBe(1.42)
    expect(s.insight.usage.cost_usd).toBeNull()
    expect(s.insight.usage.calls).toBe(0)
  })

  it('reads the snapshot until last_seq reaches the replay target', () => {
    const s: RoomState = fromSnapshot(snapshot)
    expect(headerUsage(s).cost_usd).toBe(1.42)
    expect(headerUsage({ ...s, last_seq: 119 }).cost_usd).toBe(1.42)
    expect(headerUsage({ ...s, last_seq: 120 }).cost_usd).toBeNull()
  })

  it('reads the fold when the snapshot has no insight (an older server)', () => {
    const s = fromSnapshot({ ...snapshot, insight: undefined })
    expect(s.snapshot_usage).toBeUndefined()
    expect(headerUsage(s)).toBe(s.insight.usage)
    expect(headerUsage(emptyRoomState('r'))).toEqual(emptyInsight().usage)
  })
})
