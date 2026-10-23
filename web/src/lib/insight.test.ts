import { describe, expect, it } from 'vitest'
import {
  chipText,
  chipTooltip,
  costLabel,
  elapsedLabel,
  gbDaysLabel,
  infraFacts,
  modelCostText,
  tokensLabel,
  unreadAgeLabel,
} from './insight'
import type { AgentStats } from '../api/types'
import { emptyInsight } from '../state/types'
import type { InsightState } from '../state/types'

const withUsage = (u: Partial<InsightState['usage']>, over: Partial<InsightState> = {}): InsightState => {
  const i = emptyInsight()
  return { ...i, ...over, usage: { ...i.usage, ...u } }
}

describe('insight formatting', () => {
  it('elapsedLabel', () => {
    expect(elapsedLabel(12_900)).toBe('12s')
    expect(elapsedLabel(130_000)).toBe('2m 10s')
    expect(elapsedLabel(3_720_000)).toBe('1 h 2 min')
    expect(elapsedLabel(3_600_000)).toBe('1 h')
    expect(elapsedLabel(526_200_000)).toBe('6 d 2 h')
    expect(elapsedLabel(-5)).toBe('0s')
  })

  it('tokensLabel', () => {
    expect(tokensLabel(812_200)).toBe('812k tokens')
    expect(tokensLabel(1_200_000)).toBe('1.2M tokens')
    expect(tokensLabel(640)).toBe('640 tokens')
  })

  it('costLabel: price, lower bound, tokens, nothing', () => {
    expect(costLabel(withUsage({ calls: 2, cost_usd: 0.42 }).usage)).toBe('$0.42')
    expect(costLabel(withUsage({ calls: 3, unpriced_calls: 1, cost_usd: 0.42 }).usage)).toBe('≥ $0.42')
    expect(costLabel(withUsage({ calls: 2, unpriced_calls: 2, cost_usd: null, tokens: 812_200 }).usage)).toBe('812k tokens')
    expect(costLabel(withUsage({}).usage)).toBe('')
    expect(costLabel(withUsage({ calls: 1, cost_usd: 0.004 }).usage)).toBe('<$0.01')
  })

  it('chipText leaves out the cost before the first call', () => {
    expect(chipText(withUsage({}, { steps: 0 }), 12_000)).toBe('0 steps · 12s')
    expect(chipText(withUsage({ calls: 9, cost_usd: 0.42 }, { steps: 11 }), 130_000)).toBe('$0.42 · 11 steps · 2m 10s')
    expect(chipText(withUsage({}, { steps: 1 }), 1000)).toBe('1 step · 1s')
  })

  it('chipTooltip lists calls, tokens, models, first hypothesis and tool errors', () => {
    const i = withUsage(
      { calls: 9, tokens: 812_200, cache_read_tokens: 41_000, models: { 'claude-opus-5-5': 7, 'claude-haiku-5-5': 2 } },
      { time_to_first_hypothesis_s: 48, tool_errors: 1 },
    )
    expect(chipTooltip(i)).toBe(
      '9 model calls · 812,200 tokens (41k cache reads) · claude-opus-5-5 ×7, claude-haiku-5-5 ×2 · first hypothesis in 48 s · 1 tool error',
    )
    expect(chipTooltip(emptyInsight())).toBe('No model calls yet')
  })
})

describe('pipeline cost view (X6)', () => {
  const stats = { pipeline_id: 'orders', window_days: 90, rooms: 23, median_cost_usd: 0.42, cost_available: true } as AgentStats

  it('modelCostText', () => {
    expect(modelCostText(stats)).toBe('median $0.42 per room · 23 closed rooms · 90 d')
    expect(modelCostText({ ...stats, rooms: 1, median_cost_usd: 0.004 })).toBe('median <$0.01 per room · 1 closed room · 90 d')
    expect(modelCostText({ ...stats, cost_available: false })).toBe('23 closed rooms · not priced')
    expect(modelCostText({ ...stats, median_cost_usd: -1 })).toBe('23 closed rooms · not priced')
    expect(modelCostText({ ...stats, rooms: 0, window_days: 7 })).toBe('no closed rooms in 7 d')
  })

  it('gbDaysLabel', () => {
    expect(gbDaysLabel(30)).toBe('30 GB-days')
    expect(gbDaysLabel(1.074)).toBe('1.07 GB-days')
    expect(gbDaysLabel(1234.56)).toBe('1,234.6 GB-days')
    expect(gbDaysLabel(0.004)).toBe('<0.01 GB-days')
    expect(gbDaysLabel(0)).toBe('0 GB-days')
  })

  it('unreadAgeLabel', () => {
    expect(unreadAgeLabel(45)).toBe('45 s')
    expect(unreadAgeLabel(720)).toBe('12 min')
    expect(unreadAgeLabel(5400)).toBe('1 h 30 min')
    expect(unreadAgeLabel(7200)).toBe('2 h')
    expect(unreadAgeLabel(183600)).toBe('2 d 3 h')
  })

  it('infraFacts keeps a missing metric null, never 0', () => {
    const facts = infraFacts({
      from: '',
      to: '',
      samples: 2,
      wal_retained_gb_days: null,
      dlq_records: 1,
      oldest_unread_age_s: 0,
    })
    expect(facts.map((f) => [f.label, f.value])).toEqual([
      ['WAL retained', null],
      ['Dead-letter queue', '1 record'],
      ['Oldest unread', '0 s'],
    ])
  })
})
