import { describe, expect, it } from 'vitest'
import { chipText, chipTooltip, costLabel, elapsedLabel, tokensLabel } from './insight'
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
