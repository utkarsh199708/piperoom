import { describe, expect, it } from 'vitest'
import type { ShadowMetrics } from '../api/types'
import { formatSeconds, formatTokens, metricsHeadline, parseTerms, sinceParam, termsProblem } from './shadow'

const base: ShadowMetrics = {
  rooms: 16,
  labelled: 16,
  graded: 15,
  layer_correct: 14,
  root_cause: { yes: 12, partial: 2, no: 1 },
  by_status: { hypothesised: 15, error: 1 },
  median_time_to_first_s: 120,
  median_time_to_final_s: 240,
  median_human_mttr_s: 3120,
  total_tokens: 380_000,
  tokens_available: true,
  per_room: [],
}

describe('formatSeconds', () => {
  it.each([
    [-1, 'n/a'],
    [0, '0 s'],
    [45, '45 s'],
    [240, '4 min'],
    [3120, '52 min'],
    [3599, '1.0 h'],
    [7200, '2.0 h'],
  ])('%s -> %s', (s, want) => expect(formatSeconds(s)).toBe(want))
  it('treats null and NaN as n/a', () => {
    expect(formatSeconds(null)).toBe('n/a')
    expect(formatSeconds(Number.NaN)).toBe('n/a')
  })
})

describe('formatTokens', () => {
  it.each([
    [900, '900'],
    [380_000, '380k'],
    [2_500_000, '2.5M'],
  ])('%s -> %s', (n, want) => expect(formatTokens(n)).toBe(want))
})

describe('metricsHeadline', () => {
  it('builds the pilot line', () => {
    expect(metricsHeadline(base)).toBe('14 of 16 matched, median 4 min vs 52 min, 380k tokens')
  })
  it('says tokens were not counted when the runner reports none', () => {
    expect(metricsHeadline({ ...base, tokens_available: false })).toContain('tokens not counted')
  })
  it('shows n/a for a median nobody has', () => {
    expect(metricsHeadline({ ...base, median_time_to_final_s: -1 })).toContain('median n/a vs 52 min')
  })
})

describe('terms', () => {
  it('splits on lines, trims and drops blanks, keeps commas', () => {
    expect(parseTerms(' schema change \n\n  a, b \n')).toEqual(['schema change', 'a, b'])
  })
  it('flags empty, too many and too long', () => {
    expect(termsProblem([])).not.toBeNull()
    expect(termsProblem(Array.from({ length: 21 }, (_, i) => `t${i}`))).not.toBeNull()
    expect(termsProblem(['x'.repeat(201)])).not.toBeNull()
    expect(termsProblem(['x'.repeat(200)])).toBeNull()
    expect(termsProblem(Array.from({ length: 20 }, (_, i) => `t${i}`))).toBeNull()
  })
})

describe('sinceParam', () => {
  it('turns a date into start-of-day UTC and ignores anything else', () => {
    expect(sinceParam('2026-10-01')).toBe('2026-10-01T00:00:00Z')
    expect(sinceParam('')).toBe('')
    expect(sinceParam('yesterday')).toBe('')
  })
})
