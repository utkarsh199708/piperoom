import { describe, expect, it } from 'vitest'
import { formatDuration, investigationSummary, leaseLabel, stepTookMs } from './investigation'
import type { StepState } from '../state/types'

const step = (took: (number | undefined)[]): StepState => ({
  id: 's',
  title: 't',
  messages: [],
  tool_calls: took.map((took_ms, i) => ({ tool_use_id: `u${i}`, tool: 'x', input: null, took_ms })),
})

describe('investigation helpers', () => {
  it('formats durations', () => {
    expect(formatDuration(412)).toBe('412 ms')
    expect(formatDuration(3200)).toBe('3.2 s')
    expect(formatDuration(130_000)).toBe('2m 10s')
  })

  it('sums took_ms per step and returns null when none was reported', () => {
    expect(stepTookMs(step([100, 20]))).toBe(120)
    expect(stepTookMs(step([undefined]))).toBeNull()
  })

  it('summarises the steps', () => {
    expect(investigationSummary([step([60_000]), step([70_000])])).toBe('2 steps · 2m 10s')
    expect(investigationSummary([step([undefined])])).toBe('1 step')
  })

  it('labels the lease in minutes', () => {
    const now = Date.parse('2026-10-11T10:00:00Z')
    expect(leaseLabel('2026-10-11T10:24:00Z', now)).toBe('lease 24m')
    expect(leaseLabel('2026-10-11T09:00:00Z', now)).toBeNull()
    expect(leaseLabel(undefined, now)).toBeNull()
  })
})
