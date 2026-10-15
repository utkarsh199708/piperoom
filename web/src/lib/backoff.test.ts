import { describe, expect, it } from 'vitest'
import { equalJitter } from './backoff'

describe('equalJitter', () => {
  it.each([
    [0, 5000],
    [0.5, 7500],
    [1, 10000],
  ])('with random %s waits %s ms of a 10 s ceiling', (r, want) => {
    expect(equalJitter(10000, () => r)).toBe(want)
  })

  it('stays within [ceiling/2, ceiling] for any draw in [0, 1)', () => {
    for (const r of [0, 0.001, 0.25, 0.999999]) {
      const d = equalJitter(8000, () => r)
      expect(d).toBeGreaterThanOrEqual(4000)
      expect(d).toBeLessThanOrEqual(8000)
    }
  })

  it('uses Math.random by default', () => {
    const d = equalJitter(1000)
    expect(d).toBeGreaterThanOrEqual(500)
    expect(d).toBeLessThanOrEqual(1000)
  })
})
