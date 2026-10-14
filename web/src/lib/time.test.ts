import { describe, expect, it } from 'vitest'
import { durationLabel, utcLabel } from './time'

describe('utcLabel', () => {
  it('formats an RFC 3339 time as UTC, and leaves an invalid one alone', () => {
    expect(utcLabel('2026-10-14T06:06:12Z')).toBe('2026-10-14 06:06:12 UTC')
    expect(utcLabel('2026-10-14T08:06:12+02:00')).toBe('2026-10-14 06:06:12 UTC')
    expect(utcLabel('not a time')).toBe('not a time')
  })
})

describe('durationLabel', () => {
  const t0 = '2026-10-14T10:00:00Z'
  it.each([
    ['2026-10-14T10:06:00Z', '6 min'],
    ['2026-10-14T10:00:20Z', '1 min'],
    ['2026-10-14T11:12:00Z', '1 h 12 min'],
    ['2026-10-14T12:00:00Z', '2 h'],
    ['2026-10-16T13:00:00Z', '2 d 3 h'],
  ])('%s -> %s', (closed, want) => {
    expect(durationLabel(t0, closed)).toBe(want)
  })
  it('is empty for a bad or negative span', () => {
    expect(durationLabel(t0, 'x')).toBe('')
    expect(durationLabel('2026-10-14T11:00:00Z', t0)).toBe('')
  })
})
