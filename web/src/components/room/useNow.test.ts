import { describe, expect, it } from 'vitest'
import { minutesLeft } from './useNow'

describe('minutesLeft', () => {
  const now = Date.parse('2026-10-14T10:00:00Z')
  it.each([
    // A 30 m grant read a few seconds later (or with the server clock a little ahead) is "30m", not "31m".
    ['2026-10-14T10:30:05Z', 30],
    ['2026-10-14T10:30:00Z', 30],
    ['2026-10-14T10:29:59Z', 29],
    // Under a minute left still reads 1, so 0 always means expired.
    ['2026-10-14T10:00:20Z', 1],
    ['2026-10-14T10:00:00Z', 0],
    ['2026-10-14T09:00:00Z', 0],
  ])('%s → %i', (iso, want) => {
    expect(minutesLeft(iso, now)).toBe(want)
  })

  it('is null without a time', () => {
    expect(minutesLeft(undefined, now)).toBeNull()
    expect(minutesLeft('nope', now)).toBeNull()
  })
})
