import { describe, expect, it } from 'vitest'
import { leaseExpired, minutesLeft } from './useNow'

const now = Date.parse('2026-10-14T10:00:00Z')

describe('minutesLeft', () => {
  it('floors: a fresh 30-minute lease plus a few seconds reads 30, then 29', () => {
    expect(minutesLeft('2026-10-14T10:30:05Z', now)).toBe(30)
    expect(minutesLeft('2026-10-14T10:30:00Z', now + 1000)).toBe(29)
  })
  it('is 0 under a minute and for the past; null without a time', () => {
    expect(minutesLeft('2026-10-14T10:00:40Z', now)).toBe(0)
    expect(minutesLeft('2026-10-14T09:00:00Z', now)).toBe(0)
    expect(minutesLeft(undefined, now)).toBeNull()
    expect(minutesLeft('x', now)).toBeNull()
  })
})

describe('leaseExpired', () => {
  it('is true only once the instant has passed', () => {
    expect(leaseExpired('2026-10-14T10:00:40Z', now)).toBe(false)
    expect(leaseExpired('2026-10-14T10:00:00Z', now)).toBe(true)
    expect(leaseExpired('2026-10-14T09:59:00Z', now)).toBe(true)
    expect(leaseExpired(undefined, now)).toBe(false)
  })
})
