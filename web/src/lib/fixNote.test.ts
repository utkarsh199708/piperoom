import { describe, expect, it } from 'vitest'
import { emptyRoomState } from '../state/types'
import { FIX_NOTE_WINDOW_MS, canLogFix } from './fixNote'

const member = { id: 'arjun', name: 'Arjun', layers: [] }
const closedAt = '2026-10-15T04:00:00Z'
const closed = { ...emptyRoomState('r1'), status: 'closed' as const, closed_at: closedAt }

describe('canLogFix', () => {
  it('an open room always allows it to a member, never to a viewer', () => {
    expect(canLogFix(emptyRoomState('r1'), member)).toBe(true)
    expect(canLogFix(emptyRoomState('r1'), { ...member, role: 'viewer' })).toBe(false)
  })

  it('a closed room allows it for 24 hours after the close', () => {
    const t = Date.parse(closedAt)
    expect(canLogFix(closed, member, t + 60_000)).toBe(true)
    expect(canLogFix(closed, member, t + FIX_NOTE_WINDOW_MS - 1)).toBe(true)
    expect(canLogFix(closed, member, t + FIX_NOTE_WINDOW_MS)).toBe(false)
  })

  it('a closed room with no readable close time does not offer it', () => {
    expect(canLogFix({ ...closed, closed_at: undefined }, member)).toBe(false)
    expect(canLogFix({ ...closed, closed_at: 'yesterday' }, member)).toBe(false)
  })

  it('a viewer never gets it, closed or not', () => {
    expect(canLogFix(closed, { ...member, role: 'viewer' }, Date.parse(closedAt) + 1000)).toBe(false)
  })
})
