import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { noticeForExpiredSession, noticeForRoute, rememberRoomTitle } from './signinNotice'

beforeEach(() => {
  rememberRoomTitle(undefined)
  sessionStorage.clear()
})
afterEach(() => {
  window.location.hash = ''
})

describe('noticeForExpiredSession', () => {
  it('is null off a room route', () => {
    window.location.hash = '#/'
    expect(noticeForExpiredSession()).toBeNull()
  })

  it('is always the neutral kind, never a "signed in elsewhere" claim', () => {
    window.location.hash = '#/rooms/r_a'
    expect(noticeForExpiredSession()?.kind).toBe('expired')
  })

  it('names the room that was on screen', () => {
    window.location.hash = '#/rooms/r_a'
    rememberRoomTitle('orders stale')
    expect(noticeForExpiredSession()).toEqual({ kind: 'expired', title: 'orders stale', roomId: 'r_a' })
  })

  it('does not name a room remembered for another room id', () => {
    window.location.hash = '#/rooms/r_a'
    rememberRoomTitle('orders stale')
    window.location.hash = '#/rooms/r_b'
    expect(noticeForExpiredSession()?.title).toBeUndefined()
  })
})

describe('noticeForRoute', () => {
  it('keeps the title while the URL still points at its room', () => {
    window.location.hash = '#/rooms/r_a'
    const n = { kind: 'expired', title: 'orders stale', roomId: 'r_a' } as const
    expect(noticeForRoute(n)).toBe(n)
  })

  it('drops the title once the URL points at another room', () => {
    window.location.hash = '#/rooms/r_b'
    const n = noticeForRoute({ kind: 'expired', title: 'orders stale', roomId: 'r_a' })
    expect(n?.title).toBeUndefined()
    expect(n?.roomId).toBe('r_b')
  })

  it('passes null through', () => {
    expect(noticeForRoute(null)).toBeNull()
  })
})
