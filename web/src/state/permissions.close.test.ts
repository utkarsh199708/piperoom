import { describe, expect, it } from 'vitest'
import { canClose, canWithdraw, ownsAnyLayer } from './permissions'
import type { CurrentUser } from './permissions'
import { emptyRoomState } from './types'
import type { RoomState } from './types'

const room = (over: Partial<RoomState> = {}): RoomState => ({
  ...emptyRoomState('r1'),
  driver: { id: 'kabir', name: 'Kabir' },
  ...over,
})
const user = (over: Partial<CurrentUser> = {}): CurrentUser => ({ id: 'sam', name: 'Sam', layers: [], role: 'member', ...over })

describe('canClose (Q0, QA G16)', () => {
  it('the driver can, whatever layers they own', () => {
    expect(canClose(room(), user({ id: 'kabir' }))).toBe(true)
  })

  it('a non-driver who approves a layer can', () => {
    expect(canClose(room(), user({ layers: ['kafka-connect'] }))).toBe(true)
    expect(canClose(room({ driver: null }), user({ layers: ['warehouse'] }))).toBe(true)
  })

  it('a non-driver with no layer, and a layer the pipeline does not have, cannot', () => {
    expect(canClose(room(), user())).toBe(false)
    expect(canClose(room(), user({ layers: ['made-up'] }))).toBe(false)
  })

  it('the solo user can', () => {
    expect(canClose(room(), user({ solo: true }))).toBe(true)
  })

  it('a viewer cannot, even as a driver or an owner', () => {
    expect(canClose(room(), user({ id: 'kabir', role: 'viewer' }))).toBe(false)
    expect(canClose(room(), user({ role: 'viewer', layers: ['consumer'] }))).toBe(false)
  })

  it('nobody can close a closed room', () => {
    expect(canClose(room({ status: 'closed' }), user({ id: 'kabir', layers: ['consumer'] }))).toBe(false)
  })

  it('ownsAnyLayer', () => {
    expect(ownsAnyLayer(user({ layers: ['postgres', 'x'] }))).toBe(true)
    expect(ownsAnyLayer(user({ layers: ['x'] }))).toBe(false)
  })
})

describe('canWithdraw (Q0, QA G9)', () => {
  const open = { status: 'open' } as const

  it('only the driver, on an open proposal of an open room', () => {
    expect(canWithdraw(room(), user({ id: 'kabir' }), open)).toBe(true)
    expect(canWithdraw(room(), user({ layers: ['consumer'] }), open)).toBe(false)
    expect(canWithdraw(room({ driver: null }), user(), open)).toBe(false)
  })

  it('not a proposal that was approved, rejected or withdrawn already', () => {
    for (const status of ['approved', 'rejected', 'withdrawn'] as const) {
      expect(canWithdraw(room(), user({ id: 'kabir' }), { status })).toBe(false)
    }
  })

  it('not in a closed room, not for a viewer', () => {
    expect(canWithdraw(room({ status: 'closed' }), user({ id: 'kabir' }), open)).toBe(false)
    expect(canWithdraw(room(), user({ id: 'kabir', role: 'viewer' }), open)).toBe(false)
  })
})
