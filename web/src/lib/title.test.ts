import { describe, expect, it } from 'vitest'
import { documentTitle, plainText } from './title'
import { roomStatus } from './roomStatus'

const idle = roomStatus({
  status: 'open',
  opened_at: '2026-10-14T10:00:00Z',
  driver: {},
  agent: { state: 'idle' },
  proposals: [],
  steps: 0,
})

describe('documentTitle', () => {
  it('needs me beats the status label', () => {
    expect(documentTitle({ status: idle, roomTitle: 'Orders stale', unread: 0, needsMe: true })).toBe(
      '● Needs you — Orders stale · Piperoom',
    )
  })
  it('otherwise the derived label, with an unread prefix', () => {
    expect(documentTitle({ status: idle, roomTitle: 'Orders stale', unread: 0, needsMe: false })).toBe(
      'Idle · waiting for a steer — Orders stale · Piperoom',
    )
    expect(documentTitle({ status: idle, roomTitle: 'x', unread: 3, needsMe: false })).toMatch(/^\(3\) /)
  })
  it('strips control and bidi characters from the room title', () => {
    expect(plainText(`a${String.fromCharCode(0x202e)}b
c`)).toBe('a b c')
  })
})
