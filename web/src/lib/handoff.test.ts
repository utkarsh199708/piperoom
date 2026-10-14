import { describe, expect, it } from 'vitest'
import { emptyRoomState } from '../state/types'
import { handoffFailureText, handoffInFlight } from './handoff'

const request = (expires_at: string) => ({
  request_id: 'hr_1',
  to: { kind: 'human' as const, id: 'kabir', name: 'Kabir' },
  stay_as_watcher: false,
  expires_at,
})

describe('handoffInFlight', () => {
  const t = Date.parse('2026-10-11T10:00:00Z')

  it('is null with no request', () => {
    expect(handoffInFlight(emptyRoomState('r1'), t)).toBeNull()
  })

  it('is the request until expires_at, then null (a server that died mid-request)', () => {
    const s = { ...emptyRoomState('r1'), handoff_request: request('2026-10-11T10:03:00Z') }
    expect(handoffInFlight(s, t)?.request_id).toBe('hr_1')
    expect(handoffInFlight(s, Date.parse('2026-10-11T10:03:00Z'))).toBeNull()
    expect(handoffInFlight(s, Date.parse('2026-10-11T10:04:00Z'))).toBeNull()
  })

  it('an unreadable expires_at keeps the request in flight until an event ends it', () => {
    const s = { ...emptyRoomState('r1'), handoff_request: request('not a time') }
    expect(handoffInFlight(s, t)).not.toBeNull()
  })
})

describe('handoffFailureText', () => {
  it('maps the fixed reason codes and never echoes an unknown one', () => {
    expect(handoffFailureText('summary_failed')).toMatch(/could not write the summary/)
    expect(handoffFailureText('room_closed')).toMatch(/room closed/)
    expect(handoffFailureText('<script>')).toBe(handoffFailureText('internal_error'))
  })
})
