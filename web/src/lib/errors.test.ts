import { describe, expect, it } from 'vitest'
import { ApiClientError } from '../api/client'
import { OFFLINE_TEXT, errorText, friendlyError, isNetworkError } from './errors'

describe('friendlyError', () => {
  it('maps lease_held and stale_handoff to short user text', () => {
    expect(friendlyError('lease_held', 'control/grant: kabir holds the lease until 10:35')).toBe(
      'someone else holds control',
    )
    expect(friendlyError('stale_handoff', 'handoff/accept: the hand-off is no longer pending')).toBe(
      'this hand-off is out of date',
    )
  })

  it('maps the shadow codes, and leaves already_decided to the server text', () => {
    expect(friendlyError('not_revealed', 'raw')).toMatch(/still finishing/)
    expect(friendlyError('not_labelled', 'raw')).toMatch(/blind label/)
    expect(friendlyError('room_open', 'raw')).toMatch(/still open; this is available once it is closed/)
    expect(friendlyError('already_decided', 'layer already decided')).toBe('layer already decided')
  })

  it('maps handoff_in_progress to a friendly line (409 from a second hand-off click)', () => {
    expect(friendlyError('handoff_in_progress', 'a hand-off is in progress')).toMatch(/already being written/)
  })

  it('keeps other server codes as they are', () => {
    expect(friendlyError('not_driver', 'only the driver may steer')).toBe('only the driver may steer')
    expect(friendlyError('room_closed', 'the room is closed')).toBe('the room is closed')
  })
})

describe('errorText', () => {
  it('uses friendly text for lease_held and stale_handoff API errors', () => {
    expect(errorText(new ApiClientError(409, 'lease_held', 'raw server message'))).toBe(
      'someone else holds control',
    )
    expect(errorText(new ApiClientError(409, 'stale_handoff', 'raw server message'))).toBe(
      'this hand-off is out of date',
    )
  })

  it('keeps the server message for other codes and falls back for other values', () => {
    expect(errorText(new ApiClientError(403, 'not_driver', 'only the driver may steer'))).toBe(
      'only the driver may steer',
    )
    expect(errorText(new Error('boom'))).toBe('boom')
    expect(errorText('oops')).toBe('oops')
  })
})

describe('isNetworkError', () => {
  it('is true for a fetch TypeError and the browsers\' wording, false for an API error', () => {
    expect(isNetworkError(new TypeError('Failed to fetch'))).toBe(true)
    expect(isNetworkError(new Error('NetworkError when attempting to fetch resource.'))).toBe(true)
    expect(isNetworkError(new Error('Load failed'))).toBe(true)
    expect(isNetworkError(new ApiClientError(409, 'room_closed', 'closed'))).toBe(false)
    expect(isNetworkError(new Error('boom'))).toBe(false)
  })
  it('errorText answers OFFLINE_TEXT for them', () => {
    expect(errorText(new TypeError('Failed to fetch'))).toBe(OFFLINE_TEXT)
    expect(OFFLINE_TEXT).toMatch(/your message is kept/)
  })
})
