import { describe, expect, it } from 'vitest'
import { ApiClientError } from '../api/client'
import { isSoloConfirmError, soloConfirmText } from './solo'

// The exact answer of internal/room/access.go (authorizeLayer) for a solo approve
// without the right confirm.
const SERVER_MESSAGE = 'solo mode: type confirm "orders/kafka" to approve'

describe('isSoloConfirmError', () => {
  it('matches the real server answer: 400 validation "solo mode: ..."', () => {
    expect(isSoloConfirmError(new ApiClientError(400, 'validation', SERVER_MESSAGE))).toBe(true)
  })

  it('does not match a 400 validation with another message', () => {
    expect(isSoloConfirmError(new ApiClientError(400, 'validation', 'layer is required'))).toBe(false)
    expect(isSoloConfirmError(new ApiClientError(400, 'validation', `x ${SERVER_MESSAGE}`))).toBe(false)
  })

  it('does not match another status or code with the same message', () => {
    expect(isSoloConfirmError(new ApiClientError(403, 'not_layer_owner', SERVER_MESSAGE))).toBe(false)
    expect(isSoloConfirmError(new ApiClientError(403, 'validation', SERVER_MESSAGE))).toBe(false)
    expect(isSoloConfirmError(new ApiClientError(400, 'bad_request', SERVER_MESSAGE))).toBe(false)
  })

  it('does not match a value that is not an ApiClientError', () => {
    expect(isSoloConfirmError(new Error(SERVER_MESSAGE))).toBe(false)
    expect(isSoloConfirmError(undefined)).toBe(false)
  })
})

describe('soloConfirmText', () => {
  it('is <pipeline_id>/<layer>, the string the server compares', () => {
    expect(soloConfirmText('orders', 'kafka')).toBe('orders/kafka')
  })
})
