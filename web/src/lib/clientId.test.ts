import { afterEach, describe, expect, it, vi } from 'vitest'
import { newClientId } from './clientId'

// docs/api.md, Idempotent posts: 1-64 characters of [A-Za-z0-9_-].
const FORMAT = /^[A-Za-z0-9_-]{1,64}$/
const UUID_V4 = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/

afterEach(() => {
  vi.unstubAllGlobals()
})

describe('newClientId', () => {
  it('is a fresh UUID v4 in the contract format', () => {
    const a = newClientId()
    expect(a).toMatch(FORMAT)
    expect(a).toMatch(UUID_V4)
    expect(newClientId()).not.toBe(a)
  })

  it('works without crypto.randomUUID (an insecure context)', () => {
    const real = globalThis.crypto
    vi.stubGlobal('crypto', { getRandomValues: (b: Uint8Array) => real.getRandomValues(b) })
    const a = newClientId()
    expect(a).toMatch(UUID_V4)
  })
})
