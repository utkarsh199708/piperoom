import { describe, expect, it } from 'vitest'
import { isLoopbackHost } from './loopback'

describe('isLoopbackHost', () => {
  it('treats undefined and false (Vite defaults) as loopback', () => {
    expect(isLoopbackHost(undefined)).toBe(true)
    expect(isLoopbackHost(false)).toBe(true)
  })

  it('treats localhost as loopback', () => {
    expect(isLoopbackHost('localhost')).toBe(true)
    expect(isLoopbackHost('  LOCALHOST  ')).toBe(true)
  })

  it('treats a bare --host (true, binds 0.0.0.0) as non-loopback', () => {
    expect(isLoopbackHost(true)).toBe(false)
  })

  it('accepts 127.0.0.1 as loopback', () => {
    expect(isLoopbackHost('127.0.0.1')).toBe(true)
  })

  it('accepts the whole 127/8 range as loopback', () => {
    expect(isLoopbackHost('127.0.0.0')).toBe(true)
    expect(isLoopbackHost('127.255.255.255')).toBe(true)
  })

  it('accepts ::1 as loopback', () => {
    expect(isLoopbackHost('::1')).toBe(true)
  })

  it('rejects a hostname that merely starts with 127.', () => {
    expect(isLoopbackHost('127.attacker.example')).toBe(false)
  })

  it('rejects non-loopback IP literals', () => {
    expect(isLoopbackHost('10.0.0.1')).toBe(false)
    expect(isLoopbackHost('192.168.1.1')).toBe(false)
    expect(isLoopbackHost('2001:db8::1')).toBe(false)
  })
})
