import { describe, expect, it } from 'vitest'

// Order matters: the first test leaves data behind, the second proves setup.ts cleared it.
describe('test isolation', () => {
  it('leaves something in storage', () => {
    localStorage.setItem('piperoom.draft.r1', 'left over')
    sessionStorage.setItem('k', 'v')
    expect(localStorage.length + sessionStorage.length).toBe(2)
  })
  it('starts the next test with empty storage', () => {
    expect(localStorage.length).toBe(0)
    expect(sessionStorage.length).toBe(0)
  })
})
