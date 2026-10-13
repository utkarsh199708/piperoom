import { afterEach, describe, expect, it, vi } from 'vitest'
import { THEME_KEY, applyTheme, currentTheme, readStoredTheme, storeTheme } from './theme'

afterEach(() => {
  vi.restoreAllMocks()
  window.localStorage.clear()
  document.documentElement.removeAttribute('data-theme')
})

describe('theme storage', () => {
  it('remembers the choice and reads it back', () => {
    expect(readStoredTheme()).toBeNull()
    storeTheme('light')
    expect(window.localStorage.getItem(THEME_KEY)).toBe('light')
    expect(readStoredTheme()).toBe('light')
  })

  it('ignores a stored value that is not a theme', () => {
    window.localStorage.setItem(THEME_KEY, 'neon')
    expect(readStoredTheme()).toBeNull()
  })

  it('survives localStorage that throws (private window, blocked site data)', () => {
    vi.spyOn(Storage.prototype, 'getItem').mockImplementation(() => {
      throw new DOMException('blocked', 'SecurityError')
    })
    vi.spyOn(Storage.prototype, 'setItem').mockImplementation(() => {
      throw new DOMException('blocked', 'SecurityError')
    })
    expect(readStoredTheme()).toBeNull()
    expect(() => storeTheme('light')).not.toThrow()
    expect(['dark', 'light']).toContain(currentTheme())
  })
})

describe('currentTheme', () => {
  it('prefers what the page script already put on <html>', () => {
    window.localStorage.setItem(THEME_KEY, 'dark')
    applyTheme('light')
    expect(document.documentElement.getAttribute('data-theme')).toBe('light')
    expect(currentTheme()).toBe('light')
  })

  it('falls back to the saved choice, then to dark', () => {
    window.localStorage.setItem(THEME_KEY, 'light')
    expect(currentTheme()).toBe('light')
    window.localStorage.clear()
    expect(currentTheme()).toBe('dark') // jsdom has no matchMedia: dark is the default
  })
})
