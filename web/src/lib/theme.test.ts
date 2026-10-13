import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { applyTheme, effectiveTheme, getTheme, setTheme, THEME_KEY } from './theme'

function reset() {
  localStorage.clear()
  delete document.documentElement.dataset.theme
  document.head.querySelector('meta[name="color-scheme"]')?.remove()
}

describe('theme', () => {
  beforeEach(reset)
  afterEach(() => {
    reset()
    vi.restoreAllMocks()
    vi.unstubAllGlobals()
  })

  it('defaults to system', () => {
    expect(getTheme()).toBe('system')
  })

  it('ignores an unknown stored value', () => {
    localStorage.setItem(THEME_KEY, 'neon')
    expect(getTheme()).toBe('system')
  })

  it('setTheme pins, stores and sets data-theme', () => {
    setTheme('light')
    expect(localStorage.getItem(THEME_KEY)).toBe('light')
    expect(getTheme()).toBe('light')
    expect(document.documentElement.dataset.theme).toBe('light')
    setTheme('dark')
    expect(document.documentElement.dataset.theme).toBe('dark')
  })

  it('setTheme("system") clears the store and the attribute', () => {
    setTheme('light')
    setTheme('system')
    expect(localStorage.getItem(THEME_KEY)).toBeNull()
    expect(document.documentElement.dataset.theme).toBeUndefined()
  })

  it('applyTheme paints the stored choice', () => {
    localStorage.setItem(THEME_KEY, 'light')
    applyTheme()
    expect(document.documentElement.dataset.theme).toBe('light')
    localStorage.removeItem(THEME_KEY)
    applyTheme()
    expect(document.documentElement.dataset.theme).toBeUndefined()
  })

  it('keeps the color-scheme meta in step', () => {
    const meta = document.createElement('meta')
    meta.setAttribute('name', 'color-scheme')
    meta.setAttribute('content', 'dark light')
    document.head.appendChild(meta)
    setTheme('light')
    expect(meta.getAttribute('content')).toBe('light')
    setTheme('system')
    expect(meta.getAttribute('content')).toBe('dark light')
  })

  it('survives a blocked localStorage', () => {
    vi.spyOn(Storage.prototype, 'getItem').mockImplementation(() => {
      throw new Error('blocked')
    })
    vi.spyOn(Storage.prototype, 'setItem').mockImplementation(() => {
      throw new Error('blocked')
    })
    expect(getTheme()).toBe('system')
    expect(() => setTheme('light')).not.toThrow()
    expect(document.documentElement.dataset.theme).toBe('light')
  })

  it('effectiveTheme follows the pin, then the OS, then dark', () => {
    expect(effectiveTheme()).toBe('dark') // jsdom has no matchMedia
    vi.stubGlobal('matchMedia', (q: string) => ({ matches: q.includes('light') }))
    expect(effectiveTheme()).toBe('light')
    setTheme('dark')
    expect(effectiveTheme()).toBe('dark')
  })
})
