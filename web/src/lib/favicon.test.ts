import { afterEach, describe, expect, it } from 'vitest'
import { setFavicon } from './favicon'

afterEach(() => {
  document.head.innerHTML = ''
})

describe('setFavicon', () => {
  it('swaps the href and is idempotent', () => {
    document.head.innerHTML = '<link id="favicon" rel="icon" href="/favicon.svg">'
    const link = document.getElementById('favicon')!
    setFavicon(true)
    expect(link.getAttribute('href')).toBe('/favicon-alert.svg')
    setFavicon(true)
    expect(link.getAttribute('href')).toBe('/favicon-alert.svg')
    setFavicon(false)
    expect(link.getAttribute('href')).toBe('/favicon.svg')
  })
  it('does nothing without the link', () => {
    expect(() => setFavicon(true)).not.toThrow()
  })
})
