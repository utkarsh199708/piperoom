import { describe, expect, it } from 'vitest'
import { escapeRegExp, markTerms } from './unbacked'

describe('markTerms', () => {
  it('returns one plain run without terms', () => {
    expect(markTerms('slot fell behind', [])).toEqual([{ text: 'slot fell behind', unbacked: false }])
    expect(markTerms('slot fell behind', undefined)).toEqual([{ text: 'slot fell behind', unbacked: false }])
  })

  it('marks the first case-insensitive occurrence and keeps the original case', () => {
    expect(markTerms('The Order_Sync consumer and order_sync again', ['order_sync'])).toEqual([
      { text: 'The ', unbacked: false },
      { text: 'Order_Sync', unbacked: true },
      { text: ' consumer and order_sync again', unbacked: false },
    ])
  })

  it('marks several terms in text order', () => {
    const runs = markTerms('lag is 4821 on pg.public.orders', ['pg.public.orders', '4821'])
    expect(runs.filter((r) => r.unbacked).map((r) => r.text)).toEqual(['4821', 'pg.public.orders'])
    expect(runs.map((r) => r.text).join('')).toBe('lag is 4821 on pg.public.orders')
  })

  it('does not throw on regex-special terms', () => {
    expect(() => markTerms('call a.b(c) now', ['a.b(c)', '[', '\\', '(?<'])).not.toThrow()
    expect(markTerms('call a.b(c) now', ['a.b(c)']).filter((r) => r.unbacked)).toEqual([{ text: 'a.b(c)', unbacked: true }])
  })

  it('does not treat a dot as a wildcard', () => {
    expect(markTerms('axb', ['a.b'])).toEqual([{ text: 'axb', unbacked: false }])
  })

  it('ignores a term that is not in the text, an empty term and an overlap', () => {
    expect(markTerms('abc', ['zzz', ''])).toEqual([{ text: 'abc', unbacked: false }])
    expect(markTerms('abcdef', ['abcd', 'cde']).filter((r) => r.unbacked)).toEqual([{ text: 'abcd', unbacked: true }])
  })

  it('escapes', () => {
    expect(new RegExp(escapeRegExp('a.b*c')).test('a.b*c')).toBe(true)
    expect(new RegExp(escapeRegExp('a.b*c')).test('axbbc')).toBe(false)
  })
})
