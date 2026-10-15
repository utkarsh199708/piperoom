import { describe, expect, it } from 'vitest'
import { initials } from './people'
import { neutralizeBadgeText } from './verification'

const FFFD = '�'

describe('neutralizeBadgeText: spellings of "verified"', () => {
  it.each([
    ['small capitals', 'ᴠᴇʀɪꜰɪᴇᴅ'],
    ['Lisu', 'ꓦꓰꓣꓲꓝꓲꓰꓓ'],
    ['Cherokee', 'ᏙᎬᎡᏆᎰᏆᎬᎠ'],
    ['circled letters', 'ⓥⓔⓡⓘⓕⓘⓔⓓ'],
    ['U+028B for v', 'ʋerified'],
    ['full width', 'ｖｅｒｉｆｉｅｄ'],
    ['Cyrillic mix', 'vеrifiеd'],
    ['"unverified" in small capitals', 'ᴜɴᴠᴇʀɪꜰɪᴇᴅ'],
    ['small caps with Cyrillic ғ', 'ᴠᴇʀɪғɪᴇᴅ'],
    ['Canadian Syllabics ᐯ', 'ᐯERIFIED'],
    ['logical or ∨', '∨erified'],
    ['n-ary logical or ⋁', '⋁erified'],
    ['negative squared', '🆅🅴🆁🅸🅵🅸🅴🅳'],
    ['negative circled', '🅥🅔🅡🅘🅕🅘🅔🅓'],
    ['Cherokee small letters', 'ꮩꭼꭱꮖꮀꮖꭼꭰ'],
    ['℮ for e', 'v℮rified'],
    ['ⱱ for v', 'ⱱerified'],
    ['ⱴ for v', 'ⱴerified'],
    ['Old Italic 𐌖', '𐌖erified'],
    ['hyphen split', 'veri-fied'],
    ['dot split', 'v.e.r.i.f.i.e.d'],
    ['space split', 'v e r i f i e d'],
    ['upper-case with a space', 'VERI FIED'],
    ['NBSP split', 'veri\u00a0fied'],
    ['ideographic space split', 'veri\u3000fied'],
    ['"verify"', 'v.e.r.i.f.y'],
    ['"verification"', 'Verification'],
  ])('replaces %s', (_label, word) => {
    expect(neutralizeBadgeText(`Mal ${word}`)).toBe(`Mal ${FFFD}`)
  })

  it.each([
    ['U+1FBB1', '🮱'],
    ['U+1F5F8', '🗸'],
    ['U+1F5F9', '🗹'],
    ['check mark', '✓'],
    ['heavy check mark', '✔'],
    ['white heavy check mark', '✅'],
  ])('replaces the check-like symbol %s', (_label, sym) => {
    expect(neutralizeBadgeText(`Mal ${sym}`)).toBe(`Mal ${FFFD}`)
  })

  it('leaves ordinary names alone', () => {
    for (const n of ['Eve Rifkin', 'Steve Rifield', 'Priya Nair', 'Kabir', 'Zoë Müller', 'Renée', 'Arjun (arjun)', '山田 太郎']) {
      expect(neutralizeBadgeText(n)).toBe(n)
    }
  })
})

describe('initials', () => {
  it('skips symbols, emoji and check marks, and falls back to the id', () => {
    expect(initials('✅ Priya', 'priya')).toBe('PR')
    expect(initials('🮱 ✓ ✔', 'u_9')).toBe('U9')
    expect(initials('', 'kabir')).toBe('KA')
    expect(initials('✅')).toBe('?')
  })
})
