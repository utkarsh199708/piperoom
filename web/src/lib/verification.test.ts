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
    for (const n of ['Priya Nair', 'Kabir', 'Zoë Müller', 'Renée', 'Arjun (arjun)', '山田 太郎']) {
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
