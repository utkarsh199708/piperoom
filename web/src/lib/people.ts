import { neutralizeBadgeText } from './verification'

// Avatar colours and initials. The palette is the design's (docs/design/ui-v2,
// .p-kabir .p-arjun ...): each is a dark fill behind white text, so contrast
// holds in both themes. Known dev users keep their own colour; anyone else gets
// a stable one from a hash of the id.
const KNOWN: Record<string, string> = {
  kabir: '#b45309',
  arjun: '#1d4ed8',
  priya: '#0f766e',
  meera: '#be185d',
  rohan: '#4d7c0f',
}

const PALETTE = Object.values(KNOWN)

export function personColor(id: string): string {
  const key = id.toLowerCase()
  if (Object.hasOwn(KNOWN, key)) return KNOWN[key]
  let h = 0
  for (let i = 0; i < key.length; i++) h = (h * 31 + key.charCodeAt(i)) >>> 0
  return PALETTE[h % PALETTE.length]
}

/** Two upper-case letters: "Kabir" -> "KA". */
/**
 * Two letters or digits from a display name. The name is untrusted (an IdP user sets their own),
 * so symbols, emoji and check marks are skipped: an avatar must never show "✅" next to a name.
 * `fallback` (the server id) is tried when the name has no letter at all.
 */
export function initials(name: string, fallback = ''): string {
  const pick = (t: string) =>
    Array.from(neutralizeBadgeText(t))
      .filter((c) => /[\p{L}\p{N}]/u.test(c))
      .slice(0, 2)
      .join('')
      .toUpperCase()
  return pick(name) || pick(fallback) || '?'
}
