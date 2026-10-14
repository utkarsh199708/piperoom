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
export function initials(name: string): string {
  return (name.trim() || '?').slice(0, 2).toUpperCase()
}
