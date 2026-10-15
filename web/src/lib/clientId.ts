/**
 * A new idempotency key for one user action (docs/api.md, Idempotent posts; #459): a UUID v4,
 * never derived from user data. crypto.randomUUID exists only in secure contexts, so plain-HTTP
 * dev hosts get the same format from getRandomValues.
 */
export function newClientId(): string {
  const c = globalThis.crypto
  if (typeof c?.randomUUID === 'function') return c.randomUUID()
  const b = c.getRandomValues(new Uint8Array(16))
  b[6] = (b[6] & 0x0f) | 0x40
  b[8] = (b[8] & 0x3f) | 0x80
  const h = Array.from(b, (x) => x.toString(16).padStart(2, '0')).join('')
  return `${h.slice(0, 8)}-${h.slice(8, 12)}-${h.slice(12, 16)}-${h.slice(16, 20)}-${h.slice(20)}`
}
