import { isIP } from 'node:net'

// Returns true only when `host` (Vite's resolved `server.host`) binds to the
// loopback interface. Vite accepts a hostname as well as an IP literal here, so
// a value like `127.attacker.example` — a hostname that merely starts with "127."
// — must be treated as non-loopback. Only real IP literals in 127.0.0.0/8 (and
// `::1`) count as loopback; everything else is assumed to be a non-loopback
// hostname.
export function isLoopbackHost(host: string | boolean | undefined): boolean {
  // Vite's default host is localhost; `false` resolves the same way.
  if (host === undefined || host === false) return true
  // A bare `vite --host` binds 0.0.0.0.
  if (host === true) return false
  const h = host.trim().toLowerCase()
  if (h === 'localhost') return true
  switch (isIP(h)) {
    case 4:
      return h.startsWith('127.')
    case 6:
      return h === '::1'
    default:
      return false
  }
}
