// params_hash helpers (auth PR 4, docs/design/auth.md 3.1). The hash is a hex
// SHA-256 of the frozen proposal; the UI shows it and sends it back unchanged.
import type { Identity } from '../api/types'

/** The short form shown next to the full hash: its first 12 characters. */
export function shortHash(hash: string): string {
  return hash.slice(0, 12)
}

/** "user:priya, oidc" / "backup:group:sre, oidc" / "solo, dev": grant and method. */
export function describeIdentity(identity: Identity | undefined): string {
  if (!identity) return ''
  return [identity.grant, identity.method].filter(Boolean).join(', ')
}

/** Everything else the identity says, for a tooltip. */
export function identityDetails(identity: Identity | undefined): string {
  if (!identity) return ''
  const rows: Array<[string, string | undefined]> = [
    ['iss', identity.iss],
    ['sub', identity.sub],
    ['session', identity.session_id],
    ['auth_time', identity.auth_time],
  ]
  return rows
    .filter(([, v]) => v)
    .map(([k, v]) => `${k}: ${v}`)
    .join('\n')
}
