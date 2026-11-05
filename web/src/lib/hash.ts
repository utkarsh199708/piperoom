// params_hash helpers (auth PR 4, docs/design/auth.md 3.1). The hash is a hex
// SHA-256 of the frozen proposal; the UI shows it and sends it back unchanged.
import type { Identity } from '../api/types'

/** The short form shown next to the full hash: its first 12 characters. */
export function shortHash(hash: string): string {
  return hash.slice(0, 12)
}

/**
 * How the approver held the right, in words: "user:priya" → "named owner", "group:dataeng" →
 * "via dataeng", "backup:group:sre" → "backup approver", "solo" → "solo mode". The raw grant
 * stays in the tooltip (identityDetails) for an audit.
 */
export function grantLabel(grant: string | undefined): string {
  if (!grant) return ''
  if (grant === 'solo') return 'solo mode'
  if (grant.startsWith('backup:')) return 'backup approver'
  if (grant.startsWith('group:')) return `via ${grant.slice('group:'.length)}`
  if (grant.startsWith('user:')) return 'named owner'
  return grant
}

const METHOD_LABEL: Record<string, string> = { oidc: 'SSO', dev: 'dev sign-in', proxy: 'proxy sign-in' }

/** "named owner, SSO" / "backup approver, SSO" / "solo mode, dev sign-in": grant and method in words. */
export function describeIdentity(identity: Identity | undefined): string {
  if (!identity) return ''
  const method = identity.method ? (METHOD_LABEL[identity.method] ?? identity.method) : ''
  return [grantLabel(identity.grant), method].filter(Boolean).join(', ')
}

/** Everything else the identity says, for a tooltip. */
export function identityDetails(identity: Identity | undefined): string {
  if (!identity) return ''
  const rows: Array<[string, string | undefined]> = [
    ['grant', identity.grant],
    ['method', identity.method],
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
