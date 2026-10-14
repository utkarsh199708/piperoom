// Error handling of the integrations API: friendly text per code, and the parts of an
// error body the connect drawer reads (422 checks, 400 field).
import { ApiClientError } from '../api/client'
import type { Check } from '../api/integrations'
import { errorText } from './errors'

/** Friendly text for the integrations error codes (docs/api.md, Integrations). */
export function integrationErrorText(e: unknown): string {
  if (!(e instanceof ApiClientError)) return errorText(e)
  switch (e.code) {
    case 'duplicate':
      return 'This is already connected. Open it from the checklist to change it.'
    case 'managed_by_env':
      return 'This is set by environment variables. Change them on the server.'
    case 'secret_key_missing':
      return 'Typed-in secrets need PIPEROOM_SECRET_KEY on the server. Use a secret manager reference instead, or ask the operator to set the key.'
    case 'rate_limited':
      return 'Too many tests: at most 10 a minute. Wait a moment and try again.'
    case 'not_admin':
      return 'Only an admin can change integrations.'
    case 'admin_needs_oidc':
      return 'Integration changes are off while the server uses dev sign-in on a network address.'
    case 'provider_unreachable':
      return 'The secret manager did not answer.'
    default:
      return e.message
  }
}

export function checksOf(e: unknown): Check[] | null {
  if (!(e instanceof ApiClientError) || e.code !== 'checks_failed') return null
  const b = e.body as { checks?: unknown } | undefined
  return Array.isArray(b?.checks) ? (b.checks as Check[]) : []
}

export function fieldOf(e: unknown): string | null {
  if (!(e instanceof ApiClientError) || e.code !== 'validation') return null
  const b = e.body as { error?: { field?: unknown } } | undefined
  return typeof b?.error?.field === 'string' ? b.error.field : null
}
