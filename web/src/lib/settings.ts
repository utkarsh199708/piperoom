// Pure helpers for the Settings pages (docs/api.md, "Admin: settings and connection tests").
import { ApiClientError } from '../api/client'
import type { AdminConnection, ConnectionSetting, ProbeCheck, ProbeResult, ProbeStatus } from '../api/types'
import { errorText, isNetworkError } from './errors'

/** A Go server encodes an empty slice as `null`: read every list through this. */
export function lines<T>(list: readonly T[] | null | undefined): readonly T[] {
  return list ?? []
}

const KIND_LABEL: Record<string, string> = {
  source_db: 'Postgres source',
  kafka: 'Kafka',
  connect: 'Kafka Connect',
  schema_registry: 'Schema Registry',
  warehouse: 'Warehouse',
}

/** The name a person knows a connection kind by; an unknown kind from a newer server is shown as sent. */
export function kindLabel(kind: string): string {
  return Object.hasOwn(KIND_LABEL, kind) ? KIND_LABEL[kind] : kind
}

/** "Process-wide" or "Pipeline orders". */
export function scopeLabel(c: Pick<AdminConnection, 'scope' | 'pipeline_id'>): string {
  return c.scope === 'pipeline' ? `Pipeline ${c.pipeline_id}` : 'Process-wide'
}

/**
 * What the value cell of a settings row shows. A secret is never shown, whatever the server sent:
 * only whether it is set. The `from` text says where it comes from (`env NAME`).
 */
export function settingValue(s: ConnectionSetting): { kind: 'secret'; set: boolean } | { kind: 'text'; text: string } {
  if (s.secret) return { kind: 'secret', set: s.set === true }
  return { kind: 'text', text: s.display }
}

/** Words for a check status, so the result never depends on colour or icon shape alone. */
export const STATUS_LABEL: Record<ProbeStatus, string> = {
  ok: 'ok',
  fail: 'failed',
  warn: 'warning',
  skip: 'skipped',
}

/** A status this client does not know (a newer server) is treated as a warning. */
export function knownStatus(s: string): ProbeStatus {
  return Object.hasOwn(STATUS_LABEL, s) ? (s as ProbeStatus) : 'warn'
}

/** Counts per status, for the summary line. */
export function countChecks(checks: readonly ProbeCheck[]): Record<ProbeStatus, number> {
  const n: Record<ProbeStatus, number> = { ok: 0, fail: 0, warn: 0, skip: 0 }
  for (const c of checks) n[knownStatus(c.status)]++
  return n
}

/** The one line that announces a finished test: pass or fail first, then what was found. */
export function probeSummary(r: ProbeResult): string {
  const checks = lines(r.checks)
  const n = countChecks(checks)
  const parts = [`${n.ok} of ${checks.length} checks ok`]
  if (n.fail) parts.push(`${n.fail} failed`)
  if (n.warn) parts.push(`${n.warn} with a warning`)
  if (n.skip) parts.push(`${n.skip} skipped`)
  return `${r.ok ? 'Test passed' : 'Test failed'}: ${parts.join(', ')} (${r.took_ms} ms)`
}

/** The text shown when POST /api/admin/connections/test itself fails (not when a check fails). */
export function testErrorText(e: unknown): string {
  if (e instanceof ApiClientError) {
    if (e.status === 429) return 'Too many tests. Wait a minute, then try again.'
    if (e.status === 404) return 'The server no longer has this connection. Reload the page to see the current list.'
    if (e.status === 403) return 'Only admins can test connections.'
  }
  if (isNetworkError(e)) return "Can't reach Piperoom. Check your network, then run the test again."
  return errorText(e)
}

/** True when a thrown value is the server saying this person is not an admin (403 not_allowed or viewer). */
export function isForbidden(e: unknown): boolean {
  return e instanceof ApiClientError && e.status === 403
}
