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

const KIND_ROLE: Record<string, string> = {
  source_db: 'Change source',
  kafka: 'Event stream',
  connect: 'Connectors',
  schema_registry: 'Schemas',
  warehouse: 'Destination',
}

/** What the connection is for, in one or two words. */
export function kindRole(kind: string): string {
  return Object.hasOwn(KIND_ROLE, kind) ? KIND_ROLE[kind] : 'Connection'
}

/** "Process-wide" or "Pipeline orders". */
export function scopeLabel(c: Pick<AdminConnection, 'scope' | 'pipeline_id'>): string {
  return c.scope === 'pipeline' ? `Pipeline ${c.pipeline_id}` : 'Process-wide'
}

/** First letter upper case, underscores as spaces: the fallback label for an id this client does not know. */
function humanize(id: string): string {
  const s = id.replace(/_/g, ' ').trim()
  return s ? s.charAt(0).toUpperCase() + s.slice(1) : id
}

const SETTING_LABEL: Record<string, string> = {
  source_db_url: 'Connection string',
  warehouse_db_url: 'Connection string',
  warehouse_kind: 'Warehouse type',
  kafka_brokers: 'Bootstrap servers',
  kafka_security_protocol: 'Security protocol',
  kafka_sasl_mechanism: 'SASL mechanism',
  kafka_sasl_username: 'SASL username',
  kafka_sasl_password: 'SASL password',
  kafka_tls_ca_file: 'TLS CA certificate',
  kafka_tls_cert_file: 'TLS client certificate',
  kafka_tls_key_file: 'TLS client key',
  kafka_tls_server_name: 'TLS server name',
  connect_url: 'REST endpoint',
  connect_username: 'Username',
  connect_password: 'Password',
  connect_tls_ca_file: 'TLS CA certificate',
  connect_tls_cert_file: 'TLS client certificate',
  connect_tls_key_file: 'TLS client key',
  schema_registry_url: 'Endpoint',
  schema_registry_username: 'Username',
  schema_registry_password: 'Password',
  schema_registry_tls_ca_file: 'TLS CA certificate',
  schema_registry_tls_cert_file: 'TLS client certificate',
  schema_registry_tls_key_file: 'TLS client key',
}

/** The label of a settings key ("kafka_brokers" → "Bootstrap servers"). */
export function settingLabel(key: string): string {
  return Object.hasOwn(SETTING_LABEL, key) ? SETTING_LABEL[key] : humanize(key)
}

const CHECK_LABEL: Record<string, string> = {
  config: 'Configuration',
  reach: 'Network reachability',
  login: 'Authentication',
  read_only: 'Read-only by default',
  statement_timeout: 'Statement timeout',
  read_slots: 'Replication slots readable',
  read_publications: 'Publications readable',
  select_tables: 'Source tables readable',
  can_write: 'No write access',
  select_sink: 'Sink tables readable',
  topics: 'Topics present',
  offsets: 'Offsets readable',
  groups: 'Consumer groups',
  acls: 'Least-privilege ACLs',
  connectors: 'Connectors present',
  config_read: 'Connector config readable',
  subjects: 'Schema subjects',
  compatibility: 'Compatibility level',
  kind: 'Connection type',
}

/** The label of a check id ("read_only" → "Read-only by default"); the id stays available as a tooltip. */
export function checkLabel(id: string): string {
  return Object.hasOwn(CHECK_LABEL, id) ? CHECK_LABEL[id] : humanize(id)
}

/**
 * What the value cell of a settings row shows. A secret is never shown, whatever the server sent:
 * only whether it is set.
 */
export function settingValue(s: ConnectionSetting): { kind: 'secret'; set: boolean } | { kind: 'text'; text: string } {
  if (s.secret) return { kind: 'secret', set: s.set === true }
  return { kind: 'text', text: s.display }
}

/** Where a setting comes from, split for display: `env NAME` → environment variable NAME. */
export function settingSource(from: string): { label: string; name: string } {
  const f = (from ?? '').trim()
  if (f.startsWith('env ')) return { label: 'Environment', name: f.slice(4) }
  if (f.startsWith('file ')) return { label: 'File', name: f.slice(5) }
  if (f === 'value') return { label: 'Owners file', name: '' }
  return { label: f, name: '' }
}

/**
 * The address a person recognises a connection by: its first shown endpoint (an address, set in
 * monospace), else a phrase saying whether its connection string is set.
 */
export function endpointSummary(c: AdminConnection): { text: string; address: boolean } {
  const settings = lines(c.settings)
  const shown = settings.find((s) => !s.secret && s.display && /(_url|_brokers)$/.test(s.key))
  if (shown) return { text: shown.display, address: true }
  const dsn = settings.find((s) => /_db_url$/.test(s.key))
  if (dsn) return { text: dsn.secret && dsn.set ? 'Connection string set (hidden)' : 'Connection string not set', address: false }
  return { text: 'No endpoint reported', address: false }
}

/** "user:priya" → "priya", "group:streaming" → "group streaming": how a person reads an owner. */
export function principalLabel(p: string): string {
  if (p.startsWith('user:')) return p.slice(5)
  if (p.startsWith('group:')) return `group ${p.slice(6)}`
  return p
}

/** Words for a check status, so the result never depends on colour or icon shape alone. */
export const STATUS_LABEL: Record<ProbeStatus, string> = {
  ok: 'passed',
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

const plural = (n: number, one: string, many: string) => `${n} ${n === 1 ? one : many}`

/** The overall state of one connection, as the status pill and the summary strip show it. */
export type ConnectionHealth = 'untested' | 'testing' | 'healthy' | 'warnings' | 'failed' | 'error'

/** The health a finished test means: failed beats warnings beats healthy. */
export function resultHealth(r: ProbeResult): ConnectionHealth {
  if (!r.ok) return 'failed'
  return countChecks(lines(r.checks)).warn > 0 ? 'warnings' : 'healthy'
}

/** The words of a status pill. */
export function healthLabel(h: ConnectionHealth, r?: ProbeResult): string {
  switch (h) {
    case 'untested':
      return 'Not tested'
    case 'testing':
      return 'Testing…'
    case 'healthy':
      return 'Healthy'
    case 'warnings':
      return plural(r ? countChecks(lines(r.checks)).warn : 0, 'warning', 'warnings')
    case 'failed':
      return 'Failed'
    case 'error':
      return 'Test error'
  }
}

/** The one line that announces a finished test: the outcome first, then what was found. */
export function probeSummary(r: ProbeResult): string {
  const checks = lines(r.checks)
  const n = countChecks(checks)
  const took = `${r.took_ms} ms`
  if (!r.ok) {
    const extra = n.warn ? `, ${plural(n.warn, 'warning', 'warnings')}` : ''
    return `Failed: ${n.fail} of ${plural(checks.length, 'check', 'checks')} failed${extra} · ${took}`
  }
  if (n.warn) return `Works, with ${plural(n.warn, 'warning', 'warnings')}: ${n.ok} of ${plural(checks.length, 'check', 'checks')} passed · ${took}`
  return `Healthy: ${n.ok} of ${plural(checks.length, 'check', 'checks')} passed · ${took}`
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
