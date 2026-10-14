// Integrations API (docs/api.md, "Integrations"). Mirrors internal/integrations/spec
// (contract file): field names and JSON match the Go structs exactly.
import { request } from './client'

export type Category =
  | 'source_db'
  | 'kafka'
  | 'connect'
  | 'registry'
  | 'warehouse'
  | 'model'
  | 'alerts'
  | 'chat'
  | 'secret_manager'

/** spec.Categories order: the order of the checklist rows and the catalog groups. */
export const CATEGORIES: Category[] = [
  'source_db',
  'kafka',
  'connect',
  'registry',
  'warehouse',
  'model',
  'alerts',
  'chat',
  'secret_manager',
]

export type FieldType = 'text' | 'secret' | 'number' | 'select' | 'bool' | 'pem' | 'json_file'

/** spec.FieldType.IsSecret: these fields are write-only. */
export function isSecretField(t: FieldType): boolean {
  return t === 'secret' || t === 'pem' || t === 'json_file'
}

export interface Field {
  name: string
  label: string
  help: string
  type: FieldType
  required: boolean
  default: string
  options: string[]
  /** An RE2 regexp the server enforces; "" = none. */
  pattern: string
  /**
   * A non-secret field that may hold a `secret://` reference instead of text (the user of a
   * Vault database lease); sent as `{"$ref": …}` in config.
   */
  allow_ref?: boolean
}

export interface CatalogKind {
  id: string
  category: Category
  title: string
  summary: string
  logo: string
  reads: string[]
  never: string[]
  fields: Field[]
  /** The category is needed for a first room (setup checklist). */
  required: boolean
  has_setup_script: boolean
}

export type CheckStatus = 'pass' | 'warn' | 'fail' | 'skipped'

export interface Check {
  id: string
  title: string
  status: CheckStatus
  detail: string
  fix: string
}

export type IntegrationStatus = 'connected' | 'failing' | 'disabled' | 'untested'

/** A secret field held as a reference: `{"$ref": "secret://…"}`. */
export interface SecretRefValue {
  $ref: string
}

export type ConfigValue = string | number | boolean | SecretRefValue

export interface Integration {
  id: string
  workspace_id: string
  category: Category
  kind: string
  title: string
  config: Record<string, ConfigValue>
  secrets_set: string[]
  version: number
  enabled: boolean
  status: IntegrationStatus
  last_test: Check[]
  last_tested_at: string | null
  updated_by: string
  updated_at: string
  source: 'db' | 'env'
}

export type AuditAction =
  | 'created'
  | 'updated'
  | 'tested'
  | 'enabled'
  | 'disabled'
  | 'deleted'
  | 'secret_rotated'

export interface AuditEntry {
  seq: number
  workspace_id: string
  integration_id: string
  action: AuditAction
  actor: string
  changed: string[]
  at: string
}

export type AlertSource = 'alertmanager' | 'grafana' | 'pagerduty' | 'datadog' | 'generic'

export interface AlertSetup {
  source: AlertSource
  integration_id: string
  webhook_url: string
  snippet: string
  snippet_lang: string
  secret_set: boolean
  secret_source: 'db' | 'env' | ''
}

/**
 * One `secrets.<field>` of a write body: a string = typed-in value, `{"$ref"}` = reference,
 * `null` = clear. A field left out keeps the stored value.
 */
export type SecretWrite = string | SecretRefValue | null

export interface WriteBody {
  kind: string
  /** POST /test only: stored secrets of this row fill the secret fields the body leaves out. */
  id?: string
  config: Record<string, string | number | boolean | SecretRefValue>
  secrets: Record<string, SecretWrite>
}

/** The body of a 422 checks_failed answer. */
export interface ChecksFailedBody {
  error: { code: string; message: string }
  checks: Check[]
}

const base = '/api/integrations'
const enc = encodeURIComponent

export const integrationsApi = {
  catalog: () => request<CatalogKind[]>(`${base}/catalog`),
  setupScript: (kind: string, config: WriteBody['config']) =>
    request<{ script: string }>(`${base}/catalog/${enc(kind)}/setup-script`, {
      method: 'POST',
      body: JSON.stringify({ config }),
    }),
  list: () => request<Integration[]>(base),
  /** Whether typed-in secrets can be saved (PIPEROOM_SECRET_KEY is set). */
  status: () => request<{ secret_key_set: boolean }>(`${base}/status`),
  test: (body: WriteBody) =>
    request<{ checks: Check[] }>(`${base}/test`, { method: 'POST', body: JSON.stringify(body) }),
  create: (body: WriteBody) =>
    request<{ integration: Integration; checks: Check[] }>(base, {
      method: 'POST',
      body: JSON.stringify(body),
    }),
  update: (id: string, version: number, body: WriteBody) =>
    request<{ integration: Integration; checks: Check[] }>(`${base}/${enc(id)}`, {
      method: 'PUT',
      headers: { 'If-Match': String(version) },
      body: JSON.stringify(body),
    }),
  setEnabled: (id: string, enabled: boolean) =>
    request<Integration>(`${base}/${enc(id)}/${enabled ? 'enable' : 'disable'}`, { method: 'POST' }),
  remove: (id: string) => request<void>(`${base}/${enc(id)}`, { method: 'DELETE' }),
  audit: (id: string, limit = 100) =>
    request<AuditEntry[]>(`${base}/${enc(id)}/audit?limit=${limit}`),
  alertSetup: (source: string) => request<AlertSetup>(`${base}/alerts/${enc(source)}/setup`),
  rotateAlertSecret: (source: string) =>
    request<AlertSetup & { secret: string; restart_required?: boolean }>(`${base}/alerts/${enc(source)}/rotate`, {
      method: 'POST',
    }),
  secretNames: (id: string, prefix: string) =>
    request<{ names: string[]; list_supported: boolean }>(
      `${base}/${enc(id)}/secrets?prefix=${enc(prefix)}`,
    ),
  secretKeys: (id: string, ref: string) =>
    request<{ keys: string[] }>(`${base}/${enc(id)}/secrets/keys?ref=${enc(ref)}`),
}

export type IntegrationsApi = typeof integrationsApi
