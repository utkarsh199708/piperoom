// Pure logic of the Integrations page: form state from a card schema, the write body,
// secret:// references and the setup checklist rows. No React, no fetch.
import { CATEGORIES, isSecretField } from '../api/integrations'
import type {
  CatalogKind,
  Category,
  Check,
  Field,
  Integration,
  SecretWrite,
  WriteBody,
} from '../api/integrations'

export const CATEGORY_LABEL: Record<Category, string> = {
  source_db: 'Source database',
  kafka: 'Kafka',
  connect: 'Kafka Connect',
  registry: 'Schema Registry',
  warehouse: 'Warehouse',
  model: 'AI model',
  alerts: 'Alerts',
  chat: 'Chat',
  secret_manager: 'Secret managers',
}

export const REF_PREFIX = 'secret://'

// ---- secret:// references ----

export interface SecretRef {
  provider: string
  /** The secret's name inside the provider, without a leading slash. */
  path: string
  /** `#key` of a JSON / key-value secret; "" = the whole secret. */
  key: string
  /** `?version=v`; "" = latest. */
  version: string
}

/** Parse `secret://<provider>/<path>[#key][?version=v]`; null when it is not one. */
export function parseRef(ref: string): SecretRef | null {
  const s = ref.trim()
  if (!s.startsWith(REF_PREFIX)) return null
  let rest = s.slice(REF_PREFIX.length)
  let version = ''
  const q = rest.indexOf('?')
  if (q >= 0) {
    version = new URLSearchParams(rest.slice(q + 1)).get('version') ?? ''
    rest = rest.slice(0, q)
  }
  let key = ''
  const h = rest.indexOf('#')
  if (h >= 0) {
    key = rest.slice(h + 1)
    rest = rest.slice(0, h)
  }
  const slash = rest.indexOf('/')
  if (slash <= 0) return null
  const provider = rest.slice(0, slash)
  const path = rest.slice(slash + 1)
  if (!path) return null
  return { provider, path, key, version }
}

/** The reference text for its parts. */
export function buildRef({ provider, path, key, version }: SecretRef): string {
  let s = `${REF_PREFIX}${provider}/${path.replace(/^\/+/, '')}`
  if (key) s += `#${key}`
  if (version) s += `?version=${encodeURIComponent(version)}`
  return s
}

/** The `secret://<provider>/<path>` part, no key or version (what the keys endpoint takes). */
export function refBase(ref: SecretRef): string {
  return buildRef({ ...ref, key: '', version: '' })
}

function isRefValue(v: unknown): v is { $ref: string } {
  return typeof v === 'object' && v !== null && typeof (v as { $ref?: unknown }).$ref === 'string'
}

// ---- form state ----

export type SecretMode = 'manager' | 'value'

export interface SecretFieldState {
  mode: SecretMode
  /** Typed-in value (write-only; never filled from the server). */
  value: string
  /** secret:// reference, in manager mode. */
  ref: string
  /** The row has a typed-in value for this field ("set"). */
  stored: boolean
  /** The row has any value for this field: typed-in or a secret:// reference. */
  hasStored: boolean
  /** The person asked to remove the stored value (sent as `null`, in either mode). */
  clear: boolean
}

export interface FormState {
  config: Record<string, string | boolean>
  secrets: Record<string, SecretFieldState>
}

/** The secret-manager rows a reference can name, by their `name` (provider id). */
export function secretManagers(list: Integration[]): Integration[] {
  return list.filter((i) => i.category === 'secret_manager' && managerName(i) !== '')
}

export function managerName(i: Integration): string {
  const n = i.config.name
  return typeof n === 'string' ? n : ''
}

/**
 * The form for a card: the stored row's values when editing, else the field defaults.
 * A secret field starts in "From a secret manager" when it is a reference, or when it has
 * no stored value and a secret manager is connected (design: the vault is the first option).
 */
export function initialForm(kind: CatalogKind, existing: Integration | undefined, hasManagers: boolean): FormState {
  const config: FormState['config'] = {}
  const secrets: FormState['secrets'] = {}
  for (const f of kind.fields) {
    const stored = existing?.config[f.name]
    if (isSecretField(f.type)) {
      const isRef = isRefValue(stored)
      const typed = !isRef && (existing?.secrets_set ?? []).includes(f.name)
      secrets[f.name] = {
        mode: isRef || (!typed && hasManagers) ? 'manager' : 'value',
        value: '',
        ref: isRef ? stored.$ref : '',
        stored: typed,
        hasStored: typed || isRef,
        clear: false,
      }
      continue
    }
    if (f.type === 'bool') {
      config[f.name] = stored === undefined || isRefValue(stored) ? f.default === 'true' : stored === true || stored === 'true'
    } else {
      config[f.name] = stored === undefined || isRefValue(stored) ? f.default : String(stored)
    }
  }
  return { config, secrets }
}

/** The non-secret values in their JSON types; empty optional fields are left out. */
export function configBody(kind: CatalogKind, form: FormState): WriteBody['config'] {
  const out: WriteBody['config'] = {}
  for (const f of kind.fields) {
    if (isSecretField(f.type)) continue
    const v = form.config[f.name]
    if (f.type === 'bool') {
      out[f.name] = v === true
      continue
    }
    const s = typeof v === 'string' ? v.trim() : ''
    if (s === '') continue
    if (f.type === 'number') {
      const n = Number(s)
      // A non-number goes as text so the server answers 400 validation for this field.
      out[f.name] = Number.isFinite(n) ? n : s
    } else {
      out[f.name] = s
    }
  }
  return out
}

/** One `secrets.<field>`, or undefined to leave it out (= keep the stored value). */
export function secretWrite(s: SecretFieldState): SecretWrite | undefined {
  if (s.clear) return null
  if (s.mode === 'manager') {
    const ref = s.ref.trim()
    return ref ? { $ref: ref } : undefined
  }
  return s.value !== '' ? s.value : undefined
}

/** The write body of test, create and update (docs/api.md, Write body). */
export function buildWriteBody(kind: CatalogKind, form: FormState, id?: string): WriteBody {
  const secrets: WriteBody['secrets'] = {}
  for (const f of kind.fields) {
    if (!isSecretField(f.type)) continue
    const s = form.secrets[f.name]
    if (!s) continue
    const w = secretWrite(s)
    if (w !== undefined) secrets[f.name] = w
  }
  const body: WriteBody = { kind: kind.id, config: configBody(kind, form), secrets }
  if (id) body.id = id
  return body
}

/** Does a secret field have a value after this edit (typed, referenced, or kept)? */
function secretHasValue(s: SecretFieldState | undefined, existing: Integration | undefined, name: string): boolean {
  if (!s) return false
  if (s.clear) return false
  if (s.mode === 'manager') return s.ref.trim() !== '' || isRefValue(existing?.config[name])
  return s.value !== '' || s.stored || isRefValue(existing?.config[name])
}

function patternOk(pattern: string, value: string): boolean {
  if (!pattern) return true
  try {
    return new RegExp(pattern).test(value)
  } catch {
    // RE2 syntax JS does not know: the server checks it.
    return true
  }
}

/**
 * Fields that pick where a credential is sent (address, port, TLS, SASL). The server
 * refuses to send a stored typed-in secret once one of them changes (integrations.md §8),
 * so the form asks for the secret again. Keep in step with destinationFields in
 * internal/integrations/values.go.
 */
const DESTINATION_FIELDS = new Set([
  'url', 'host', 'port', 'brokers', 'bootstrap', 'account', 'account_id', 'address', 'vault_url',
  'server_name', 'region', 'tenant_id', 'project', 'sslmode', 'tls', 'protocol', 'mechanism',
])

function normField(f: Field, v: unknown): string {
  if (v === undefined || v === null || isRefValue(v) || String(v).trim() === '') return f.default ?? ''
  if (f.type === 'bool') return v === true || v === 'true' ? 'true' : 'false'
  return String(v).trim()
}

/** The first destination field the edit changes, or '' (always '' when creating). */
export function changedDestination(kind: CatalogKind, form: FormState, existing?: Integration): string {
  if (!existing) return ''
  for (const f of kind.fields) {
    if (isSecretField(f.type) || !DESTINATION_FIELDS.has(f.name)) continue
    if (normField(f, form.config[f.name]) !== normField(f, existing.config[f.name])) return f.name
  }
  return ''
}

/** Client-side checks before a call; the server enforces the same rules (400 validation). */
export function validateForm(kind: CatalogKind, form: FormState, existing?: Integration): Record<string, string> {
  const errors: Record<string, string> = {}
  const moved = changedDestination(kind, form, existing)
  for (const f of kind.fields) {
    if (isSecretField(f.type)) {
      const s = form.secrets[f.name]
      if (s?.mode === 'manager' && s.ref.trim() !== '' && !parseRef(s.ref)) {
        errors[f.name] = 'A reference looks like secret://<manager>/<name>#<key>'
      } else if (moved && s?.stored && !s.clear && secretWrite(s) === undefined) {
        errors[f.name] = `Enter it again: ${moved} changed, so the stored value is not sent to the new address`
      } else if (f.required && !secretHasValue(s, existing, f.name)) {
        errors[f.name] = 'Required'
      }
      continue
    }
    if (f.type === 'bool') continue
    const v = String(form.config[f.name] ?? '').trim()
    if (kind.category === 'secret_manager' && f.name === 'name' && existing && managerName(existing) !== '' && v !== managerName(existing)) {
      errors[f.name] = 'The name cannot be changed. Add a new secret manager with the new name, then delete this one.'
      continue
    }
    if (v === '') {
      if (f.required) errors[f.name] = 'Required'
      continue
    }
    if (f.type === 'number' && !Number.isFinite(Number(v))) errors[f.name] = 'Enter a number'
    else if (f.type === 'select' && f.options.length > 0 && !f.options.includes(v)) errors[f.name] = 'Pick one of the options'
    else if (!patternOk(f.pattern, v)) errors[f.name] = 'This value has the wrong format'
  }
  return errors
}

// ---- checks ----

export function anyFailed(checks: Check[]): boolean {
  return checks.some((c) => c.status === 'fail')
}

export function countFailed(checks: Check[]): number {
  return checks.filter((c) => c.status === 'fail').length
}

/** A passing read-only check and no failure: the card earns the "verified read-only" badge. */
export function verifiedReadOnly(i: Integration): boolean {
  return !anyFailed(i.last_test) && i.last_test.some((c) => c.id.includes('read_only') && c.status === 'pass')
}

// ---- setup checklist ----

export type RowStatus = 'not_started' | 'testing' | 'connected' | 'attention' | 'disabled' | 'untested'

export interface ChecklistRow {
  category: Category
  label: string
  /** Needed for a first room (any kind of the category is `required`). */
  required: boolean
  kinds: CatalogKind[]
  /** The rows of this category (alerts may have one per source). */
  integrations: Integration[]
  status: RowStatus
  /** Set by environment variables: read-only here. */
  managedByEnv: boolean
  readOnlyVerified: boolean
}

const STATUS_RANK: Record<RowStatus, number> = {
  attention: 0,
  testing: 1,
  untested: 2,
  disabled: 3,
  not_started: 4,
  connected: 5,
}

/** A stored row's server status as a pill status. */
export function viewStatus(i: Integration): RowStatus {
  switch (i.status) {
    case 'connected':
      return 'connected'
    case 'failing':
      return 'attention'
    case 'disabled':
      return 'disabled'
    default:
      return 'untested'
  }
}

/**
 * One row per category of the catalog, in spec order. Secret managers are not a row:
 * they are listed apart, since any number can be connected. A category with several
 * rows (alerts) is connected when any is, and needs attention when any fails.
 */
export function checklistRows(catalog: CatalogKind[], list: Integration[], testing: Set<Category> = new Set()): ChecklistRow[] {
  const rows: ChecklistRow[] = []
  for (const category of CATEGORIES) {
    if (category === 'secret_manager') continue
    const kinds = catalog.filter((k) => k.category === category)
    if (kinds.length === 0) continue
    const integrations = list.filter((i) => i.category === category)
    let status: RowStatus = 'not_started'
    if (integrations.length > 0) {
      const statuses = integrations.map(viewStatus)
      status = statuses.includes('attention')
        ? 'attention'
        : statuses.includes('connected')
          ? 'connected'
          : statuses.reduce((a, b) => (STATUS_RANK[a] <= STATUS_RANK[b] ? a : b))
    }
    if (testing.has(category)) status = 'testing'
    rows.push({
      category,
      label: CATEGORY_LABEL[category],
      required: kinds.some((k) => k.required),
      kinds,
      integrations,
      status,
      managedByEnv: integrations.some((i) => i.source === 'env'),
      readOnlyVerified: integrations.length > 0 && integrations.every(verifiedReadOnly),
    })
  }
  return rows
}

export interface ChecklistProgress {
  connected: number
  total: number
  /** Required rows that are not connected yet, by label. */
  missingRequired: string[]
}

export function checklistProgress(rows: ChecklistRow[]): ChecklistProgress {
  return {
    connected: rows.filter((r) => r.status === 'connected').length,
    total: rows.length,
    missingRequired: rows.filter((r) => r.required && r.status !== 'connected').map((r) => r.label),
  }
}

/** "A", "A and B", "A, B and C". */
export function joinAnd(items: string[]): string {
  if (items.length <= 1) return items.join('')
  return `${items.slice(0, -1).join(', ')} and ${items[items.length - 1]}`
}

/**
 * The stored row a card edits: alerts by kind (one per source), the rest by category (one
 * per workspace). Secret managers: undefined, since many of one kind can be connected; each
 * is opened from its own row.
 */
export function integrationForKind(kind: CatalogKind, list: Integration[]): Integration | undefined {
  if (kind.category === 'secret_manager') return undefined
  if (kind.category === 'alerts') return list.find((i) => i.kind === kind.id)
  return list.find((i) => i.category === kind.category)
}

/** `alerts-grafana` → `grafana`; "" for any other kind. */
export function alertSourceOf(kindId: string): string {
  return kindId.startsWith('alerts-') ? kindId.slice('alerts-'.length) : ''
}

export function fieldById(kind: CatalogKind, name: string): Field | undefined {
  return kind.fields.find((f) => f.name === name)
}
