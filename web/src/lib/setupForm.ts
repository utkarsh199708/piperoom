// The Set up form's values and their rules (docs/api.md POST /api/setup, U1-12a). Pure, so the
// table tests pin exactly what the server receives; room validation is the server's, this is
// only the first pass that saves a round trip.
import type { SetupRequest, SetupTargetInput } from '../api/types'

export const SOURCE_KINDS = ['postgres', 'mysql', 'sqlserver', 'mongodb', 'oracle'] as const
export const SETUP_TARGETS = ['compose', 'host', 'strimzi', 'msk', 'confluent', 'aiven', 'dbzserver', 'embedded'] as const
export const TARGET_KINDS = ['postgres', 'kafka', 'connect', 'registry', 'kubernetes', 'aws', 'confluent', 'aiven', 'host'] as const
export const SETUP_CLASSES = ['local', 'shared', 'production'] as const

/** How a target kind reads in a form; an unknown kind falls back to its own value. */
export const TARGET_KIND_LABELS: Record<string, string> = {
  postgres: 'postgres',
  kafka: 'kafka',
  connect: 'connect',
  registry: 'schema registry',
  kubernetes: 'kubernetes',
  aws: 'aws',
  confluent: 'confluent',
  aiven: 'aiven',
  host: 'host',
}

/** What the empty form starts with: the demo's Compose + Postgres case. */
export const EMPTY_SETUP_FORM: SetupFormValues = {
  tenant: '',
  pipelineId: '',
  title: '',
  sourceKind: 'postgres',
  target: 'compose',
  topicPrefix: '',
  tables: '',
  targets: [{ kind: 'postgres', address: '', class: '' }],
}

export interface SetupTargetValues {
  kind: string
  address: string
  class: string
}

export interface SetupFormValues {
  tenant: string
  pipelineId: string
  title: string
  sourceKind: string
  target: string
  topicPrefix: string
  /** One table per line (a comma also separates): "public.orders". */
  tables: string
  targets: SetupTargetValues[]
}

const PIPELINE_ID = /^[a-zA-Z0-9_.-]+$/
const TOPIC_PREFIX = /^[A-Za-z0-9][A-Za-z0-9._-]*$/
const TABLE = /^[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*$/

/** Split a pasted list on lines and commas, trim, drop blanks, keep the first of a repeat. */
export function splitNames(raw: string): string[] {
  return [...new Set(raw.split(/[\n,]+/).map((s) => s.trim()).filter((s) => s !== ''))]
}

export class SetupFormError extends Error {
  field: string
  constructor(field: string, message: string) {
    super(message)
    this.name = 'SetupFormError'
    this.field = field
  }
}

/** The request POST /api/setup takes, or a SetupFormError naming the field to fix. */
export function setupRequestFrom(values: SetupFormValues): SetupRequest {
  const pipelineId = values.pipelineId.trim()
  const title = values.title.trim()
  const tenant = values.tenant.trim()
  const topicPrefix = values.topicPrefix.trim()
  if (!pipelineId) throw new SetupFormError('pipelineId', 'Pipeline id is required.')
  if (!PIPELINE_ID.test(pipelineId)) {
    throw new SetupFormError('pipelineId', 'Pipeline id may hold letters, digits, ".", "_" and "-" only.')
  }
  if (!title) throw new SetupFormError('title', 'Title is required.')
  if (!values.sourceKind) throw new SetupFormError('sourceKind', 'Choose a source.')
  if (!values.target) throw new SetupFormError('target', 'Choose a target.')
  if (!topicPrefix) throw new SetupFormError('topicPrefix', 'Topic prefix is required.')
  if (!TOPIC_PREFIX.test(topicPrefix)) {
    throw new SetupFormError('topicPrefix', 'Topic prefix must start with a letter or digit and hold letters, digits, ".", "_" and "-".')
  }
  const tables = splitNames(values.tables)
  if (tables.length === 0) throw new SetupFormError('tables', 'Name at least one source table (schema.table).')
  for (const table of tables) {
    if (!TABLE.test(table)) throw new SetupFormError('tables', `"${table}" is not a schema.table name.`)
  }
  const targets: SetupTargetInput[] = []
  for (const [i, t] of values.targets.entries()) {
    const address = t.address.trim()
    if (!t.kind) throw new SetupFormError(`target-${i}-kind`, 'Choose the target kind.')
    if (!address) throw new SetupFormError(`target-${i}-address`, 'Target address is required.')
    targets.push({ kind: t.kind, address, ...(t.class ? { class: t.class } : {}) })
  }
  return {
    pipeline_id: pipelineId,
    title,
    source_kind: values.sourceKind,
    target: values.target,
    topic_prefix: topicPrefix,
    tables,
    targets,
    ...(tenant ? { tenant } : {}),
  }
}
