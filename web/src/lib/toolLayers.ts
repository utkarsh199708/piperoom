// Which pipeline layer a tool belongs to, from its name prefix (connect_status ->
// kafka-connect). The timeline shows the tool as a badge in that layer's colour.
// An unknown prefix gives '' and the badge stays neutral.
const PREFIXES: [string, string][] = [
  ['connect_', 'kafka-connect'],
  ['schema_', 'schema-registry'],
  ['pg_', 'postgres'],
  ['mysql_', 'mysql'],
  ['kafka_', 'kafka'],
  ['consumer_', 'consumer'],
  ['warehouse_', 'warehouse'],
  ['wh_', 'warehouse'],
  ['dag_', 'orchestrator'],
  ['dbt_', 'transform'],
]

export function toolLayer(tool: string): string {
  const hit = PREFIXES.find(([prefix]) => tool.startsWith(prefix))
  return hit ? hit[1] : ''
}

/** "key=value" tags for a tool input: `{connector: "x"}` -> `["connector=x"]`. Long values are cut. */
export function argTags(input: unknown, maxLen = 48): string[] {
  if (input == null) return []
  if (typeof input !== 'object' || Array.isArray(input)) return [cut(stringify(input), maxLen)]
  return Object.entries(input as Record<string, unknown>).map(([k, v]) => `${k}=${cut(stringify(v), maxLen)}`)
}

function stringify(v: unknown): string {
  if (typeof v === 'string') return v
  try {
    return JSON.stringify(v) ?? String(v)
  } catch {
    return String(v)
  }
}

function cut(s: string, max: number): string {
  const flat = s.replace(/\s+/g, ' ')
  return flat.length > max ? `${flat.slice(0, max - 1)}…` : flat
}

/** 412 -> "412 ms", 1234 -> "1.2 s". */
export function formatMs(ms: number): string {
  return ms >= 1000 ? `${(ms / 1000).toFixed(1)} s` : `${Math.round(ms)} ms`
}
