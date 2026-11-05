// The params form of "Propose a fix yourself" (P3d-1): reads an action's `params_schema` (GET
// /api/actions, a JSON Schema object) and turns it into form fields and back into params. Pure, so
// the tests pin exactly what the server receives. Only string, integer, boolean, enum and string
// arrays get a field in this wave; any other property is edited as JSON, and a schema that is not a
// plain object of properties is edited as one JSON document. The server validates again (Validate,
// CheckScope): this is the first pass that saves a round trip, never the safety boundary.

/** An action's ParamsSchema as the server sends it: a JSON Schema object. */
export type ParamsSchema = Record<string, unknown>

export type FieldKind = 'string' | 'integer' | 'boolean' | 'enum' | 'string[]' | 'json'

export interface ParamField {
  name: string
  kind: FieldKind
  required: boolean
  description?: string
  /** 'enum': the allowed values, in schema order (strings, numbers or booleans). */
  options?: (string | number | boolean)[]
  /** 'string' and 'string[]' (per item): the schema's pattern, when the browser can compile it. */
  pattern?: string
  minimum?: number
  maximum?: number
  minItems?: number
  maxItems?: number
  minLength?: number
  maxLength?: number
}

/** The form of one schema: one field per property, or the whole params as one JSON document. */
export type ParamsFormSpec = { mode: 'fields'; fields: ParamField[] } | { mode: 'json' }

/**
 * Field values as the inputs hold them: text for every text input (an integer as typed, an enum as
 * the index of its option, a string array one item per line, JSON as typed), a boolean for a
 * checkbox. In 'json' mode the one key is JSON_KEY.
 */
export type ParamsValues = Record<string, string | boolean>

export const JSON_KEY = ''

function isRecord(v: unknown): v is Record<string, unknown> {
  return typeof v === 'object' && v !== null && !Array.isArray(v)
}

function num(v: unknown): number | undefined {
  return typeof v === 'number' && Number.isFinite(v) ? v : undefined
}

function str(v: unknown): string | undefined {
  return typeof v === 'string' && v !== '' ? v : undefined
}

/** A pattern the browser cannot compile (Go RE2 syntax JS lacks) is left to the server. */
function usablePattern(v: unknown): string | undefined {
  if (typeof v !== 'string' || v === '') return undefined
  try {
    new RegExp(v, 'u')
    return v
  } catch {
    return undefined
  }
}

// A property's `type` may be a list ("string" or ["string","null"]); only a single type, or one
// type plus "null", maps to a field.
function singleType(t: unknown): string | undefined {
  if (typeof t === 'string') return t
  if (Array.isArray(t)) {
    const rest = t.filter((x) => x !== 'null')
    if (rest.length === 1 && typeof rest[0] === 'string') return rest[0]
  }
  return undefined
}

// Keywords that change what a value may be in ways a plain field cannot show.
const COMBINATORS = ['oneOf', 'anyOf', 'allOf', 'not', 'if', '$ref', 'const']

function fieldFor(name: string, prop: unknown, required: boolean): ParamField {
  const base: ParamField = { name, kind: 'json', required }
  if (!isRecord(prop)) return base
  const description = str(prop.description)
  if (description) base.description = description
  if (COMBINATORS.some((k) => k in prop)) return base

  if (Array.isArray(prop.enum)) {
    const opts = prop.enum.filter(
      (v): v is string | number | boolean => typeof v === 'string' || typeof v === 'number' || typeof v === 'boolean',
    )
    if (opts.length > 0 && opts.length === prop.enum.length) return { ...base, kind: 'enum', options: opts }
    return base
  }

  switch (singleType(prop.type)) {
    case 'string':
      return {
        ...base,
        kind: 'string',
        pattern: usablePattern(prop.pattern),
        minLength: num(prop.minLength),
        maxLength: num(prop.maxLength),
      }
    case 'integer':
      return { ...base, kind: 'integer', minimum: num(prop.minimum), maximum: num(prop.maximum) }
    case 'boolean':
      return { ...base, kind: 'boolean' }
    case 'array': {
      const items = prop.items
      if (!isRecord(items) || singleType(items.type) !== 'string') return base
      if (COMBINATORS.some((k) => k in items) || Array.isArray(items.enum)) return base
      return {
        ...base,
        kind: 'string[]',
        pattern: usablePattern(items.pattern),
        minItems: num(prop.minItems),
        maxItems: num(prop.maxItems),
      }
    }
    default:
      return base
  }
}

/** The form for one schema. Properties keep the schema's order; the required ones are marked. */
export function formSpec(schema: unknown): ParamsFormSpec {
  if (!isRecord(schema)) return { mode: 'json' }
  if (COMBINATORS.some((k) => k in schema)) return { mode: 'json' }
  if (schema.type !== undefined && schema.type !== 'object') return { mode: 'json' }
  const props = schema.properties
  if (!isRecord(props) || Object.keys(props).length === 0) return { mode: 'json' }
  const required = new Set(Array.isArray(schema.required) ? schema.required.filter((r) => typeof r === 'string') : [])
  // A required name with no property would never get a field, so the form could never be valid.
  for (const r of required) if (!(r in props)) return { mode: 'json' }
  return { mode: 'fields', fields: Object.entries(props).map(([n, p]) => fieldFor(n, p, required.has(n))) }
}

/** The values an empty form starts with: each property's `default` where the schema gives one. */
export function initialValues(schema: unknown, spec: ParamsFormSpec = formSpec(schema)): ParamsValues {
  if (spec.mode === 'json') return { [JSON_KEY]: '{}' }
  const props = isRecord(schema) && isRecord(schema.properties) ? schema.properties : {}
  const out: ParamsValues = {}
  for (const f of spec.fields) {
    const p = props[f.name]
    const d = isRecord(p) ? p.default : undefined
    switch (f.kind) {
      case 'boolean':
        out[f.name] = d === true
        break
      case 'enum': {
        const i = d === undefined ? -1 : (f.options ?? []).findIndex((o) => o === d)
        out[f.name] = i >= 0 ? String(i) : ''
        break
      }
      case 'string[]':
        out[f.name] = Array.isArray(d) ? d.filter((x) => typeof x === 'string').join('\n') : ''
        break
      case 'json':
        out[f.name] = d === undefined ? '' : JSON.stringify(d, null, 2)
        break
      default:
        out[f.name] = typeof d === 'string' || typeof d === 'number' ? String(d) : ''
    }
  }
  return out
}

/** One item per line; a comma does not split, since a value may hold one. Blank lines are dropped. */
export function splitLines(raw: string): string[] {
  return raw
    .split('\n')
    .map((s) => s.trim())
    .filter((s) => s !== '')
}

export type ParamsResult =
  | { ok: true; params: Record<string, unknown> }
  | { ok: false; errors: Record<string, string> }

function text(values: ParamsValues, name: string): string {
  const v = values[name]
  return typeof v === 'string' ? v : ''
}

function matches(pattern: string, s: string): boolean {
  return new RegExp(pattern, 'u').test(s)
}

/**
 * The params object the server receives, or one message per field to fix. An optional field left
 * empty (or an optional checkbox left off) is omitted, so the action's own default applies.
 */
export function paramsFrom(spec: ParamsFormSpec, values: ParamsValues): ParamsResult {
  if (spec.mode === 'json') {
    const raw = text(values, JSON_KEY).trim()
    let parsed: unknown
    try {
      parsed = JSON.parse(raw === '' ? '{}' : raw)
    } catch (e) {
      return { ok: false, errors: { [JSON_KEY]: `Not valid JSON: ${(e as Error).message}` } }
    }
    if (!isRecord(parsed)) return { ok: false, errors: { [JSON_KEY]: 'Params must be a JSON object ({ … }).' } }
    return { ok: true, params: parsed }
  }

  const params: Record<string, unknown> = {}
  const errors: Record<string, string> = {}
  for (const f of spec.fields) {
    if (f.kind === 'boolean') {
      const on = values[f.name] === true
      if (on || f.required) params[f.name] = on
      continue
    }
    const raw = text(values, f.name)
    const trimmed = f.kind === 'json' || f.kind === 'string[]' ? raw.trim() : raw
    if (trimmed.trim() === '') {
      if (f.required) errors[f.name] = 'Required.'
      continue
    }
    switch (f.kind) {
      case 'string': {
        const s = trimmed.trim()
        if (f.minLength !== undefined && s.length < f.minLength) errors[f.name] = `At least ${f.minLength} characters.`
        else if (f.maxLength !== undefined && s.length > f.maxLength) errors[f.name] = `At most ${f.maxLength} characters.`
        else if (f.pattern && !matches(f.pattern, s)) errors[f.name] = 'Does not match the expected format.'
        else params[f.name] = s
        break
      }
      case 'integer': {
        const s = trimmed.trim()
        if (!/^-?\d+$/.test(s) || !Number.isSafeInteger(Number(s))) {
          errors[f.name] = 'Must be a whole number.'
          break
        }
        const n = Number(s)
        if (f.minimum !== undefined && n < f.minimum) errors[f.name] = `Must be at least ${f.minimum}.`
        else if (f.maximum !== undefined && n > f.maximum) errors[f.name] = `Must be at most ${f.maximum}.`
        else params[f.name] = n
        break
      }
      case 'enum': {
        const opt = (f.options ?? [])[Number(trimmed)]
        if (opt === undefined || !/^\d+$/.test(trimmed)) errors[f.name] = 'Choose one of the options.'
        else params[f.name] = opt
        break
      }
      case 'string[]': {
        const items = splitLines(trimmed)
        const bad = f.pattern ? items.find((s) => !matches(f.pattern as string, s)) : undefined
        if (bad !== undefined) errors[f.name] = `"${bad}" does not match the expected format.`
        else if (f.minItems !== undefined && items.length < f.minItems) errors[f.name] = `At least ${f.minItems} item${f.minItems === 1 ? '' : 's'}.`
        else if (f.maxItems !== undefined && items.length > f.maxItems) errors[f.name] = `At most ${f.maxItems} item${f.maxItems === 1 ? '' : 's'}.`
        else params[f.name] = items
        break
      }
      case 'json': {
        try {
          params[f.name] = JSON.parse(trimmed)
        } catch (e) {
          errors[f.name] = `Not valid JSON: ${(e as Error).message}`
        }
        break
      }
    }
  }
  return Object.keys(errors).length > 0 ? { ok: false, errors } : { ok: true, params }
}
