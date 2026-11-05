import { describe, expect, it } from 'vitest'
import { JSON_KEY, formSpec, initialValues, paramsFrom, splitLines } from './paramsForm'

describe('formSpec', () => {
  it.each([
    ['string', { type: 'string' }, 'string'],
    ['integer', { type: 'integer', minimum: 0 }, 'integer'],
    ['boolean', { type: 'boolean' }, 'boolean'],
    ['enum', { type: 'string', enum: ['a', 'b'] }, 'enum'],
    ['string[]', { type: 'array', items: { type: 'string' } }, 'string[]'],
    ['nullable string', { type: ['string', 'null'] }, 'string'],
    ['number (not in this wave)', { type: 'number' }, 'json'],
    ['object', { type: 'object', properties: { a: { type: 'string' } } }, 'json'],
    ['array of integers', { type: 'array', items: { type: 'integer' } }, 'json'],
    ['oneOf', { oneOf: [{ type: 'string' }, { type: 'integer' }] }, 'json'],
    ['no type', {}, 'json'],
    ['enum with an object', { enum: ['a', { b: 1 }] }, 'json'],
  ])('%s maps to %s', (_name, prop, kind) => {
    const spec = formSpec({ type: 'object', properties: { x: prop } })
    expect(spec.mode).toBe('fields')
    if (spec.mode === 'fields') expect(spec.fields[0].kind).toBe(kind)
  })

  it.each([
    ['not an object', 'nope'],
    ['no properties', { type: 'object' }],
    ['empty properties', { type: 'object', properties: {} }],
    ['top-level oneOf', { oneOf: [{ type: 'object' }] }],
    ['not type object', { type: 'array' }],
    ['required name without a property', { type: 'object', properties: { a: { type: 'string' } }, required: ['b'] }],
  ])('%s is edited as one JSON document', (_name, schema) => {
    expect(formSpec(schema)).toEqual({ mode: 'json' })
  })

  it('keeps property order, marks required fields and keeps descriptions', () => {
    const spec = formSpec({
      type: 'object',
      properties: { connector: { type: 'string', description: 'connector name' }, force: { type: 'boolean' } },
      required: ['connector'],
    })
    expect(spec).toEqual({
      mode: 'fields',
      fields: [
        { name: 'connector', kind: 'string', required: true, description: 'connector name', pattern: undefined, minLength: undefined, maxLength: undefined },
        { name: 'force', kind: 'boolean', required: false },
      ],
    })
  })

  it('drops a pattern the browser cannot compile and leaves it to the server', () => {
    const spec = formSpec({ type: 'object', properties: { a: { type: 'string', pattern: '(?P<x>a)' } } })
    expect(spec.mode === 'fields' && spec.fields[0].pattern).toBeUndefined()
  })
})

describe('initialValues', () => {
  it('takes each default and leaves the rest empty', () => {
    const schema = {
      type: 'object',
      properties: {
        s: { type: 'string', default: 'x' },
        n: { type: 'integer', default: 3 },
        b: { type: 'boolean', default: true },
        e: { enum: ['low', 'high'], default: 'high' },
        l: { type: 'array', items: { type: 'string' }, default: ['a', 'b'] },
        j: { type: 'object', default: { k: 1 } },
        none: { type: 'string' },
      },
    }
    expect(initialValues(schema)).toEqual({ s: 'x', n: '3', b: true, e: '1', l: 'a\nb', j: '{\n  "k": 1\n}', none: '' })
  })

  it('starts the JSON fallback with an empty object', () => {
    expect(initialValues('nope')).toEqual({ [JSON_KEY]: '{}' })
  })
})

describe('splitLines', () => {
  it('splits on lines only, trims and drops blanks', () => {
    expect(splitLines(' a \n\nb,c\n  ')).toEqual(['a', 'b,c'])
  })
})

describe('paramsFrom', () => {
  const schema = {
    type: 'object',
    properties: {
      connector: { type: 'string', pattern: '^[a-z-]+$' },
      partitions: { type: 'integer', minimum: 1, maximum: 10 },
      force: { type: 'boolean' },
      mode: { enum: ['earliest', 'latest', 5] },
      tables: { type: 'array', items: { type: 'string', pattern: '^[a-z]+\\.[a-z]+$' }, minItems: 1, maxItems: 2 },
      range: { type: 'object' },
    },
    required: ['connector', 'tables'],
  }
  const spec = formSpec(schema)
  const base = { ...initialValues(schema), connector: 'orders-src', tables: 'public.orders' }

  it('builds the params and omits optional fields left empty', () => {
    expect(paramsFrom(spec, base)).toEqual({ ok: true, params: { connector: 'orders-src', tables: ['public.orders'] } })
  })

  it('converts every kind', () => {
    const r = paramsFrom(spec, { ...base, partitions: ' 4 ', force: true, mode: '2', tables: 'public.a\n public.b ', range: '{"from": 1}' })
    expect(r).toEqual({
      ok: true,
      params: { connector: 'orders-src', partitions: 4, force: true, mode: 5, tables: ['public.a', 'public.b'], range: { from: 1 } },
    })
  })

  it.each([
    ['connector', { connector: '' }, 'Required.'],
    ['connector', { connector: 'Bad Name' }, 'Does not match the expected format.'],
    ['partitions', { partitions: '1.5' }, 'Must be a whole number.'],
    ['partitions', { partitions: 'ten' }, 'Must be a whole number.'],
    ['partitions', { partitions: '0' }, 'Must be at least 1.'],
    ['partitions', { partitions: '11' }, 'Must be at most 10.'],
    ['mode', { mode: '9' }, 'Choose one of the options.'],
    ['tables', { tables: '\n \n' }, 'Required.'],
    ['tables', { tables: 'orders' }, '"orders" does not match the expected format.'],
    ['tables', { tables: 'a.b\nc.d\ne.f' }, 'At most 2 items.'],
  ])('%s: %o → %s', (field, patch, msg) => {
    const r = paramsFrom(spec, { ...base, ...patch })
    expect(r.ok).toBe(false)
    if (!r.ok) expect(r.errors[field]).toBe(msg)
  })

  it('reports invalid JSON in a JSON field', () => {
    const r = paramsFrom(spec, { ...base, range: '{from: 1}' })
    expect(r.ok).toBe(false)
    if (!r.ok) expect(r.errors.range).toMatch(/^Not valid JSON/)
  })

  it('sends a required checkbox even when off', () => {
    const s = formSpec({ type: 'object', properties: { force: { type: 'boolean' } }, required: ['force'] })
    expect(paramsFrom(s, { force: false })).toEqual({ ok: true, params: { force: false } })
  })

  it('parses the whole-document fallback and wants an object', () => {
    const s = formSpec('nope')
    expect(paramsFrom(s, { [JSON_KEY]: ' {"a": [1]} ' })).toEqual({ ok: true, params: { a: [1] } })
    expect(paramsFrom(s, { [JSON_KEY]: '' })).toEqual({ ok: true, params: {} })
    const arr = paramsFrom(s, { [JSON_KEY]: '[1]' })
    expect(arr.ok === false && arr.errors[JSON_KEY]).toBe('Params must be a JSON object ({ … }).')
    const bad = paramsFrom(s, { [JSON_KEY]: '{' })
    expect(bad.ok === false && bad.errors[JSON_KEY]).toMatch(/^Not valid JSON/)
  })
})
