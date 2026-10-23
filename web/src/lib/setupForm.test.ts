import { describe, expect, it } from 'vitest'
import { EMPTY_SETUP_FORM, SetupFormError, setupRequestFrom, splitNames } from './setupForm'
import type { SetupFormValues } from './setupForm'

const form = (over: Partial<SetupFormValues> = {}): SetupFormValues => ({
  ...EMPTY_SETUP_FORM,
  pipelineId: 'orders',
  title: 'Watch orders',
  topicPrefix: 'orders',
  tables: 'public.orders\npublic.order_items',
  targets: [
    { kind: 'postgres', address: 'db.acme:5432', class: 'production' },
    { kind: 'kafka', address: 'kafka.acme:9092', class: '' },
  ],
  ...over,
})

const codeOf = (fn: () => unknown): string => {
  try {
    fn()
  } catch (err) {
    if (err instanceof SetupFormError) return err.field
    throw err
  }
  return 'none'
}

describe('setupRequestFrom', () => {
  it('builds the request the server documents, without empty optional fields', () => {
    expect(setupRequestFrom(form())).toEqual({
      pipeline_id: 'orders',
      title: 'Watch orders',
      source_kind: 'postgres',
      target: 'compose',
      topic_prefix: 'orders',
      tables: ['public.orders', 'public.order_items'],
      targets: [
        { kind: 'postgres', address: 'db.acme:5432', class: 'production' },
        { kind: 'kafka', address: 'kafka.acme:9092' },
      ],
    })
  })

  it('keeps the tenant only when the person typed one', () => {
    expect(setupRequestFrom(form({ tenant: ' acme ' }))).toMatchObject({ tenant: 'acme' })
    expect(setupRequestFrom(form())).not.toHaveProperty('tenant')
  })

  it.each([
    ['pipeline id', form({ pipelineId: '' }), 'pipelineId'],
    ['pipeline id shape', form({ pipelineId: 'orders/1' }), 'pipelineId'],
    ['title', form({ title: '  ' }), 'title'],
    ['topic prefix', form({ topicPrefix: '' }), 'topicPrefix'],
    ['topic prefix shape', form({ topicPrefix: '-orders' }), 'topicPrefix'],
    ['tables empty', form({ tables: ' , \n' }), 'tables'],
    ['table shape', form({ tables: 'orders' }), 'tables'],
    ['target address', form({ targets: [{ kind: 'kafka', address: '  ', class: '' }] }), 'target-0-address'],
    ['target kind', form({ targets: [{ kind: '', address: 'k:9092', class: '' }] }), 'target-0-kind'],
  ])('refuses a bad %s with the field that needs the person', (_name, values, field) => {
    expect(codeOf(() => setupRequestFrom(values))).toBe(field)
  })
})

describe('splitNames', () => {
  it('splits lines and commas, trims, drops blanks and repeats, and keeps order', () => {
    expect(splitNames(' public.orders,public.items\n\n public.orders \n public.payments ')).toEqual([
      'public.orders',
      'public.items',
      'public.payments',
    ])
  })
})
