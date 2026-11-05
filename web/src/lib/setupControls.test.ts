import { describe, expect, it } from 'vitest'
import {
  EMPTY_EXTENSION,
  SETUP_CLASS_ORDER,
  SetupControlError,
  approverCopy,
  classesAbove,
  effectiveClass,
  extensionBody,
  highestClass,
  secretNameError,
  secretValueError,
  watchedByCopy,
  watchedByFor,
  windowFromInputs,
} from './setupControls'

describe('class raises (U1-12b; docs/api.md target class)', () => {
  it('offers strictly higher classes only, and none at the top', () => {
    const cases: [string, string[]][] = [
      ['local', ['shared', 'production']],
      ['shared', ['production']],
      ['production', []],
      // An absent class reads as the server's floor, shared: it is never itself a raise, and local
      // is never offered because the server refuses a registered target below shared.
      ['', ['production']],
      ['nonsense', ['production']],
    ]
    for (const [current, want] of cases) {
      expect(classesAbove(current), current).toEqual(want)
    }
  })

  it('every offered class ranks above the current one', () => {
    for (const current of SETUP_CLASS_ORDER) {
      for (const offered of classesAbove(current)) {
        expect(SETUP_CLASS_ORDER.indexOf(offered)).toBeGreaterThan(SETUP_CLASS_ORDER.indexOf(current))
      }
    }
  })

  it('reads an unknown class as shared, never as local', () => {
    expect(effectiveClass('')).toBe('shared')
    expect(effectiveClass('garbage')).toBe('shared')
    expect(effectiveClass('local')).toBe('local')
  })
})

describe('where the pipeline will be watched (design §3.7)', () => {
  it('is this server unless every target is local', () => {
    const cases: [{ class: string }[], string | undefined, 'server' | 'local'][] = [
      [[{ class: 'local' }], undefined, 'local'],
      [[{ class: 'local' }, { class: 'local' }], undefined, 'local'],
      [[{ class: 'local' }, { class: 'shared' }], undefined, 'server'],
      [[{ class: 'production' }], undefined, 'server'],
      [[], undefined, 'server'],
      [[{ class: 'local' }], 'server', 'server'],
      [[{ class: 'production' }], 'local', 'local'],
    ]
    for (const [targets, registered, want] of cases) {
      expect(watchedByFor(targets, registered), JSON.stringify([targets, registered])).toBe(want)
    }
  })

  it('fills the registration card sentence', () => {
    expect(watchedByCopy('server', false)).toMatch(/Watched by this server once it is registered/)
    expect(watchedByCopy('server', true)).toBe('Watched by this server.')
    expect(watchedByCopy('local', false)).toMatch(/Watched locally while piperoom-setup runs/)
    expect(watchedByCopy('local', false)).toMatch(/cannot reach 127\.0\.0\.1/)
    expect(watchedByCopy('local', true)).toBe('Watched locally while piperoom-setup runs.')
  })

  it('takes the highest target class', () => {
    expect(highestClass([])).toBe('shared')
    expect(highestClass([{ class: 'local' }])).toBe('local')
    expect(highestClass([{ class: 'local' }, { class: 'production' }])).toBe('production')
    expect(highestClass([{ class: 'nonsense' }])).toBe('shared')
  })
})

describe('who approves registration (design §3.6)', () => {
  it('needs someone other than the requester on a team server', () => {
    const copy = approverCopy(false)
    expect(copy).toMatch(/tenant admin/)
    expect(copy).toMatch(/overlaps/)
    expect(copy).toMatch(/someone other than the person asking/)
    expect(copy).toMatch(/does not count on a team server/)
  })

  it('lets the solo driver approve alone', () => {
    expect(approverCopy(true)).toMatch(/solo mode/)
    expect(approverCopy(true)).toMatch(/only one required/)
  })
})

describe('the secret field first pass (docs/api.md setup/secret)', () => {
  it('accepts the documented names and refuses the rest', () => {
    const cases: [string, boolean][] = [
      ['DB_PASSWORD', true],
      ['_token', true],
      ['a', true],
      ['9start', false],
      ['has-dash', false],
      ['has space', false],
      ['', false],
      ['x'.repeat(129), false],
      ['x'.repeat(128), true],
    ]
    for (const [name, ok] of cases) {
      expect(secretNameError(name) === undefined, name).toBe(ok)
    }
  })

  it('counts bytes: at least 6 and at most 4 KiB', () => {
    expect(secretValueError('')).toBe('A secret value is required.')
    expect(secretValueError('abcde')).toBe('A secret value is at least 6 bytes.')
    expect(secretValueError('abcdef')).toBeUndefined()
    expect(secretValueError('éé')).toBe('A secret value is at least 6 bytes.') // 4 bytes of UTF-8 in 2 characters
    expect(secretValueError('x'.repeat(4096))).toBeUndefined()
    expect(secretValueError('x'.repeat(4097))).toBe('A secret value is at most 4 KiB.')
  })
})

describe('pipeline extend body (docs/api.md setup/pipeline)', () => {
  it('carries only the lists that name something', () => {
    expect(extensionBody(EMPTY_EXTENSION)).toBeNull()
    expect(
      extensionBody({
        connectors: 'orders-src, orders-outbox',
        tables: 'public.shipments\npublic.returns',
        consumerGroups: ' ',
        sinkTables: 'analytics.orders',
      }),
    ).toEqual({
      add_connectors: ['orders-src', 'orders-outbox'],
      add_tables: ['public.shipments', 'public.returns'],
      add_sink_tables: ['analytics.orders'],
    })
  })

  it('drops a repeated name and keeps the first', () => {
    expect(
      extensionBody({ ...EMPTY_EXTENSION, consumerGroups: 'w1\nw1\nw2' }),
    ).toEqual({ add_consumer_groups: ['w1', 'w2'] })
  })
})

describe('maintenance window (docs/api.md setup/maintenance-window)', () => {
  const now = new Date('2026-10-22T10:00:00Z')
  const pad = (n: number) => String(n).padStart(2, '0')
  // datetime-local takes the browser's local time; build the input from the instant so the test
  // does not depend on the machine's zone.
  const localInput = (d: Date) =>
    `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}T${pad(d.getHours())}:${pad(d.getMinutes())}`
  const later = (hours: number) => new Date(now.getTime() + hours * 60 * 60 * 1000)

  it('turns two inputs into the RFC 3339 pair', () => {
    const start = later(2)
    const end = later(4)
    expect(windowFromInputs(localInput(start), localInput(end), now)).toEqual({
      start: start.toISOString(),
      end: end.toISOString(),
    })
  })

  it('refuses a window over four hours', () => {
    expect(() => windowFromInputs(localInput(later(1)), localInput(later(5.5)), now)).toThrow(SetupControlError)
    expect(() => windowFromInputs(localInput(later(1)), localInput(later(5.5)), now)).toThrow(/four hours/)
  })

  it('refuses an end before the start, a past window and a window too far ahead', () => {
    expect(() => windowFromInputs(localInput(later(2)), localInput(later(1)), now)).toThrow(/end after it starts/)
    expect(() => windowFromInputs(localInput(later(-3)), localInput(later(-2)), now)).toThrow(/end in the future/)
    expect(() => windowFromInputs(localInput(later(25)), localInput(later(26)), now)).toThrow(/at most 24 hours from now/)
    expect(() => windowFromInputs('', localInput(later(1)), now)).toThrow(/when the window starts/)
    expect(() => windowFromInputs(localInput(later(1)), '', now)).toThrow(/when the window ends/)
  })
})
