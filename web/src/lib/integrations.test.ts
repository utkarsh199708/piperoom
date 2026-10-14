import { describe, expect, it } from 'vitest'
import {
  buildRef,
  buildWriteBody,
  checklistProgress,
  checklistRows,
  initialForm,
  integrationForKind,
  joinAnd,
  parseRef,
  secretWrite,
  validateForm,
  verifiedReadOnly,
} from './integrations'
import type { SecretFieldState } from './integrations'
import { check, connectKind, grafanaKind, postgresKind, snowflakeKind, vaultKind, vaultRow, view } from '../test/integrationsData'

describe('parseRef / buildRef', () => {
  const cases: [string, ReturnType<typeof parseRef>][] = [
    ['secret://vault-corp/database/creds/piperoom-ro#password', { provider: 'vault-corp', path: 'database/creds/piperoom-ro', key: 'password', version: '' }],
    ['secret://aws/prod/db', { provider: 'aws', path: 'prod/db', key: '', version: '' }],
    ['secret://aws/prod/db#pw?version=3', { provider: 'aws', path: 'prod/db', key: 'pw', version: '3' }],
    ['  secret://files/etc/pw  ', { provider: 'files', path: 'etc/pw', key: '', version: '' }],
    ['vault://x/y', null],
    ['secret://onlyprovider', null],
    ['secret:///path', null],
    ['secret://p/', null],
    ['', null],
  ]
  it.each(cases)('parses %j', (input, want) => {
    expect(parseRef(input)).toEqual(want)
  })

  it('round-trips and strips a leading slash in the name', () => {
    expect(buildRef({ provider: 'v', path: '/a/b', key: 'k', version: '' })).toBe('secret://v/a/b#k')
    const r = 'secret://aws/prod/db#pw?version=3'
    expect(buildRef(parseRef(r)!)).toBe(r)
  })
})

describe('initialForm', () => {
  it('uses defaults for a new card and starts secrets on the manager when one is connected', () => {
    const f = initialForm(postgresKind, undefined, true)
    expect(f.config).toEqual({ host: '', port: '5432', sslmode: 'require', replica: false })
    expect(f.secrets.password).toEqual({ mode: 'manager', value: '', ref: '', stored: false, hasStored: false, clear: false })
    expect(initialForm(postgresKind, undefined, false).secrets.password.mode).toBe('value')
  })

  it('fills stored values; a typed secret is "stored", never its value; a reference opens in manager mode', () => {
    const typed = initialForm(postgresKind, view(), true)
    expect(typed.config).toEqual({ host: 'db.corp', port: '5432', sslmode: 'require', replica: false })
    expect(typed.secrets.password).toMatchObject({ mode: 'value', value: '', stored: true })

    const ref = initialForm(postgresKind, view({ config: { host: 'h', password: { $ref: 'secret://v/a#p' } } }), false)
    expect(ref.secrets.password).toMatchObject({ mode: 'manager', ref: 'secret://v/a#p', stored: false, hasStored: true })
    expect(typed.secrets.password.hasStored).toBe(true)
  })
})

describe('secretWrite', () => {
  const s = (p: Partial<SecretFieldState>): SecretFieldState => ({ mode: 'value', value: '', ref: '', stored: false, hasStored: false, clear: false, ...p })
  it.each([
    ['blank value keeps', s({}), undefined],
    ['typed value sets', s({ value: 'pw' }), 'pw'],
    ['clear sends null', s({ stored: true, clear: true }), null],
    ['reference', s({ mode: 'manager', ref: ' secret://v/a#p ' }), { $ref: 'secret://v/a#p' }],
    ['empty reference keeps', s({ mode: 'manager', value: 'ignored' }), undefined],
    ['clearing a stored reference sends null', s({ mode: 'manager', ref: 'secret://v/a#p', hasStored: true, clear: true }), null],
  ])('%s', (_name, state, want) => {
    expect(secretWrite(state)).toEqual(want)
  })
})

describe('buildWriteBody', () => {
  it('types config values, drops empty optional ones, and adds id only for a test', () => {
    const f = initialForm(postgresKind, undefined, false)
    f.config.host = ' db.corp '
    f.config.port = ''
    f.config.replica = true
    f.secrets.password.value = 'pw'
    expect(buildWriteBody(postgresKind, f)).toEqual({
      kind: 'postgres-source',
      config: { host: 'db.corp', sslmode: 'require', replica: true },
      secrets: { password: 'pw' },
    })
    f.config.port = '6543'
    const body = buildWriteBody(postgresKind, f, 'int_1')
    expect(body.config.port).toBe(6543)
    expect(body.id).toBe('int_1')
  })

  it('leaves a kept secret out', () => {
    const f = initialForm(postgresKind, view(), false)
    expect(buildWriteBody(postgresKind, f).secrets).toEqual({})
  })
})

describe('validateForm', () => {
  it('flags missing required, bad pattern, bad select and number', () => {
    const f = initialForm(postgresKind, undefined, false)
    f.config.host = 'bad host!'
    f.config.port = 'abc'
    f.config.sslmode = 'nope'
    expect(validateForm(postgresKind, f)).toEqual({
      host: 'This value has the wrong format',
      port: 'Enter a number',
      sslmode: 'Pick one of the options',
      password: 'Required',
    })
  })

  it('accepts a kept stored secret and rejects a malformed reference', () => {
    const f = initialForm(postgresKind, view(), false)
    expect(validateForm(postgresKind, f, view())).toEqual({})
    f.secrets.password = { ...f.secrets.password, mode: 'manager', ref: 'vault/x' }
    expect(validateForm(postgresKind, f, view()).password).toMatch(/secret:\/\//)
  })

  it('asks for a stored secret again when the address changes', () => {
    const f = initialForm(postgresKind, view(), false)
    f.config.host = 'attacker.example'
    expect(validateForm(postgresKind, f, view()).password).toMatch(/Enter it again: host changed/)
    f.secrets.password = { ...f.secrets.password, value: 'typed-again' }
    expect(validateForm(postgresKind, f, view())).toEqual({})
  })

  it('a secret manager name is fixed once saved', () => {
    const f = initialForm(vaultKind, vaultRow, false)
    expect(validateForm(vaultKind, f, vaultRow).name).toBeUndefined()
    f.config.name = 'vault-other'
    expect(validateForm(vaultKind, f, vaultRow).name).toMatch(/cannot be changed/)
  })

  it('a required secret that is cleared is missing', () => {
    const f = initialForm(postgresKind, view(), false)
    f.secrets.password.clear = true
    expect(validateForm(postgresKind, f, view()).password).toBe('Required')
  })
})

describe('checklist', () => {
  const catalog = [postgresKind, connectKind, snowflakeKind, vaultKind, grafanaKind]

  it('one row per category in spec order, secret managers apart', () => {
    const rows = checklistRows(catalog, [])
    expect(rows.map((r) => r.category)).toEqual(['source_db', 'connect', 'warehouse', 'alerts'])
    expect(rows.map((r) => r.required)).toEqual([true, true, false, false])
    expect(rows.every((r) => r.status === 'not_started')).toBe(true)
  })

  it('maps statuses, env rows and testing, and reports what a first room still needs', () => {
    const list = [
      view({ last_test: [check('read_only', 'pass')] }),
      view({ id: 'env_connect', category: 'connect', kind: 'connect-rest', status: 'failing', source: 'env', version: 0 }),
      vaultRow,
    ]
    const rows = checklistRows(catalog, list, new Set(['warehouse']))
    const by = Object.fromEntries(rows.map((r) => [r.category, r]))
    expect(by.source_db.status).toBe('connected')
    expect(by.source_db.readOnlyVerified).toBe(true)
    expect(by.connect.status).toBe('attention')
    expect(by.connect.managedByEnv).toBe(true)
    expect(by.warehouse.status).toBe('testing')
    expect(checklistProgress(rows)).toEqual({ connected: 1, total: 4, missingRequired: ['Kafka Connect'] })
  })

  it('verified read-only needs a passing read_only check and no failure', () => {
    expect(verifiedReadOnly(view({ last_test: [check('read_only', 'pass')] }))).toBe(true)
    expect(verifiedReadOnly(view({ last_test: [check('read_only', 'pass'), check('login', 'fail')] }))).toBe(false)
    expect(verifiedReadOnly(view({ last_test: [] }))).toBe(false)
  })

  it('finds the row a card edits', () => {
    const list = [view(), vaultRow, view({ id: 'int_g', category: 'alerts', kind: 'alerts-grafana' })]
    expect(integrationForKind(postgresKind, list)?.id).toBe('int_1')
    expect(integrationForKind(vaultKind, list)).toBeUndefined()
    expect(integrationForKind(grafanaKind, list)?.id).toBe('int_g')
  })

  it('joins names for the progress sentence', () => {
    expect(joinAnd([])).toBe('')
    expect(joinAnd(['A'])).toBe('A')
    expect(joinAnd(['A', 'B', 'C'])).toBe('A, B and C')
  })
})
