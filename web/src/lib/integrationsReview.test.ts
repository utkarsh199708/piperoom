import { describe, expect, it } from 'vitest'
import { ApiClientError } from '../api/client'
import type { CatalogKind } from '../api/integrations'
import { buildWriteBody, checklistProgress, checklistRows, initialForm, validateForm } from './integrations'
import { fieldOf, integrationErrorText } from './integrationErrors'
import { postgresKind, view } from '../test/integrationsData'

// A Postgres card whose user may come from a Vault database lease (allow_ref).
const leaseKind: CatalogKind = {
  ...postgresKind,
  fields: [
    ...postgresKind.fields,
    { name: 'user', label: 'User', help: '', type: 'text', required: true, default: '', options: [], pattern: '^[A-Za-z0-9_.@-]+$', allow_ref: true },
  ],
}

describe('allow_ref text fields', () => {
  it('sends a secret:// value as a reference and reads one back as text', () => {
    const f = initialForm(leaseKind, undefined, false)
    f.config.host = 'db.corp'
    f.config.user = 'secret://vault/database/creds/ro#username'
    f.secrets.password = { ...f.secrets.password, mode: 'manager', ref: 'secret://vault/database/creds/ro#password' }
    expect(validateForm(leaseKind, f)).toEqual({})
    expect(buildWriteBody(leaseKind, f).config.user).toEqual({ $ref: 'secret://vault/database/creds/ro#username' })
    const stored = view({ config: { host: 'db.corp', user: { $ref: 'secret://vault/database/creds/ro#username' } } })
    expect(initialForm(leaseKind, stored, true).config.user).toBe('secret://vault/database/creds/ro#username')
  })

  it('still checks the pattern of plain text and the shape of a reference', () => {
    const f = initialForm(leaseKind, undefined, false)
    f.config.host = 'db.corp'
    f.config.user = 'bad user!'
    expect(validateForm(leaseKind, f).user).toBeTruthy()
    f.config.user = 'secret://onlyprovider'
    expect(validateForm(leaseKind, f).user).toMatch(/reference/)
  })
})

describe('env rows in the setup checklist', () => {
  it('counts an untested env row as connected', () => {
    const env = view({ id: 'env_source_db', source: 'env', status: 'untested', version: 0 })
    const rows = checklistRows([postgresKind], [env])
    expect(rows[0].status).toBe('connected')
    expect(checklistProgress(rows).missingRequired).toEqual([])
  })
})

describe('new integration error codes', () => {
  it('explains a key mismatch and an access refusal, and reads their field', () => {
    expect(integrationErrorText(new ApiClientError(409, 'secret_key_mismatch', 'x'))).toMatch(/PIPEROOM_SECRET_KEY_PREVIOUS/)
    const denied = new ApiClientError(403, 'secret_access_denied', 'x', { error: { code: 'secret_access_denied', message: 'x', field: 'password' } })
    expect(integrationErrorText(denied)).toMatch(/prefix/)
    expect(fieldOf(denied)).toBe('password')
    const inUse = new ApiClientError(409, 'in_use', 'enabled integrations read secrets from this manager: PostgreSQL (int_1)')
    expect(integrationErrorText(inUse)).toMatch(/PostgreSQL \(int_1\)/)
  })
})
