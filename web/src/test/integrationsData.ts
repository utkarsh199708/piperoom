// Test data for the Integrations page, shaped like docs/api.md.
import type { CatalogKind, Check, Field, Integration } from '../api/integrations'

export function field(name: string, type: Field['type'], extra: Partial<Field> = {}): Field {
  return { name, label: name[0].toUpperCase() + name.slice(1), help: '', type, required: false, default: '', options: [], pattern: '', ...extra }
}

export const postgresKind: CatalogKind = {
  id: 'postgres-source',
  category: 'source_db',
  title: 'PostgreSQL',
  summary: 'The CDC source database',
  logo: 'postgres',
  reads: ['replication slot state', 'table privileges'],
  never: ['writes to any table'],
  required: true,
  has_setup_script: true,
  fields: [
    field('host', 'text', { required: true, help: 'Database host name', pattern: '^[A-Za-z0-9.-]+$' }),
    field('port', 'number', { default: '5432' }),
    field('sslmode', 'select', { options: ['disable', 'require', 'verify-full'], default: 'require', required: true }),
    field('replica', 'bool', { label: 'Read from a replica' }),
    field('password', 'secret', { required: true }),
  ],
}

export const connectKind: CatalogKind = {
  id: 'connect-rest',
  category: 'connect',
  title: 'Kafka Connect REST',
  summary: 'Connector and task status',
  logo: 'connect',
  reads: ['connector status'],
  never: ['restarts connectors'],
  required: true,
  has_setup_script: false,
  fields: [field('url', 'text', { required: true }), field('password', 'secret')],
}

export const snowflakeKind: CatalogKind = {
  id: 'snowflake',
  category: 'warehouse',
  title: 'Snowflake',
  summary: 'The sink warehouse',
  logo: 'snowflake',
  reads: ['row counts'],
  never: ['writes'],
  required: false,
  has_setup_script: true,
  fields: [field('account', 'text', { required: true }), field('private_key', 'pem', { required: true }), field('creds', 'json_file')],
}

export const vaultKind: CatalogKind = {
  id: 'secrets-vault',
  category: 'secret_manager',
  title: 'HashiCorp Vault',
  summary: 'Read secrets at run time',
  logo: 'vault',
  reads: ['secret names you pick'],
  never: ['writes secrets'],
  required: false,
  has_setup_script: true,
  fields: [field('name', 'text', { required: true }), field('token', 'secret', { required: true })],
}

export const grafanaKind: CatalogKind = {
  id: 'alerts-grafana',
  category: 'alerts',
  title: 'Grafana alerting',
  summary: 'Open a room when Grafana alerting fires',
  logo: 'grafana',
  reads: ['the alerts it sends to Piperoom'],
  never: ['calls Grafana alerting back'],
  required: false,
  has_setup_script: false,
  fields: [],
}

export function view(over: Partial<Integration> = {}): Integration {
  return {
    id: 'int_1',
    workspace_id: 'default',
    category: 'source_db',
    kind: 'postgres-source',
    title: 'PostgreSQL',
    config: { host: 'db.corp', port: 5432, sslmode: 'require', replica: false },
    secrets_set: ['password'],
    version: 3,
    enabled: true,
    status: 'connected',
    last_test: [],
    last_tested_at: null,
    updated_by: 'priya',
    updated_at: '2026-10-14T09:12:01Z',
    source: 'db',
    ...over,
  }
}

export const vaultRow = view({
  id: 'int_vault',
  category: 'secret_manager',
  kind: 'secrets-vault',
  title: 'HashiCorp Vault',
  config: { name: 'vault-corp' },
  secrets_set: ['token'],
})

export function check(id: string, status: Check['status'], extra: Partial<Check> = {}): Check {
  return { id, title: `Check ${id}`, status, detail: '', fix: '', ...extra }
}
