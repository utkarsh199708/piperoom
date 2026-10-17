import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen, waitFor, within } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import { ApiClientError, api } from '../../api/client'
import type { AdminConfig, ProbeResult } from '../../api/types'

const ctx = vi.hoisted(() => ({
  user: { id: 'priya', name: 'Priya', layers: [], admin: true } as { id: string; name: string; layers: string[]; admin?: boolean },
}))
vi.mock('@/context/user', () => ({
  useCurrentUser: () => ({ user: ctx.user, setUserId: vi.fn(), logout: vi.fn(), sessionEpoch: 0, busy: false }),
}))

import { SettingsPage } from './SettingsPage'

const config = (): AdminConfig => ({
  source: 'file',
  owners_file: 'owners.yaml',
  users_file: 'users.yaml',
  connections: [
    {
      id: 'process-wide.kafka',
      scope: 'process-wide',
      pipeline_id: '',
      kind: 'kafka',
      used_by: ['orders'],
      settings: [
        { key: 'kafka_brokers', display: 'localhost:29092', secret: false, from: 'env KAFKA_BROKERS' },
        { key: 'kafka_sasl_password', display: '', secret: true, set: false, from: 'env KAFKA_SASL_PASSWORD' },
      ],
    },
    {
      id: 'pipeline.orders.source_db',
      scope: 'pipeline',
      pipeline_id: 'orders',
      kind: 'source_db',
      used_by: ['orders'],
      settings: [
        { key: 'source_db_url', display: '', secret: true, set: true, from: 'env ORDERS_DB_URL' },
        // A server that wrongly sent a value for a secret: it must still not reach the page.
        { key: 'source_db_password', display: 'hunter2-LEAK', secret: true, set: true, from: 'file /run/secrets/pw' },
        { key: 'source_db_host', display: 'db.internal:5432', secret: false, from: 'value' },
      ],
    },
    { id: 'process-wide.connect', scope: 'process-wide', pipeline_id: '', kind: 'connect', used_by: [], settings: [] },
  ],
  pipelines: [
    {
      id: 'orders',
      title: 'Orders CDC',
      tenant: 'default',
      connections: 'own',
      connectors: ['pg-orders-connector'],
      consumer_groups: ['order-sync'],
      source_tables: ['public.orders'],
      sink_tables: ['analytics.orders'],
      owners: [
        { layer: 'postgres', principals: ['user:priya'] },
        { layer: 'kafka', principals: ['group:streaming', 'user:arjun'] },
      ],
      shadow: true,
    },
    {
      id: 'payments',
      title: 'Payments',
      tenant: 'finance',
      connections: 'process-wide',
      connectors: null as unknown as string[], // a Go server sends null for an empty list
      consumer_groups: [],
      source_tables: ['public.payments'],
      sink_tables: [],
      owners: [],
      shadow: false,
    },
  ],
  people: [
    { id: 'priya', name: 'Priya', layers: ['postgres', 'schema-registry'], admin: true },
    { id: 'arjun', name: 'Arjun', layers: ['kafka'], admin: false },
  ],
})

const probe: ProbeResult = {
  kind: 'source_db',
  ok: false,
  took_ms: 212,
  checks: [
    { id: 'reach', status: 'ok', ms: 4 },
    { id: 'read_only', status: 'ok', ms: 2, detail: 'default_transaction_read_only = on' },
    { id: 'writes', status: 'warn', ms: 3, problem: 'the login can write', fix: 'REVOKE INSERT ON ALL TABLES IN SCHEMA public FROM piperoom_ro;' },
    {
      id: 'read_slots',
      status: 'fail',
      ms: 3,
      problem: 'role cannot read pg_replication_slots',
      fix: 'GRANT pg_read_all_stats TO piperoom_ro;',
    },
    { id: 'read_tables', status: 'skip', ms: 0 },
  ],
}

const renderPage = () =>
  render(
    <MemoryRouter>
      <SettingsPage />
    </MemoryRouter>,
  )

const apiError = (status: number, code: string, message = code) => new ApiClientError(status, code, message)

beforeEach(() => {
  ctx.user = { id: 'priya', name: 'Priya', layers: [], admin: true }
})
afterEach(() => vi.restoreAllMocks())

describe('SettingsPage access', () => {
  it('a non-admin sees the admins-only state and the page never asks for the config', () => {
    ctx.user = { id: 'arjun', name: 'Arjun', layers: [] }
    const spy = vi.spyOn(api, 'adminConfig')
    renderPage()
    expect(screen.getByText('Settings are for admins')).toBeInTheDocument()
    expect(spy).not.toHaveBeenCalled()
    expect(screen.queryByRole('tablist')).toBeNull()
  })

  it.each([
    ['not_allowed', 'admins only'],
    ['viewer', 'viewers cannot'],
  ])('the API answering 403 %s shows the same admins-only state', async (code, message) => {
    vi.spyOn(api, 'adminConfig').mockRejectedValue(apiError(403, code, message))
    renderPage()
    expect(await screen.findByText('Settings are for admins')).toBeInTheDocument()
    expect(screen.queryByText(message)).toBeNull()
  })

  it('shows a failure with Retry, and Retry loads again', async () => {
    const spy = vi.spyOn(api, 'adminConfig').mockRejectedValueOnce(apiError(500, 'internal', 'boom')).mockResolvedValue(config())
    renderPage()
    expect(await screen.findByRole('alert')).toHaveTextContent('boom')
    fireEvent.click(screen.getByRole('button', { name: 'Retry' }))
    expect(await screen.findByRole('tablist', { name: 'Settings sections' })).toBeInTheDocument()
    expect(spy).toHaveBeenCalledTimes(2)
  })
})

describe('SettingsPage banner and export', () => {
  it('says the page is read only, names the files, and Export YAML points at the export route', async () => {
    vi.spyOn(api, 'adminConfig').mockResolvedValue(config())
    renderPage()
    const banner = await screen.findByRole('region', { name: 'Where this configuration comes from' })
    expect(banner).toHaveTextContent('Read only.')
    expect(banner).toHaveTextContent('owners.yaml')
    expect(banner).toHaveTextContent('users.yaml')
    const exp = within(banner).getByRole('link', { name: 'Export YAML' })
    expect(exp).toHaveAttribute('href', '/api/admin/config/export')
    expect(exp).toHaveAttribute('download', 'piperoom-config.yaml')
  })
})

describe('Connections tab', () => {
  it('renders one card per connection with kind, scope, used-by and settings', async () => {
    vi.spyOn(api, 'adminConfig').mockResolvedValue(config())
    renderPage()
    const kafka = await screen.findByRole('group', { name: 'Kafka' })
    expect(within(kafka).getByText('Process-wide')).toBeInTheDocument()
    expect(within(kafka).getByText('orders')).toBeInTheDocument()
    expect(within(kafka).getByText('localhost:29092')).toBeInTheDocument()
    expect(within(kafka).getByText('env KAFKA_BROKERS')).toBeInTheDocument()

    const pg = screen.getByRole('group', { name: 'Postgres source' })
    expect(within(pg).getByText('Pipeline orders')).toBeInTheDocument()
    expect(within(pg).getByText('db.internal:5432')).toBeInTheDocument()

    const connect = screen.getByRole('group', { name: 'Kafka Connect' })
    expect(within(connect).getByText('no pipeline')).toBeInTheDocument()
    expect(within(connect).getByText('No settings reported.')).toBeInTheDocument()
  })

  it('shows a secret only as set or not set with where it comes from, never a value', async () => {
    vi.spyOn(api, 'adminConfig').mockResolvedValue(config())
    const { container } = renderPage()
    const kafka = await screen.findByRole('group', { name: 'Kafka' })
    const row = within(kafka).getByText('kafka_sasl_password').closest('tr')!
    expect(row).toHaveTextContent('not set')
    expect(row).toHaveTextContent('env KAFKA_SASL_PASSWORD')

    const pg = screen.getByRole('group', { name: 'Postgres source' })
    expect(within(pg).getByText('source_db_url').closest('tr')).toHaveTextContent('set')
    expect(within(pg).getByText('source_db_url').closest('tr')).toHaveTextContent('env ORDERS_DB_URL')
    const leaky = within(pg).getByText('source_db_password').closest('tr')!
    expect(leaky).toHaveTextContent('file /run/secrets/pw')
    expect(container).not.toHaveTextContent('hunter2-LEAK')
  })
})

describe('Test connection', () => {
  const open = async () => {
    vi.spyOn(api, 'adminConfig').mockResolvedValue(config())
    renderPage()
    const card = await screen.findByRole('group', { name: 'Postgres source' })
    return card
  }

  it('posts the connection id, shows a spinner, then the checks with ok, warn, fail and skip', async () => {
    let resolve!: (r: ProbeResult) => void
    const spy = vi.spyOn(api, 'testConnection').mockReturnValue(new Promise((r) => (resolve = r)))
    const card = await open()
    fireEvent.click(within(card).getByRole('button', { name: /Test connection/ }))
    expect(spy).toHaveBeenCalledWith('pipeline.orders.source_db')
    expect(within(card).getByRole('button', { name: /Test connection/ })).toBeDisabled()
    expect(within(card).getByText('Testing…', { selector: 'button' })).toBeInTheDocument()
    // A second click while it runs sends nothing.
    fireEvent.click(within(card).getByRole('button', { name: /Test connection/ }))
    expect(spy).toHaveBeenCalledTimes(1)

    resolve(probe)
    const list = await within(card).findByRole('list', { name: 'Checks' })
    const items = within(list).getAllByRole('listitem')
    expect(items.map((li) => li.getAttribute('data-status'))).toEqual(['ok', 'ok', 'warn', 'fail', 'skip'])
    // Each row says its status in words, not only by icon or colour.
    expect(items[0]).toHaveTextContent(/reach\s*ok/)
    expect(items[2]).toHaveTextContent(/writes\s*warning/)
    expect(items[3]).toHaveTextContent(/read_slots\s*failed/)
    expect(items[4]).toHaveTextContent(/read_tables\s*skipped/)
    expect(items[3]).toHaveTextContent('role cannot read pg_replication_slots')
    expect(items[3]).toHaveTextContent('GRANT pg_read_all_stats TO piperoom_ro;')
    expect(items[1]).toHaveTextContent('default_transaction_read_only = on')
    expect(items[3]).toHaveTextContent('3 ms')

    // The button is usable again.
    expect(within(card).getByRole('button', { name: /Test connection/ })).toBeEnabled()
  })

  it('announces a pass/fail summary in a live region, with the total time', async () => {
    vi.spyOn(api, 'testConnection').mockResolvedValue(probe)
    const card = await open()
    const live = card.querySelector('[role="status"][aria-live="polite"]')!
    expect(live).toBeEmptyDOMElement() // present before the answer, so the answer is announced
    fireEvent.click(within(card).getByRole('button', { name: /Test connection/ }))
    await waitFor(() => expect(live).toHaveTextContent('Test failed: 2 of 5 checks ok, 1 failed, 1 with a warning, 1 skipped (212 ms)'))

    vi.spyOn(api, 'testConnection').mockResolvedValue({ kind: 'source_db', ok: true, took_ms: 40, checks: [{ id: 'reach', status: 'ok', ms: 4 }] })
    fireEvent.click(within(card).getByRole('button', { name: /Test connection/ }))
    await waitFor(() => expect(live).toHaveTextContent('Test passed: 1 of 1 checks ok (40 ms)'))
  })

  it('Copy writes the fix to the clipboard', async () => {
    const writeText = vi.fn().mockResolvedValue(undefined)
    Object.defineProperty(navigator, 'clipboard', { value: { writeText }, configurable: true })
    vi.spyOn(api, 'testConnection').mockResolvedValue(probe)
    const card = await open()
    fireEvent.click(within(card).getByRole('button', { name: /Test connection/ }))
    fireEvent.click(await within(card).findByRole('button', { name: 'Copy fix for read_slots' }))
    expect(writeText).toHaveBeenCalledWith('GRANT pg_read_all_stats TO piperoom_ro;')
    expect(await within(card).findByText('Copied')).toBeInTheDocument()
    // Only a check with a fix has a Copy button.
    expect(within(card).getAllByRole('button', { name: /^Copy fix for/ })).toHaveLength(2)
  })

  it('429 asks to wait a minute', async () => {
    vi.spyOn(api, 'testConnection').mockRejectedValue(apiError(429, 'rate_limited', 'slow down'))
    const card = await open()
    fireEvent.click(within(card).getByRole('button', { name: /Test connection/ }))
    expect(await within(card).findByRole('alert')).toHaveTextContent('Too many tests. Wait a minute, then try again.')
    expect(within(card).getByRole('button', { name: /Test connection/ })).toBeEnabled()
  })

  it('404 says the connection is gone', async () => {
    vi.spyOn(api, 'testConnection').mockRejectedValue(apiError(404, 'not_found'))
    const card = await open()
    fireEvent.click(within(card).getByRole('button', { name: /Test connection/ }))
    expect(await within(card).findByRole('alert')).toHaveTextContent('no longer has this connection')
  })

  it('a network error says Piperoom cannot be reached, and a later test replaces the error', async () => {
    const spy = vi.spyOn(api, 'testConnection').mockRejectedValueOnce(new TypeError('Failed to fetch'))
    const card = await open()
    fireEvent.click(within(card).getByRole('button', { name: /Test connection/ }))
    expect(await within(card).findByRole('alert')).toHaveTextContent("Can't reach Piperoom")
    spy.mockResolvedValue(probe)
    fireEvent.click(within(card).getByRole('button', { name: /Test connection/ }))
    expect(await within(card).findByRole('list', { name: 'Checks' })).toBeInTheDocument()
    expect(within(card).queryByRole('alert')).toBeNull()
  })

  it('only the tested card changes', async () => {
    vi.spyOn(api, 'testConnection').mockResolvedValue(probe)
    const card = await open()
    fireEvent.click(within(card).getByRole('button', { name: /Test connection/ }))
    await within(card).findByRole('list', { name: 'Checks' })
    expect(within(screen.getByRole('group', { name: 'Kafka' })).queryByRole('list', { name: 'Checks' })).toBeNull()
  })
})

describe('Pipelines and People tabs', () => {
  const openTab = async (name: RegExp) => {
    vi.spyOn(api, 'adminConfig').mockResolvedValue(config())
    renderPage()
    const tab = await screen.findByRole('tab', { name })
    fireEvent.mouseDown(tab)
    fireEvent.click(tab)
  }

  it('lists every pipeline with its connectors, groups, tables, owners and shadow badge', async () => {
    await openTab(/^Pipelines/)
    const table = await screen.findByRole('table', { name: 'Pipelines' })
    const rows = within(table).getAllByRole('row')
    expect(rows).toHaveLength(3)
    const orders = rows[1]
    expect(orders).toHaveTextContent('orders')
    expect(orders).toHaveTextContent('Orders CDC')
    expect(orders).toHaveTextContent('default')
    expect(within(orders).getByText('own')).toBeInTheDocument()
    expect(orders).toHaveTextContent('pg-orders-connector')
    expect(orders).toHaveTextContent('order-sync')
    expect(orders).toHaveTextContent('public.orders')
    expect(orders).toHaveTextContent('analytics.orders')
    expect(orders).toHaveTextContent('postgres')
    expect(orders).toHaveTextContent('user:priya')
    expect(orders).toHaveTextContent('kafka')
    expect(orders).toHaveTextContent('group:streaming, user:arjun')
    expect(within(orders).getByText('shadow')).toBeInTheDocument()

    const payments = rows[2]
    expect(within(payments).getByText('process-wide')).toBeInTheDocument()
    expect(within(payments).getByText('off')).toBeInTheDocument()
    expect(payments).toHaveTextContent('finance')
  })

  it('lists every person with layers and an admin badge', async () => {
    await openTab(/^People/)
    const table = await screen.findByRole('table', { name: 'People' })
    const rows = within(table).getAllByRole('row')
    expect(rows).toHaveLength(3)
    expect(rows[1]).toHaveTextContent('priya')
    expect(rows[1]).toHaveTextContent('Priya')
    expect(rows[1]).toHaveTextContent('postgres')
    expect(rows[1]).toHaveTextContent('schema-registry')
    expect(within(rows[1]).getByText('admin')).toBeInTheDocument()
    expect(rows[2]).toHaveTextContent('Arjun')
    expect(within(rows[2]).queryByText('admin')).toBeNull()
  })
})
