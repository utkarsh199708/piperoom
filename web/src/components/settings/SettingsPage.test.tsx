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

  it('the API answering 404 (no admin routes, --demo) says settings are not available, with no Retry', async () => {
    const spy = vi.spyOn(api, 'adminConfig').mockRejectedValue(apiError(404, 'not_found', 'not found'))
    // The Model panel has its own Retry when /api/models fails; keep it pending so only this page's state is checked.
    vi.spyOn(api, 'models').mockReturnValue(new Promise(() => {}))
    renderPage()
    expect(await screen.findByText('Settings are not available on this server')).toBeInTheDocument()
    expect(screen.getByText(/demo mode/)).toBeInTheDocument()
    expect(screen.queryByRole('button', { name: 'Retry' })).toBeNull()
    expect(screen.queryByText('not found')).toBeNull()
    expect(screen.queryByRole('tablist')).toBeNull()
    expect(spy).toHaveBeenCalledTimes(1)
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
  it('renders one row per connection with its role, endpoint, scope and users, and a summary strip', async () => {
    vi.spyOn(api, 'adminConfig').mockResolvedValue(config())
    renderPage()
    const kafka = await screen.findByRole('group', { name: 'Kafka' })
    expect(kafka).toHaveTextContent('Event stream')
    expect(within(kafka).getAllByText('localhost:29092').length).toBeGreaterThan(0)
    expect(kafka).toHaveTextContent('used by orders')
    expect(kafka).toHaveTextContent('Not tested')

    const pg = screen.getByRole('group', { name: 'Postgres source' })
    expect(within(pg).getByText('Pipeline orders')).toBeInTheDocument()
    expect(pg).toHaveTextContent('Connection string set (hidden)')
    expect(within(pg).getByText('db.internal:5432')).toBeInTheDocument()

    const connect = screen.getByRole('group', { name: 'Kafka Connect' })
    expect(connect).toHaveTextContent('no pipeline uses it')
    expect(within(connect).getByText('No settings reported.')).toBeInTheDocument()

    const strip = screen.getByRole('group', { name: 'Connection summary' })
    expect(strip).toHaveTextContent('Connections3')
    expect(strip).toHaveTextContent('Not tested3')
  })

  it('labels settings for people and shows a secret only as set or not set with its source, never a value', async () => {
    vi.spyOn(api, 'adminConfig').mockResolvedValue(config())
    const { container } = renderPage()
    const kafka = await screen.findByRole('group', { name: 'Kafka' })
    expect(within(kafka).getByText('Bootstrap servers').closest('[data-setting]')).toHaveTextContent('KAFKA_BROKERS')
    const pw = within(kafka).getByText('SASL password').closest('[data-setting]')!
    expect(pw).toHaveTextContent('not set')
    expect(pw).toHaveTextContent('Environment KAFKA_SASL_PASSWORD')

    const pg = screen.getByRole('group', { name: 'Postgres source' })
    const dsn = within(pg).getByText('Connection string').closest('[data-setting]')!
    expect(dsn).toHaveTextContent('set')
    expect(dsn).toHaveTextContent('ORDERS_DB_URL')
    const leaky = pg.querySelector('[data-setting="source_db_password"]')!
    expect(leaky).toHaveTextContent('File /run/secrets/pw')
    expect(container).not.toHaveTextContent('hunter2-LEAK')
  })

  it('details open and close from the chevron', async () => {
    vi.spyOn(api, 'adminConfig').mockResolvedValue(config())
    renderPage()
    const kafka = await screen.findByRole('group', { name: 'Kafka' })
    const toggle = within(kafka).getByRole('button', { name: 'Show details: Kafka' })
    expect(toggle).toHaveAttribute('aria-expanded', 'false')
    fireEvent.click(toggle)
    expect(within(kafka).getByRole('button', { name: 'Hide details: Kafka' })).toHaveAttribute('aria-expanded', 'true')
    expect(within(kafka).getByRole('region', { name: 'Configuration of Kafka' })).toBeVisible()
  })
})

describe('Test connection', () => {
  const open = async () => {
    vi.spyOn(api, 'adminConfig').mockResolvedValue(config())
    renderPage()
    return screen.findByRole('group', { name: 'Postgres source' })
  }

  it('posts the connection id, shows Testing…, then opens the details with labelled checks', async () => {
    let resolve!: (r: ProbeResult) => void
    const spy = vi.spyOn(api, 'testConnection').mockReturnValue(new Promise((r) => (resolve = r)))
    const card = await open()
    fireEvent.click(within(card).getByRole('button', { name: /Test connection/ }))
    expect(spy).toHaveBeenCalledWith('pipeline.orders.source_db')
    expect(within(card).getByRole('button', { name: /Test connection/ })).toBeDisabled()
    expect(within(card).getAllByText('Testing…').length).toBeGreaterThan(0)
    // A second click while it runs sends nothing.
    fireEvent.click(within(card).getByRole('button', { name: /Test connection/ }))
    expect(spy).toHaveBeenCalledTimes(1)

    resolve(probe)
    const list = await within(card).findByRole('list', { name: 'Checks' })
    const items = within(list).getAllByRole('listitem')
    expect(items.map((li) => li.getAttribute('data-status'))).toEqual(['ok', 'ok', 'warn', 'fail', 'skip'])
    // Each row has a label a person reads and says its status in words, not only by icon or colour.
    expect(items[0]).toHaveTextContent(/Network reachability\s*passed/)
    expect(items[1]).toHaveTextContent(/Read-only by default\s*passed/)
    expect(items[2]).toHaveTextContent(/Writes\s*warning/) // an id this client does not know, humanized
    expect(items[3]).toHaveTextContent(/Replication slots readable\s*failed/)
    expect(items[4]).toHaveTextContent(/Read tables\s*skipped/)
    expect(items[3]).toHaveTextContent('role cannot read pg_replication_slots')
    expect(items[3]).toHaveTextContent('GRANT pg_read_all_stats TO piperoom_ro;')
    expect(items[1]).toHaveTextContent('default_transaction_read_only = on')
    expect(items[3]).toHaveTextContent('3 ms')
    expect(card).toHaveTextContent('Failed')

    expect(within(card).getByRole('button', { name: /Test connection/ })).toBeEnabled()
  })

  it('announces the outcome in a live region, with the total time', async () => {
    vi.spyOn(api, 'testConnection').mockResolvedValue(probe)
    const card = await open()
    const live = card.querySelector('[role="status"][aria-live="polite"]')!
    expect(live).toBeEmptyDOMElement() // present before the answer, so the answer is announced
    fireEvent.click(within(card).getByRole('button', { name: /Test connection/ }))
    await waitFor(() => expect(live).toHaveTextContent('Failed: 1 of 5 checks failed, 1 warning · 212 ms'))

    vi.spyOn(api, 'testConnection').mockResolvedValue({ kind: 'source_db', ok: true, took_ms: 40, checks: [{ id: 'reach', status: 'ok', ms: 4 }] })
    fireEvent.click(within(card).getByRole('button', { name: /Test connection/ }))
    await waitFor(() => expect(live).toHaveTextContent('Healthy: 1 of 1 check passed · 40 ms'))
    expect(card).toHaveTextContent('Healthy')
  })

  it('Copy writes the fix to the clipboard', async () => {
    const writeText = vi.fn().mockResolvedValue(undefined)
    Object.defineProperty(navigator, 'clipboard', { value: { writeText }, configurable: true })
    vi.spyOn(api, 'testConnection').mockResolvedValue(probe)
    const card = await open()
    fireEvent.click(within(card).getByRole('button', { name: /Test connection/ }))
    fireEvent.click(await within(card).findByRole('button', { name: 'Copy fix: Replication slots readable' }))
    expect(writeText).toHaveBeenCalledWith('GRANT pg_read_all_stats TO piperoom_ro;')
    expect(await within(card).findByText('Copied')).toBeInTheDocument()
    // Only a check with a fix has a Copy button.
    expect(within(card).getAllByRole('button', { name: /^Copy fix:/ })).toHaveLength(2)
  })

  it('429 asks to wait a minute', async () => {
    vi.spyOn(api, 'testConnection').mockRejectedValue(apiError(429, 'rate_limited', 'slow down'))
    const card = await open()
    fireEvent.click(within(card).getByRole('button', { name: /Test connection/ }))
    expect(await within(card).findByRole('alert')).toHaveTextContent('Too many tests. Wait a minute, then try again.')
    expect(within(card).getByRole('button', { name: /Test connection/ })).toBeEnabled()
    expect(card).toHaveTextContent('Test error')
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
    expect(await within(card).findByRole('alert')).toHaveTextContent('reach Piperoom')
    spy.mockResolvedValue(probe)
    fireEvent.click(within(card).getByRole('button', { name: /Test connection/ }))
    expect(await within(card).findByRole('list', { name: 'Checks' })).toBeInTheDocument()
    expect(within(card).queryByRole('alert')).toBeNull()
  })

  it('only the tested row changes', async () => {
    vi.spyOn(api, 'testConnection').mockResolvedValue(probe)
    const card = await open()
    fireEvent.click(within(card).getByRole('button', { name: /Test connection/ }))
    await within(card).findByRole('list', { name: 'Checks' })
    const kafka = screen.getByRole('group', { name: 'Kafka' })
    expect(within(kafka).queryByRole('list', { name: 'Checks' })).toBeNull()
    expect(kafka).toHaveTextContent('Not tested')
  })

  it('Test all tests every connection one at a time and the summary strip counts the outcomes', async () => {
    const order: string[] = []
    let inFlight = 0
    let maxInFlight = 0
    vi.spyOn(api, 'testConnection').mockImplementation(async (id: string) => {
      order.push(id)
      inFlight++
      maxInFlight = Math.max(maxInFlight, inFlight)
      await new Promise((r) => setTimeout(r, 0))
      inFlight--
      if (id === 'process-wide.kafka') return { kind: 'kafka', ok: true, took_ms: 9, checks: [{ id: 'reach', status: 'ok', ms: 9 }] }
      if (id === 'process-wide.connect') throw apiError(429, 'rate_limited')
      return probe
    })
    vi.spyOn(api, 'adminConfig').mockResolvedValue(config())
    renderPage()
    fireEvent.click(await screen.findByRole('button', { name: 'Test all' }))
    const strip = screen.getByRole('group', { name: 'Connection summary' })
    await waitFor(() => expect(strip).toHaveTextContent('Not tested0'))
    expect(order).toEqual(['process-wide.kafka', 'pipeline.orders.source_db', 'process-wide.connect'])
    expect(maxInFlight).toBe(1)
    expect(strip).toHaveTextContent('Healthy1')
    expect(strip).toHaveTextContent('Needs attention2')
    await waitFor(() => expect(screen.getByRole('button', { name: 'Test all' })).toBeEnabled())
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

  it('shows every pipeline as a card with its data flow, owners by name, and mode', async () => {
    await openTab(/^Pipelines/)
    const orders = await screen.findByRole('group', { name: 'Orders CDC' })
    expect(orders).toHaveTextContent('orders')
    expect(orders).toHaveTextContent('tenant default')
    expect(within(orders).getByText('own connections')).toBeInTheDocument()
    expect(orders).toHaveTextContent('pg-orders-connector')
    expect(orders).toHaveTextContent('order-sync')
    expect(orders).toHaveTextContent('public.orders')
    expect(orders).toHaveTextContent('analytics.orders')
    expect(orders).toHaveTextContent('postgres')
    expect(orders).toHaveTextContent('priya')
    expect(orders).not.toHaveTextContent('user:priya')
    expect(orders).toHaveTextContent('group streaming, arjun')
    expect(within(orders).getByText('shadow mode')).toBeInTheDocument()

    const payments = screen.getByRole('group', { name: 'Payments' })
    expect(within(payments).getByText('shared connections')).toBeInTheDocument()
    expect(within(payments).getByText('live')).toBeInTheDocument()
    expect(payments).toHaveTextContent('tenant finance')
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

